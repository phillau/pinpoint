/*
 * Copyright 2016 NAVER Corp.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.navercorp.pinpoint.profiler.context.storage;

import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanChunkFactory;
import com.navercorp.pinpoint.profiler.context.SpanChunkList;
import com.navercorp.pinpoint.profiler.context.SpanEvent;
import com.navercorp.pinpoint.profiler.sender.DataSender;
import com.navercorp.pinpoint.rpc.util.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author Taejin Koo
 */
public class GlobalStorage extends AbstractBufferedStorageEventDispatcher implements Storage {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();

    private final int flushBufferSize;

    private final SpanChunkFactory spanChunkFactory;

    private final DataSender dataSender;

    public GlobalStorage(DataSender dataSender, int bufferSize, SpanChunkFactory spanChunkFactory) {
        super(new BufferedStorageFactory(dataSender, bufferSize, spanChunkFactory));

        this.dataSender = dataSender;
        this.flushBufferSize = bufferSize;
        this.spanChunkFactory = spanChunkFactory;
    }

    @Override
    public void store(SpanEvent spanEvent) {
        if (closed) {
            return;
        }
        executor.execute(spanEvent);
    }

    @Override
    public void store(Span span) {
        if (closed) {
            return;
        }
        executor.execute(span);
    }

    @Override
    public void flush() {
        if (closed) {
            return;
        }
        executor.execute(new FlushEvent(true));
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        executor.execute(new CloseEvent(true));
    }

    @Override
    protected void store0(Span span) {
        BufferedStorage storage = get(span.getSpanId());

        storage.store(span);

        removeIfEmpty(storage);
    }

    @Override
    protected void store0(SpanEvent spanEvent) {
        Span span = spanEvent.getSpan();
        BufferedStorage storage = get(span.getSpanId());

        storage.store(spanEvent);

        removeIfEmpty(storage);
    }

    @Override
    protected void flush0(FlushEvent flushEvent) {
        if (flushEvent == null) {
            return;
        }

        if (flushEvent.isFlushAll()) {
            List<BufferedStorage> bufferedStorageList = getAll();
            if (ListUtils.isEmpty(bufferedStorageList)) {
                return;
            }
            flushExceededStorage(bufferedStorageList, 0, flushBufferSize);
        } else {
            if (flushEvent.getSpanId() != null) {
                flush0(find(flushEvent.getSpanId()));
            }

            if (flushEvent.getMaximumBufferSize() != -1 || flushEvent.getExpiryTime() != -1) {
                List<BufferedStorage> bufferedStorageList = getAll();
                if (ListUtils.isEmpty(bufferedStorageList)) {
                    return;
                }

                // flush first old accessed storage
                Collections.sort(bufferedStorageList, new Comparator<BufferedStorage>() {
                    @Override
                    public int compare(BufferedStorage o1, BufferedStorage o2) {
                        if (o1.getLastAccessTime() > o2.getLastAccessTime()) {
                            return 1;
                        }
                        return -1;
                    }
                });

                if (flushEvent.getExpiryTime() != -1) {
                    long flushAxisTime = System.currentTimeMillis() - flushEvent.getExpiryTime();
                    flushOldStorage(bufferedStorageList, flushAxisTime, flushBufferSize);
                }

                if (flushEvent.getMaximumBufferSize() != -1) {
                    int maximumCapacity = flushEvent.getMaximumBufferSize();
                    flushExceededStorage(bufferedStorageList, maximumCapacity, flushBufferSize);
                }
            }
        }
    }

    private void flush0(BufferedStorage storage) {
        if (storage == null) {
            return;
        }

        storage.flush();

        removeIfEmpty(storage);
    }

    private void flushOldStorage(List<BufferedStorage> storageList, long flushAxisTime, int flushBufferSize) {
        while (true) {
            BufferedStorage first = ListUtils.getFirst(storageList);
            if (first == null) {
                break;
            }

            if (first.getLastAccessTime() < flushAxisTime) {
                List<BufferedStorage> flushBufferedStorageList = getStorageListBySpanEventSize(storageList, flushBufferSize);
                if (!ListUtils.isEmpty(flushBufferedStorageList)) {
                    send0(flushBufferedStorageList);
                    storageList.removeAll(flushBufferedStorageList);
                }
            } else {
                break;
            }
        }
    }

    private void flushExceededStorage(List<BufferedStorage> storageList, int maximumCapacity, int flushBufferSize) {
        if (maximumCapacity < 0) {
            throw new IllegalArgumentException("maximumCapacity may not be negative");
        }

        while (getAllBufferedSize() > maximumCapacity) {
            List<BufferedStorage> flushBufferedStorageList = getStorageListBySpanEventSize(storageList, flushBufferSize);
            if (!ListUtils.isEmpty(flushBufferedStorageList)) {
                send0(flushBufferedStorageList);
                storageList.removeAll(flushBufferedStorageList);
            }
        }
    }

    private List<BufferedStorage> getStorageListBySpanEventSize(List<BufferedStorage> storageList, int getSpanEventMaxSize) {
        int totalSpanEventSize = 0;

        List<BufferedStorage> result = new ArrayList<BufferedStorage>();
        for (BufferedStorage storage : storageList) {
            int currentSpanEventSize = storage.getSize();

            if (totalSpanEventSize + currentSpanEventSize > getSpanEventMaxSize) {
                break;
            }

            totalSpanEventSize += currentSpanEventSize;
            result.add(storage);
        }

        if (result.size() == 0) {
            return Collections.emptyList();
        } else {
            return result;
        }
    }

    private void send0(List<BufferedStorage> storageList) {
        if (ListUtils.isEmpty(storageList)) {
            return;
        }

        try {
            int storageSize = ListUtils.size(storageList);
            if (storageSize == 1) {
                flush0(storageList.get(0));
            } else {
                List<List<SpanEvent>> spanEventLists = new ArrayList<List<SpanEvent>>(storageSize);

                for (BufferedStorage storage : storageList) {
                    List<SpanEvent> spanEventList = storage.drainBuffers();

                    spanEventLists.add(spanEventList);
                }

                SpanChunkList spanChunkList = spanChunkFactory.createSpanChunkList(spanEventLists);
                dataSender.send(spanChunkList);
            }
        } finally {
            removeIfEmpty(storageList);
        }
    }


    @Override
    protected void close0(CloseEvent closeEvent) {
        FlushEvent flushEvent = new FlushEvent(closeEvent.isCloseAll());
        flushEvent.setSpanId(closeEvent.getSpanId());
        flushEvent.setExpiryTime(closeEvent.getExpiryTime());
        flushEvent.setMaximumBufferSize(closeEvent.getMaximumBufferSize());

        flush0(flushEvent);
    }

}

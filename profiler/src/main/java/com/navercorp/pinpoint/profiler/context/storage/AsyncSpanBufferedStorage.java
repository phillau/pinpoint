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

import com.navercorp.pinpoint.profiler.context.AsyncSpanChunk;
import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanChunk;
import com.navercorp.pinpoint.profiler.context.SpanChunkFactory;
import com.navercorp.pinpoint.profiler.context.SpanEvent;
import com.navercorp.pinpoint.profiler.sender.DataSender;
import com.navercorp.pinpoint.rpc.util.ListUtils;
import com.navercorp.pinpoint.thrift.dto.TSpanEvent;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Taejin Koo
 */
public class AsyncSpanBufferedStorage implements Storage {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSpanBufferedStorage.class);
    private static final boolean isDebug = logger.isDebugEnabled();

    private static final int DEFAULT_BUFFER_SIZE = 20;
    private static final int DEFAULT_SPAN_CHUNK_FLUSH_BUFFER_MINIMUM_RATE = 70;

    private final int minimumCount;
    private final int maximumCount;

    private final Map<Span, List<SpanEvent>> progressingSpanRepository = new IdentityHashMap<Span, List<SpanEvent>>();
    private final List<Span> completedSpanRepository = new ArrayList<Span>();
    private final SpanChunkFactory spanChunkFactory;

    private final DataSender dataSender;

    public AsyncSpanBufferedStorage(DataSender dataSender, SpanChunkFactory spanChunkFactory) {
        this(dataSender, spanChunkFactory, DEFAULT_BUFFER_SIZE);
    }

    public AsyncSpanBufferedStorage(DataSender dataSender, SpanChunkFactory spanChunkFactory, int bufferSize) {
        this(dataSender, spanChunkFactory, bufferSize, DEFAULT_SPAN_CHUNK_FLUSH_BUFFER_MINIMUM_RATE);
    }

    public AsyncSpanBufferedStorage(DataSender dataSender, SpanChunkFactory spanChunkFactory, int bufferSize, int flushBufferMinimumRate) {
        if (dataSender == null) {
            throw new NullPointerException("dataSender must not be null");
        }
        if (spanChunkFactory == null) {
            throw new NullPointerException("spanChunkFactory must not be null");
        }
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must greater than zero");
        }
        if (flushBufferMinimumRate <= 0 && flushBufferMinimumRate > 100) {
            throw new IllegalArgumentException("flushBufferMinimumRate must allow 1 ~ 100");
        }

        int minimumCount = floorDiv(bufferSize * flushBufferMinimumRate, 100);
        this.minimumCount = Math.max(minimumCount, 1);
        this.maximumCount = bufferSize;

        this.spanChunkFactory = spanChunkFactory;

        this.dataSender = dataSender;
    }

    private int floorDiv(int x, int y) {
        int r = x / y;
        // if the signs are different and modulo not zero, round down
        if ((x ^ y) < 0 && (r * y != x)) {
            r--;
        }
        return r;
    }

    @Override
    public void store(SpanEvent spanEvent) {
        Span span = spanEvent.getSpan();

        List<SpanEvent> spanEventList = progressingSpanRepository.get(span);
        if (spanEventList == null) {
            spanEventList = new ArrayList<SpanEvent>(maximumCount);
            progressingSpanRepository.put(span, spanEventList);
        }
        spanEventList.add(spanEvent);

        if (spanEventList.size() >= maximumCount) {
            progressingSpanRepository.remove(span);
            final SpanChunk spanChunk = spanChunkFactory.create(spanEventList);
            send(spanChunk);
        }
    }

    @Override
    public void store(Span span) {
        logger.warn("store Span:{}", span, span.hashCode());

        List<SpanEvent> spanEventList = progressingSpanRepository.remove(span);
        if (spanEventList != null && !spanEventList.isEmpty()) {
            span.setSpanEventList((List) spanEventList);
        }
        completedSpanRepository.add(span);

        flush(false);
    }


    private void flush(boolean all) {
        while (true) {
            List<Span> flushSpanList = getFlushSpanList(completedSpanRepository);
            if (ListUtils.isEmpty(flushSpanList)) {
                break;
            }

            completedSpanRepository.removeAll(flushSpanList);
            send(flushSpanList);
        }

        if (all) {
            send(completedSpanRepository);
            completedSpanRepository.clear();
        }
    }

    private void flushSpanEvent() {
        Collection<List<SpanEvent>> spanEventListRepository = progressingSpanRepository.values();
        for (List<SpanEvent> spanEventList : spanEventListRepository) {
            final SpanChunk spanChunk = spanChunkFactory.create(spanEventList);
            send(spanChunk);
        }

        progressingSpanRepository.clear();
    }

    private List<Span> getFlushSpanList(List<Span> spanList) {
        int flushSpanEventCount = 0;
        List<Span> flushSpanList = new ArrayList<Span>();

        for (Span span : spanList) {
            List<TSpanEvent> spanEventList = span.getSpanEventList();
            int spanEventSize = ListUtils.size(spanEventList);

            if (flushSpanEventCount + spanEventSize > maximumCount) {
                break;
            }

            flushSpanEventCount += spanEventSize;
            flushSpanList.add(span);
        }

        if (isFlushAvailable(flushSpanList, flushSpanEventCount, spanList)) {
            return flushSpanList;
        } else {
            return Collections.emptyList();
        }
    }

    private boolean isFlushAvailable(List<Span> targetSpanList, int targetSpanEventCount, List<Span> totalSpanList) {
        if (targetSpanList.size() == 0) {
            return false;
        }

        if (targetSpanList.size() == totalSpanList.size()) {
            return isAvailableFlushSpanEventCount(targetSpanEventCount);
        } else {
            return true;
        }
    }

    private boolean isAvailableFlushSpanEventCount(int spanEventCount) {
        if (spanEventCount < minimumCount) {
            return false;
        }
        if (spanEventCount > maximumCount) {
            return false;
        }

        return true;
    }

    private boolean send(List<Span> spanList) {
        int spanListSize = spanList.size();
        if (spanListSize == 1) {
            send(spanList.remove(0));
            return true;
        } else if (spanListSize > 1) {
            AsyncSpanChunk asyncSpanChunk = spanChunkFactory.createAsyncSpanChunk(spanList);
            send(asyncSpanChunk);
            return true;
        } else {
            return false;
        }
    }

    private void send(TBase object) {
        if (isDebug) {
            logger.debug("[AsyncSpanBufferedStorage] Flush data:{}", object);
        }
        dataSender.send(object);
    }

    @Override
    public boolean isEmpty() {
        if (progressingSpanRepository.size() > 0) {
            return false;
        }
        if (completedSpanRepository.size() > 0) {
            return false;
        }
        return true;
    }

    @Override
    public void flush() {
        flush(true);
    }

    @Override
    public void close() {
        flush(true);
        flushSpanEvent();
        clear();
    }

    private void clear() {
        progressingSpanRepository.clear();
        completedSpanRepository.clear();
    }

    @Override
    public String toString() {
        return "AsyncSpanBufferedStorage{" +
                "dataSender=" + dataSender +
                ", minimumCount=" + minimumCount +
                ", maximumCount=" + maximumCount +
                '}';
    }

}

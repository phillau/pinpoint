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

import com.navercorp.pinpoint.bootstrap.config.ProfilerConfig;
import com.navercorp.pinpoint.profiler.AgentInformation;
import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanEvent;
import com.navercorp.pinpoint.profiler.sender.AsyncQueueingExecutor;
import com.navercorp.pinpoint.profiler.sender.AsyncQueueingExecutorListener;
import com.navercorp.pinpoint.profiler.sender.DataSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * @author Taejin Koo
 */
public class DefaultStorageEventDispatcher implements Storage, StorageEventDispatcher {

    private static final int DEFAULT_ASYNC_QUEUE_SIZE = 500;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();
    private volatile boolean closed;

    private final StorageRepository<Storage> storageRepository;
    private AsyncQueueingExecutor<Object> executor;

    public DefaultStorageEventDispatcher(DataSender dataSender, ProfilerConfig config, AgentInformation agentInformation) {
        AsyncSpanBufferedStorageFactory asyncSpanBufferedStorageFactory = new AsyncSpanBufferedStorageFactory(dataSender, config, agentInformation);
        this.storageRepository = new AsyncSpanBufferedStorageRepository<Storage>(asyncSpanBufferedStorageFactory);
    }

    @Override
    public boolean start(int queueSize) {
        synchronized (this) {
            if (!closed && executor == null) {
                executor = createAsyncQueueingExecutor(queueSize, "DefaultStorageEventDispatcher-Executor");
                logger.info("start() initialization completed.");
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean stop() {
        synchronized (this) {
            if (closed) {
                return false;
            }
            closed = true;
        }

        close0(true);
        executor.stop();
        executor = null;

        logger.info("stop() completed.");
        return true;
    }

    private AsyncQueueingExecutor<Object> createAsyncQueueingExecutor(int queueSize, String executorName) {
        final AsyncQueueingExecutor<Object> executor = new AsyncQueueingExecutor<Object>(queueSize, executorName);
        executor.setListener(new AsyncQueueingExecutorListener<Object>() {
            @Override
            public void execute(Collection<Object> messageList) {
                Object[] dataList = messageList.toArray();

                final int size = messageList.size();
                for (int i = 0; i < size; i++) {
                    try {
                        execute(dataList[i]);
                    } catch (Throwable th) {
                        logger.warn("Unexpected Error. Cause:{}", th.getMessage(), th);
                    }
                }
            }

            @Override
            public void execute(Object message) {
                if (message instanceof SpanEvent) {
                    store0((SpanEvent) message);
                } else if (message instanceof Span) {
                    store0((Span) message);
                } else if (message instanceof FlushEvent) {
                    Long spanId = ((FlushEvent) message).spanId;
                    if (spanId == null) {
                        flush0();
                    } else {
                        flush0(spanId);
                    }
                } else if (message instanceof CloseEvent) {
                    Long spanId = ((CloseEvent) message).spanId;
                    if (spanId == null) {
                        close0();
                    } else {
                        close0(spanId);
                    }
                } else {
                    logger.warn("Unknown argument({}).", message);
                }
            }
        });
        return executor;
    }

    @Override
    public void store(SpanEvent spanEvent) {
        if (closed) {
            return;
        }
        executor.execute(spanEvent);
    }

    private void store0(SpanEvent spanEvent) {
        Span span = spanEvent.getSpan();
        Storage storage = storageRepository.get(span.getSpanId());
        storage.store(spanEvent);
    }

    @Override
    public void store(Span span) {
        if (closed) {
            return;
        }
        executor.execute(span);
    }

    private void store0(Span span) {
        Storage storage = storageRepository.get(span.getSpanId());
        storage.store(span);
    }

    @Override
    public boolean isEmpty() {
        return storageRepository.size() == 0;
    }

    @Override
    public void flush() {
        if (closed) {
            return;
        }
        executor.execute(new FlushEvent());
    }

    private void flush0() {
        List<Storage> storageList = storageRepository.getAll();
        for (Storage storage : storageList) {
            storage.flush();
        }
    }

    @Override
    public void flush(long spanid) {
        if (closed) {
            return;
        }
        executor.execute(new FlushEvent(spanid));
    }

    private void flush0(long spanId) {
        Storage storage = storageRepository.get(spanId);
        storage.flush();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        executor.execute(new CloseEvent());
    }

    private void close0() {
        close0(false);
    }

    private void close0(boolean force) {
        if (force) {
            List<Storage> storageList = storageRepository.getAll();
            for (Storage storage : storageList) {
                storage.close();
                storageRepository.remove(storage);
            }
        } else {
            List<Storage> storageList = storageRepository.getAll();
            for (Storage storage : storageList) {
                storage.flush();
                if (storage.isEmpty()) {
                    storageRepository.remove(storage);
                }
            }
        }
    }


    @Override
    public void close(long spanId) {
        if (closed) {
            return;
        }
        executor.execute(new CloseEvent(spanId));
    }

    private void close0(long spanId) {
        Storage storage = storageRepository.get(spanId);
        if (storage.isEmpty()) {
            storageRepository.remove(spanId);
        }
    }

    private class FlushEvent {

        private final Long spanId;

        public FlushEvent() {
            this.spanId = null;
        }

        public FlushEvent(long spanId) {
            this.spanId = spanId;
        }

    }

    private class CloseEvent {

        private final Long spanId;

        public CloseEvent() {
            this.spanId = null;
        }

        public CloseEvent(long spanId) {
            this.spanId = spanId;
        }

    }

}

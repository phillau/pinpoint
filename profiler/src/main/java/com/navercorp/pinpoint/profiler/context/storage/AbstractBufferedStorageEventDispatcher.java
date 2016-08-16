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
import com.navercorp.pinpoint.profiler.context.SpanEvent;
import com.navercorp.pinpoint.profiler.sender.AsyncQueueingExecutor;
import com.navercorp.pinpoint.profiler.sender.AsyncQueueingExecutorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * @author Taejin Koo
 */
public abstract class AbstractBufferedStorageEventDispatcher implements StorageEventDispatcher {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();

    protected volatile boolean closed;

    protected AsyncQueueingExecutor<Object> executor;
    private final StorageRepository<BufferedStorage> storageRepository;

    public AbstractBufferedStorageEventDispatcher(BufferedStorageFactory bufferedStorageFactory) {
        this.storageRepository = new BufferedStorageRepository<BufferedStorage>(bufferedStorageFactory);
    }

    @Override
    public boolean start(int queueSize) {
        if (queueSize <= 0) {
            throw new IllegalArgumentException("queueSize must greater than zero");
        }

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

        close0(new CloseEvent(true));
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
                if (isDebug) {
                    logger.debug("handleMessage event:{}", message);
                }

                if (message instanceof SpanEvent) {
                    store0((SpanEvent) message);
                } else if (message instanceof Span) {
                    store0((Span) message);
                } else if (message instanceof FlushEvent) {
                    flush0((FlushEvent) message);
                } else if (message instanceof CloseEvent) {
                    close0((CloseEvent) message);
                } else {
                    logger.warn("Unknown argument({}).", message);
                }
            }
        });
        return executor;
    }

    @Override
    public void flush(FlushEvent flushEvent) {
        if (closed) {
            return;
        }
        executor.execute(flushEvent);
    }

    @Override
    public void close(CloseEvent closeEvent) {
        if (closed) {
            return;
        }
        executor.execute(closeEvent);
    }

    protected abstract void store0(Span span);

    protected abstract void store0(SpanEvent spanEvent);

    protected abstract void flush0(FlushEvent flushEvent);

    protected abstract void close0(CloseEvent closeEvent);


    protected BufferedStorage get(long spanId) {
        return storageRepository.get(spanId);
    }

    protected BufferedStorage find(long spanId) {
        return storageRepository.find(spanId);
    }

    protected List<BufferedStorage> getAll() {
        return storageRepository.getAll();
    }

    protected int getAllBufferedSize() {
        int size = 0;
        for (BufferedStorage storage : getAll()) {
            size += storage.getSize();
        }

        return size;
    }

    protected boolean remove(long spanId) {
        return storageRepository.remove(spanId);
    }

    protected void remove(List<BufferedStorage> storageList) {
        for (BufferedStorage storage : storageList) {
            remove(storage);
        }
    }

    protected boolean remove(BufferedStorage storage) {
        if (storage == null) {
            throw new NullPointerException("storage may not be null");
        }
        storage.close();
        return storageRepository.remove(storage);
    }


    protected void removeIfEmpty(List<BufferedStorage> storageList) {
        for (BufferedStorage storage : storageList) {
            removeIfEmpty(storage);
        }
    }

    protected boolean removeIfEmpty(BufferedStorage storage) {
        if (storage == null) {
            throw new NullPointerException("storage may not be null");
        }

        if (storage.isEmpty()) {
            storage.close();
            return storageRepository.remove(storage);
        }
        return false;
    }

}

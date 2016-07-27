package com.navercorp.pinpoint.profiler.context.storage;

import com.navercorp.pinpoint.profiler.context.Span;

/**
 * @author Taejin Koo
 */
public class DefaultAsyncSupportedStorageFactory implements AsyncSupportedStorageFactory {

    private final StorageFactory storageFactory;
    private final StorageEventDispatcher storageEventDispatcher;

    public DefaultAsyncSupportedStorageFactory(StorageFactory storageFactory, StorageEventDispatcher storageEventDispatcher) {
        if (storageFactory == null) {
            throw new NullPointerException("storageFactory may not be null");
        }
        if (storageEventDispatcher == null) {
            throw new NullPointerException("storageEventDispatcher may not be null");
        }

        this.storageFactory = storageFactory;
        this.storageEventDispatcher = storageEventDispatcher;
    }

    @Override
    public Storage createStorage() {
        Storage storage = storageFactory.createStorage();
        return new CloseTriggerStorage(storage);
    }

    @Override
    public Storage createAsyncStorage() {
        return new NonClosableStorage(storageEventDispatcher);
    }


    private class CloseTriggerStorage extends TriggerStorage {

        private Long spanId = null;

        public CloseTriggerStorage(Storage storage) {
            super(storage);
        }

        @Override
        void triggerStore(Span span) {
            spanId = span.getSpanId();
        }

        @Override
        void triggerClose() {
            if (spanId != null) {
                storageEventDispatcher.close(spanId);
            }
        }

    }

}

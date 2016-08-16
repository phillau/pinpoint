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

/**
 * @author Taejin Koo
 */
public class DefaultGlobalSupportedStorageFactory implements GlobalSupportedStorageFactory {

    private final StorageFactory storageFactory;
    private final GlobalStorage globalStorage;

    public DefaultGlobalSupportedStorageFactory(StorageFactory storageFactory, GlobalStorage globalStorage) {
        if (storageFactory == null) {
            throw new NullPointerException("storageFactory may not be null");
        }
        if (globalStorage == null) {
            throw new NullPointerException("globalStorage may not be null");
        }

        this.storageFactory = storageFactory;
        this.globalStorage = globalStorage;
    }

    @Override
    public Storage createStorage() {
        Storage storage = storageFactory.createStorage();
        return new CloseTriggerStorage(storage);
    }

    @Override
    public Storage getGlobalStorage() {
        return new PassingSpanEventStorage(globalStorage);
    }

    private class CloseTriggerStorage extends TriggerStorage {

        private Long spanId = null;

        public CloseTriggerStorage(Storage storage) {
            super(storage);
        }

        @Override
        protected void triggerStore(Span span) {
            spanId = span.getSpanId();
        }

        @Override
        protected void triggerClose() {
            if (spanId != null) {
                CloseEvent closeEvent = new CloseEvent();
                closeEvent.setSpanId(spanId);
                globalStorage.close(closeEvent);
            }
        }

    }

}


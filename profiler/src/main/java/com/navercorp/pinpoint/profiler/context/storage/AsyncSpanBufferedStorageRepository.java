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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Taejin Koo
 */
public class AsyncSpanBufferedStorageRepository<T extends Storage> implements StorageRepository<T> {

    private final Map<Long, T> repository = new HashMap<Long, T>();

    private final AsyncSpanBufferedStorageFactory storageFactory;

    public AsyncSpanBufferedStorageRepository(AsyncSpanBufferedStorageFactory storageFactory) {
        this.storageFactory = storageFactory;
    }

    @Override
    public T get(long spanId) {
        T storage = repository.get(spanId);
        if (storage == null) {
            storage = (T) storageFactory.createStorage();
            repository.put(spanId, storage);
        }
        return storage;
    }

    @Override
    public List<T> getAll() {
        Collection<T> storages = repository.values();
        return new ArrayList<T>(storages);
    }

    @Override
    public boolean remove(long spanId) {
        Storage removed = repository.remove(spanId);
        return removed != null;
    }

    @Override
    public boolean remove(T storage) {
        return repository.values().remove(storage);
    }

    @Override
    public int size() {
        return repository.size();
    }

}
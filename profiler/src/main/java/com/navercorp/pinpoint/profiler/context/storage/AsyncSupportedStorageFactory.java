package com.navercorp.pinpoint.profiler.context.storage;

/**
 * @author Taejin Koo
 */
public interface AsyncSupportedStorageFactory extends StorageFactory {

    Storage createAsyncStorage();

}

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
import com.navercorp.pinpoint.profiler.context.SpanChunkFactory;
import com.navercorp.pinpoint.profiler.sender.DataSender;

/**
 * @author Taejin Koo
 */
public class AsyncSpanBufferedStorageFactory implements StorageFactory {

    private final int bufferSize;

    private final SpanChunkFactory spanChunkFactory;

    private final DataSender dataSender;

    public AsyncSpanBufferedStorageFactory(DataSender dataSender, ProfilerConfig config, AgentInformation agentInformation) {
        if (dataSender == null) {
            throw new NullPointerException("dataSender must not be null");
        }
        if (config == null) {
            throw new NullPointerException("config must not be null");
        }
        this.dataSender = dataSender;
        this.bufferSize = config.getIoBufferingBufferSize();
        this.spanChunkFactory = new SpanChunkFactory(agentInformation);
    }

    @Override
    public AsyncSpanBufferedStorage createStorage() {
        AsyncSpanBufferedStorage storage = new AsyncSpanBufferedStorage(dataSender, spanChunkFactory, bufferSize);
        return storage;
    }

    @Override
    public String toString() {
        return "AsyncSpanBufferedStorageFactory{" +
                "bufferSize=" + bufferSize +
                ", dataSender=" + dataSender +
                '}';
    }

}


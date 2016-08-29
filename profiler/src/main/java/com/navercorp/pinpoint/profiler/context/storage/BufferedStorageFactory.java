/*
 * Copyright 2014 NAVER Corp.
 *
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
 */

package com.navercorp.pinpoint.profiler.context.storage;

import com.navercorp.pinpoint.bootstrap.config.ProfilerConfig;
import com.navercorp.pinpoint.profiler.AgentInformation;
import com.navercorp.pinpoint.profiler.context.SpanChunkFactory;
import com.navercorp.pinpoint.profiler.context.storage.flush.DispatcherFlusher;
import com.navercorp.pinpoint.profiler.context.storage.flush.GlobalAutoFlusher;
import com.navercorp.pinpoint.profiler.context.storage.flush.RemoteFlusher;
import com.navercorp.pinpoint.profiler.context.storage.flush.SpanEventThresholdCondition;
import com.navercorp.pinpoint.profiler.context.storage.flush.StorageFlusher;
import com.navercorp.pinpoint.profiler.sender.DataSender;

/**
 * @author emeroad
 */
public class BufferedStorageFactory implements StorageFactory {

    private final StorageFlusher flusher;
    private final int bufferSize;
    private final SpanChunkFactory spanChunkFactory;

    public BufferedStorageFactory(DataSender dataSender, ProfilerConfig config, AgentInformation agentInformation) {
        if (dataSender == null) {
            throw new NullPointerException("dataSender must not be null");
        }
        if (config == null) {
            throw new NullPointerException("config must not be null");
        }
        RemoteFlusher remoteFlusher = new RemoteFlusher(dataSender);
        if (config.isIoGlobalStorageEnable()) {
            DispatcherFlusher dispatcherFlusher = new DispatcherFlusher(remoteFlusher);

            int globalStorageBufferSize = config.getIoGlobalStorageBufferSize();
            int upperLimitPercent = config.getIoGlobalStorageUseUpperLimitPercent();

            SpanEventThresholdCondition condition = new SpanEventThresholdCondition(globalStorageBufferSize, upperLimitPercent);
            GlobalAutoFlusher globalAutoFlusher = new GlobalAutoFlusher(dataSender, globalStorageBufferSize);
            globalAutoFlusher.start(config.getIoGlobalStorageFlushInterval());

            dispatcherFlusher.addFlusherCondition(condition, globalAutoFlusher);

            this.flusher = dispatcherFlusher;
        } else {
            this.flusher = remoteFlusher;
        }

        this.bufferSize = config.getIoBufferingBufferSize();
        this.spanChunkFactory = new SpanChunkFactory(agentInformation);
    }

    @Override

    public Storage createStorage() {
        BufferedStorage bufferedStorage = new BufferedStorage(this.flusher, spanChunkFactory, this.bufferSize);
        return bufferedStorage;
    }

    @Override
    public String toString() {
        return "BufferedStorageFactory{" +
                "bufferSize=" + bufferSize +
                ", flusher=" + flusher +
                '}';
    }

}

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

/**
 * @author Taejin Koo
 */
public class FlushEvent {

    private final boolean flushAll;

    private Long spanId;

    private long expiryTime = -1;

    private int maximumBufferSize = -1;

    public FlushEvent() {
        this(false);
    }

    public FlushEvent(boolean flushAll) {
        this.flushAll = flushAll;
    }

    public boolean isFlushAll() {
        return flushAll;
    }

    public Long getSpanId() {
        return spanId;
    }

    public void setSpanId(Long spanId) {
        this.spanId = spanId;
    }

    public long getExpiryTime() {
        return expiryTime;
    }

    public void setExpiryTime(long expiryTime) {
        this.expiryTime = expiryTime;
    }

    public int getMaximumBufferSize() {
        return maximumBufferSize;
    }

    public void setMaximumBufferSize(int maximumBufferSize) {
        this.maximumBufferSize = maximumBufferSize;
    }

    @Override
    public String toString() {
        return "FlushEvent{" +
                "flushAll=" + flushAll +
                ", spanId=" + spanId +
                ", expiryTime=" + expiryTime +
                ", maximumBufferSize=" + maximumBufferSize +
                '}';
    }

}

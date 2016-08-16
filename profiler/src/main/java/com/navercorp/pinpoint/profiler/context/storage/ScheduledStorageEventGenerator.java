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

import com.navercorp.pinpoint.rpc.util.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * @author Taejin Koo
 */
public class ScheduledStorageEventGenerator {

    private static final long DEFAULT_FLUSH_DELAY = 60000;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private volatile boolean closed;

    private final Timer timer;

    private final StorageEventDispatcher storageEventDispatcher;

    public ScheduledStorageEventGenerator(StorageEventDispatcher storageEventDispatcher) {
        if (storageEventDispatcher == null) {
            throw new NullPointerException("storageEventDispatcher may not be null");
        }

        this.storageEventDispatcher = storageEventDispatcher;
        this.timer = new Timer(ClassUtils.simpleClassName(this) + "-Timer", true);
    }

    public void start(FlushEvent flushEvent, long flushDelay) {
        if (flushDelay <= 0) {
            throw new IllegalArgumentException("flushDelay must greater than 0");
        }

        FlushEvent copiedFlushEvent = new FlushEvent(flushEvent.isFlushAll());
        copiedFlushEvent.setSpanId(flushEvent.getSpanId());
        copiedFlushEvent.setExpiryTime(flushEvent.getExpiryTime());
        copiedFlushEvent.setMaximumBufferSize(flushEvent.getMaximumBufferSize());

        timer.schedule(new FlushTimerTask(copiedFlushEvent, flushDelay), flushDelay);
    }

    public void stop() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }

        if (timer != null) {
            timer.cancel();
        }
    }

    class FlushTimerTask extends TimerTask {

        private final FlushEvent flushEvent;
        private final long delay;

        public FlushTimerTask(FlushEvent flushEvent, long delay) {
            this.flushEvent = flushEvent;
            this.delay = delay;
        }

        @Override
        public void run() {
            try {
                storageEventDispatcher.flush(flushEvent);
            } catch (Exception e) {
                logger.warn("FlushTimerTask failed. error:{}", e.getMessage(), e);
            } finally {
                if (timer != null && !closed) {
                    timer.schedule(new FlushTimerTask(flushEvent, delay), delay);
                }
            }
        }

    }

}

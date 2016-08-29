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

package com.navercorp.pinpoint.profiler.context.storage.flush;

import com.navercorp.pinpoint.profiler.context.RandomTSpan;
import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanChunk;
import com.navercorp.pinpoint.profiler.context.SpanEvent;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Taejin Koo
 */
public class DispatcherFlusherTest {

    private RandomTSpan randomSpanFactory = new RandomTSpan();

    @Test
    public void dispatchTest1() throws Exception {
        CountingStorageFlusher defaultFlusher = new CountingStorageFlusher();
        DispatcherFlusher dispatcherFlusher = new DispatcherFlusher(defaultFlusher);

        CountingStorageFlusher neverExecutFlusher = new CountingStorageFlusher();
        dispatcherFlusher.addFlusherCondition(new AlwaysRejectFlushCondition(), neverExecutFlusher);

        dispatcherFlusher.flush((SpanEvent)null);
        Assert.assertEquals(1, defaultFlusher.getSpanEventCount());
        Assert.assertEquals(0, defaultFlusher.getSpanChunkCount());
        Assert.assertEquals(0, defaultFlusher.getSpanCount());

        SpanChunk spanChunk = randomSpanFactory.createSpanChunk(5);
        dispatcherFlusher.flush(spanChunk);
        Assert.assertEquals(1, defaultFlusher.getSpanEventCount());
        Assert.assertEquals(1, defaultFlusher.getSpanChunkCount());
        Assert.assertEquals(0, defaultFlusher.getSpanCount());

        Span span = randomSpanFactory.createSpan(5);
        dispatcherFlusher.flush(span);
        Assert.assertEquals(1, defaultFlusher.getSpanEventCount());
        Assert.assertEquals(1, defaultFlusher.getSpanChunkCount());
        Assert.assertEquals(1, defaultFlusher.getSpanCount());
    }

    @Test
    public void dispatchTest2() throws Exception {
        CountingStorageFlusher defaultFlusher = new CountingStorageFlusher();
        DispatcherFlusher dispatcherFlusher = new DispatcherFlusher(defaultFlusher);

        CountingStorageFlusher alwaysExecuteFlusher = new CountingStorageFlusher();
        dispatcherFlusher.addFlusherCondition(new AlwaysAcceptFlushCondition(), alwaysExecuteFlusher);

        dispatcherFlusher.flush((SpanEvent)null);
        Assert.assertEquals(0, defaultFlusher.getSpanEventCount());
        Assert.assertEquals(0, defaultFlusher.getSpanChunkCount());
        Assert.assertEquals(0, defaultFlusher.getSpanCount());

        SpanChunk spanChunk = randomSpanFactory.createSpanChunk(5);
        dispatcherFlusher.flush(spanChunk);
        Assert.assertEquals(0, defaultFlusher.getSpanEventCount());
        Assert.assertEquals(0, defaultFlusher.getSpanChunkCount());
        Assert.assertEquals(0, defaultFlusher.getSpanCount());

        Span span = randomSpanFactory.createSpan(5);
        dispatcherFlusher.flush(span);
        Assert.assertEquals(0, defaultFlusher.getSpanEventCount());
        Assert.assertEquals(0, defaultFlusher.getSpanChunkCount());
        Assert.assertEquals(0, defaultFlusher.getSpanCount());
    }

    class AlwaysRejectFlushCondition implements SpanEventFlushCondition, SpanFlushCondition, SpanChunkFlushCondition {

        @Override
        public boolean matches(SpanChunk spanChunk, StorageFlusher flusher) {
            return false;
        }

        @Override
        public boolean matches(SpanEvent spanEvent, StorageFlusher flusher) {
            return false;
        }

        @Override
        public boolean matches(Span span, StorageFlusher flusher) {
            return false;
        }

    }

    class AlwaysAcceptFlushCondition implements SpanEventFlushCondition, SpanFlushCondition, SpanChunkFlushCondition {

        @Override
        public boolean matches(SpanChunk spanChunk, StorageFlusher flusher) {
            return true;
        }

        @Override
        public boolean matches(SpanEvent spanEvent, StorageFlusher flusher) {
            return true;
        }

        @Override
        public boolean matches(Span span, StorageFlusher flusher) {
            return true;
        }

    }

}

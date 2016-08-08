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

package com.navercorp.pinpoint.profiler.context;

import com.navercorp.pinpoint.profiler.AgentInformation;

import java.util.ArrayList;
import java.util.List;

/**
 * @author emeroad
 */
public class SpanChunkFactory {

    private final AgentInformation agentInformation;

    public SpanChunkFactory(AgentInformation agentInformation) {
        if (agentInformation == null) {
            throw new NullPointerException("agentInformation must not be null");
        }
        this.agentInformation = agentInformation;
    }

    public SpanChunk create(final List<SpanEvent> flushData) {
        assertFlushData(flushData);

        final SpanEvent first = flushData.get(0);
        if (first == null) {
            throw new IllegalStateException("first SpanEvent is null");
        }
        final Span parentSpan = first.getSpan();
        final String agentId = this.agentInformation.getAgentId();

        final SpanChunk spanChunk = new SpanChunk(flushData);
        spanChunk.setAgentId(agentId);
        spanChunk.setApplicationName(this.agentInformation.getApplicationName());
        spanChunk.setAgentStartTime(this.agentInformation.getStartTime());
        spanChunk.setApplicationServiceType(this.agentInformation.getServerType().getCode());

        spanChunk.setServiceType(parentSpan.getServiceType());


        final byte[] transactionId = parentSpan.getTransactionId();
        spanChunk.setTransactionId(transactionId);


        spanChunk.setSpanId(parentSpan.getSpanId());

        spanChunk.setEndPoint(parentSpan.getEndPoint());
        return spanChunk;
    }

    public AsyncSpanChunk createAsyncSpanChunk(final List<Span> flushData) {
        final Span first = flushData.get(0);
        if (first == null) {
            throw new IllegalStateException("first SpanEvent is null");
        }
        List<AsyncSpan> asyncSpanList = new ArrayList<AsyncSpan>(flushData.size());
        for (Span span : flushData) {
            asyncSpanList.add(new AsyncSpan(span));
        }
        return createAsyncSpanChunk(first, asyncSpanList);
    }

    public AsyncSpanChunk createAsyncSpanChunk(final Span span, final List<AsyncSpan> flushData) {
        assertFlushData(flushData);

        if (span == null) {
            throw new IllegalStateException("span may not be null");
        }

        final String agentId = this.agentInformation.getAgentId();

        final AsyncSpanChunk asyncSpanChunk = new AsyncSpanChunk(flushData);
        asyncSpanChunk.setAgentId(agentId);
        asyncSpanChunk.setApplicationName(agentId);
        asyncSpanChunk.setAgentStartTime(span.getAgentStartTime());
        asyncSpanChunk.setApplicationServiceType(span.getApplicationServiceType());
        asyncSpanChunk.setServiceType(span.getServiceType());

        final byte[] transactionId = span.getTransactionId();
        asyncSpanChunk.setTransactionId(transactionId);

        asyncSpanChunk.setSpanId(span.getSpanId());
        asyncSpanChunk.setStartTime(span.getStartTime());
        asyncSpanChunk.setEndPoint(span.getEndPoint());

        return asyncSpanChunk;
    }

    private void assertFlushData(List flushData) {
        if (flushData == null) {
            throw new NullPointerException("flushData must not be null");
        }
        // TODO must be equals to or greater than 1
        final int size = flushData.size();
        if (size < 1) {
            throw new IllegalArgumentException("flushData.size() < 1 size:" + size);
        }
    }

}

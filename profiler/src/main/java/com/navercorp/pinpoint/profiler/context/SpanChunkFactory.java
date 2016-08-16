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
        if (flushData == null) {
            throw new NullPointerException("flushData must not be null");
        }
        final Span parentSpan = getParentSpan(flushData);
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

    public SpanChunkList createSpanChunkList(List<List<SpanEvent>> spanEventLists) {
        if (spanEventLists == null) {
            throw new NullPointerException("spanEventLists must not be null");
        }
        // TODO must be equals to or greater than 1
        final int size = spanEventLists.size();
        if (size < 1) {
            throw new IllegalArgumentException("spanEventLists.size() < 1 size:" + size);
        }

        List<SpanEvent> firstSpanEventList = spanEventLists.get(0);
        if (firstSpanEventList == null) {
            throw new NullPointerException("firstSpanEventList must not be null");
        }
        final Span parentSpan = getParentSpan(firstSpanEventList);

        List<LiteSpanChunk> liteSpanChunkList = new ArrayList<LiteSpanChunk>(size);
        for (List<SpanEvent> spanEventList : spanEventLists) {
            LiteSpanChunk liteSpanChunk = createLiteSpanChunk(spanEventList);
            liteSpanChunkList.add(liteSpanChunk);
        }

        SpanChunkList spanChunkList = new SpanChunkList(liteSpanChunkList);
        spanChunkList.setApplicationName(parentSpan.getApplicationName());
        spanChunkList.setAgentId(parentSpan.getAgentId());
        spanChunkList.setAgentStartTime(parentSpan.getAgentStartTime());

        return spanChunkList;
    }

    private LiteSpanChunk createLiteSpanChunk(final List<SpanEvent> spanEventList) {
        if (spanEventList == null) {
            throw new NullPointerException("spanEventList must not be null");
        }
        final Span parentSpan = getParentSpan(spanEventList);

        final LiteSpanChunk spanChunk = new LiteSpanChunk(spanEventList);
        spanChunk.setServiceType(parentSpan.getServiceType());
        spanChunk.setTransactionId(parentSpan.getTransactionId());
        spanChunk.setSpanId(parentSpan.getSpanId());

        if (parentSpan.isSetEndPoint()) {
            spanChunk.setEndPoint(parentSpan.getEndPoint());
        }

        if (parentSpan.isSetApplicationServiceType()) {
            spanChunk.setApplicationServiceType(parentSpan.getApplicationServiceType());
        }

        return spanChunk;
    }

    private Span getParentSpan(List<SpanEvent> spanEventList) {
        // TODO must be equals to or greater than 1
        final int size = spanEventList.size();
        if (size < 1) {
            throw new IllegalArgumentException("spanEventList.size() < 1 size:" + size);
        }

        final SpanEvent first = spanEventList.get(0);
        if (first == null) {
            throw new IllegalStateException("first SpanEvent is null");
        }
        return first.getSpan();
    }

}

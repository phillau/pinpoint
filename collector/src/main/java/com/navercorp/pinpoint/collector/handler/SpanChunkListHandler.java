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

package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.thrift.dto.TLiteSpanChunk;
import com.navercorp.pinpoint.thrift.dto.TSpanChunk;
import com.navercorp.pinpoint.thrift.dto.TSpanChunkList;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Taejin Koo
 */
@Service
public class SpanChunkListHandler implements SimpleHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    @Qualifier("spanChunkHandler")
    private SimpleHandler spanChunkHandler;

    public void handleSimple(TBase<?, ?> tbase) {
        if (!(tbase instanceof TSpanChunkList)) {
            throw new IllegalArgumentException("unexpected tbase:" + tbase + " expected:" + this.getClass().getName());
        }

        TSpanChunkList spanChunkList = (TSpanChunkList) tbase;
        List<TLiteSpanChunk> liteSpanChunkList = spanChunkList.getLiteSpanChunkList();

        for (TLiteSpanChunk liteSpanChunk : liteSpanChunkList) {

            TSpanChunk spanChunk = new TSpanChunk();
            spanChunk.setServiceType(liteSpanChunk.getServiceType());
            spanChunk.setTransactionId(liteSpanChunk.getTransactionId());

            spanChunk.setSpanId(liteSpanChunk.getSpanId());

            if (liteSpanChunk.isSetEndPoint()) {
                spanChunk.setEndPoint(liteSpanChunk.getEndPoint());
            }
            if (liteSpanChunk.isSetApplicationServiceType()) {
                spanChunk.setApplicationServiceType(liteSpanChunk.getApplicationServiceType());
            }

            spanChunk.setSpanEventList(liteSpanChunk.getSpanEventList());
            spanChunkHandler.handleSimple(spanChunk);
        }
    }

}

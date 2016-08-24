package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.thrift.dto.TSpan;
import com.navercorp.pinpoint.thrift.dto.TSpanAndSpanChunkList;
import com.navercorp.pinpoint.thrift.dto.TSpanChunk;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;

/**
 * @author Taejin Koo
 */
public class SpanAndSpanChunkListHandler implements SimpleHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    @Qualifier("spanChunkHandler")
    private SimpleHandler spanChunkHandler;

    @Autowired
    @Qualifier("spanHandler")
    private SimpleHandler spanHandler;


    public void handleSimple(TBase<?, ?> tbase) {
        if (!(tbase instanceof TSpanAndSpanChunkList)) {
            throw new IllegalArgumentException("unexpected tbase:" + tbase + " expected:" + this.getClass().getName());
        }

        TSpanAndSpanChunkList spanAndSpanChunkList = (TSpanAndSpanChunkList) tbase;

        List<TSpanChunk> spanChunkList = spanAndSpanChunkList.getSpanChunkList();
        for (TSpanChunk spanChunk : spanChunkList) {
            spanChunkHandler.handleSimple(spanChunk);
        }

        List<TSpan> spanList = spanAndSpanChunkList.getSpanList();
        for (TSpan span : spanList) {
            spanHandler.handleSimple(span);
        }
    }

}


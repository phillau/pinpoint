package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.thrift.dto.TSpan;
import com.navercorp.pinpoint.thrift.dto.TSpanAndSpanChunkList;
import com.navercorp.pinpoint.thrift.dto.TSpanChunk;
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
        if (logger.isDebugEnabled()) {
            logger.debug("Received SpanAndSpanChunkList={}", spanAndSpanChunkList);
        }

        if (spanAndSpanChunkList.isSetSpanChunkList()) {
            List<TSpanChunk> spanChunkList = spanAndSpanChunkList.getSpanChunkList();
            for (TSpanChunk spanChunk : spanChunkList) {
                spanChunkHandler.handleSimple(spanChunk);
            }
        }

        if (spanAndSpanChunkList.isSetSpanList()) {
            List<TSpan> spanList = spanAndSpanChunkList.getSpanList();
            for (TSpan span : spanList) {
                spanHandler.handleSimple(span);
            }
        }
    }

}


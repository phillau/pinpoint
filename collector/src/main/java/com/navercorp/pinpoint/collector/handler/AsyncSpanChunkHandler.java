package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.thrift.dto.TAsyncSpan;
import com.navercorp.pinpoint.thrift.dto.TAsyncSpanChunk;
import com.navercorp.pinpoint.thrift.dto.TSpan;
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
public class AsyncSpanChunkHandler implements SimpleHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired()
    @Qualifier("spanHandler")
    private SpanHandler spanHandler;

    public void handleSimple(TBase<?, ?> tbase) {
        logger.warn("AsyncSpanChunkHandler handleSimple");

        if (!(tbase instanceof TAsyncSpanChunk)) {
            throw new IllegalArgumentException("unexpected tbase:" + tbase + " expected:" + this.getClass().getName());
        }

        TAsyncSpanChunk asyncSpanChunk = (TAsyncSpanChunk) tbase;
        List<TAsyncSpan> asyncSpanList = asyncSpanChunk.getAsyncSpanList();

        logger.warn("AsyncSpanChunkHandler handleSimple SpanSize:{}", asyncSpanList.size());

        for (TAsyncSpan asyncSpan : asyncSpanList) {
            TSpan span = new TSpan();
            span.setApplicationName(asyncSpanChunk.getApplicationName());
            span.setAgentId(asyncSpanChunk.getAgentId());
            span.setAgentStartTime(asyncSpanChunk.getAgentStartTime());
            span.setApplicationServiceType(asyncSpanChunk.getApplicationServiceType());
            span.setServiceType(asyncSpanChunk.getServiceType());
            span.setTransactionId(asyncSpanChunk.getTransactionId());
            span.setSpanId(asyncSpanChunk.getSpanId());
            span.setEndPoint(asyncSpanChunk.getEndPoint());
            span.setStartTime(asyncSpanChunk.getStartTime());

            span.setElapsed(asyncSpan.getElapsed());
            span.setSpanEventList(asyncSpan.getSpanEventList());

            spanHandler.handleSimple(span);
        }
    }

}

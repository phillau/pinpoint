package com.navercorp.pinpoint.profiler.context.storage.flush;

import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanChunk;
import com.navercorp.pinpoint.profiler.context.SpanEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Taejin Koo
 */
public class LogStorageFlusher implements StorageFlusher {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void flush(SpanEvent spanEvent) {
        logger.debug("log spanEvent:{}", spanEvent);
    }

    @Override
    public void flush(SpanChunk spanChunk) {
        logger.debug("log spanChunk:{}", spanChunk);
    }

    @Override
    public void flush(Span span) {
        logger.debug("log span:{}", span);
    }

}

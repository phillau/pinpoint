package com.navercorp.pinpoint.profiler.context.storage.flush;

import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanChunk;
import com.navercorp.pinpoint.profiler.context.SpanEvent;

/**
 * @author Taejin Koo
 */
public interface StorageFlusher {

    void flush(SpanEvent spanEvent);

    void flush(SpanChunk spanChunk);

    void flush(Span span);

}

package com.navercorp.pinpoint.profiler.context.storage.flush;

import com.navercorp.pinpoint.profiler.context.Span;

/**
 * @author Taejin Koo
 */
public interface SpanFlushCondition extends FlushCondition {

    boolean matches(Span span, StorageFlusher flusher);

}

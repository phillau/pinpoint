package com.navercorp.pinpoint.profiler.context.storage.flush;

import com.navercorp.pinpoint.profiler.context.SpanEvent;

/**
 * @author Taejin Koo
 */
public interface SpanEventFlushCondition extends FlushCondition {

    boolean matches(SpanEvent spanEvent, StorageFlusher flusher);

}

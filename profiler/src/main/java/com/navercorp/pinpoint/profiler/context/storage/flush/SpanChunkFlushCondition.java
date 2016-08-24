package com.navercorp.pinpoint.profiler.context.storage.flush;

import com.navercorp.pinpoint.profiler.context.SpanChunk;

/**
 * @author Taejin Koo
 */
public interface SpanChunkFlushCondition extends FlushCondition {

    boolean matches(SpanChunk spanChunk, StorageFlusher flusher);

}

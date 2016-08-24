package com.navercorp.pinpoint.profiler.context.storage;

import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanEvent;
import com.navercorp.pinpoint.profiler.context.storage.flush.StorageFlusher;

/**
 * @author Taejin Koo
 */
public class DirectFlushStorage implements Storage {

    private final StorageFlusher flusher;

    public DirectFlushStorage(StorageFlusher flusher) {
        this.flusher = flusher;
    }

    @Override
    public void store(SpanEvent spanEvent) {
        flusher.flush(spanEvent);
    }

    @Override
    public void store(Span span) {
        flusher.flush(span);
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

}

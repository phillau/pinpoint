package com.navercorp.pinpoint.profiler.context.storage;

import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanEvent;

/**
 * @author Taejin Koo
 */
public class TriggerStorage implements Storage {

    private final Storage storage;

    public TriggerStorage(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void store(SpanEvent spanEvent) {
        storage.store(spanEvent);
        triggerStore(spanEvent);
    }

    protected void triggerStore(SpanEvent spanEvent) {
    }

    @Override
    public void store(Span span) {
        storage.store(span);
        triggerStore(span);
    }

    protected void triggerStore(Span span) {
    }

    @Override
    public void flush() {
        storage.flush();
        triggerFlush();
    }

    protected void triggerFlush() {
    }

    @Override
    public void close() {
        storage.close();
        triggerClose();
    }

    protected void triggerClose() {
    }

}

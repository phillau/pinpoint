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

    void triggerStore(SpanEvent spanEvent) {
    }

    @Override
    public void store(Span span) {
        storage.store(span);
        triggerStore(span);
    }

    void triggerStore(Span span) {
    }

    @Override
    public boolean isEmpty() {
        return storage.isEmpty();
    }

    @Override
    public void flush() {
        storage.flush();
        triggerFlush();
    }

    void triggerFlush() {
    }

    @Override
    public void close() {
        storage.close();
        triggerClose();
    }

    void triggerClose() {
    }

}

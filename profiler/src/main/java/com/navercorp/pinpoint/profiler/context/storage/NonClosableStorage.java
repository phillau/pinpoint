package com.navercorp.pinpoint.profiler.context.storage;

import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanEvent;

/**
 * @author Taejin Koo
 */
public class NonClosableStorage implements Storage {

    private final Storage delegator;

    public NonClosableStorage(Storage storage) {
        if (storage == null) {
            throw new NullPointerException("storage may not be null");
        }

        this.delegator = storage;
    }


    @Override
    public void store(SpanEvent spanEvent) {
        delegator.store(spanEvent);
    }

    @Override
    public void store(Span span) {
        delegator.store(span);
    }

    @Override
    public boolean isEmpty() {
        return delegator.isEmpty();
    }

    @Override
    public void flush() {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }

}

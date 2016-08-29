package com.navercorp.pinpoint.profiler.context.storage.flush;

import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanChunk;
import com.navercorp.pinpoint.profiler.context.SpanEvent;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Taejin Koo
 */
public class DispatcherFlusher implements StorageFlusher {

    private final StorageFlusher defaultFlusher;

    private final Map<SpanEventFlushCondition, StorageFlusher> spanEventFlusherRepository
            = new LinkedHashMap<SpanEventFlushCondition, StorageFlusher>();
    private final Map<SpanChunkFlushCondition, StorageFlusher> spanChunkFlusherRepository
            = new LinkedHashMap<SpanChunkFlushCondition, StorageFlusher>();
    private final Map<SpanFlushCondition, StorageFlusher> spanFlusherRepository
            = new LinkedHashMap<SpanFlushCondition, StorageFlusher>();

    public DispatcherFlusher(StorageFlusher defaultFlusher) {
        if (defaultFlusher == null) {
            throw new NullPointerException("defaultFlusher may not be null");
        }

        this.defaultFlusher = defaultFlusher;
    }

    public void addFlusherCondition(FlushCondition condition, StorageFlusher flusher) {
        if (flusher == null) {
            throw new NullPointerException("flusher may not be null");
        }

        if (condition instanceof SpanEventFlushCondition) {
            spanEventFlusherRepository.put((SpanEventFlushCondition) condition, flusher);
        }

        if (condition instanceof SpanChunkFlushCondition) {
            spanChunkFlusherRepository.put((SpanChunkFlushCondition) condition, flusher);
        }

        if (condition instanceof SpanFlushCondition) {
            spanFlusherRepository.put((SpanFlushCondition) condition, flusher);
        }
    }

    @Override
    public void flush(SpanEvent spanEvent) {
        for (Map.Entry<SpanEventFlushCondition, StorageFlusher> entry : spanEventFlusherRepository.entrySet()) {
            SpanEventFlushCondition condition = entry.getKey();
            StorageFlusher flusher = entry.getValue();
            if (condition.matches(spanEvent, flusher)) {
                flusher.flush(spanEvent);
                return;
            }
        }

        defaultFlusher.flush(spanEvent);
    }

    @Override
    public void flush(SpanChunk spanChunk) {
        for (Map.Entry<SpanChunkFlushCondition, StorageFlusher> entry : spanChunkFlusherRepository.entrySet()) {
            SpanChunkFlushCondition condition = entry.getKey();
            StorageFlusher flusher = entry.getValue();
            if (condition.matches(spanChunk, flusher)) {
                flusher.flush(spanChunk);
                return;
            }
        }

        defaultFlusher.flush(spanChunk);
    }

    @Override
    public void flush(Span span) {
        for (Map.Entry<SpanFlushCondition, StorageFlusher> entry : spanFlusherRepository.entrySet()) {
            SpanFlushCondition condition = entry.getKey();
            StorageFlusher flusher = entry.getValue();
            if (condition.matches(span, flusher)) {
                flusher.flush(span);
                return;
            }
        }

        defaultFlusher.flush(span);
    }

}

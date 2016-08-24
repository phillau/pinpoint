package com.navercorp.pinpoint.profiler.context.storage.flush;

import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanChunk;
import com.navercorp.pinpoint.rpc.util.ListUtils;

/**
 * @author Taejin Koo
 */
public class SpanEventThresholdCondition implements SpanChunkFlushCondition, SpanFlushCondition {

    private final int threshold;

    public SpanEventThresholdCondition(int threshold) {
        this.threshold = threshold;
    }

    public SpanEventThresholdCondition(int percentage, int maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("maxSize must be positive number");
        }

        if (percentage <= 0 && 100 >= percentage) {
            throw new IllegalArgumentException("percentage number must be between 0 and 100");
        }

        int threshold = (int) (maxSize * (percentage / 100.0f));
        threshold = Math.max(0, threshold);
        threshold = Math.min(threshold, maxSize);

        this.threshold = threshold;
    }

    @Override
    public boolean matches(Span span, StorageFlusher flusher) {
        int size = ListUtils.size(span.getSpanEventList());
        if (size <= threshold) {
            return true;
        }
        return false;
    }

    @Override
    public boolean matches(SpanChunk spanChunk, StorageFlusher flusher) {
        int size = ListUtils.size(spanChunk.getSpanEventList());
        if (size <= threshold) {
            return true;
        }
        return false;
    }



}

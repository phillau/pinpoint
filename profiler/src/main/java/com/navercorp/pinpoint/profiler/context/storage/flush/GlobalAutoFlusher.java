package com.navercorp.pinpoint.profiler.context.storage.flush;

import com.navercorp.pinpoint.common.util.PinpointThreadFactory;
import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanChunk;
import com.navercorp.pinpoint.profiler.context.SpanEvent;
import com.navercorp.pinpoint.profiler.sender.DataSender;
import com.navercorp.pinpoint.rpc.util.AssertUtils;
import com.navercorp.pinpoint.rpc.util.ListUtils;
import com.navercorp.pinpoint.thrift.dto.TSpanAndSpanChunkList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Taejin Koo
 */
public class GlobalAutoFlusher implements StorageFlusher {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();

    private final Vector<SpanChunk> spanChunkList = new Vector<SpanChunk>();
    private final Vector<Span> spanList = new Vector<Span>();

    private final int flushBufferSize;

    private final DataSender dataSender;

    private ScheduledExecutorService executor;
    private long period;


    public GlobalAutoFlusher(DataSender dataSender, int flushBufferSize) {
        if (dataSender == null) {
            throw new NullPointerException("dataSender may not be null");
        }
        if (flushBufferSize <= 0) {
            throw new IllegalArgumentException("flushBufferSize must be positive number");
        }

        this.dataSender = dataSender;
        this.flushBufferSize = flushBufferSize;
    }

    @Override
    public void flush(SpanEvent spanEvent) {
        throw new UnsupportedOperationException("flush(SpanEvent spanEvent)");
    }

    @Override
    public void flush(SpanChunk spanChunk) {
        AssertUtils.assertNotNull(spanChunk);
        AssertUtils.assertTrue(ListUtils.size(spanChunk.getSpanEventList()) <= flushBufferSize,
                "SpanEvent size(" + ListUtils.size(spanChunk.getSpanEventList()) + ") must be less than " + flushBufferSize);

        spanChunkList.add(spanChunk);
    }

    @Override
    public void flush(Span span) {
        AssertUtils.assertNotNull(span);
        AssertUtils.assertTrue(ListUtils.size(span.getSpanEventList()) <= flushBufferSize,
                "SpanEvent size(" + ListUtils.size(span.getSpanEventList()) + ") must be less than " + flushBufferSize);

        spanList.add(span);
    }

    public void start(long period) {
        executor = Executors.newScheduledThreadPool(1, new PinpointThreadFactory("Pinpoint-Global-Storage-Auto-Flusher", true));

        executor.scheduleAtFixedRate(new FlushTask(), 0L, period, TimeUnit.MILLISECONDS);
        this.period = period;
    }

    public void stop() {
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(3000 + period, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private final class FlushTask implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(this.getClass());

        public FlushTask() {
        }

        @Override
        public void run() {
            int spanChunkListSize = spanChunkList.size();
            SpanChunk[] spanChunks = new SpanChunk[spanChunkListSize];
            spanChunkList.toArray(spanChunks);

            int spanListSize = spanList.size();
            Span[] spans = new Span[spanListSize];
            spanList.toArray(spans);

            flush0(spanChunks, spans);
        }

        private void flush0(SpanChunk[] spanChunks, Span[] spans) {
            SpanAndSpanChunkListDataHolder dataHolder = new SpanAndSpanChunkListDataHolder(flushBufferSize);

            for (SpanChunk spanChunk : spanChunks) {
                boolean added = dataHolder.addSpanChunk(spanChunk);
                if (!added) {
                    send(dataHolder.createSpanAndSpanChunkList());
                    dataHolder.clear();

                    dataHolder.addSpanChunk(spanChunk);
                }
            }

            for (Span span : spans) {
                boolean added = dataHolder.addSpan(span);
                if (!added) {
                    send(dataHolder.createSpanAndSpanChunkList());
                    dataHolder.clear();

                    dataHolder.addSpan(span);
                }
            }

            send(dataHolder.createSpanAndSpanChunkList());
        }

        private void send(TSpanAndSpanChunkList spanAndSpanChunkList) {
            if (spanAndSpanChunkList == null) {
                return;
            }

            dataSender.send(spanAndSpanChunkList);
        }

    }

    private class SpanAndSpanChunkListDataHolder {

        private final int maxSpanEventSize;
        private final List<SpanChunk> spanChunkList = new ArrayList<SpanChunk>();
        private final List<Span> spanList = new ArrayList<Span>();
        private int canStoreSpanEventSize;

        private SpanAndSpanChunkListDataHolder(int maxSpanEventSize) {
            this.maxSpanEventSize = maxSpanEventSize;
            this.canStoreSpanEventSize = maxSpanEventSize;
        }

        private boolean addSpanChunk(SpanChunk spanChunk) {
            AssertUtils.assertNotNull(spanChunk);

            int spanEventSize = ListUtils.size(spanChunk.getSpanEventList());
            if (spanEventSize > canStoreSpanEventSize) {
                return false;
            }

            spanChunkList.add(spanChunk);
            canStoreSpanEventSize -= spanEventSize;
            return true;
        }

        private boolean addSpan(Span span) {
            AssertUtils.assertNotNull(span);

            int spanEventSize = ListUtils.size(span.getSpanEventList());
            if (spanEventSize > canStoreSpanEventSize) {
                return false;
            }

            spanList.add(span);
            canStoreSpanEventSize -= spanEventSize;
            return true;
        }

        private TSpanAndSpanChunkList createSpanAndSpanChunkList() {
            TSpanAndSpanChunkList spanAndSpanChunkList = new TSpanAndSpanChunkList();

            boolean empty = true;
            if (!ListUtils.isEmpty(spanChunkList)) {
                empty = false;
                spanAndSpanChunkList.setSpanChunkList((List) spanChunkList);
            }
            if (!ListUtils.isEmpty(spanList)) {
                empty = false;
                spanAndSpanChunkList.setSpanList((List) spanList);
            }

            if (empty) {
                return null;
            }
            return spanAndSpanChunkList;
        }

        private void clear() {
            spanChunkList.clear();
            spanList.clear();
            canStoreSpanEventSize = maxSpanEventSize;
        }

        private boolean isEmpty() {
            return ListUtils.size(spanChunkList) == 0 && ListUtils.size(spanList) == 0;
        }

    }

}

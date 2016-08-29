package com.navercorp.pinpoint.profiler.context.storage.flush;

import com.navercorp.pinpoint.profiler.context.Span;
import com.navercorp.pinpoint.profiler.context.SpanChunk;
import com.navercorp.pinpoint.profiler.context.SpanEvent;
import com.navercorp.pinpoint.profiler.sender.DataSender;

/**
 * @author Taejin Koo
 */
public class RemoteFlusher implements StorageFlusher {

    private final DataSender dataSender;

    public RemoteFlusher(DataSender dataSender) {
        if (dataSender == null) {
            throw new NullPointerException("dataSender may not be null");
        }

        this.dataSender = dataSender;
    }

    @Override
    public void flush(SpanEvent spanEvent) {
        throw new UnsupportedOperationException("flush(SpanEvent spanEvent)");
    }

    @Override
    public void flush(SpanChunk spanChunk) {
        dataSender.send(spanChunk);
    }

    @Override
    public void flush(Span span) {
        dataSender.send(span);
    }


    @Override
    public String toString() {
        return "RemoteFlusher{" +
                "dataSender=" + dataSender +
                '}';
    }

}

package com.navercorp.pinpoint.profiler.context.storage;

import com.navercorp.pinpoint.rpc.util.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * @author Taejin Koo
 */
public class ScheduledStorageEventGenerator {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final long DEFAULT_FLUSH_DELAY = 10000;
    private static final long DEFAULT_CLOSE_DELAY = 60000;

    private volatile boolean closed;

    private final Timer timer;

    private final StorageEventDispatcher storageEventDispatcher;

    public ScheduledStorageEventGenerator(StorageEventDispatcher storageEventDispatcher) {
        if (storageEventDispatcher == null) {
            throw new NullPointerException("storageEventDispatcher may not be null");
        }

        this.storageEventDispatcher = storageEventDispatcher;
        this.timer = new Timer(ClassUtils.simpleClassName(this) + "-Timer", true);
    }

    public void start(long flushDelay, long closeDelay) {
        timer.schedule(new FlushTimerTask(flushDelay), flushDelay);
        timer.schedule(new CloseTimerTask(closeDelay), closeDelay);
    }

    public void stop() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }

        if (timer != null) {
            timer.cancel();
        }
    }

    class FlushTimerTask extends TimerTask {

        private final long delay;

        public FlushTimerTask(long delay) {
            this.delay = delay;
        }

        @Override
        public void run() {
            try {
                storageEventDispatcher.flush();
            } catch (Exception e) {
                logger.warn("FlushTimerTask failed. error:{}", e.getMessage(), e);
            } finally {
                if (timer != null && !closed) {
                    timer.schedule(new FlushTimerTask(delay), delay);
                }
            }
        }

    }

    class CloseTimerTask extends TimerTask {

        private final long delay;

        public CloseTimerTask(long delay) {
            this.delay = delay;
        }

        @Override
        public void run() {
            try {
                storageEventDispatcher.close();
            } catch (Exception e) {
                logger.warn("CloseTimerTask failed. error:{}", e.getMessage(), e);
            } finally {
                if (timer != null && !closed) {
                    timer.schedule(new CloseTimerTask(delay), delay);
                }
            }
        }

    }

}

package org.apache.flink.playgrounds.ops.clickcount.source;

import org.apache.flink.playgrounds.ops.clickcount.records.ClickEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.playgrounds.ops.clickcount.ClickEventCount.WINDOW_SIZE;

public class KafkaMockSource implements SourceFunction<ClickEvent> {

    public static final int EVENTS_PER_WINDOW = 1000;
    private static final List<String> pages = Arrays.asList("/help", "/index", "/shop", "/jobs", "/about", "/news");

    //this calculation is only accurate as long as pages.size() * EVENTS_PER_WINDOW divides the
    //window size
    public static final long DELAY = WINDOW_SIZE.toMilliseconds() / pages.size() / EVENTS_PER_WINDOW;

    private volatile boolean isRunning = true;

    public KafkaMockSource() {
    }

    @Override
    public void run(SourceContext<ClickEvent> sourceContext) throws Exception {
        ClickIterator clickIterator = new ClickIterator();
        while(isRunning){
            ClickEvent next = clickIterator.next();
            sourceContext.collect(next);
            long random_delay = WINDOW_SIZE.toMilliseconds() / pages.size() / ((int)(200 + (Math.random()*(1000-200)+1)));
            TimeUnit.MILLISECONDS.sleep(random_delay);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    static class ClickIterator  {

        private Map<String, Long> nextTimestampPerKey;
        private int nextPageIndex;

        ClickIterator() {
            nextTimestampPerKey = new HashMap<>();
            nextPageIndex = 0;
        }

        ClickEvent next() {
            String page = nextPage();
            return new ClickEvent(nextTimestamp(page), page);
        }

        private Date nextTimestamp(String page) {
            long nextTimestamp = nextTimestampPerKey.getOrDefault(page, System.currentTimeMillis());
            nextTimestampPerKey.put(page, nextTimestamp + WINDOW_SIZE.toMilliseconds() / (int)(200 + (Math.random()*(1000-200)+1)));
            return new Date(nextTimestamp);
        }

        private String nextPage() {
            String nextPage = pages.get(nextPageIndex);
            if (nextPageIndex == pages.size() - 1) {
                nextPageIndex = 0;
            } else {
                nextPageIndex++;
            }
            return nextPage;
        }
    }
}

package com.dantri.crawler.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CrawlQueueManager {
    private static final Logger log = LoggerFactory.getLogger(CrawlQueueManager.class);
    private final BlockingQueue<UrlTask> queue;

    public CrawlQueueManager(int cap) {
        queue = new LinkedBlockingQueue<>(cap);
        log.info("Queue capacity = {}", cap);
    }

    public void pushTask(UrlTask t) {
        if (!queue.offer(t)) {
            if (log.isDebugEnabled()) log.debug("Drop URL, queue full: {}", t.getUrl());
        }
    }

    public UrlTask takeTask() {
        try { return queue.take(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); return null; }
    }
}

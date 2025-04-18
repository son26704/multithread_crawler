package com.dantri.crawler.job;

import com.dantri.crawler.config.ConfigLoader;
import com.dantri.crawler.queue.CrawlQueueManager;
import com.dantri.crawler.queue.UrlTask;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrawlJob implements Job {
    private static final Logger log = LoggerFactory.getLogger(CrawlJob.class);

    @Override
    public void execute(JobExecutionContext context) {
        try {
            CrawlQueueManager q = (CrawlQueueManager)context.getJobDetail()
                    .getJobDataMap().get("queueManager");
            ConfigLoader.getStartUrls().forEach(u -> {
                q.pushTask(new UrlTask(u,0));
                log.info("Scheduled startUrl: {}", u);
            });
        } catch (Exception e) {
            log.error("CrawlJob error", e);
        }
    }
}

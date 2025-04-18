package com.dantri.crawler;

import com.dantri.crawler.config.ConfigLoader;
import com.dantri.crawler.parser.UniversalArticleParser;
import com.dantri.crawler.queue.CrawlQueueManager;
import com.dantri.crawler.scheduler.SchedulerManager;
import com.dantri.crawler.storage.ArticleStorage;
import com.dantri.crawler.visited.VisitedUrlsManager;
import com.dantri.crawler.worker.CrawlWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class MainCrawler {
    private static final Logger log = LoggerFactory.getLogger(MainCrawler.class);

    public static void main(String[] args) {
        try {
            log.info("Starting crawler...");

            int maxLevel   = ConfigLoader.getDefaultMaxLevel();
            int threads    = ConfigLoader.getThreadPoolSize();
            int capacity   = Math.max(ConfigLoader.getMaxUrlsPerCrawl() * 200, 50_000);

            CrawlQueueManager queue       = new CrawlQueueManager(capacity);
            VisitedUrlsManager visited    = new VisitedUrlsManager();
            UniversalArticleParser parser = new UniversalArticleParser();
            ArticleStorage storage        = new ArticleStorage();

            AtomicBoolean running = new AtomicBoolean(true);
            // Khởi worker
            for (int i = 0; i < threads; i++) {
                Thread t = new Thread(
                        new CrawlWorker(queue, visited, parser, storage, maxLevel, running),
                        "Worker-" + i
                );
                t.start();
            }

            // Lập lịch push start URLs
            SchedulerManager scheduler = new SchedulerManager();
            scheduler.start(queue);

            System.out.println("Press ENTER to stop...");
            System.in.read();

            running.set(false);
            scheduler.shutdown();
            log.info("Crawler stopped.");
        } catch (Exception e) {
            log.error("Fatal error in MainCrawler", e);
        }
    }
}

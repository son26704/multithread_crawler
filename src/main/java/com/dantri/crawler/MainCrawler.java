package com.dantri.crawler;

import com.dantri.crawler.config.ConfigLoader;
import com.dantri.crawler.parser.UniversalArticleParser;
import com.dantri.crawler.queue.CrawlQueueManager;
import com.dantri.crawler.scheduler.SchedulerManager;
import com.dantri.crawler.storage.ArticleStorage;
import com.dantri.crawler.visited.NonArticleStore;
import com.dantri.crawler.visited.VisitedUrlsManager;
import com.dantri.crawler.worker.CrawlWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class MainCrawler {
    private static final Logger log = LoggerFactory.getLogger(MainCrawler.class);

    public static void main(String[] args) {
        try {
            log.info("Starting crawler...");

            int maxLevel = ConfigLoader.getDefaultMaxLevel();
            int threads = ConfigLoader.getThreadPoolSize();
            int cap = Math.max(ConfigLoader.getMaxUrlsPerCrawl() * 200, 50_000);

            CrawlQueueManager queue = new CrawlQueueManager(cap);
            VisitedUrlsManager visited = new VisitedUrlsManager();
            NonArticleStore nonArticleStore = new NonArticleStore();
            UniversalArticleParser parser = new UniversalArticleParser();
            ArticleStorage storage = new ArticleStorage();

            // Khởi tạo thread pool
            AtomicBoolean running = new AtomicBoolean(true);
            for (int i = 0; i < threads; i++) {
                Thread t = new Thread(
                        new CrawlWorker(queue, visited, nonArticleStore, parser, storage, maxLevel, running),
                        "Worker-" + i
                );
                t.start();
            }

            SchedulerManager scheduler = new SchedulerManager();
            scheduler.start(queue);

            System.out.println("Press ENTER to stop...");

            // Chặn luồng chính
            try (Scanner sc = new Scanner(System.in)) {
                sc.nextLine();
            }

            running.set(false);
            scheduler.shutdown();
            log.info("Crawler stopped.");
        } catch (Exception e) {
            log.error("Error in MainCrawler", e);
        }
    }
}
package com.dantri.crawler.worker;

import com.dantri.crawler.config.ConfigLoader;
import com.dantri.crawler.domain.Article;
import com.dantri.crawler.parser.UniversalArticleParser;
import com.dantri.crawler.queue.CrawlQueueManager;
import com.dantri.crawler.queue.UrlTask;
import com.dantri.crawler.storage.ArticleStorage;
import com.dantri.crawler.visited.VisitedUrlsManager;
import com.dantri.crawler.visited.NonArticleStore;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class CrawlWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CrawlWorker.class);

    private static final Set<String> inProgress = ConcurrentHashMap.newKeySet();

    private final CrawlQueueManager queue;
    private final VisitedUrlsManager visited;
    private final NonArticleStore nonArticleStore;
    private final UniversalArticleParser parser;
    private final ArticleStorage storage;
    private final int maxLevel;
    private final AtomicBoolean running;
    private final List<String> seeds;
    private final long sixMonthsMillis = ConfigLoader.getSixMonthsMillis();

    public CrawlWorker(CrawlQueueManager queue,
                       VisitedUrlsManager visited,
                       NonArticleStore nonArticleStore,
                       UniversalArticleParser parser,
                       ArticleStorage storage,
                       int maxLevel,
                       AtomicBoolean running) {
        this.queue = queue;
        this.visited = visited;
        this.nonArticleStore = nonArticleStore;
        this.parser = parser;
        this.storage = storage;
        this.maxLevel = maxLevel;
        this.running = running;
        this.seeds = ConfigLoader.getStartUrls();
    }

    @Override
    public void run() {
        log.info("Worker started: {}", Thread.currentThread().getName());
        while (running.get()) {
            UrlTask task = queue.takeTask();
            if (task == null) continue;

            String url   = task.getUrl();
            int level    = task.getLevel();

            // Seed URLs: chỉ extract links, không mark visited/non-article
            if (seeds.contains(url)) {
                if (level < maxLevel) extractAndQueueLinks(url, level + 1);
                continue;
            }

            // Article visited check
            if (visited.isVisited(url)) {
                continue;
            }
            // Non-article check
            if (nonArticleStore.isNonArticle(url)) {
                continue;
            }
            if (!inProgress.add(url)) {
                continue;
            }

            try {
                Article art = parser.parseUrl(url);

                // Lấy được thống tin --> là Article
                // Không lấy được --> là Non-article

                // Kiểm tra article lấy được publishTime?
                if (art != null && art.getPublishTime() != null) {
                    long age = System.currentTimeMillis() - art.getPublishTime().getTime();
                    if (age <= sixMonthsMillis) { // Lọc 6 tháng
                        storage.save(art);
                    }
                    visited.markVisited(url);
                } else {
                    // Đánh dấu non-article với TTL
                    nonArticleStore.markNonArticle(url);
                }

                // Lấy outlinks
                if (level < maxLevel) {
                    extractAndQueueLinks(url, level + 1);
                }
            } finally {
                // Trả khóa
                inProgress.remove(url);
            }
        }
    }

    private void extractAndQueueLinks(String pageUrl, int nextLevel) {
        try {
            Document doc = Jsoup.connect(pageUrl)
                    .userAgent("Mozilla/5.0")
                    .timeout(8000)
                    .get();
            String baseDomain = new URI(pageUrl).getHost();

            doc.select("a[href]").forEach(e -> {
                String href = e.absUrl("href").split("#")[0];
                if (href.isBlank()) return;
                try {
                    if (!new URI(href).getHost().equals(baseDomain)) return;
                } catch (Exception ignored) {}

                // Đẩy các outlinks tìm được vào queue
                if (!visited.isVisited(href)
                        && !nonArticleStore.isNonArticle(href)
                        && inProgress.add(href)) {
                    queue.pushTask(new UrlTask(href, nextLevel));
                    inProgress.remove(href);
                }
            });
        } catch (Exception ex) {
            log.debug("Error extracting links from {}: {}", pageUrl, ex.getMessage());
        }
    }
}

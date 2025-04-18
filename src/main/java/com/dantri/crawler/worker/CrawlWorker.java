package com.dantri.crawler.worker;

import com.dantri.crawler.domain.Article;
import com.dantri.crawler.parser.UniversalArticleParser;
import com.dantri.crawler.queue.CrawlQueueManager;
import com.dantri.crawler.queue.UrlTask;
import com.dantri.crawler.storage.ArticleStorage;
import com.dantri.crawler.visited.VisitedUrlsManager;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

public class CrawlWorker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CrawlWorker.class);

    private final CrawlQueueManager queue;
    private final VisitedUrlsManager visited;
    private final UniversalArticleParser parser;
    private final ArticleStorage storage;
    private final int maxLevel;
    private final AtomicBoolean running;

    public CrawlWorker(CrawlQueueManager queue,
                       VisitedUrlsManager visited,
                       UniversalArticleParser parser,
                       ArticleStorage storage,
                       int maxLevel,
                       AtomicBoolean running) {
        this.queue = queue;
        this.visited = visited;
        this.parser = parser;
        this.storage = storage;
        this.maxLevel = maxLevel;
        this.running = running;
    }

    @Override
    public void run() {
        log.info("Worker started: {}", Thread.currentThread().getName());
        while (running.get()) {
            UrlTask task = queue.takeTask();
            if (task == null) continue;

            String url = task.getUrl();
            int level = task.getLevel();

            // 1) Nếu đã visit (bài hoặc page), bỏ qua
            if (visited.isVisited(url)) {
                continue;
            }
            // 2) Đánh dấu tất cả URL vào visited_urls.txt
            visited.markVisited(url);

            // 3) Thử parse thành Article
            Article art = parser.parseUrl(url);
            if (art != null) {
                storage.save(art);
            }

            // 4) BFS tiếp outlinks nếu chưa vượt maxLevel
            if (level < maxLevel) {
                extractAndQueueLinks(url, level + 1);
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
                // Chỉ queue nếu chưa visited
                if (!visited.isVisited(href)) {
                    queue.pushTask(new UrlTask(href, nextLevel));
                }
            });
        } catch (Exception e) {
            log.debug("Error extracting links from {}: {}", pageUrl, e.getMessage());
        }
    }
}

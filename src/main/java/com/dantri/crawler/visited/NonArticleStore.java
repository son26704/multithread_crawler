package com.dantri.crawler.visited;

import com.dantri.crawler.config.ConfigLoader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NonArticleStore {
    private static final Logger log = LoggerFactory.getLogger(NonArticleStore.class);
    private static final Path FILE = Paths.get("data/non_article_urls.jsonl");
    private static final long TTL_MS = ConfigLoader.getNonArticleTTL();

    private final Map<String, Long> map = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public NonArticleStore() {
        load();
        // schedule cleanup every minute
        scheduler.scheduleAtFixedRate(this::cleanupExpired, 1, 1, TimeUnit.MINUTES);
    }

    private void load() {
        if (!Files.exists(FILE)) return;
        try (Stream<String> lines = Files.lines(FILE, StandardCharsets.UTF_8)) {
            lines.forEach(line -> {
                try {
                    JsonNode n = mapper.readTree(line);
                    String url = n.get("url").asText();
                    Instant inst = Instant.parse(n.get("skippedAt").asText());
                    map.put(url, inst.toEpochMilli());
                } catch (Exception e) {
                    log.warn("Skipping invalid line in non-article store: {}", line);
                }
            });
            log.info("Loaded {} non-article URLs", map.size());
        } catch (IOException e) {
            log.error("Error reading non-article JSONL store", e);
        }
    }

    /** Clean up expired entries and rewrite file if any removed */
    private synchronized void cleanupExpired() {
        long now = System.currentTimeMillis();
        boolean changed = map.entrySet().removeIf(e -> (now - e.getValue()) >= TTL_MS);
        if (changed) {
            rewriteStore();
            log.info("Cleaned up expired non-article URLs, remaining={}", map.size());
        }
    }

    public synchronized boolean isNonArticle(String url) {
        Long t = map.get(url);
        if (t == null) return false;
        if ((System.currentTimeMillis() - t) >= TTL_MS) {
            map.remove(url);
            rewriteStore();
            return false;
        }
        return true;
    }

    public synchronized void markNonArticle(String url) {
        long now = System.currentTimeMillis();
        map.put(url, now);
        try {
            Files.createDirectories(FILE.getParent());
            ObjectNode node = mapper.createObjectNode();
            node.put("url", url);
            node.put("skippedAt", Instant.ofEpochMilli(now).toString());
            Files.writeString(
                    FILE,
                    mapper.writeValueAsString(node) + "\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            log.error("Failed to append to non-article store: {}", url, e);
        }
    }

    private synchronized void rewriteStore() {
        try {
            Files.createDirectories(FILE.getParent());
            Files.write(
                    FILE,
                    map.entrySet().stream().map(e -> {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("url", e.getKey());
                        node.put("skippedAt", Instant.ofEpochMilli(e.getValue()).toString());
                        try {
                            return mapper.writeValueAsString(node);
                        } catch (JsonProcessingException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    }).collect(Collectors.toList()),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
        } catch (IOException e) {
            log.error("Failed to rewrite non-article store", e);
        }
    }
}

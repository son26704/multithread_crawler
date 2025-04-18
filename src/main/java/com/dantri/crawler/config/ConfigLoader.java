package com.dantri.crawler.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);
    private static JsonNode root;

    static { load(); }

    private static void load() {
        try (InputStream is = ConfigLoader.class.getResourceAsStream("/config.json")) {
            if (is == null) throw new RuntimeException("config.json not found");
            root = new ObjectMapper().readTree(is);
            logger.info("Loaded config.json");
        } catch (Exception e) {
            logger.error("Error loading config.json", e);
            throw new RuntimeException(e);
        }
    }

    public static List<String> getStartUrls() {
        List<String> urls = new ArrayList<>();
        root.path("startUrls").forEach(n -> urls.add(n.asText()));
        return urls;
    }

    public static int getMaxUrlsPerCrawl() {
        return root.path("settings").path("maxUrlsPerCrawl").asInt(100);
    }

    public static int getDefaultMaxLevel() {
        return root.path("settings").path("defaultMaxLevel").asInt(2);
    }

    public static long getRequestDelayMs() {
        return root.path("settings").path("requestDelayMs").asLong(200);
    }

    public static int getThreadPoolSize() {
        return root.path("settings").path("threadPoolSize").asInt(4);
    }
}

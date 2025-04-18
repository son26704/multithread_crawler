package com.dantri.crawler.visited;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class VisitedUrlsManager {
    private static final Logger log = LoggerFactory.getLogger(VisitedUrlsManager.class);
    private static final String FILE = "data/visited_urls.txt";
    private final Set<String> visited = ConcurrentHashMap.newKeySet();

    public VisitedUrlsManager() {
        load();
    }

    private void load() {
        File f = new File(FILE);
        if (!f.exists()) return;
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                visited.add(line.trim());
            }
            log.info("Loaded {} visited article URLs", visited.size());
        } catch (IOException e) {
            log.warn("Failed to load visited_urls.txt", e);
        }
    }

    public synchronized void markVisited(String url) {
        if (!visited.add(url)) {
            return;
        }
        try (FileWriter fw = new FileWriter(FILE, true)) {
            fw.append(url).append('\n');
        } catch (IOException e) {
            log.warn("Failed to write visited URL {}", url, e);
        }
    }

    public boolean isVisited(String url) {
        return visited.contains(url);
    }
}

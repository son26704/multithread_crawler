package com.dantri.crawler.queue;

public class UrlTask {
    private final String url;
    private final int level;
    public UrlTask(String u, int l) { url = u; level = l; }
    public String getUrl() { return url; }
    public int getLevel() { return level; }
}

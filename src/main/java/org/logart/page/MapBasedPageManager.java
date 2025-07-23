package org.logart.page;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MapBasedPageManager implements PageManager {
    private final AtomicLong nextId = new AtomicLong(0);
    private final ConcurrentMap<Long, Page> pages = new ConcurrentHashMap<>();
    private final Set<Long> free = ConcurrentHashMap.newKeySet();

    @Override
    public Page allocatePage() {
        return allocatePage(false);
    }

    @Override
    public Page allocateLeafPage() {
        return allocatePage(true);
    }

    private Page allocatePage(boolean leaf) {
        return pages.compute(nextId.getAndIncrement(), (id, page) -> {
            if (page == null) {
                return new InMemoryPage(id, leaf);
            }
            return page; // If the page already exists, return it
        });
    }

    @Override
    public Page readPage(long pageId) {
        return pages.get(pageId);
    }

    @Override
    public void writePage(long pageId, Page page) {

    }

    @Override
    public void freePage(long pageId) {
        if (pages.remove(pageId) != null) {
            free.add(pageId);
        }
    }

    @Override
    public void close() {

    }

    public Set<Long> getAllAllocatedPageIds() {
        return pages.keySet();
    }

    public Set<Long> getFreedNodeIds() {
        return free;
    }
}

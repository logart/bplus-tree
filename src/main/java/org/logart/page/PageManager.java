package org.logart.page;

public interface PageManager {
    Page allocatePage();

    Page allocateLeafPage();

    Page readPage(long nodeId);

    void writePage(long pageId, Page page);

    void freePage(long pageId);

    void close();
}

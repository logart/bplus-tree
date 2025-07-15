package org.logart.page;

import org.logart.Page;
import org.logart.node.BTreeNode;

public interface PageManager {
    Page allocatePage();

    Page allocateLeafPage();

    Page readPage(long nodeId);

    void writePage(long pageId, Page page);

    void freePage(Page page);

    void close();
}

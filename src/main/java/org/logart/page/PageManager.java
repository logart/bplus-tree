package org.logart.page;

import org.logart.node.BTreeNode;

public interface PageManager {
    Page open();

    Page allocatePage();

    Page allocateLeafPage();

    Page readPage(long nodeId);

    void writePage(long pageId, Page page);

    void writeRoot(BTreeNode root);

    void freePage(long pageId);

    void close();
}

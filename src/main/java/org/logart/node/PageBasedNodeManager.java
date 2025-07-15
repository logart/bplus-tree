package org.logart.node;

import org.logart.page.PageManager;

public class PageBasedNodeManager implements NodeManager {
    private final PageManager pageManager;

    public PageBasedNodeManager(PageManager pageManager) {
        this.pageManager = pageManager;
    }

    @Override
    public BTreeNode allocateNode() {
        return new PersistentBTreeNode(pageManager.allocatePage());
    }

    public BTreeNode allocateLeafNode() {
        return new PersistentBTreeNode(pageManager.allocateLeafPage());
    }

    public BTreeNode readNode(long nodeId) {
        return new PersistentBTreeNode(pageManager.readPage(nodeId));
    }

    @Override
    public void writeNode(long nodeId, BTreeNode node) {

    }

    @Override
    public void close() {

    }
}

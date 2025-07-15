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

    @Override
    public BTreeNode allocateNode(long parentId) {
        return null;
    }

    public BTreeNode allocateLeafNode() {
        return new PersistentBTreeNode(pageManager.allocateLeafPage());
    }

    @Override
    public BTreeNode allocateLeafNode(long parentId) {
        return null;
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

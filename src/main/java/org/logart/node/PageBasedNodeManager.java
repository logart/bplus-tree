package org.logart.node;

import org.logart.Versioned;
import org.logart.VersionedRefCounter;
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
        pageManager.writePage(nodeId, ((PersistentBTreeNode) node).page());
    }

    @Override
    public void freeNode(long nodeId, long version) {
        pageManager.freePage(nodeId);
    }

    @Override
    public boolean advanceVersion(Versioned<BTreeNode> currentVersionedRoot, BTreeNode newRoot) {
        return false;
    }

    @Override
    public Versioned<BTreeNode> lockVersion() {
        return null;
    }

    @Override
    public void releaseVersion(Versioned<BTreeNode> versionedRoot) {

    }

    @Override
    public void close() {

    }

    @Override
    public VersionedRefCounter refCounter() {
        return null;
    }
}

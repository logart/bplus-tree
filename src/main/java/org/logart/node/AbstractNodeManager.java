package org.logart.node;

import org.logart.Versioned;
import org.logart.VersionedRefCounter;
import org.logart.page.Page;
import org.logart.page.PageManager;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class AbstractNodeManager implements NodeManager {
    private final PageManager pageManager;
    private final VersionedRefCounter<BTreeNode> versionRefCounter;

    private final ConcurrentMap<Long, BTreeNode> nodes = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<PageAndVersion> freeCandidates = new ConcurrentSkipListSet<>();

    public AbstractNodeManager(PageManager pageManager) {
        this.pageManager = pageManager;
        this.versionRefCounter = new VersionedRefCounter<>(this::allocateLeafNode); // start with an empty node
    }

    @Override
    public BTreeNode allocateNode() {
        return allocateNode(false);
    }

    @Override
    public BTreeNode allocateLeafNode() {
        return allocateNode(true);
    }

    private BTreeNode allocateNode(boolean leaf) {
        Page page;
        if (leaf) {
            page = pageManager.allocateLeafPage(); // allocate a page for the node, this is a no-op in memory
        } else {
            page = pageManager.allocatePage();
        }
        BTreeNode result = allocateNodeBackedByPage(page);
        nodes.put(result.id(), result);
        return result;
    }

    @Override
    public BTreeNode readNode(long nodeId) {
        // todo read from nodes
        Page page = pageManager.readPage(nodeId);
        if (page == null) { // the page is deleted and could not be read
            return null;
        }
        BTreeNode readNode = allocateNodeBackedByPage(page);
        nodes.put(nodeId, readNode);
        return readNode;
    }

    @Override
    public void writeNode(long nodeId, BTreeNode node) {

    }

    @Override
    public void freeNode(long nodeId, long version) {
        if (versionRefCounter.getRefCount(version) > 0) {
            //postpone cleanup, a version in use
            //cleanup will be done after a version is released
            freeCandidates.add(new PageAndVersion(nodeId, version));
            return;
        }
        if (nodes.remove(nodeId) != null) {
            pageManager.freePage(nodeId);
        }
    }

    @Override
    public boolean advanceVersion(Versioned<BTreeNode> currentVersionedRoot, BTreeNode newRoot) {
        boolean versionMovedOn = versionRefCounter.advanceVersion(currentVersionedRoot, newRoot);

        if (versionMovedOn) {
            long greatestUnusedVersion = versionRefCounter.cleanUpTo(currentVersionedRoot.version());
            if (greatestUnusedVersion >= 0) {
                removePotentiallyFreedNodes(greatestUnusedVersion);
            }
        }
        return versionMovedOn;
    }

    private void removePotentiallyFreedNodes(long version) {
        boolean proceed = true;
        while (proceed) {
            PageAndVersion first;
            try {
                first = freeCandidates.first();
            } catch (NoSuchElementException e) {
//                this is crazy I need to do it
                // no candidates to remove
                return;
            }
            // no items
            proceed = first != null
                    // this version could be in use still
                    && first.version() <= version;
            if (proceed) {
                freeCandidates.remove(first);
                freeNode(first.nodeId(), first.version());
            }
        }
    }

    @Override
    public Versioned<BTreeNode> lockVersion() {
        return versionRefCounter.lockVersion();
    }

    @Override
    public void releaseVersion(Versioned<BTreeNode> versionedRoot) {
        versionRefCounter.releaseVersion(versionedRoot);

    }

    @Override
    public void close() {

    }

    @Override
    public VersionedRefCounter<BTreeNode> refCounter() {
        return versionRefCounter;
    }

    protected abstract BTreeNode allocateNodeBackedByPage(Page page);

    private record PageAndVersion(long nodeId,
                                  long version) implements Comparable<PageAndVersion> {
        @Override
        public int compareTo(PageAndVersion o) {
            int versionCompare = Long.compare(this.version, o.version);
            return versionCompare == 0
                    ? Long.compare(this.nodeId, o.nodeId)
                    : versionCompare;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            PageAndVersion that = (PageAndVersion) o;
            return nodeId == that.nodeId && version == that.version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, version);
        }
    }
}

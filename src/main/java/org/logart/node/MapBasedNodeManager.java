package org.logart.node;

import org.logart.Versioned;
import org.logart.VersionedRefCounter;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

public class MapBasedNodeManager implements NodeManager {
    private final AtomicLong nextId = new AtomicLong(0);
    private final ConcurrentMap<Long, BTreeNode> nodes = new ConcurrentHashMap<>();
    private final Set<Long> free = ConcurrentHashMap.newKeySet();
    private final VersionedRefCounter<BTreeNode> versionRefCounter;
    private final ConcurrentSkipListSet<PageAndVersion> freeCandidates = new ConcurrentSkipListSet<>();

    public MapBasedNodeManager() {
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
        InMemoryBTreeNode result = new InMemoryBTreeNode(nextId.getAndIncrement(), leaf);
        nodes.put(result.id(), result);
        return result;
    }

    @Override
    public BTreeNode readNode(long nodeId) {
        return nodes.get(nodeId);
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
            free.add(nodeId);
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
            PageAndVersion first = freeCandidates.pollFirst();
            // no items
            proceed = first != null
                    // this version could be in use still
                    && first.version() <= version;
            if (proceed) {
                freeNode(first.nodeId(), first.version());
            } else {
                // put first back to candidates
                if (first != null) {
                    freeCandidates.add(first);
                }
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

    public Set<Long> getAllAllocatedNodeIds() {
        return nodes.keySet();
    }

    public Set<Long> getFreedNodeIds() {
        return free;
    }

    private record PageAndVersion(long nodeId, long version) implements Comparable<PageAndVersion> {
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

package org.logart.node;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MapBasedNodeManager implements NodeManager {
    private final AtomicLong nextId = new AtomicLong(0);
    private final ConcurrentMap<Long, BTreeNode> nodes = new ConcurrentHashMap<>();
    private final Set<Long> free = ConcurrentHashMap.newKeySet();

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
    public void freeNode(long nodeId) {
        if (nodes.remove(nodeId) != null) {
            free.add(nodeId);
        }
    }

    @Override
    public void close() {

    }

    public Set<Long> getAllAllocatedNodeIds() {
        return nodes.keySet();
    }

    public Set<Long> getFreedNodeIds() {
        return free;
    }
}

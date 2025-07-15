package org.logart.node;

import java.util.HashMap;
import java.util.Map;

public class MapBasedNodeManager implements NodeManager {
    private long nextId = 0;
    private final Map<Long, BTreeNode> nodes = new HashMap<>();

    @Override
    public BTreeNode allocateNode() {
        return allocateNode(false);
    }

    @Override
    public BTreeNode allocateLeafNode() {
        return allocateNode(true);
    }

    private BTreeNode allocateNode(boolean leaf) {
        InMemoryBTreeNode result = new InMemoryBTreeNode(nextId, leaf);
        nextId++;
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
    public void close() {

    }
}

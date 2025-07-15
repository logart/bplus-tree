package org.logart.node;

import java.util.HashMap;
import java.util.Map;

public class MapBasedNodeManager implements NodeManager {
    private long nextId = 0;
    private final Map<Long, BTreeNode> nodes = new HashMap<>();

    @Override
    public BTreeNode allocateNode(long parentId) {
        return allocateNode(parentId, false);
    }

    @Override
    public BTreeNode allocateNode() {
        return allocateNode(-1);
    }

    @Override
    public BTreeNode allocateLeafNode() {
        return allocateNode(-1, true);
    }

    @Override
    public BTreeNode allocateLeafNode(long parentId) {
        return allocateNode(parentId, true);
    }

    private BTreeNode allocateNode(long parentId, boolean leaf) {
        InMemoryBTreeNode result = new InMemoryBTreeNode(nextId, parentId, leaf);
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

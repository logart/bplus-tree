package org.logart.node;

public interface NodeManager {
    BTreeNode allocateNode();

    BTreeNode allocateLeafNode();

    BTreeNode readNode(long nodeId);

    void writeNode(long nodeId, BTreeNode node);

    void close();
}

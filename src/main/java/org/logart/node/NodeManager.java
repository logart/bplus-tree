package org.logart.node;

import java.util.Set;

public interface NodeManager {
    BTreeNode allocateNode();

    BTreeNode allocateLeafNode();

    BTreeNode readNode(long nodeId);

    void writeNode(long nodeId, BTreeNode node);

    void freeNode(long nodeId);

    void close();
}

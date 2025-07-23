package org.logart.node;

import org.logart.Versioned;
import org.logart.VersionedRefCounter;

public interface NodeManager {
    BTreeNode allocateNode();

    BTreeNode allocateLeafNode();

    BTreeNode readNode(long nodeId);

    void writeNode(long nodeId, BTreeNode node);

    void freeNode(long nodeId, long version);

    boolean advanceVersion(Versioned<BTreeNode> currentVersionedRoot, BTreeNode newRoot);

    Versioned<BTreeNode> lockVersion();

    void releaseVersion(Versioned<BTreeNode> versionedRoot);

    void close();

    VersionedRefCounter<BTreeNode> refCounter();
}

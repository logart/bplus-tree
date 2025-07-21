package org.logart.tree;

import org.logart.node.BTreeNode;

import java.util.Set;

public record PutResult(
        BTreeNode nodeCopy,
        Set<Long> oldNodes
) {
}

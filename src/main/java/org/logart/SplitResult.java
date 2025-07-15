package org.logart;

import org.logart.node.BTreeNode;

import java.util.Set;

public record SplitResult(
        BTreeNode nodeCopy,
        byte[] promotedKey,
        BTreeNode left,
        BTreeNode right,
        Set<Long> oldNodes
) {
}

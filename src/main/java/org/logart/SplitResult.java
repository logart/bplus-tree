package org.logart;

import org.logart.node.BTreeNode;

public record SplitResult(
        BTreeNode nodeCopy,
        byte[] promotedKey,
        BTreeNode left,
        BTreeNode right
) {
}

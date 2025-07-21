package org.logart.tree;

import org.logart.node.BTreeNode;

import java.util.Set;

public record SplitResult(
        byte[] promotedKey,
        BTreeNode left,
        BTreeNode right
) {
}

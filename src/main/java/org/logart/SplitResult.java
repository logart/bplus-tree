package org.logart;

import org.logart.node.BTreeNode;

public class SplitResult {
    byte[] promotedKey;
    BTreeNode left;
    BTreeNode right;

    public SplitResult(byte[] promotedKey, BTreeNode left, BTreeNode right) {
        this.promotedKey = promotedKey;
        this.left = left;
        this.right = right;
    }
}

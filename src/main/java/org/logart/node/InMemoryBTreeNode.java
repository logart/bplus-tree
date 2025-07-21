package org.logart.node;

import java.util.Arrays;
import java.util.Comparator;

public class InMemoryBTreeNode implements BTreeNode {
    private static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;
    // keep page size small for memory testing,
    // this should produce more splits that are good for testing
    private static final int PAGE_SIZE = 3;

    private final long id;
    private byte[][] keys;
    private byte[][] values;
    private long[] children;

    private int numKeys = 0;

    private final boolean leaf;

    public InMemoryBTreeNode(long id, boolean leaf) {
        this.id = id;
        this.keys = new byte[PAGE_SIZE][];
        this.values = new byte[PAGE_SIZE][];
        this.children = new long[PAGE_SIZE + 1]; // children should have +1 because the first key should have left children reference
        Arrays.fill(this.children, -1);
        this.leaf = leaf;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        int idx = searchKeyIdx(key);
        if (COMPARATOR.compare(keys[idx], key) == 0) {
            values[idx] = value;
            return; // key already exists, update value
        }
        if (numKeys > idx) {
            System.arraycopy(keys, idx, keys, idx + 1, numKeys - idx);
            System.arraycopy(values, idx, values, idx + 1, numKeys - idx);
        }
        keys[idx] = key;
        values[idx] = value;
        numKeys++;
    }

    @Override
    public byte[] get(byte[] key) {
        // todo binary search for key
        // does not matter much for in-memory implementation
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null && java.util.Arrays.equals(keys[i], key)) {
                return values[i];
            }
        }
        return null;
    }

    @Override
    public byte[][] get(int idx) {
        return keys[idx] != null ? new byte[][]{keys[idx], values[idx]} : null;
    }

    @Override
    public boolean isAlmostFull(long capacity) {
        // we can cache this value, but again for speed and simplicity it does not matter for in-memory implementation
        return numKeys == PAGE_SIZE - 1;
    }

    @Override
    public void copyChildren(BTreeNode node, int startIdx, int endIdx) {
        if (node.isLeaf()) {
            throw new UnsupportedOperationException("Cannot copy children from a leaf node.");
        }
        this.keys = new byte[PAGE_SIZE][];
        this.children = new long[PAGE_SIZE + 1];
        Arrays.fill(this.children, -1);
        this.numKeys = endIdx - startIdx;

        System.arraycopy(((InMemoryBTreeNode) node).keys, startIdx, keys, 0, endIdx - startIdx);
        System.arraycopy(((InMemoryBTreeNode) node).children, startIdx, children, 0, endIdx - startIdx + 1);
    }

    @Override
    public void addChildren(byte[] key, long leftPageId, long rightPageId) {
        int idx = searchKeyIdx(key);
        System.arraycopy(keys, idx, keys, idx + 1, numKeys - idx);
        System.arraycopy(children, idx, children, idx + 1, numKeys - idx + 1);
        keys[idx] = key;
        children[idx] = leftPageId;
        children[idx + 1] = rightPageId;
        numKeys++;
    }

    @Override
    public void replaceChild(long childId, long newId) {
        for (int i = 0; i < children.length; i++) {
            if (children[i] == childId) {
                children[i] = newId;
                return;
            }
        }
    }

    @Override
    public long findChild(byte[] key) {
        if (isLeaf()) {
            throw new UnsupportedOperationException("Leaf nodes do not have children.");
        }
        int idx = searchKeyIdx(key);
        if (idx < numKeys && COMPARATOR.compare(keys[idx], key) == 0) {
            return children[idx + 1];
        }
        return children[idx];
    }

    protected int searchKeyIdx(byte[] key) {
        // todo replace with binary search
        int idx = 0;
        while (idx < numKeys && keys[idx] != null && COMPARATOR.compare(keys[idx], key) < 0) {
            idx++;
        }
        return idx;
    }

    @Override
    public int numKeys() {
        return numKeys;
    }

    @Override
    public boolean isLeaf() {
        return leaf;
    }

    @Override
    public void copy(BTreeNode node) {
        this.numKeys = node.numKeys();
        this.keys = Arrays.copyOf(((InMemoryBTreeNode) node).keys, PAGE_SIZE);
        this.values = Arrays.copyOf(((InMemoryBTreeNode) node).values, PAGE_SIZE);
        this.children = Arrays.copyOf(((InMemoryBTreeNode) node).children, PAGE_SIZE + 1);
    }

    @Override
    public long[] childrenDebugTODOREMOVE() {
        return children;
    }

    @Override
    public String toString() {
        return "InMemoryBTreeNode{" +
                "id=" + id +
                ", l=" + leaf +
                ", data=" + dataToString() +
                ", children=" + Arrays.toString(children) +
                '}';
    }

    private String dataToString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < keys.length; i++) {
            result.append("{key[").append(i).append("]=").append(keys[i] == null ? null : new String(keys[i]));
        }
        return result.toString();
    }
}

package org.logart.page.memory;

import org.logart.page.Page;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class InMemoryPage implements Page {
    // keep page size small for memory testing,
    // this should produce more splits that are good for testing
    private static final int PAGE_SIZE = 3;
    private static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;

    private final long id;
    private final boolean leaf;
    private byte[][] keys;
    private byte[][] values;
    private long[] children;

    private int numKeys = 0;

    public InMemoryPage(long id, boolean leaf) {
        this.id = id;
        this.leaf = leaf;
        this.keys = new byte[PAGE_SIZE][];
        this.values = new byte[PAGE_SIZE][];
        this.children = new long[PAGE_SIZE + 1]; // children should have +1 because the first key should have left children reference
        Arrays.fill(this.children, -1);
    }

    @Override
    public long pageId() {
        return id;
    }

    @Override
    public int getEntryCount() {
        return numKeys;
    }

    @Override
    public boolean put(byte[] key, byte[] value) {
        int idx = searchKeyIdx(key);
        if (COMPARATOR.compare(keys[idx], key) == 0) {
            values[idx] = value;
            return false; // key already exists, update value
        }
        if (numKeys > idx) {
            System.arraycopy(keys, idx, keys, idx + 1, numKeys - idx);
            System.arraycopy(values, idx, values, idx + 1, numKeys - idx);
        }
        keys[idx] = key;
        values[idx] = value;
        numKeys++;
        return true;
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
    public boolean isLeaf() {
        return leaf;
    }

    @Override
    public byte[][] getEntry(byte[] key) {
        return getEntry(searchKeyIdx(key));
    }

    @Override
    public byte[][] getEntry(int idx) {
        return (idx < numKeys) ? new byte[][]{keys[idx], values[idx]} : null;
    }

    @Override
    public boolean isAlmostFull(long capacity) {
        return numKeys == PAGE_SIZE - 1;
    }

    @Override
    public long getChild(byte[] key) {
        int idx = searchKeyIdx(key);
        if (idx < numKeys && COMPARATOR.compare(keys[idx], key) == 0) {
            return children[idx + 1];
        }
        return children[idx];
    }

    @Override
    public boolean addChild(byte[] key, long leftPageId, long rightPageId) {
        int idx = searchKeyIdx(key);
        System.arraycopy(keys, idx, keys, idx + 1, numKeys - idx);
        System.arraycopy(children, idx, children, idx + 1, numKeys - idx + 1);
        keys[idx] = key;
        children[idx] = leftPageId;
        children[idx + 1] = rightPageId;
        numKeys++;
        return true;
    }

    @Override
    public ByteBuffer buffer() {
        throw new UnsupportedOperationException("InMemoryPage does not support buffer operation. Use getEntry or put instead.");
    }

    @Override
    public void copy(Page page) {
        if (!(page instanceof InMemoryPage memPage)) {
            throw new IllegalArgumentException("Can only copy from InMemoryPage.");
        }
        this.numKeys = memPage.numKeys;
        this.keys = Arrays.copyOf(memPage.keys, PAGE_SIZE);
        this.values = Arrays.copyOf(memPage.values, PAGE_SIZE);
        this.children = Arrays.copyOf(memPage.children, PAGE_SIZE + 1);
    }

    @Override
    public void copyChildren(Page page, int startIdx, int endIdx) {
        if (!(page instanceof InMemoryPage memPage)) {
            throw new IllegalArgumentException("Can only copy from InMemoryPage.");
        }
        this.keys = new byte[PAGE_SIZE][];
        this.children = new long[PAGE_SIZE + 1];
        Arrays.fill(this.children, -1);
        this.numKeys = endIdx - startIdx;

        System.arraycopy(memPage.keys, startIdx, keys, 0, endIdx - startIdx);
        System.arraycopy(memPage.children, startIdx, children, 0, endIdx - startIdx + 1);
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
    public long[] childrenDbugTODOREMOVE() {
        return children;
    }

    protected int searchKeyIdx(byte[] key) {
        // todo replace with binary search
        int idx = 0;
        while (idx < numKeys && keys[idx] != null && COMPARATOR.compare(keys[idx], key) < 0) {
            idx++;
        }
        return idx;
    }
}

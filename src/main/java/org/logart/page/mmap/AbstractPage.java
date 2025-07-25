package org.logart.page.mmap;

import org.logart.page.Page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;

import static org.logart.page.mmap.PageFactory.IS_DELETED;
import static org.logart.page.mmap.PageFactory.LEAF_FLAG;

public abstract class AbstractPage implements Page {
    public static final int PAGE_SIZE = 4096;
    protected static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;

    protected static final int HEADER_SIZE = 32;
    public static final int FREE_SPACE_OFFSET = 11;
    protected static final int PAGE_ID_OFFSET = 1;
    protected static final int ENTRY_COUNT_OFFSET = 9;      // after page type + page id
    protected static final int SLOT_KEY_SIZE = 2;
    protected static final int SLOT_CHILD_POINTER = 8;
    protected static final int SLOT_SIZE = SLOT_KEY_SIZE + SLOT_CHILD_POINTER;               // each slot is a 2-byte pointer to payload

    public static final int FULL_FLAG = 0b0100_0000;

    private final ByteBuffer buffer;

    public AbstractPage(ByteBuffer buffer) {
        this.buffer = buffer.order(ByteOrder.BIG_ENDIAN);
    }

    @Override
    public int getEntryCount() {
        return Short.toUnsignedInt(buffer.getShort(ENTRY_COUNT_OFFSET));
    }

    protected void setEntryCount(int count) {
        buffer.putShort(ENTRY_COUNT_OFFSET, (short) count);
    }

    protected int getFreeSpaceOffset() {
        return Short.toUnsignedInt(buffer.getShort(FREE_SPACE_OFFSET));
    }

    protected void setFreeSpaceOffset(int offset) {
        assert offset >= HEADER_SIZE : "Free space offset must be greater than header size";
        buffer.putShort(FREE_SPACE_OFFSET, (short) offset);
    }

    @Override
    public byte[][] getEntry(byte[] key) {
        PageLoc pageLoc = searchKeyIdx(key);
        if (pageLoc.idx() == -1 || pageLoc.k() == null) {
            return null;
        }
        return new byte[][]{pageLoc.k(), pageLoc.v()};
    }

    protected PageLoc searchKeyIdx(byte[] key) {
        int l = 0;
        int r = getEntryCount();
        int idx = -1;
        int compare = 0;
        while (l < r) {
            idx = (l + r) / 2;
            byte[][] entry = getEntry(idx);
            compare = COMPARATOR.compare(key, entry[0]);
            if (compare < 0) {
                r = idx;
            } else if (compare > 0) {
                l = idx + 1;
            } else {
                return new PageLoc(idx, entry[0], entry[1], compare); // found
            }
        }
        if (compare > 0) {
            idx++;
        }
        return new PageLoc(idx, null, null, compare);
    }

    @Override
    public boolean isLeaf() {
        byte pageMeta = buffer.get(0);
        // we compare with 128 instead of 1 because 0b1000_0000 does not fit in a byte
        // and the whole expression is converted to an int by java
        return (pageMeta & LEAF_FLAG) == LEAF_FLAG;
    }

    @Override
    public boolean isDeleted() {
        byte pageMeta = buffer.get(0);
        return (pageMeta & IS_DELETED) == IS_DELETED;
    }

    @Override
    public void markDeleted() {
        byte pageMeta = buffer.get(0);
        buffer.put(0, (byte) (pageMeta | IS_DELETED));
    }

    public long pageId() {
        return buffer.getLong(PAGE_ID_OFFSET);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    @Override
    public void copy(Page page) {
        long currentId = pageId();
        page.buffer().rewind();
        buffer.rewind();
        buffer.put(page.buffer());
        buffer.putLong(PAGE_ID_OFFSET, currentId); // Ensure the page ID remains the same
    }

    @Override
    public void copyChildren(Page page, int startIdx, int endIdx) {
        throw new UnsupportedOperationException("TODO implement");
    }

    @Override
    public void replaceChild(long childId, long newId) {
        for (int i = 0; i < getEntryCount(); i++) {
            long child = getChild(i);
            if (child == childId) {
                buffer.putLong(HEADER_SIZE + SLOT_CHILD_POINTER + (SLOT_SIZE * i) + SLOT_KEY_SIZE, newId);
                return;
            }
        }
    }

    @Override
    public long[] childrenDbugTODOREMOVE() {
        return new long[0];
    }

    public long getChild(byte[] key) {
        PageLoc pageLoc = searchKeyIdx(key);
        if (pageLoc.idx() < getEntryCount() && pageLoc.cmp() >= 0) {
            return getChild(pageLoc.idx() + 1);
        }
        return getChild(pageLoc.idx());
    }

    private long getChild(int idx) {
        return buffer.getLong(HEADER_SIZE + SLOT_CHILD_POINTER + (SLOT_SIZE * (idx - 1)) + SLOT_KEY_SIZE);
    }

    @Override
    public boolean addChild(byte[] key, long left, long right) {
        int entryCount = getEntryCount();
        PageLoc pageLoc = searchKeyIdx(key);

        int slotOffset = HEADER_SIZE + SLOT_CHILD_POINTER + SLOT_SIZE * entryCount;

        int freeSpaceOffset = getFreeSpaceOffset();
        int payloadSize = 2 + key.length;

        if (slotOffset > freeSpaceOffset - payloadSize) {
            // write info about page is full
            byte pageMeta = buffer.get(0);
            pageMeta = (byte) (pageMeta | FULL_FLAG);
            buffer.put(0, pageMeta);
            return false; // Not enough space
        }

        // Write key-value to payload area
        int dataStart = freeSpaceOffset - payloadSize;
        int kvOffset = dataStart;
        buffer.putShort(kvOffset, (short) key.length);
        kvOffset += 2;
        buffer.put(kvOffset, key);

        // Write slot
        int idx = pageLoc.idx();
        if (idx >= 0 && idx < entryCount) {
            // move bigger entry to the right
            int start = HEADER_SIZE + SLOT_SIZE * idx;
            int end = slotOffset;
            byte[] tmp = new byte[end - start];
            // leave two bytes for the new entry
            buffer.get(start, tmp);

            buffer.putLong(start - SLOT_CHILD_POINTER, left);
            buffer.putShort(start, (short) dataStart);
            buffer.putLong(start + SLOT_KEY_SIZE, right);
            buffer.put(start + SLOT_SIZE, tmp);
        } else {
            buffer.putLong(slotOffset - SLOT_CHILD_POINTER, left);
            buffer.putShort(slotOffset, (short) dataStart);
            buffer.putLong(slotOffset + SLOT_KEY_SIZE, right);
        }

        // Update header
        setFreeSpaceOffset(freeSpaceOffset - payloadSize);
        setEntryCount(entryCount + 1);

        return true;
    }

    public static long bytesToLong(byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalArgumentException("Byte array must be 8 bytes long");
        }

        return ((long) (bytes[0] & 0xFF) << 56) |
                ((long) (bytes[1] & 0xFF) << 48) |
                ((long) (bytes[2] & 0xFF) << 40) |
                ((long) (bytes[3] & 0xFF) << 32) |
                ((long) (bytes[4] & 0xFF) << 24) |
                ((long) (bytes[5] & 0xFF) << 16) |
                ((long) (bytes[6] & 0xFF) << 8) |
                ((long) (bytes[7] & 0xFF));
    }
}

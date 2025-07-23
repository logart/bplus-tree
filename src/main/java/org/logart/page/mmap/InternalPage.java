package org.logart.page.mmap;

import org.logart.page.Page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;

import static org.logart.page.mmap.PageFactory.IS_DELETED;
import static org.logart.page.mmap.PageFactory.LEAF_FLAG;

public class InternalPage implements Page {
    public static final int PAGE_SIZE = 4096;
    private static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;

    // Header offsets
    public static final int FREE_SPACE_OFFSET = 11;
    private static final int PAGE_ID_OFFSET = 1;
    private static final int HEADER_SIZE = 32;
    private static final int ENTRY_COUNT_OFFSET = 9;      // after page type + page id
    private static final int SLOT_KEY_SIZE = 2;
    private static final int SLOT_CHILD_POINTER = 8;
    private static final int SLOT_SIZE = SLOT_KEY_SIZE + SLOT_CHILD_POINTER;               // each slot is a 2-byte pointer to payload
    public static final int FULL_FLAG = 0b0100_0000;

    private final ByteBuffer buffer;

    public InternalPage(ByteBuffer buffer) {
        this.buffer = buffer.order(ByteOrder.BIG_ENDIAN);
    }

    public static Page newPage(long pageId, ByteBuffer buf) {
        /**
         * page format:
         * Page metadata:       1 byte
         *      Page Type	    1 bit	Leaf or internal
         *      Full flag	    1 bit	Indicates if the page is full
         *      Is deleted 	    1 bit	Indicates if the page is deleted
         *      Padding 	    5 bits	Reserved for future use
         * Page ID	            8 bytes	This page's ID
         * Number of entries	2 bytes	Slot count
         * Free space offset	2 bytes	Start of free space
         * Right sibling ptr	8 bytes	Only for leaf pages
         * Parent ptr / unused	8 bytes	Optional
         * Reserved	~5 bytes	Padding
         *
         * Left child pointer:  8 bytes	Only for internal pages
         * Slot table:          2 bytes per entry + 8 bytes per child pointer
         * Free space:          variable size
         * Payload:             variable size
         */
        buf.put(0, (byte) 0); // First bit = Leaf/Internal
        buf.putLong(PAGE_ID_OFFSET, pageId);
        buf.putShort(ENTRY_COUNT_OFFSET, (short) 0);
        buf.putShort(FREE_SPACE_OFFSET, (short) PAGE_SIZE);
        return new InternalPage(buf);
    }

    public static Page readPage(ByteBuffer buffer) {
        return new InternalPage(buffer);
    }

    public int getEntryCount() {
        return Short.toUnsignedInt(buffer.getShort(ENTRY_COUNT_OFFSET));
    }

    private void setEntryCount(int count) {
        buffer.putShort(ENTRY_COUNT_OFFSET, (short) count);
    }

    private int getFreeSpaceOffset() {
        return Short.toUnsignedInt(buffer.getShort(FREE_SPACE_OFFSET));
    }

    private void setFreeSpaceOffset(int offset) {
        assert offset >= HEADER_SIZE : "Free space offset must be greater than header size";
        buffer.putShort(FREE_SPACE_OFFSET, (short) offset);
    }

    @Override
    public boolean put(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("InternalPage does not support put operation directly. Use addChild instead.");
    }

    @Override
    public byte[] get(byte[] key) {
        throw new UnsupportedOperationException("InternalPage does not support put operation directly. Use getChild instead.");
    }

    public byte[][] getEntry(byte[] key) {
        InternalPageLoc pageLoc = searchKeyIdx(key);
        if (pageLoc.idx() == -1 || pageLoc.k() == null) {
            return null;
        }
        return new byte[][]{pageLoc.k(), null};
    }

    private InternalPageLoc searchKeyIdx(byte[] key) {
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
                return new InternalPageLoc(idx, entry[0], compare); // found
            }
        }
        return new InternalPageLoc(idx, null, compare);
    }

    public byte[][] getEntry(int index) {
        int entryCount = getEntryCount();
        if (index >= entryCount) return null;

        int slotOffset = HEADER_SIZE + SLOT_CHILD_POINTER + SLOT_SIZE * index;
        int kvOffset = Short.toUnsignedInt(buffer.getShort(slotOffset));

        int keyLen = Short.toUnsignedInt(buffer.getShort(kvOffset));
        kvOffset += 2;
        byte[] key = new byte[keyLen];
        buffer.get(kvOffset, key);

        return new byte[][]{key};
    }

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

    @Override
    public boolean isAlmostFull(long capacity) {
        byte pageMeta = buffer.get(0);
        return (pageMeta & FULL_FLAG) == FULL_FLAG
                || getFreeSpaceOffset() < HEADER_SIZE + SLOT_CHILD_POINTER + SLOT_SIZE * getEntryCount() + 2 + capacity;
    }

    public long pageId() {
        return buffer.getLong(PAGE_ID_OFFSET);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    @Override
    public void copy(Page page) {
        buffer.rewind();
        buffer.put(page.buffer());
    }

    @Override
    public void copyChildren(Page page, int startIdx, int endIdx) {

    }

    @Override
    public void replaceChild(long childId, long newId) {

    }

    @Override
    public long[] childrenDbugTODOREMOVE() {
        return new long[0];
    }

    public long getChild(byte[] key) {
        InternalPageLoc pageLoc = searchKeyIdx(key);
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
        InternalPageLoc pageLoc = searchKeyIdx(key);

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

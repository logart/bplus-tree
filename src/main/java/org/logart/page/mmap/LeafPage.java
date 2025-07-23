package org.logart.page.mmap;


import org.logart.page.Page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;

import static org.logart.page.mmap.InternalPage.FULL_FLAG;
import static org.logart.page.mmap.PageFactory.IS_DELETED;
import static org.logart.page.mmap.PageFactory.LEAF_FLAG;

public class LeafPage implements Page {
    public static final int PAGE_SIZE = 4096;
    private static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;

    // Header offsets
    public static final int FREE_SPACE_OFFSET = 11;
    private static final int PAGE_ID_OFFSET = 1;
    private static final int HEADER_SIZE = 32;
    private static final int ENTRY_COUNT_OFFSET = 9;      // after page type + page id
    private static final int SLOT_SIZE = 2;               // each slot is a 2-byte pointer to payload

    private final ByteBuffer buffer;

    public LeafPage(ByteBuffer buffer) {
        this.buffer = buffer.order(ByteOrder.BIG_ENDIAN);
    }

    public static Page newPage(long pageId, ByteBuffer buf) {
        /**
         * page format:
         * Page metadata:       1 byte
         *      Page Type	    1 bit	Leaf or internal
         *      Padding 	    7 bits	Reserved for future use
         * Page ID	            8 bytes	This page's ID
         * Number of entries	2 bytes	Slot count
         * Free space offset	2 bytes	Start of free space
         * Right sibling ptr	8 bytes	Only for leaf pages
         * Parent ptr / unused	8 bytes	Optional
         * Reserved	~5 bytes	Padding
         *
         * Slot table:          2 bytes per entry
         * Free space:          variable size
         * Payload:             variable size
         */
        buf.put(0, (byte) 0b1000_0000); // First bit = Leaf/Internal
        buf.putLong(PAGE_ID_OFFSET, pageId);
        buf.putShort(ENTRY_COUNT_OFFSET, (short) 0);
        buf.putShort(FREE_SPACE_OFFSET, (short) PAGE_SIZE);
        return new LeafPage(buf);
    }

    public static Page readPage(ByteBuffer buffer) {
        return new LeafPage(buffer);
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

    public boolean put(byte[] key, byte[] value) {
        int entryCount = getEntryCount();
        LeafPageLoc pageLoc = searchKeyIdx(key);
        int slotOffset = SLOT_SIZE * entryCount + HEADER_SIZE;

        int freeSpaceOffset = getFreeSpaceOffset();
        int payloadSize = 2 + key.length + 2 + value.length;

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
        kvOffset += key.length;
        buffer.putShort(kvOffset, (short) value.length);
        kvOffset += 2;
        buffer.put(kvOffset, value);

        // Write slot
        setFreeSpaceOffset(freeSpaceOffset - payloadSize);

        int idx = pageLoc.idx();
        if (pageLoc.k() != null && COMPARATOR.compare(pageLoc.k(), key) == 0) {
            // Key already exists, update value
            buffer.putShort(HEADER_SIZE + SLOT_SIZE * idx, (short) dataStart);
            return true;
        }
        if (idx >= 0 && idx < entryCount) {
            // move bigger entry to the right
            int start = HEADER_SIZE + SLOT_SIZE * idx;
            int end = slotOffset;
            byte[] tmp = new byte[end - start];
            // leave two bytes for the new entry
            buffer.get(start, tmp);

            buffer.putShort(start, (short) dataStart);
            buffer.put(start + SLOT_SIZE, tmp);
        } else {
            buffer.putShort(slotOffset, (short) dataStart);
        }

        // Update header
        setEntryCount(entryCount + 1);

        return true;
    }

    @Override
    public byte[] get(byte[] key) {
        byte[][] entry = getEntry(key);
        return entry != null
                ? entry[1]
                : null;
    }

    @Override
    public byte[][] getEntry(byte[] key) {
        LeafPageLoc pageLoc = searchKeyIdx(key);
        if (pageLoc.idx() == -1 || pageLoc.k() == null) {
            return null;
        }
        return new byte[][]{pageLoc.k(), pageLoc.v()};
    }

    private LeafPageLoc searchKeyIdx(byte[] key) {
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
                return new LeafPageLoc(idx, entry[0], entry[1]); // found
            }
        }
        if (compare > 0) {
            idx++;
        }
        return new LeafPageLoc(idx, null, null);
    }

    public byte[][] getEntry(int index) {
        int entryCount = getEntryCount();
        if (index >= entryCount) return null;

        int slotOffset = HEADER_SIZE + SLOT_SIZE * index;
        int kvOffset = Short.toUnsignedInt(buffer.getShort(slotOffset));

        int keyLen = Short.toUnsignedInt(buffer.getShort(kvOffset));
        kvOffset += 2;
        byte[] key = new byte[keyLen];
        buffer.get(kvOffset, key);
        kvOffset += keyLen;

        int valueLen = Short.toUnsignedInt(buffer.getShort(kvOffset));
        kvOffset += 2;
        byte[] value = new byte[valueLen];
        buffer.get(kvOffset, value);

        return new byte[][]{key, value};
    }

    public boolean isLeaf() {
        byte pageMeta = buffer.get(0);
        // we compare with 128 instead of 1 because 0b1000_0000 does not fit in a byte
        // and the whole expression is converted to an int by java
        return (pageMeta & LEAF_FLAG) == LEAF_FLAG;
    }

    @Override
    public boolean isAlmostFull(long capacity) {
        byte pageMeta = buffer.get(0);
        return (pageMeta & FULL_FLAG) == FULL_FLAG
                || (getFreeSpaceOffset() - HEADER_SIZE) < capacity; // Check if free space is less than capacity
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
    public long getChild(byte[] key) {
        throw new UnsupportedOperationException("Leaf pages do not have children.");
    }

    @Override
    public boolean addChild(byte[] key, long left, long right) {
        throw new UnsupportedOperationException("Leaf pages do not have children.");
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
        buffer.rewind();
        page.buffer().rewind();
        buffer.put(page.buffer());
        buffer.putLong(PAGE_ID_OFFSET, currentId); // Ensure the page ID remains the same
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
}

package org.logart;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Page {
    public static final int PAGE_SIZE = 4096;

    // Header offsets
    public static final int LEAF_FLAG = 0b1000_0000;
    public static final int FREE_SPACE_OFFSET = 11;
    private static final int PAGE_ID_OFFSET = 1;
    private static final int HEADER_SIZE = 32;
    private static final int ENTRY_COUNT_OFFSET = 9;      // after page type + page id
    private static final int SLOT_SIZE = 2;               // each slot is a 2-byte pointer to payload
    private static final int FULL_FLAG = 0b0100_0000;

    private final ByteBuffer buffer;

    public Page(ByteBuffer buffer) {
        this.buffer = buffer.order(ByteOrder.BIG_ENDIAN);
    }

    public static Page newPage(long pageId, boolean isLeaf, ByteBuffer buf) {
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
         */
        buf.put(0, (byte) (isLeaf ? 0b1000_0000 : 0)); // First bit = Leaf/Internal
        buf.putLong(PAGE_ID_OFFSET, pageId);
        buf.putShort(ENTRY_COUNT_OFFSET, (short) 0);
        buf.putShort(FREE_SPACE_OFFSET, (short) HEADER_SIZE);
        return new Page(buf);
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
        int slotOffset = PAGE_SIZE - SLOT_SIZE * (entryCount + 1);
        int freeSpaceOffset = getFreeSpaceOffset();

        int payloadSize = 2 + key.length + 2 + value.length;

        if (slotOffset < freeSpaceOffset + payloadSize) {
            // write info about page is full
            byte pageMeta = buffer.get(0);
            pageMeta = (byte) (pageMeta | FULL_FLAG);
            buffer.put(0, pageMeta);
            return false; // Not enough space
        }

        // Write key-value to payload area
        int kvOffset = freeSpaceOffset;
        buffer.putShort(kvOffset, (short) key.length);
        kvOffset += 2;
        buffer.put(kvOffset, key);
        kvOffset += key.length;
        buffer.putShort(kvOffset, (short) value.length);
        kvOffset += 2;
        buffer.put(kvOffset, value);

        // Write slot
        buffer.putShort(slotOffset, (short) freeSpaceOffset);

        // Update header
        setFreeSpaceOffset(freeSpaceOffset + payloadSize);
        setEntryCount(entryCount + 1);

        return true;
    }

    public byte[][] getEntry(int index) {
        int entryCount = getEntryCount();
        if (index >= entryCount) return null;

        int slotOffset = PAGE_SIZE - SLOT_SIZE * (index + 1);
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

    public byte[][] loadKeys() {
        int entryCount = getEntryCount();

        // todo THIS IS SUPER INEFFICIENT, FIX IT BEFORE SEND FOR REVIEW
        byte[][] result = new byte[entryCount][];
        for (int i = 0; i < entryCount; i++) {
            result[i] = getEntry(i)[0];
        }
        return result;
    }

    public boolean isLeaf() {
        byte pageMeta = buffer.get(0);
        // we compare with 128 instead of 1 because 0b1000_0000 does not fit in a byte
        // and the whole expression is converted to an int by java
        return (pageMeta & LEAF_FLAG) == LEAF_FLAG;
    }

    public boolean isFull() {
        byte pageMeta = buffer.get(0);
        return (pageMeta & FULL_FLAG) == FULL_FLAG;
    }

    public long pageId() {
        return buffer.getLong(PAGE_ID_OFFSET);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

//    public void force() {
//        buffer.force();
//    }
}

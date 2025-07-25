package org.logart.page.mmap;


import org.logart.page.Page;

import java.nio.ByteBuffer;

public class LeafPage extends AbstractPage implements Page {
    public LeafPage(ByteBuffer buffer) {
        super(buffer);
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

    public boolean put(byte[] key, byte[] value) {
        int entryCount = getEntryCount();
        PageLoc pageLoc = searchKeyIdx(key);
        int slotOffset = SLOT_SIZE * entryCount + HEADER_SIZE;

        int freeSpaceOffset = getFreeSpaceOffset();
        int payloadSize = 2 + key.length + 2 + value.length;

        if (slotOffset > freeSpaceOffset - payloadSize) {
            // write info about page is full
            byte pageMeta = buffer().get(0);
            pageMeta = (byte) (pageMeta | FULL_FLAG);
            buffer().put(0, pageMeta);
            return false; // Not enough space
        }

        // Write key-value to payload area
        int dataStart = freeSpaceOffset - payloadSize;
        int kvOffset = dataStart;
        buffer().putShort(kvOffset, (short) key.length);
        kvOffset += 2;
        buffer().put(kvOffset, key);
        kvOffset += key.length;
        buffer().putShort(kvOffset, (short) value.length);
        kvOffset += 2;
        buffer().put(kvOffset, value);

        // Write slot
        setFreeSpaceOffset(freeSpaceOffset - payloadSize);

        int idx = pageLoc.idx();
        if (pageLoc.k() != null && COMPARATOR.compare(pageLoc.k(), key) == 0) {
            // Key already exists, update value
            buffer().putShort(HEADER_SIZE + SLOT_SIZE * idx, (short) dataStart);
            return true;
        }
        if (idx >= 0 && idx < entryCount) {
            // move bigger entry to the right
            int start = HEADER_SIZE + SLOT_SIZE * idx;
            int end = slotOffset;
            byte[] tmp = new byte[end - start];
            // leave two bytes for the new entry
            buffer().get(start, tmp);

            buffer().putShort(start, (short) dataStart);
            buffer().put(start + SLOT_SIZE, tmp);
        } else {
            buffer().putShort(slotOffset, (short) dataStart);
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

    public byte[][] getEntry(int index) {
        int entryCount = getEntryCount();
        if (index >= entryCount) return null;

        int slotOffset = HEADER_SIZE + SLOT_SIZE * index;
        int kvOffset = Short.toUnsignedInt(buffer().getShort(slotOffset));

        int keyLen = Short.toUnsignedInt(buffer().getShort(kvOffset));
        kvOffset += 2;
        byte[] key = new byte[keyLen];
        buffer().get(kvOffset, key);
        kvOffset += keyLen;

        int valueLen = Short.toUnsignedInt(buffer().getShort(kvOffset));
        kvOffset += 2;
        byte[] value = new byte[valueLen];
        buffer().get(kvOffset, value);

        return new byte[][]{key, value};
    }

    @Override
    public boolean isAlmostFull(long capacity) {
        byte pageMeta = buffer().get(0);
        return (pageMeta & FULL_FLAG) == FULL_FLAG
                || (getFreeSpaceOffset() - HEADER_SIZE) < capacity; // Check if free space is less than capacity
    }

    @Override
    public long getChild(byte[] key) {
        throw new UnsupportedOperationException("Leaf pages do not have children.");
    }

    @Override
    public boolean addChild(byte[] key, long left, long right) {
        throw new UnsupportedOperationException("Leaf pages do not have children.");
    }

    @Override
    public void copyChildren(Page page, int startIdx, int endIdx) {
        throw new UnsupportedOperationException("Leaf pages do not have children.");
    }

    @Override
    public void replaceChild(long childId, long newId) {
        throw new UnsupportedOperationException("Leaf pages do not have children.");
    }

    @Override
    public long[] childrenDbugTODOREMOVE() {
        throw new UnsupportedOperationException("Leaf pages do not have children.");
    }
}

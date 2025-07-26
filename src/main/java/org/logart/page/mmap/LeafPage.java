package org.logart.page.mmap;


import org.logart.page.Page;

import java.nio.ByteBuffer;

public class LeafPage extends AbstractPage implements Page {
    protected static final int SLOT_SIZE = 2;               // each slot is a 2-byte pointer to payload
    public static final int PAYLOAD_SIZE_FIELD_SIZE = 2;

    public LeafPage(ByteBuffer buffer) {
        super(buffer);
    }

    public static Page newPage(long pageId, ByteBuffer buf) {
        /**
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
        int slotOffset = SLOT_SIZE * entryCount + HEADER_SIZE;

        int freeSpaceOffset = getFreeSpaceOffset();
        int payloadSize = key.length + value.length;
        int payloadSizeWithMeta = payloadSize + PAYLOAD_SIZE_FIELD_SIZE * 2; // two size fields for key and value

        // we need to reserve space for slot offset too
        if (isFull() || availableSpace() < payloadSize + internalOverhead()) {
            // write info about page is full
            byte pageMeta = buffer().get(0);
            pageMeta = (byte) (pageMeta | FULL_FLAG);
            buffer().put(0, pageMeta);
            return false; // Not enough space
        }

        // Write key-value to payload area
        int dataStart = freeSpaceOffset - payloadSizeWithMeta;
        int kvOffset = dataStart;
        buffer().putShort(kvOffset, (short) key.length);
        kvOffset += 2;
        buffer().put(kvOffset, key);
        kvOffset += key.length;
        buffer().putShort(kvOffset, (short) value.length);
        kvOffset += 2;
        buffer().put(kvOffset, value);

        // Write slot
        setFreeSpaceOffset(freeSpaceOffset - payloadSizeWithMeta);

        PageLoc pageLoc = searchKeyIdx(key);
        int idx = pageLoc.idx();
        if (pageLoc.k() != null && pageLoc.cmp() == 0) {
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
    protected short entrySize() {
        return SLOT_SIZE;
    }

    @Override
    protected short padding() {
        // no padding for leaf pages
        return 0;
    }

    @Override
    protected int internalOverhead() {
        return PAYLOAD_SIZE_FIELD_SIZE * 2 + SLOT_SIZE;// one for the key and one for the value
    }
}

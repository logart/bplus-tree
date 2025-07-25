package org.logart.page.mmap;


import org.logart.page.Page;

import java.nio.ByteBuffer;

public class LeafPage extends AbstractPage implements Page {
    protected static final int SLOT_SIZE = 2;               // each slot is a 2-byte pointer to payload
    public static final int PAYLOAD_SIZE_FIELD_SIZE = 2;

    public LeafPage(ByteBuffer buffer, boolean sanityCheckEnabled) {
        super(buffer, sanityCheckEnabled);
    }

    public static Page newPage(long pageId, ByteBuffer buf) {
        return newPage(pageId, buf, true);
    }

    public static Page newPage(long pageId, ByteBuffer buf, boolean sanityCheckEnabled) {
        /**
         * Slot table:          2 bytes per entry
         * Free space:          variable size
         * Payload:             variable size
         */
        buf.put(0, (byte) 0b1000_0000); // First bit = Leaf/Internal
        buf.putLong(PAGE_ID_OFFSET, pageId);
        buf.putShort(ENTRY_COUNT_OFFSET, (short) 0);
        buf.putShort(FREE_SPACE_OFFSET, (short) PAGE_SIZE);
        return new LeafPage(buf, sanityCheckEnabled);
    }

    public static Page readPage(ByteBuffer buffer, boolean sanityCheckEnabled) {
        return new LeafPage(buffer, sanityCheckEnabled);
    }

    public boolean put(byte[] key, byte[] value) {
        int entryCount = getEntryCount();
        int slotOffset = SLOT_SIZE * entryCount + HEADER_SIZE;

        int freeSpaceOffset = getFreeSpaceOffset();
        int payloadSize = PAYLOAD_SIZE_FIELD_SIZE + key.length + PAYLOAD_SIZE_FIELD_SIZE + value.length;

        // we need to reserve space for slot offset too
        if (isFull() || availableSpace() < payloadSize + SLOT_SIZE) {
            // write info about page is full
            byte pageMeta = buffer2().get(0);
            sanityCheck();
            pageMeta = (byte) (pageMeta | FULL_FLAG);
            buffer2().put(0, pageMeta);
            sanityCheck();
            return false; // Not enough space
        }

        // Write key-value to payload area
        int dataStart = freeSpaceOffset - payloadSize;
        int kvOffset = dataStart;
        buffer2().putShort(kvOffset, (short) key.length);
        sanityCheck();
        kvOffset += 2;
        buffer2().put(kvOffset, key);
        sanityCheck();
        kvOffset += key.length;
        buffer2().putShort(kvOffset, (short) value.length);
        sanityCheck();
        kvOffset += 2;
        buffer2().put(kvOffset, value);
        sanityCheck();

        // Write slot
        setFreeSpaceOffset(freeSpaceOffset - payloadSize);

        PageLoc pageLoc = searchKeyIdx(key);
        int idx = pageLoc.idx();
        if (pageLoc.k() != null && pageLoc.cmp() == 0) {
            // Key already exists, update value
            buffer2().putShort(HEADER_SIZE + SLOT_SIZE * idx, (short) dataStart);
            sanityCheck();
            return true;
        }
        if (idx >= 0 && idx < entryCount) {
            // move bigger entry to the right
            int start = HEADER_SIZE + SLOT_SIZE * idx;
            int end = slotOffset;
            byte[] tmp = new byte[end - start];
            // leave two bytes for the new entry
            buffer2().get(start, tmp);
            sanityCheck();

            buffer2().putShort(start, (short) dataStart);
            sanityCheck();
            buffer2().put(start + SLOT_SIZE, tmp);
            sanityCheck();
        } else {
            buffer2().putShort(slotOffset, (short) dataStart);
            sanityCheck();
        }

        // Update header
        setEntryCount(entryCount + 1);
        sanityCheck();
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
        int kvOffset = Short.toUnsignedInt(buffer2().getShort(slotOffset));
        sanityCheck();

        int keyLen = Short.toUnsignedInt(buffer2().getShort(kvOffset));
        sanityCheck();

        kvOffset += 2;
        byte[] key = new byte[keyLen];
        buffer2().get(kvOffset, key);
        sanityCheck();
        kvOffset += keyLen;

        int valueLen = Short.toUnsignedInt(buffer2().getShort(kvOffset));
        sanityCheck();
        kvOffset += 2;
        byte[] value = new byte[valueLen];
        buffer2().get(kvOffset, value);
        sanityCheck();

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
    public long[] childrenDbugTODOREMOVE() {
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
        return PAYLOAD_SIZE_FIELD_SIZE * 2;// one for the key and one for the value
    }

    @Override
    protected Exception sanityCheck() {
        try {
            int cnt = getEntryCount();
            for (int i = 0; i < cnt; i++) {
                ByteBuffer buffer = buffer(true);
                short dataStart = buffer.getShort(HEADER_SIZE + (SLOT_SIZE * i));
                short kSize = buffer.getShort(dataStart);
                byte[] key = new byte[kSize];
                buffer.get(dataStart + PAYLOAD_SIZE_FIELD_SIZE, key);
                short vSize = buffer.getShort(dataStart + PAYLOAD_SIZE_FIELD_SIZE + key.length);
                byte[] value = new byte[vSize];
                buffer.get(dataStart + PAYLOAD_SIZE_FIELD_SIZE + kSize + PAYLOAD_SIZE_FIELD_SIZE, value);
                if (!isValid(key) || !isValid(value)) {
                    throw new IllegalStateException("Invalid entry at index " + i + " in LeafPage with ID: " + pageId());
                }
            }
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    private static boolean isValid(byte[] key) {
        return new String(key).matches("[a-zA-Z0-9_\\-]+");
    }
}

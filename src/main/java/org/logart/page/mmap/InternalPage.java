package org.logart.page.mmap;

import org.logart.page.Page;

import java.nio.ByteBuffer;

public class InternalPage extends AbstractPage implements Page {
    protected static final int SLOT_CHILD_POINTER = 8;
    protected static final int SLOT_SIZE = 2 + SLOT_CHILD_POINTER; // each slot is a 2-byte pointer to payload + 8-byte child pointer
    public static final int PAYLOAD_SIZE_FIELD_SIZE = 2;

    public InternalPage(ByteBuffer buffer, boolean sanityCheckEnabled) {
        super(buffer, sanityCheckEnabled);
    }

    public static Page newPage(long pageId, ByteBuffer buf) {
        return newPage(pageId, buf, true);
    }

    public static Page newPage(long pageId, ByteBuffer buf, boolean sanityCheckEnabled) {
        /**
         * Left child pointer:  8 bytes	Only for internal pages -- not yet implemented
         * Slot table:          2 bytes per entry + 8 bytes per child pointer
         * Free space:          variable size
         * Payload:             variable size
         */
        buf.put(0, (byte) 0); // First bit = Leaf/Internal
        buf.putLong(PAGE_ID_OFFSET, pageId);
        buf.putShort(ENTRY_COUNT_OFFSET, (short) 0);
        buf.putShort(FREE_SPACE_OFFSET, (short) PAGE_SIZE);
        return new InternalPage(buf, sanityCheckEnabled);
    }

    public static Page readPage(ByteBuffer buffer, boolean sanityCheckEnabled) {
        return new InternalPage(buffer, sanityCheckEnabled);
    }

    @Override
    public boolean put(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("InternalPage does not support put operation directly. Use addChild instead.");
    }

    @Override
    public byte[] get(byte[] key) {
        throw new UnsupportedOperationException("InternalPage does not support put operation directly. Use getChild instead.");
    }

    @Override
    public byte[][] getEntry(int index) {
        int entryCount = getEntryCount();
        if (index >= entryCount) return null;

        int slotOffset = HEADER_SIZE + SLOT_CHILD_POINTER + SLOT_SIZE * index;
        int kvOffset = Short.toUnsignedInt(buffer2().getShort(slotOffset));
        sanityCheck();

        int keyLen = Short.toUnsignedInt(buffer2().getShort(kvOffset));
        sanityCheck();
        kvOffset += PAYLOAD_SIZE_FIELD_SIZE;
        byte[] key = new byte[keyLen];
        buffer2().get(kvOffset, key);
        sanityCheck();

        return new byte[][]{key, null};
    }

    @Override
    public void copyChildren(Page page, int startIdx, int endIdx) {
        // copy the entire page, so we will have all the keys and children
        this.copy(page);
        InternalPage internalPage = (InternalPage) page;
        ByteBuffer src = internalPage.buffer2();
        sanityCheck();
        int offset = HEADER_SIZE + startIdx * SLOT_SIZE;
        // add a child pointer to the end since every key have left and right, this will allow capturing a right child pointer too
        int length = (endIdx - startIdx) * SLOT_SIZE + SLOT_CHILD_POINTER;
        buffer2().put(HEADER_SIZE, src, offset, length);
        sanityCheck();
        setEntryCount(endIdx - startIdx);
    }

    @Override
    public void replaceChild(long childId, long newId) {
        // we need <= here since we have +1 child compared to keys
        for (int i = 0; i <= getEntryCount(); i++) {
            long child = getChild(i);
            if (child == childId) {
                buffer2().putLong(HEADER_SIZE + (SLOT_SIZE * i), newId);
                sanityCheck();
                return;
            }
        }
    }

    @Override
    public long[] childrenDbugTODOREMOVE() {
        throw new UnsupportedOperationException("TODO implement");
    }

    @Override
    public long getChild(byte[] key) {
        PageLoc pageLoc = searchKeyIdx(key);
        if (pageLoc.idx() < getEntryCount() && pageLoc.cmp() >= 0) {
            return getChild(pageLoc.idx() + 1);
        }
        return getChild(pageLoc.idx());
    }

    private long getChild(int idx) {
        long aLong = buffer2().getLong(HEADER_SIZE + SLOT_CHILD_POINTER + (SLOT_SIZE * (idx - 1)) + SLOT_KEY_SIZE);
        sanityCheck();
        return aLong;
    }

    @Override
    public boolean addChild(byte[] key, long left, long right) {
        int entryCount = getEntryCount();
        PageLoc pageLoc = searchKeyIdx(key);

        int slotOffset = HEADER_SIZE + SLOT_CHILD_POINTER + SLOT_SIZE * entryCount;

        int freeSpaceOffset = getFreeSpaceOffset();
        int payloadSize = PAYLOAD_SIZE_FIELD_SIZE + key.length;

        if (availableSpace() < payloadSize + SLOT_SIZE) {
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
        kvOffset += PAYLOAD_SIZE_FIELD_SIZE;
        buffer2().put(kvOffset, key);
        sanityCheck();

        // Write slot
        int idx = pageLoc.idx();
        if (idx >= 0 && idx < entryCount) {
            // move bigger entry to the right
            int start = HEADER_SIZE + SLOT_SIZE * idx;
            int end = slotOffset;
            byte[] tmp = new byte[end - start - SLOT_CHILD_POINTER];
            buffer2().get(start + SLOT_CHILD_POINTER, tmp);
            sanityCheck();

            buffer2().putLong(start, left);
            sanityCheck();
            buffer2().putShort(start + SLOT_CHILD_POINTER, (short) dataStart);
            sanityCheck();
            buffer2().putLong(start + SLOT_CHILD_POINTER + SLOT_KEY_SIZE, right);
            sanityCheck();
            buffer2().put(start + SLOT_SIZE + SLOT_CHILD_POINTER, tmp);
            sanityCheck();
        } else {
            buffer2().putLong(slotOffset - SLOT_CHILD_POINTER, left);
            sanityCheck();
            buffer2().putShort(slotOffset, (short) dataStart);
            sanityCheck();
            buffer2().putLong(slotOffset + SLOT_KEY_SIZE, right);
            sanityCheck();
        }

        // Update header
        setFreeSpaceOffset(freeSpaceOffset - payloadSize);
        setEntryCount(entryCount + 1);

        return true;
    }

    @Override
    protected short entrySize() {
        return SLOT_SIZE;
    }

    @Override
    protected short padding() {
        // shift for one child pointer
        return SLOT_CHILD_POINTER;
    }

    @Override
    protected int internalOverhead() {
        return SLOT_SIZE + PAYLOAD_SIZE_FIELD_SIZE;
    }

    @Override
    protected Exception sanityCheck() {
        try {
            int cnt = getEntryCount();
            for (int i = 0; i < cnt; i++) {
                long leftId = buffer(true).getLong(HEADER_SIZE + (SLOT_SIZE * i));
                int keyOffset = buffer(true).getShort(HEADER_SIZE + (SLOT_SIZE * i) + SLOT_CHILD_POINTER);
                long rightId = buffer(true).getLong(HEADER_SIZE + (SLOT_SIZE * i) + 2 + SLOT_KEY_SIZE);

                short keyDataSize = buffer(true).getShort(keyOffset);
                byte[] key = new byte[keyDataSize];
                buffer(true).get(keyOffset + 2, key);
                if (leftId < 0 || rightId < 0) {
                    throw new IllegalStateException("Child pointers cannot be negative: left=" + leftId + ", right=" + rightId);
                }
            }
            return null;
        } catch (Exception e) {
            return e;
        }
    }
}

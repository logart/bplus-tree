package org.logart.page.mmap;

import org.logart.page.Page;

import java.nio.ByteBuffer;

public class InternalPage extends AbstractPage implements Page {
    public InternalPage(ByteBuffer buffer) {
        super(buffer);
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

    @Override
    public boolean put(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("InternalPage does not support put operation directly. Use addChild instead.");
    }

    @Override
    public byte[] get(byte[] key) {
        throw new UnsupportedOperationException("InternalPage does not support put operation directly. Use getChild instead.");
    }

    public byte[][] getEntry(int index) {
        int entryCount = getEntryCount();
        if (index >= entryCount) return null;

        int slotOffset = HEADER_SIZE + SLOT_CHILD_POINTER + SLOT_SIZE * index;
        int kvOffset = Short.toUnsignedInt(buffer().getShort(slotOffset));

        int keyLen = Short.toUnsignedInt(buffer().getShort(kvOffset));
        kvOffset += 2;
        byte[] key = new byte[keyLen];
        buffer().get(kvOffset, key);

        return new byte[][]{key, null};
    }

    @Override
    public boolean isAlmostFull(long capacity) {
        byte pageMeta = buffer().get(0);
        return (pageMeta & FULL_FLAG) == FULL_FLAG
                || getFreeSpaceOffset() < HEADER_SIZE + SLOT_CHILD_POINTER + SLOT_SIZE * getEntryCount() + 2 + capacity;
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
                buffer().putLong(HEADER_SIZE + SLOT_CHILD_POINTER + (SLOT_SIZE * i) + SLOT_KEY_SIZE, newId);
                return;
            }
        }
    }

    @Override
    public long[] childrenDbugTODOREMOVE() {
        throw new UnsupportedOperationException("TODO implement");
    }

    public long getChild(byte[] key) {
        PageLoc pageLoc = searchKeyIdx(key);
        if (pageLoc.idx() < getEntryCount() && pageLoc.cmp() >= 0) {
            return getChild(pageLoc.idx() + 1);
        }
        return getChild(pageLoc.idx());
    }

    private long getChild(int idx) {
        return buffer().getLong(HEADER_SIZE + SLOT_CHILD_POINTER + (SLOT_SIZE * (idx - 1)) + SLOT_KEY_SIZE);
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

        // Write slot
        int idx = pageLoc.idx();
        if (idx >= 0 && idx < entryCount) {
            // move bigger entry to the right
            int start = HEADER_SIZE + SLOT_SIZE * idx;
            int end = slotOffset;
            byte[] tmp = new byte[end - start];
            // leave two bytes for the new entry
            buffer().get(start, tmp);

            buffer().putLong(start - SLOT_CHILD_POINTER, left);
            buffer().putShort(start, (short) dataStart);
            buffer().putLong(start + SLOT_KEY_SIZE, right);
            buffer().put(start + SLOT_SIZE, tmp);
        } else {
            buffer().putLong(slotOffset - SLOT_CHILD_POINTER, left);
            buffer().putShort(slotOffset, (short) dataStart);
            buffer().putLong(slotOffset + SLOT_KEY_SIZE, right);
        }

        // Update header
        setFreeSpaceOffset(freeSpaceOffset - payloadSize);
        setEntryCount(entryCount + 1);

        return true;
    }
}

package org.logart.page.mmap;

import org.logart.page.Page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;

import static org.logart.page.mmap.PageFactory.IS_DELETED;
import static org.logart.page.mmap.PageFactory.LEAF_FLAG;

/**
 * page format:
 * Page metadata:       1 byte
 * Page Type	    1 bit	Leaf or internal
 * Full flag	    1 bit	Indicates if the page is full
 * Is deleted 	    1 bit	Indicates if the page is deleted
 * Padding 	    5 bits	Reserved for future use
 * Padding              7 bytes padding to align to 8 bytes
 * Page ID	            8 bytes	This page's ID
 * Number of entries	2 bytes	Slot count
 * Free space offset	2 bytes	Start of free space
 * Padding              4 bytes padding to align to 8 bytes
 * Right sibling ptr	8 bytes	Only for leaf pages -- not yet implemented
 */
public abstract class AbstractPage implements Page {
    public static final int PAGE_SIZE = 4096;
    protected static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;

    protected static final int HEADER_SIZE = 32;
    protected static final int PAGE_ID_OFFSET = 8;
    protected static final int ENTRY_COUNT_OFFSET = 16;      // after page type + page id
    public static final int FREE_SPACE_OFFSET = 18;

    protected static final int SLOT_KEY_SIZE = 2;

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
    public boolean isAlmostFull(long capacity) {
        byte pageMeta = buffer().get(0);
        return (pageMeta & FULL_FLAG) == FULL_FLAG
                || availableSpace() < capacity + internalOverhead(); // Check if free space is less than capacity
    }

    protected boolean isFull() {
        byte pageMeta = buffer.get(0);
        return (pageMeta & FULL_FLAG) == FULL_FLAG;
    }

    protected int availableSpace() {
        short entryCount = buffer.getShort(ENTRY_COUNT_OFFSET);
        short freeSpaceStart = buffer.getShort(FREE_SPACE_OFFSET);
        int slotSize = HEADER_SIZE + entrySize() * entryCount + padding();
        return freeSpaceStart - slotSize;
    }

    protected abstract short entrySize();

    protected abstract short padding();

    protected abstract int internalOverhead();

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

    @Override
    public void copy(Page page) {
        AbstractPage internalPage = (AbstractPage) page; // Ensure we are working with the same type
        long currentId = pageId();
        internalPage.buffer().rewind();
        buffer.rewind();
        buffer.put(internalPage.buffer());
        buffer.putLong(PAGE_ID_OFFSET, currentId); // Ensure the page ID remains the same
    }

    protected ByteBuffer buffer() {
        return buffer;
    }
}

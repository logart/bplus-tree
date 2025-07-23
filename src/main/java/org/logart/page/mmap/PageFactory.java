package org.logart.page.mmap;

import org.logart.page.Page;

import java.nio.ByteBuffer;

public class PageFactory {
    public static final int LEAF_FLAG = 0b1000_0000;
    public static final int IS_DELETED = 0b0010_0000;

    public static Page read(ByteBuffer buffer) {
        buffer.rewind();
        byte pageMeta = buffer.get(0);

        if ((pageMeta & LEAF_FLAG) == LEAF_FLAG) {
            return LeafPage.readPage(buffer);
        } else {
            return InternalPage.readPage(buffer);
        }
    }
}

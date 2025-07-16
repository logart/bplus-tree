package org.logart.page;

import java.nio.ByteBuffer;

public class PageFactory {
    public static final int LEAF_FLAG = 0b1000_0000;

    public static Page read(ByteBuffer buffer) {
        buffer.rewind();
        byte pageMeta = buffer.get(0);
        long pageId = buffer.getLong(1);

        if ((pageMeta & LEAF_FLAG) == LEAF_FLAG) {
            return LeafPage.newPage(pageId, buffer);
        } else {
            return InternalPage.newPage(pageId, buffer);
        }
    }
}

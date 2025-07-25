package org.logart.page.mmap;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logart.page.mmap.AbstractPage.FREE_SPACE_OFFSET;
import static org.logart.page.mmap.AbstractPage.PAGE_SIZE;
import static org.logart.page.mmap.PageFactory.LEAF_FLAG;

public class AbstractPageTest {

    @Test
    void allocateShouldWriteDefaultPageHeader() throws Exception {
        ByteBuffer pageContent = ByteBuffer.allocate(PAGE_SIZE);
        LeafPage.newPage(1, pageContent);
        assertEquals(LEAF_FLAG, LEAF_FLAG & pageContent.get(0), "Page should be leaf");
        assertEquals(PAGE_SIZE, pageContent.getShort(FREE_SPACE_OFFSET), "Free space offset should be initialized to header size");
    }

}
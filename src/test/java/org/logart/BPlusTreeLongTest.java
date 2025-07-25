package org.logart;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.logart.node.DefaultNodeManager;
import org.logart.page.mmap.MMAPBasedPageManager;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@Disabled // only run it for final tests
public class BPlusTreeLongTest {
    private BPlusTree tree;
    private Path tempFile;

    @BeforeEach
    void setup() throws Exception {
        tempFile = Files.createTempFile("bplustree-test", ".db");
        tree = new DefaultBPlusTree(new DefaultNodeManager(new MMAPBasedPageManager(tempFile.toFile(), 4096, false))); // assuming 4KB pages
    }

    @AfterEach
    void teardown() throws Exception {
        tree.close();
        Files.deleteIfExists(tempFile);
    }

    @Test
    public void shouldKeepALotOfKeys() {
        for (int i = 0; i < 10_000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            tree.put(key, value);
        }
        for (int i = 0; i < 10_000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i).getBytes();
            assertArrayEquals(value, tree.get(key), "Value for key-" + i + " should match");
        }
    }
}

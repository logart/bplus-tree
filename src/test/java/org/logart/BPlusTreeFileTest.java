package org.logart;

import org.junit.jupiter.api.Test;
import org.logart.node.DefaultNodeManager;
import org.logart.page.mmap.MMAPBasedPageManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class BPlusTreeFileTest {

    private BPlusTree tree;
    private Path tempFile;

    @Test
    void shouldReopenFileWithTree() throws IOException {
        tempFile = Files.createTempFile("bplustree-file-test", ".db");
        try {
            tree = new DefaultBPlusTree(new DefaultNodeManager(new MMAPBasedPageManager(tempFile.toFile(), 4096)));

            for (int i = 0; i < 101; i++) {
                tree.put(("key" + i).getBytes(), ("value" + i).getBytes());
                System.out.println("Inserted key: " + ("key" + i) + ", value: " + ("value" + i));
                assertArrayEquals(("value" + i).getBytes(), tree.get(("key" + i).getBytes()),
                        "Value for key" + i + " should match");
            }
            for (int i = 0; i < 101; i++) {
                System.out.println("Validating key: " + ("key" + i) + ", value: " + ("value" + i));
                assertArrayEquals(("value" + i).getBytes(), tree.get(("key" + i).getBytes()),
                        "Value for key" + i + " should match");
            }

            tree.close();

            DefaultBPlusTree reopenedTree = new DefaultBPlusTree(new DefaultNodeManager(new MMAPBasedPageManager(tempFile.toFile(), 4096)));
            reopenedTree.load();
            for (int i = 0; i < 101; i++) {
                assertArrayEquals(("value" + i).getBytes(), reopenedTree.get(("key" + i).getBytes()),
                        "Value for key" + i + " should match");
            }
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
}
package org.logart.mmap;

import org.junit.jupiter.api.Test;
import org.logart.DefaultBPlusTree;
import org.logart.node.DefaultNodeManager;
import org.logart.page.mmap.MMAPBasedPageManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class MmapFlushTest {

    private static final String JAVA_BIN = System.getProperty("java.home") + "/bin/java";
    private static final String CLASS_PATH = System.getProperty("java.class.path");

    private Process launchWriter(Path file) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(
                JAVA_BIN, "-cp", CLASS_PATH, "org.logart.mmap.MmapTreeWriter", file.toString()
        );
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        return pb.start();
    }

    @Test
    public void testWithFlush_dataShouldPersist() throws Exception {
        Path tempFile = Files.createTempFile("mmap-close-test" + System.nanoTime(), ".dat");
        Files.deleteIfExists(tempFile);
        try {
            Process p = launchWriter(tempFile); // flush
            p.waitFor();

            DefaultBPlusTree tree = new DefaultBPlusTree(new DefaultNodeManager(
                    new MMAPBasedPageManager(tempFile.toFile(), 4096, false)
            ));
            tree.load();
            int i;
            for (i = 0; i < 10_001; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] value = tree.get(key);
                assertNotNull(value, "Value for key " + i + " should not be null");
                assertArrayEquals(value, key, "Value for key " + i + " should be equal");
            }

            System.out.println("Number of items read: " + i);
            assertEquals(10_001, i, "At least one record should be read successfully, was " + i);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
}
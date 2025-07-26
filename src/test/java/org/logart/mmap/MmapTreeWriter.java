package org.logart.mmap;

import org.logart.DefaultBPlusTree;
import org.logart.node.DefaultNodeManager;
import org.logart.page.mmap.MMAPBasedPageManager;

import java.nio.file.Path;
import java.nio.file.Paths;

public class MmapTreeWriter {
    public static void main(String[] args) throws Exception {
        Path path = Paths.get(args[0]);
        DefaultBPlusTree tree = new DefaultBPlusTree(new DefaultNodeManager(
                new MMAPBasedPageManager(path.toFile(), 4096, false)
        ));
        for (int i = 0; i < 10_001; i++) {
            byte[] key = ("key" + i).getBytes();
            tree.put(key, key); // Store the key as value for verification
        }
        tree.close();
        System.exit(0);
    }
}

package org.logart;

import java.util.List;

public interface BPlusTree {

    byte[] get(byte[] key);

    void put(byte[] key, byte[] value);

    List<byte[]> getAllKeysInOrder();

    void close();
}

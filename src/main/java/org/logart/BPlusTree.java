package org.logart;

public interface BPlusTree {

    void load();

    byte[] get(byte[] key);

    void put(byte[] key, byte[] value);

    void close();
}

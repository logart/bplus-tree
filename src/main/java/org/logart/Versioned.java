package org.logart;

public class Versioned<T> {
    private final T value;
    final long version;

    public Versioned(T value, long version) {
        this.value = value;
        this.version = version;
    }

    public T get() {
        return value;
    }

    public long version() {
        return version;
    }
}

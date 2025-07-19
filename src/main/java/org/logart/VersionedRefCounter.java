package org.logart;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class VersionedRefCounter<T> {
    private final AtomicLong nextVersion = new AtomicLong(0);
    private final ConcurrentSkipListMap<Long, AtomicInteger> refCounts = new ConcurrentSkipListMap<>();

    private final AtomicReference<Versioned<T>> currentVersionRef;

    public VersionedRefCounter(Supplier<T> starter) {
        this.currentVersionRef = new AtomicReference<>(new Versioned<>(starter.get(), nextVersion.getAndIncrement()));
    }

    public boolean advanceVersion(Versioned<T> current, T next) {
        return currentVersionRef.compareAndSet(current, new Versioned<>(next, nextVersion.getAndIncrement()));
    }

    public Versioned<T> lockVersion() {
        while (true) {
            // read the current version
            Versioned<T> currentVersion = currentVersionRef.get();
            // lock current version to prevent it's deletion
            incrementRef(currentVersion.version());
            // reread the current version to ensure it is still the same
            Versioned<T> newVersion = currentVersionRef.get();

            if (currentVersion == newVersion) {
                // success
                return currentVersion;
            } else {
                // another thread has changed the version, decrement ref count and retry
                releaseVersion(currentVersion);
            }
        }
    }

    // Increment ref count for a version (if it exists)
    private void incrementRef(long version) {
        AtomicInteger counter = refCounts.computeIfAbsent(version, unused -> new AtomicInteger(0));
        counter.incrementAndGet();
    }

    public void releaseVersion(Versioned<T> versionedRoot) {
        AtomicInteger counter = refCounts.get(versionedRoot.version());
        if (counter != null) {
            counter.decrementAndGet();
        }
    }

    // Get current ref count
    public int getRefCount(long version) {
        AtomicInteger counter = refCounts.get(version);
        return counter != null ? counter.get() : 0;
    }

    public long cleanUpTo(long version) {
        return floorUsedVersion(version);
    }

    private long floorUsedVersion(long version) {
        boolean proceed = true;
        Map.Entry<Long, AtomicInteger> prev = null, current = null;
        while (proceed) {
            prev = current;
            current = refCounts.pollFirstEntry();
            // there is next version to check
            proceed = current != null
                    // the next version is still smaller than the last version we want to clean up
                    && current.getKey() <= version
                    // the next version is not in use
                    && current.getValue().get() <= 0;
        }
        if (current != null) {
            // return last entry back
            refCounts.put(current.getKey(), current.getValue());
        }
        return keyOrNone(prev);
    }

    private long keyOrNone(Map.Entry<Long, AtomicInteger> versionEntry) {
        if (versionEntry == null) {
            return -1; // No versions available
        }
        return versionEntry.getKey();
    }

    @Override
    public String toString() {
        return "VersionedRefCounter{" +
                "refCounts=" + refCounts.entrySet().stream().filter(v -> v.getValue().get() != 0).toList() +
                '}';
    }
}

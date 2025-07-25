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

    /**
     * This method is desctructive, evenrything that happened before will be lost.
     *
     * @param root    new root to start from
     * @param version new version of the root
     */
    // load should only happen once, so it's not a big deal to use synchronized here
    public synchronized void load(T root, long version) {
        this.currentVersionRef.set(new Versioned<>(root, version));
        this.nextVersion.set(version + 1);
        refCounts.clear();
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

    public int releaseVersion(Versioned<T> versionedRoot) {
        AtomicInteger counter = refCounts.get(versionedRoot.version());
        if (counter != null) {
            return counter.decrementAndGet();
        }
        return 0;
    }

    // Get current ref count
    public int getRefCount(long version) {
        AtomicInteger counter = refCounts.get(version);
        return counter != null ? counter.get() : 0;
    }

    public boolean safeToCleanUp(long version) {
        // it's safe to clean up all versions except current,
        // we read from nextVersion with getAndIncrement
        // which means if the current version is 5,
        // nextVersion will be 6
        return version < nextVersion.get() - 1;
    }

    public long cleanUpTo(long version) {
        return floorUsedVersion(version);
    }

    private long floorUsedVersion(long version) {
        Map.Entry<Long, AtomicInteger> prev = null;
        for (Map.Entry<Long, AtomicInteger> current : refCounts.entrySet()) {
            // the current version is smaller or equal to the version we want to clean up
            boolean proceed = current.getKey() <= version
                    // the current version is not in use
                    && current.getValue().get() <= 0;
            if (proceed) {
                prev = current;
            }
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

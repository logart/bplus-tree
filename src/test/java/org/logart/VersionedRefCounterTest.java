package org.logart;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class VersionedRefCounterTest {
    @Test
    public void shouldNotAdvanceVersionBackward() {
        VersionedRefCounter<String> refCounter = new VersionedRefCounter<>(() -> "Initial");

        // Attempt to advance a version backward
        Versioned<String> current = refCounter.lockVersion();
        refCounter.advanceVersion(current, "NewValue");
        Versioned<String> updated = refCounter.lockVersion();

        // The version should remain unchanged
        assertEquals(1, updated.version());
        assertEquals("NewValue", updated.get());
    }

    @Test
    public void shouldNotAllowMultipleRootUpdate() {
        VersionedRefCounter<String> refCounter = new VersionedRefCounter<>(() -> "Initial");

        // Attempt to advance a version backward
        Versioned<String> current = refCounter.lockVersion();
        Versioned<String> concurrent = refCounter.lockVersion();
        refCounter.advanceVersion(current, "NewValue");
        assertFalse(refCounter.advanceVersion(concurrent, "AlternativeNewValue"));
        Versioned<String> updated = refCounter.lockVersion();

        // The version should remain unchanged
        assertEquals(1, updated.version());
        assertEquals("NewValue", updated.get());
    }

    @Test
    public void shouldKeepOldVersionInParallelWithNewOne() {
        VersionedRefCounter<String> refCounter = new VersionedRefCounter<>(() -> "Initial");

        // Attempt to advance a version backward
        Versioned<String> current = refCounter.lockVersion();
        Versioned<String> concurrent = refCounter.lockVersion();
        refCounter.advanceVersion(current, "NewValue");
        assertFalse(refCounter.advanceVersion(concurrent, "AlternativeNewValue"));
        Versioned<String> updated = refCounter.lockVersion();

        // The version should remain unchanged
        assertNotEquals(updated.version(), concurrent.version());
        assertEquals("Initial", concurrent.get());
    }
}
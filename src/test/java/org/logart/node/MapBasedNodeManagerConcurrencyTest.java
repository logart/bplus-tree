package org.logart.node;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logart.Versioned;
import org.logart.page.MapBasedPageManager;
import org.logart.page.PageManager;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class MapBasedNodeManagerConcurrencyTest {
    private PageManager pageManager;
    private NodeManager nodeManager;

    @BeforeEach
    public void setUp() {
        pageManager = new MapBasedPageManager();
        nodeManager = new MapBasedNodeManager(pageManager);
    }

    @AfterEach
    public void tearDown() {
        nodeManager.close();
    }

    @Test
    public void testConcurrentVersionLockingAndFreeing() throws InterruptedException {
        int threads = 16;
        int opsPerThread = 100;

        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            CountDownLatch latch = new CountDownLatch(threads);
            Random random = new Random();

            Set<Long> usedVersions = ConcurrentHashMap.newKeySet();

            ConcurrentLinkedQueue<Exception> errors = new ConcurrentLinkedQueue<>();
            AtomicInteger versionConunter = new AtomicInteger(0);
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < opsPerThread; j++) {
                            Versioned<BTreeNode> version = nodeManager.lockVersion();
                            usedVersions.add(version.version());

                            BTreeNode node = nodeManager.allocateLeafNode();
                            long id = node.id();
                            nodeManager.writeNode(id, node);

                            // Simulate read
                            BTreeNode read = nodeManager.readNode(id);
                            assertNotNull(read);

                            // Simulate deferred free
                            if (random.nextBoolean()) {
                                nodeManager.freeNode(id, version.version());
                            }

                            nodeManager.releaseVersion(version);
                            usedVersions.remove(version.version());
                        }
                    } catch (Exception e) {
                        errors.add(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            executor.shutdown();
            assertTrue(errors.isEmpty(), "Some threads encountered exceptions: " + errors);

            // All nodes that were freed should no longer be in allocatedNodes
            Set<Long> allocatedNodes = ((MapBasedPageManager) pageManager).getAllAllocatedPageIds();
            Set<Long> freedNodes = ((MapBasedPageManager)pageManager).getFreedNodeIds();
            // 1 node is allocated in the constructor
            assertEquals(threads * opsPerThread + 1, allocatedNodes.size() + freedNodes.size());
            for (Long id : freedNodes) {
                assertFalse(allocatedNodes.contains(id), "Node " + id + " was freed but still in allocated set");
            }

            // All versions should be released
            assertTrue(usedVersions.isEmpty(), "Some versions were not released");
        }
    }

    @Test
    void nodeShouldNotBeFreedUntilVersionReleased() {
        Versioned<BTreeNode> version = nodeManager.lockVersion();
        BTreeNode node = nodeManager.allocateLeafNode();
        long nodeId = node.id();
        nodeManager.writeNode(nodeId, node);

        nodeManager.freeNode(nodeId, version.version());

        // Node must still be available until a version is released
        assertNotNull(nodeManager.readNode(nodeId), "Node was freed prematurely!");

        nodeManager.releaseVersion(version);

        nodeManager.advanceVersion(version, null);
        // Now the node should be gone
        assertNull(nodeManager.readNode(nodeId), "Node was not freed after version release");
    }

    @Test
    void nodeShouldNotBeReleasedIfItIsUsedWithLaterVersion() {
        // existing node
        BTreeNode node = nodeManager.allocateNode();
        long nodeId = node.id();

        // put overwriting at v1
        // get reading at v1
        Versioned<BTreeNode> vput = nodeManager.lockVersion();
        Versioned<BTreeNode> vget = nodeManager.lockVersion();

        nodeManager.freeNode(nodeId, vput.version());

        // get should be able to read still
        assertNotNull(nodeManager.readNode(nodeId), "Freed with vget still active!");

        // put done
        nodeManager.releaseVersion(vput);
        BTreeNode advancedRoot = new MockBtreeNode();
        nodeManager.advanceVersion(vput, advancedRoot);

        // get should still be able to see the node
        assertNotNull(nodeManager.readNode(nodeId), "Freed with vget still active!");

        // get done
        nodeManager.releaseVersion(vget);
        // node should be released since get is done
//        only after next advance it will be released
        assertNotNull(nodeManager.readNode(nodeId), "Freed with vget still active!");

        Versioned<BTreeNode> versionAfterPutAndGet = nodeManager.lockVersion();
        nodeManager.advanceVersion(versionAfterPutAndGet, null);
        assertNull(nodeManager.readNode(nodeId));
    }

    @Test
    void nodeShouldNotBeFreedIfAnyVersionStillActive() {
        // existing node
        BTreeNode node = nodeManager.allocateNode();
        long nodeId = node.id();

        // get reads at v1
        // get locks the version, this is long read, still in progress
        Versioned<BTreeNode> v1 = nodeManager.lockVersion();
        // put modifies at v2
        // put locks the version; this is a second put, after previous already modified the tree
        Versioned<BTreeNode> v2 = nodeManager.lockVersion();

        // put copied the node and want to free it
        nodeManager.freeNode(nodeId, v2.version());
        // get read node after it's being free, this should not cause an error since a version is locked
        // the question is how put could know BEFORE freeing that the node still will be in use
        assertNotNull(nodeManager.readNode(nodeId), "Freed with v2 still active!");

        // put done
        nodeManager.releaseVersion(v2);

        // get should still be able to see the node
        assertNotNull(nodeManager.readNode(nodeId), "Freed with v2 still active!");

        // get done
        nodeManager.releaseVersion(v1);
        // event after release of v1 we could not clean up this node since someone could still use it on v2 or v3
        assertNotNull(nodeManager.readNode(nodeId));

        nodeManager.advanceVersion(v2, null);
        // node should be released now
        assertNull(nodeManager.readNode(nodeId), "Node was not freed after both versions released");
    }

    @Test
    void multipleUsesOfSameVersionMustAllBeReleased() {
        Versioned<BTreeNode> version = nodeManager.lockVersion();
        Versioned<BTreeNode> version2 = nodeManager.lockVersion(); // used twice

        BTreeNode node = nodeManager.allocateLeafNode();
        long id = node.id();
        nodeManager.writeNode(id, node);
        nodeManager.freeNode(id, version.version());

        nodeManager.releaseVersion(version); // First release
        assertNotNull(nodeManager.readNode(id), "Node freed after first release");

        nodeManager.releaseVersion(version2); // Second release
        nodeManager.advanceVersion(version2, null);
        assertNull(nodeManager.readNode(id), "Node not freed after final release");
    }

    @Test
    void redundantFreeShouldNotCauseError() {
        BTreeNode node = nodeManager.allocateNode();
        long id = node.id();
        nodeManager.writeNode(id, node);
        nodeManager.freeNode(id, (long) (Math.random() * 1000));
        nodeManager.freeNode(id, (long) (Math.random() * 1000)); // second call should be a no-op or safe
    }

    @Test
    void nodeShouldBeReusableAcrossVersions() {
        Versioned<BTreeNode> v1 = nodeManager.lockVersion();
        BTreeNode node = nodeManager.allocateNode();
        long id = node.id();
        nodeManager.writeNode(id, node);

        // Read under v1
        assertNotNull(nodeManager.readNode(id));

        // Start another version
        Versioned<BTreeNode> v2 = nodeManager.lockVersion();
        nodeManager.freeNode(id, v2.version()); // try to free

        nodeManager.releaseVersion(v1); // v2 still live
        assertNotNull(nodeManager.readNode(id), "Freed with version 2 still active!");

        nodeManager.releaseVersion(v2); // finally safe to free
        nodeManager.advanceVersion(v2, null);
        assertNull(nodeManager.readNode(id), "Node should be freed after all versions released");
    }

    @Test
    void concurrentAccessRespectsVersionPins() throws InterruptedException {
        int threads = 1;
        long id;
        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            CountDownLatch startLatch = new CountDownLatch(threads);
            CountDownLatch finishLatch = new CountDownLatch(threads);
            BTreeNode node = nodeManager.allocateLeafNode();
            id = node.id();
            nodeManager.writeNode(id, node);

            // Simulate concurrent usage
            Set<Exception> errors = ConcurrentHashMap.newKeySet();
            Set<Long> usedVersions = ConcurrentHashMap.newKeySet();
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    Versioned<BTreeNode> version = nodeManager.lockVersion();
                    usedVersions.add(version.version());
                    startLatch.countDown(); // at least one thread should lock the version
                    try {
                        assertNotNull(nodeManager.readNode(id));
                        Thread.sleep(10); // simulate work
                    } catch (Exception e) {
                        errors.add(e);
                    } finally {
                        nodeManager.releaseVersion(version);
                        usedVersions.remove(version.version());
                    }
                    finishLatch.countDown();
                });
            }

            if (!errors.isEmpty()) {
                fail("Some threads encountered exceptions: " + errors);
            }

            startLatch.await();
            while (!usedVersions.isEmpty()) {
                usedVersions.forEach(version -> nodeManager.freeNode(id, version));
            }
            finishLatch.await();
            executor.shutdown();
        }

        assertNull(nodeManager.readNode(id), "Node was not freed after all concurrent versions finished");
    }

    @Test
    void allFreedNodesMustBeGone() {
        List<BTreeNode> nodes = IntStream.range(0, 100)
                .mapToObj(i -> nodeManager.allocateLeafNode())
                .toList();

        Versioned<BTreeNode> version = nodeManager.lockVersion();

        for (BTreeNode node : nodes) {
            nodeManager.writeNode(node.id(), node);
            nodeManager.freeNode(node.id(), version.version());
        }

        nodeManager.releaseVersion(version);
        nodeManager.advanceVersion(version, null);

        for (BTreeNode node : nodes) {
            assertNull(nodeManager.readNode(node.id()), "Node " + node.id() + " was not freed");
        }
    }

    @Test
    void shouldNotRemovePrevioouslyLockedVersion() {
        Versioned<BTreeNode> readVersion = nodeManager.lockVersion();
        Versioned<BTreeNode> writeVersion = nodeManager.lockVersion();
        nodeManager.releaseVersion(writeVersion);
        BTreeNode newRoot = nodeManager.allocateLeafNode();
        nodeManager.freeNode(writeVersion.get().id(), writeVersion.version());
        assertTrue(nodeManager.advanceVersion(writeVersion, newRoot));

        Versioned<BTreeNode> newRootVersion = nodeManager.lockVersion();
        assertNotNull(readVersion.get());
        assertTrue(readVersion.version() < newRootVersion.version());
    }
}

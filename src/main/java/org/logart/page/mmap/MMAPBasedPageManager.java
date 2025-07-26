package org.logart.page.mmap;

import org.logart.ConcurrentLinkedUniqueQueue;
import org.logart.node.BTreeNode;
import org.logart.page.Page;
import org.logart.page.PageManager;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MMAPBasedPageManager implements PageManager {
    private static final int PAGE_POINTER_SIZE = 8;

    private final FileChannel channel;
    private final int pageSize;
    private final Set<Long> pages = ConcurrentHashMap.newKeySet();
    private final Queue<Long> freePagesIds = new ConcurrentLinkedUniqueQueue<>();
    private final AtomicLong currentPageId;
    private final boolean sanityCheckEnabled;
    private final MappedByteBuffer rootPointer;

    public MMAPBasedPageManager(File file, int pageSize) throws IOException {
        this(file, pageSize, true);
    }

    public MMAPBasedPageManager(File file, int pageSize, boolean sanityCheckEnabled) throws IOException {
        this.pageSize = pageSize;
        this.channel = FileChannel.open(
                file.toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );
        this.rootPointer = channel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_POINTER_SIZE);
        this.currentPageId = new AtomicLong(channel.size() / pageSize);
        this.sanityCheckEnabled = sanityCheckEnabled;
    }

    @Override
    public Page open() {
        long rootId = rootPointer.getLong(0);
        return readPage(rootId);
    }

    /**
     * Allocates a new page either from free pages or by extending the file.
     * Returns the page id (page offset / pageSize).
     */
    public Page allocatePage() {
        Long potentialPageId = freePagesIds.poll();
        final long pageId = Objects.requireNonNullElseGet(potentialPageId, currentPageId::getAndIncrement);

        MappedByteBuffer emptyPage;
        try {
            emptyPage = channel.map(FileChannel.MapMode.READ_WRITE, pageId * pageSize + PAGE_POINTER_SIZE, pageSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Page page = InternalPage.newPage(pageId, emptyPage, sanityCheckEnabled);
        pages.add(pageId);
        writePage(pageId, page);
        return page;
    }

    /**
     * Allocates a leaf page. For now, identical to allocatePage().
     * Can differentiate later for internal vs leaf pages if needed.
     */
    public Page allocateLeafPage() {
        Long potentialPageId = freePagesIds.poll();
        final long pageId = Objects.requireNonNullElseGet(potentialPageId, currentPageId::getAndIncrement);

        MappedByteBuffer emptyPage;
        try {
            // keep first 8 bytes for a root pointer
            emptyPage = channel.map(FileChannel.MapMode.READ_WRITE, pageId * pageSize + PAGE_POINTER_SIZE, pageSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Page page = LeafPage.newPage(pageId, emptyPage, sanityCheckEnabled);
        pages.add(pageId);
        writePage(pageId, page);
        return page;
    }

    /**
     * Reads a full page into a ByteBuffer.
     */
    public Page readPage(long pageId) {
        MappedByteBuffer buffer;
        try {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, pageId * pageSize + PAGE_POINTER_SIZE, pageSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Page read = PageFactory.read(buffer, sanityCheckEnabled);
        if (read.isDeleted()) {
            throw new IllegalStateException("Page with id " + pageId + " is deleted and cannot be read.");
        }
        return read;
    }

    /**
     * Writes a full page from a ByteBuffer.
     */
    public void writePage(long pageId, Page page) {
        // todo maybe we could just do force() on underlying mmap buffer
        AbstractPage internalPage = (AbstractPage) page;
        ByteBuffer buffer = internalPage.buffer2();
        internalPage.sanityCheck();
        if (buffer.position() != 0) {
            buffer.rewind();
        }
        int remaining = buffer.remaining();
        if (remaining != pageSize) {
            throw new IllegalArgumentException("Buffer size " + remaining + " does not match page size " + pageSize);
        }
        ((MappedByteBuffer) buffer).force();
    }

    @Override
    public void writeRoot(BTreeNode root) {
        rootPointer.putLong(0, root.id());
        rootPointer.force();
    }

    @Override
    public void freePage(long pageId) {
        if (pages.remove(pageId)) {
            Page page = readPage(pageId);
            page.markDeleted();
            writePage(pageId, page);
            freePagesIds.offer(pageId);
        }
    }

    public long getAllocatedPageCount() {
        return currentPageId.get();
    }

    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close channel", e);
        }
    }

    public Set<Long> getAllAllocatedNodeIds() {
        return pages;
    }

    public Set<Long> getFreedNodeIds() {
        return Set.copyOf(freePagesIds);
    }
}
package org.logart.page.mmap;

import org.logart.ConcurrentLinkedUniqueQueue;
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
import java.util.concurrent.atomic.AtomicLong;

public class MMAPBasedPageManager implements PageManager {
    private final FileChannel channel;
    private final int pageSize;
    private final Queue<Long> freePagesIds = new ConcurrentLinkedUniqueQueue<>();
    private final AtomicLong currentPageId;

    public MMAPBasedPageManager(File file, int pageSize) throws IOException {
        this.pageSize = pageSize;
        this.channel = FileChannel.open(
                file.toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );
        this.currentPageId = new AtomicLong(channel.size() / pageSize);
    }

    /**
     * Allocates a new page either from free pages or by extending the file.
     * Returns the page id (page offset / pageSize).
     */
    public Page allocatePage() {
        Long potentialPageId = freePagesIds.poll();
        final long pageId = Objects.requireNonNullElseGet(potentialPageId, currentPageId::getAndIncrement);

        ByteBuffer emptyPage;
        try {
            emptyPage = channel.map(FileChannel.MapMode.READ_WRITE, pageId * pageSize, pageSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Page page = InternalPage.newPage(pageId, emptyPage);
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

        ByteBuffer emptyPage;
        try {
            emptyPage = channel.map(FileChannel.MapMode.READ_WRITE, pageId * pageSize, pageSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Page page = LeafPage.newPage(pageId, emptyPage);
        writePage(pageId, page);
        return page;
    }

    /**
     * Reads a full page into a ByteBuffer.
     */
    public Page readPage(long pageId) {
        MappedByteBuffer buffer;
        try {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, pageId * pageSize, pageSize);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return PageFactory.read(buffer);
    }

    /**
     * Writes a full page from a ByteBuffer.
     */
    public void writePage(long pageId, Page page) {
        // todo maybe we could just do force() on underlying mmap buffer
        ByteBuffer buffer = page.buffer();
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
    public void freePage(long pageId) {
        freePagesIds.offer(pageId);
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

    private void writePage(long position, ByteBuffer buffer) {
        try {
            assert buffer.capacity() == pageSize;
            assert channel.write(buffer, position) == pageSize : "Failed to write full page";
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write page at position " + position, e);
        }
    }
}
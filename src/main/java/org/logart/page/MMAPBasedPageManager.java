package org.logart.page;

import org.logart.page.Page;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MMAPBasedPageManager implements PageManager {
    // todo could be replaced with primitive map implementation,
    // I don't want to do it here to speed up the development and not deal with third-party libraries.
    private final ConcurrentMap<Long, ReentrantReadWriteLock> pageLocks = new ConcurrentHashMap<>();

    private final FileChannel channel;
    private final int pageSize;
    private final Queue<Long> freePagesIds = new ConcurrentLinkedQueue<>();
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
        return allocatePage(false);
    }

    /**
     * Allocates a leaf page. For now, identical to allocatePage().
     * Can differentiate later for internal vs leaf pages if needed.
     */
    public Page allocateLeafPage() {
        Long potentialPageId = freePagesIds.poll();
        final long pageId = Objects.requireNonNullElseGet(potentialPageId, currentPageId::getAndIncrement);
        // todo get buffer from mmaped file
        ByteBuffer emptyPage = ByteBuffer.allocate(pageSize);
        Page page = LeafPage.newPage(pageId, emptyPage);
        writePage(pageId, page);
        return page;
    }

    private Page allocatePage(boolean leaf) {
        Long potentialPageId = freePagesIds.poll();
        final long pageId = Objects.requireNonNullElseGet(potentialPageId, currentPageId::getAndIncrement);
        // todo get buffer from mmaped file
        ByteBuffer emptyPage = ByteBuffer.allocate(pageSize);
        Page page = InternalPage.newPage(pageId, emptyPage);
        writePage(pageId, page);
        return page;
    }

    /**
     * Reads a full page into a ByteBuffer.
     */
    public Page readPage(long pageId) {
        ReentrantReadWriteLock lock = pageLocks.computeIfAbsent(pageId, unused -> new ReentrantReadWriteLock());
        lock.readLock().lock();
        try {
            ByteBuffer buffer = ByteBuffer.allocate(pageSize);
            try {
                int read = channel.read(buffer, pageId * pageSize);
                if (read != pageSize) {
                    // todo do a better type
                    throw new UncheckedIOException(new IOException("Failed to read full page " + pageId + ", only read " + read + " bytes from page " + pageSize));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            buffer.flip();
            return PageFactory.read(buffer);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Writes a full page from a ByteBuffer.
     */
    public void writePage(long pageId, Page page) {
        ReentrantReadWriteLock lock = pageLocks.computeIfAbsent(pageId, unused -> new ReentrantReadWriteLock());
        lock.writeLock().lock();
        try {
            // todo maybe we could just do force() on underlying mmap buffer
            ByteBuffer buffer = page.buffer();
            if (buffer.position() != 0) {
                buffer.rewind();
            }
            int remaining = buffer.remaining();
            if (remaining != pageSize) {
                throw new IllegalArgumentException("Buffer size " + remaining + " does not match page size " + pageSize);
            }
            writePage(pageId * pageSize, buffer);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void freePage(long pageId) {
        pageLocks.remove(pageId);
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
package org.logart.page.mmap;

public record PageLoc(int idx, byte[] k, byte[] v, int cmp) {
}

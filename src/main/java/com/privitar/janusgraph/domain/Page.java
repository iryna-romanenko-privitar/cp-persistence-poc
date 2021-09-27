package com.privitar.janusgraph.domain;

import java.util.List;

public class Page<T> {
    private final List<T> objects;
    private final int offset;
    private final int count;

    public Page(List<T> objects, int offset, int count) {
        this.objects = objects;
        this.offset = offset;
        this.count = count;
    }

    public List<T> getObjects() {
        return objects;
    }

    public int getOffset() {
        return offset;
    }

    public int getCount() {
        return count;
    }
}

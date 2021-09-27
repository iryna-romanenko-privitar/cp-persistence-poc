package com.privitar.janusgraph.domain;

import java.util.List;

public class CursorPage<T> {
    private final List<T> objects;
    private final String cursor;
    private final int count;

    public CursorPage(List<T> objects, String cursor, int count) {
        this.objects = objects;
        this.cursor = cursor;
        this.count = count;
    }

    public List<T> getObjects() {
        return objects;
    }

    public String getCursor() {
        return cursor;
    }

    public int getCount() {
        return count;
    }
}

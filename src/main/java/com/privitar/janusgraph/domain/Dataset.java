package com.privitar.janusgraph.domain;

import java.util.UUID;

public class Dataset {

    private final UUID id;
    private final String name;

    public Dataset(UUID id, String name) {
        this.id = id;
        this.name = name;
    }
    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}

package com.privitar.janusgraph.domain;

import java.util.Set;

public class Dataset {

    private final Long id;
    private final String name;
    private final String description;
    private final Set<String> tags;
    private final String tenant;

    public Dataset(Long id, String name, String description, Set<String> tags, String tenant) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.tags = tags;
        this.tenant = tenant;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Long getId() {
        return id;
    }

    public Set<String> getTags() {
        return tags;
    }

    public String getTenant() {
        return tenant;
    }

    public String toCsvString() {
        return id + "," + name + "," + description + "," + tenant;
    }
}

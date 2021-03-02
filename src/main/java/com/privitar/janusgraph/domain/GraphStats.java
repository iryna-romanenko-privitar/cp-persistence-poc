package com.privitar.janusgraph.domain;

import java.util.List;

public class GraphStats {

    private final int datasetNodes;

    public GraphStats(int datasetNodes) {
        this.datasetNodes = datasetNodes;
    }

    public int getDatasetNodes() {
        return datasetNodes;
    }
}

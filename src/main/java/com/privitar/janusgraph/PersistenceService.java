package com.privitar.janusgraph;

import com.privitar.janusgraph.domain.Dataset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

@ApplicationScoped
public class PersistenceService {

    private final JanusGraph graph;
    private final GraphTraversalSource g;

    @Inject
    public PersistenceService(JanusClient client) {
        this.graph = client.getInstance();
        this.g = graph.traversal();
    }

    public Optional<Dataset> getDatasetById(String tenant, String id) {
        Optional<Vertex> datasetVertex = g.V(id).has("tenant", tenant).tryNext();
        return datasetVertex.map(PersistenceService::toDataset);
    }

    public Set<Dataset> getDatasetsByTenant(String tenant) {
        List<Vertex> datasetVertices = g.V().hasLabel("Dataset").has("tenant", tenant).next(10); //todo pagination
        return datasetVertices.stream().map(PersistenceService::toDataset).collect(Collectors.toSet());
    }

    public Set<Dataset> getDatasetsByOwner(String tenant, Long owner) {
        List<Vertex> datasetVertices = g.V(owner).hasLabel("User").has("tenant", tenant).outE("owns").otherV().next(10); //todo pagination
        return datasetVertices.stream().map(PersistenceService::toDataset).collect(Collectors.toSet());
    }

    public Set<Dataset> findByTag(String tenant, String tag) {
        List<Vertex> datasetVertices = g.V().hasLabel("Dataset").has("tenant", tenant).has("tags", TextP.containing(tag)).next(10);
        return datasetVertices.stream().map(PersistenceService::toDataset).collect(Collectors.toSet());
    }

    private static Dataset toDataset(Vertex datasetVertex) {
        String tagsValue = datasetVertex.value("tags");
        Set<String> tags = Arrays.stream(tagsValue.substring(1, tagsValue.length() - 1).split(",")).collect(Collectors.toSet());
        return new Dataset((Long) datasetVertex.id(), datasetVertex.value("name").toString(), datasetVertex.value("description").toString(), tags,
                datasetVertex.value("tenant").toString());
    }


}

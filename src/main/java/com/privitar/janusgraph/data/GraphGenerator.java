package com.privitar.janusgraph.data;

import com.google.common.collect.Lists;
import com.privitar.janusgraph.JanusClient;
import com.privitar.janusgraph.domain.Dataset;
import com.privitar.janusgraph.domain.GraphStats;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;

public class GraphGenerator {

    private final JanusGraph graph;

    private final Random random = new Random();

    private List<Object> datasetIds = new ArrayList<>();

    private Set<Dataset> datasets = new HashSet<>();

    private static final List<String> tenants = Lists.newArrayList("VPB", "TM", "W&L", "RD", "CGP");
    private static final List<String> tags = Lists
            .newArrayList("patients", "tests", "data", "sensitive", "customers", "orders", "sales", "salaries", "birthdays", "locations", "ages", "pets",
                    "work", "positions", "finance", "analytical", "it", "raw", "personal", "public", "address", "UK", "US", "green", "energy", "gdpr", "policy",
                    "relationships", "relatives", "documents", "travel", "preferences", "favourites", "children", "adults", "vaccinations", "procedures",
                    "diagnoses", "symptoms", "research", "analysis");

    public GraphGenerator(JanusClient client) {
        this.graph = client.getInstance();

        defineSchema();
    }

    public static void main(String[] args) {
        String pathToConf = Paths.get("")
                .toAbsolutePath() + "/src/main/resources/janusgraph-remote.properties";
        JanusClient client = new JanusClient(pathToConf);

        GraphGenerator gg = new GraphGenerator(client);

        gg.generateGraph();

        gg.generateInvalidObjects();

        System.exit(0);
    }

    public GraphStats generateGraph() {
        Map<String, List<Object>> owners = generateVertices(1000, this::generateUser);
        Map<String, List<Object>> requesters = generateVertices(200, this::generateUser);
        Map<String, List<Object>> datasets = generateVertices(10000, this::generateDataset);
        Map<String, List<Object>> assets = generateVertices(30000, this::generateAsset);
        Map<String, List<Object>> connections = generateVertices(300, this::generateConnection);

        for (String tenant : tenants) {
            generateOwnsEdges(datasets.getOrDefault(tenant, new ArrayList<>()), owners.getOrDefault(tenant, new ArrayList<>()));

            generateEdges(20000, "hasAsset", datasets.getOrDefault(tenant, new ArrayList<>()), assets.getOrDefault(tenant, new ArrayList<>()));

            generateEdges(100, "requests", requesters.getOrDefault(tenant, new ArrayList<>()), datasets.getOrDefault(tenant, new ArrayList<>()));

            generateResidesEdges(assets.getOrDefault(tenant, new ArrayList<>()), connections.getOrDefault(tenant, new ArrayList<>()));
        }

        this.datasetIds = Arrays.asList(datasets.values().toArray());

        writeToFile(tags, "tags.csv", "tag");

        writeToFile(tenants, "tenants.csv", "tenant");

        writeDatasetsToCsv();

        writeOwnerIdsToCsv(owners.values());

        return new GraphStats(datasets.size());
    }

    private void writeToFile(List<String> values, String fileName, String header) {
        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write(header);
            writer.write("\n");
            for (String value : values) {
                writer.write(value + "\n");
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    public List<Object> getDatasetIds() {
        return datasetIds;
    }

    private Map<String, List<Object>> generateVertices(int number, Supplier<Vertex> generate) {
        graph.tx().open();
        Map<String, List<Object>> idsByTenant = new HashMap<>();
        for (int i = 0; i < number; i++) {
            Vertex vertex = generate.get();
            String tenant = vertex.property("tenant").value().toString();
            List<Object> ids = idsByTenant.getOrDefault(tenant, new ArrayList<>());
            ids.add(vertex.id());
            idsByTenant.put(tenant, ids);
        }
        graph.tx().commit();
        return idsByTenant;
    }

    private String getTenant() {
        return tenants.get(random.nextInt(tenants.size()));
    }

    private Vertex generateUser() {
        Vertex userVertex = graph.addVertex("User");
        userVertex.property("name", RandomStringUtils.randomAlphabetic(5, 10));
        userVertex.property("email", RandomStringUtils.randomAlphabetic(10, 12));
        userVertex.property("tenant", getTenant());
        return userVertex;
    }

    private Vertex generateConnection() {
        Vertex connection = graph.addVertex("Connection");
        connection.property("url", RandomStringUtils.randomAlphabetic(10, 15));
        connection.property("name", RandomStringUtils.randomAlphabetic(5, 10));
        connection.property("type", RandomStringUtils.randomAlphabetic(5, 10));
        connection.property("tenant", getTenant());
        return connection;
    }

    private Vertex generateDataset() {
        Vertex dataset = graph.addVertex("Dataset");
        String name = RandomStringUtils.randomAlphabetic(5, 15);
        String description = RandomStringUtils.randomAlphabetic(15, 25);
        String tenant = getTenant();
        dataset.property("name", name);
        dataset.property("description", description);
        dataset.property("tenant", tenant);
        Set<String> assignedTags = new HashSet<>();
        for (int i = 0; i <= random.nextInt(6); i++) {
            assignedTags.add(tags.get(i));
        }
        dataset.property("tags", assignedTags);

        datasets.add(new Dataset((Long) dataset.id(), name, description, assignedTags, tenant));
        return dataset;
    }

    private Vertex generateAsset() {
        Vertex asset = graph.addVertex("Asset");
        asset.property("name", RandomStringUtils.randomAlphabetic(5, 10));
        asset.property("description", RandomStringUtils.randomAlphabetic(15, 25));
        asset.property("tenant", getTenant());
        return asset;
    }

    private void generateEdges(int number, String label, List<Object> idsFrom, List<Object> idsTo) {
        graph.tx().open();
        GraphTraversalSource traversal = graph.traversal();
        for (int i = 0; i < number; i++) {
            Vertex from = traversal.V(getRandomId(idsFrom)).next();
            Vertex to = traversal.V(getRandomId(idsTo)).next();
            from.addEdge(label, to);
        }
        graph.tx().commit();
    }

    private void generateResidesEdges(List<Object> assetsIds, List<Object> connectionIds) {
        graph.tx().open();
        GraphTraversalSource traversal = graph.traversal();
        for (Object assetId : assetsIds) {
            Vertex from = traversal.V(assetId).next();
            Vertex to = traversal.V(getRandomId(connectionIds)).next();
            Edge resides = from.addEdge("resides", to);
            resides.property("path", RandomStringUtils.randomAlphabetic(5, 7));
        }
        graph.tx().commit();
    }

    private void generateOwnsEdges(List<Object> datasetIds, List<Object> ownersIds) {
        graph.tx().open();
        GraphTraversalSource traversal = graph.traversal();
        for (Object datasetId : datasetIds) {
            Vertex to = traversal.V(datasetId).next();
            int numOwners = random.nextInt(2) + 1;
            for (int i = 0; i < numOwners; i++) {
                Vertex from = traversal.V(getRandomId(ownersIds)).next();
                from.addEdge("owns", to);
            }
        }
        graph.tx().commit();
    }

    private Object getRandomId(List<Object> ids) {
        return ids.get(random.nextInt(ids.size()));
    }

    public void generateInvalidObjects() {
        graph.tx().open();

        Vertex dataset = graph.addVertex("Dataset");
        dataset.property("name", "TestInvalidSchema");
        dataset.property("description", "Object that does not satisfy schema");
        dataset.property("tenant", "MP");
        dataset.property("location", "some location");

        Vertex asset = graph.addVertex("Asset");
        asset.property("name", "AssetWithWrongRelation");
        asset.addEdge("hasAsset", dataset);

        graph.tx().commit();
    }

    private void defineSchema() {
        JanusGraphManagement management = graph.openManagement();

        EdgeLabel ownsLabel = management.makeEdgeLabel("owns").directed().multiplicity(Multiplicity.MULTI)
                .make(); //dataset has multiple owners, owners can make multiple datasets
        EdgeLabel hasAsset = management.makeEdgeLabel("hasAsset").directed().multiplicity(Multiplicity.MULTI)
                .make(); // asset can be a part of multiple datasets, dataset can have multiple assets
        EdgeLabel residesLabel = management.makeEdgeLabel("resides").directed().multiplicity(Multiplicity.MANY2ONE)
                .make(); //asset can only reside in one connection, but there might be multiple assets there
        EdgeLabel requestsLabel = management.makeEdgeLabel("requests").directed().multiplicity(Multiplicity.MULTI).make();

        VertexLabel userVertex = management.makeVertexLabel("User").make();
        VertexLabel datasetVertex = management.makeVertexLabel("Dataset").make();
        VertexLabel assetVertex = management.makeVertexLabel("Asset").make();
        VertexLabel connectionVertex = management.makeVertexLabel("Connection").make();

        PropertyKey tenant = management.makePropertyKey("tenant").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        PropertyKey name = management.makePropertyKey("name").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        PropertyKey description = management.makePropertyKey("description").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        PropertyKey path = management.makePropertyKey("path").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        PropertyKey url = management.makePropertyKey("url").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        PropertyKey tagsProps = management.makePropertyKey("tags").dataType(String.class).cardinality(Cardinality.SET).make();
        PropertyKey type = management.makePropertyKey("type").cardinality(Cardinality.SINGLE).dataType(String.class).make();

        management.addProperties(residesLabel, path);
        management.addProperties(userVertex, tenant, name, description);
        management.addProperties(datasetVertex, tenant, name, description, tagsProps);
        management.addProperties(assetVertex, name, description, tenant);
        management.addProperties(connectionVertex, tenant, name, description, url, type);

        management.addConnection(ownsLabel, userVertex, datasetVertex);
        management.addConnection(hasAsset, datasetVertex, assetVertex);
        management.addConnection(residesLabel, assetVertex, connectionVertex);
        management.addConnection(requestsLabel, userVertex, datasetVertex);

        management.buildIndex("byNameUnique", Vertex.class).addKey(name).unique().buildCompositeIndex();

        management.buildIndex("byTenant", Vertex.class).addKey(tenant).buildCompositeIndex();

        management.buildIndex("searchByTag", Vertex.class).addKey(tagsProps, Mapping.TEXTSTRING.asParameter()).buildMixedIndex("search");

        management.buildIndex("searchByDescription", Vertex.class).addKey(description, Mapping.TEXT.asParameter()).buildMixedIndex("search");

        management.commit();
    }

    private void writeDatasetsToCsv() {
        try (FileWriter out = new FileWriter("dataset.csv")) {
            String header = "id,name,description,tags,tenant";
            out.write(header);
            out.write("\n");
            for (Dataset dataset : datasets) {
                out.write(dataset.toCsvString());
                out.write("\n");
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    private void writeOwnerIdsToCsv(Collection<List<Object>> values) {
        try (FileWriter out = new FileWriter("owners.csv")) {
            String header = "id";
            out.write(header);
            out.write("\n");
            for (Object id : values.stream().flatMap(List::stream).collect(Collectors.toList())) {
                out.write(id.toString());
                out.write("\n");
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

}

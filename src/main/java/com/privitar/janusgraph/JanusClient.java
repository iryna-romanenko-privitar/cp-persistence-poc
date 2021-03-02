package com.privitar.janusgraph;

import com.google.common.collect.Lists;
import java.nio.file.Paths;
import javax.enterprise.context.ApplicationScoped;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

@ApplicationScoped
public class JanusClient {

    private final JanusGraph instance;

    public JanusClient() {
        String pathToConf = Paths.get("")
                .toAbsolutePath() + "/classes/janusgraph-remote.properties";
        instance = JanusGraphFactory.open(pathToConf);
    }

    public JanusClient(String path) {
        instance = JanusGraphFactory.open(path);
    }

    public JanusGraph getInstance() {
        return instance;
    }

    private static void createInitialData(JanusGraph graph) {
        Vertex userVertex = graph.addVertex("User");
        userVertex.property("name", "Jane Doe");
        userVertex.property("email", "jane.doe@unknown.is");
        userVertex.property("tenant", "VBP");

        Vertex dataset = graph.addVertex("Dataset");
        dataset.property("name", "tested patients");
        dataset.property("description", "Information about patients registered with the practice and tested for Covid-19");
        dataset.property("tenant", "VBP");
        dataset.property("tags", Lists.newArrayList("patiens", "tests", "covid-19", "sensitive"));

        Vertex patientsAsset = graph.addVertex("Asset");
        patientsAsset.property("name", "patients");
        patientsAsset.property("description", "Patients registered with the practice");
        patientsAsset.property("tenant", "VBP");

        Vertex testsAsset = graph.addVertex("Asset");
        testsAsset.property("name", "tests");
        testsAsset.property("description", "Covid-19 test results");
        testsAsset.property("tenant", "VBP");

        Vertex jdbcConnection = graph.addVertex("Connection");
        jdbcConnection.property("url", "jdbc://some@connection.tada.tam");

        userVertex.addEdge("owns", dataset);
        dataset.addEdge("hasAsset", patientsAsset);
        dataset.addEdge("hasAsset", testsAsset);
        Edge testsConnection = testsAsset.addEdge("resides", jdbcConnection);
        testsConnection.property("path", "tests");
        Edge patientsConnection = patientsAsset.addEdge("resides", jdbcConnection);
        patientsConnection.property("path", "patiens");

        Vertex dataAnalyst = graph.addVertex("User");
        dataAnalyst.property("name", "Isaac Einstein");
        dataAnalyst.property("email", "relative@anywhere.com");
        dataAnalyst.addEdge("requests", dataset);

        graph.tx().commit();
    }


}

package com.privitar.janusgraph;

import com.privitar.janusgraph.domain.CursorPage;
import com.privitar.janusgraph.domain.Dataset;
import com.privitar.janusgraph.domain.Page;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.janusgraph.core.JanusGraph;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

@ApplicationScoped
public class PersistenceService {

    private final JanusGraph graph;
    private final GraphTraversalSource g;
    private final RestClient esClient = RestClient
            .builder(new HttpHost("localhost", 9200, "http")).build();

    private static final String[] BUCKETS = new String[]{
            "00", "08", "10", "18", "20", "28", "30", "38", "40", "48",
            "50", "58", "60", "68", "70", "78", "80", "88", "90", "98",
            "a0", "a8", "b0", "b8", "c0", "c8", "d0", "d8", "e0", "e8",
            "f0", "f8"
    };

    @Inject
    public PersistenceService(JanusClient client) {
        this.graph = client.getInstance();
        this.g = graph.traversal();
    }

    public Page<Dataset> getDatasetsA(int offset, int count) {
        List<Vertex> vertices = g.V()
                .hasLabel("Dataset")
//                .order()
                .skip(offset).next(count);
        List<Dataset> datasets = vertices.stream().map(PersistenceService::toDataset).collect(Collectors.toList());
        return new Page<>(datasets, offset, datasets.size());
    }

    public CursorPage<Dataset> getDatasetsB(String cursor, int count) {
        int bucket = 0, offset = 0;

        if (cursor != null) {
            String[] s = cursor.split(":", 2);
            bucket = Integer.parseInt(s[0]);
            offset = Integer.parseInt(s[1]);
        }


        List<Dataset> result = new ArrayList<>();
        while (count > 0) {
            List<Dataset> tmp;
            if (bucket + 1 < BUCKETS.length) {
                tmp = g.V()
//                        .hasLabel("Dataset")
                        .has("id", P.gte(BUCKETS[bucket]))
                        .has("id", P.lt(BUCKETS[bucket + 1]))
                        .order()
                        .skip(offset)
                        .next(count)
                        .stream().map(PersistenceService::toDataset)
                        .collect(Collectors.toList());
                result.addAll(tmp);
                if (count == tmp.size()) {
                    offset += tmp.size();
                } else {
                    bucket++;
                    offset = 0;
                }
                count -= tmp.size();

            } else {
                tmp = g.V()
//                        .hasLabel("Dataset")
                        .has("id", P.gte(BUCKETS[bucket]))
                        .order()
                        .skip(offset)
                        .next(count)
                        .stream().map(PersistenceService::toDataset)
                        .collect(Collectors.toList());
                result.addAll(tmp);
                break;
            }
        }
        String newCursor = bucket + ":" + offset;
        return new CursorPage<>(result, newCursor, result.size());
    }

    public CursorPage<Dataset> getDatasetsB2(String cursor, int count) {
        // does not preserve ordering!!
        if (cursor == null) {
            cursor = "";
        }

        List<Dataset> datasets = g.V()
            .has("id", P.gte(cursor))
            .next(count).stream().map(PersistenceService::toDataset)
            .collect(Collectors.toList());

        if (!datasets.isEmpty()) {
            cursor = datasets.get(datasets.size() - 1).getId().toString();
        }

        return new CursorPage<>(datasets, cursor, datasets.size());
    }

    public Page<Dataset> getDatasetsC(int offset, int count) {
        List<Dataset> datasets = graph.indexQuery("searchByName", "v.name:*").offset(offset).limit(count).vertexStream()
                .map(result -> toDataset(result.getElement())).collect(Collectors.toList());
        return new Page<>(datasets, offset, datasets.size());
    }

    public Page<Dataset> getDatasetsC2(int offset, int count) throws IOException, ParseException {
        Request request = new Request(
                "GET",
                "/janusgraph_byuniqueid/_search?from=" + offset + "&size=" + count);

        Response response = esClient.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Object json = new JSONParser().parse(responseBody);
        JSONArray hits = (JSONArray) ((Map)((Map) json).get("hits")).get("hits");

        List<Dataset> datasets = new ArrayList<>();
        for (Object item: hits) {
            String id = (String)((Map)((Map) item).get("_source")).get("id");
            datasets.add(new Dataset(UUID.fromString(id), ""));
        }

        return new Page<>(datasets, offset, datasets.size());
    }

    private static Dataset toDataset(Vertex datasetVertex) {
        return new Dataset(UUID.fromString(datasetVertex.value("id").toString()), datasetVertex.value("name").toString());
    }


}

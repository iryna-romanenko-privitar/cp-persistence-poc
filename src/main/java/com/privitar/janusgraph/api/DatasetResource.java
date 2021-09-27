package com.privitar.janusgraph.api;

import com.privitar.janusgraph.PersistenceService;
import com.privitar.janusgraph.domain.CursorPage;
import com.privitar.janusgraph.domain.Dataset;
import com.privitar.janusgraph.domain.Page;
import java.io.IOException;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.print.attribute.standard.Media;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.annotations.jaxrs.QueryParam;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
@RequestScoped
public class DatasetResource {
    Logger LOGGER = LoggerFactory.getLogger(DatasetResource.class);

    @Inject
    PersistenceService service;


    @GET
    @Path("/datasets/A")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatasetsA(@QueryParam("offset") int offset, @QueryParam("count") int count) {
        Page<Dataset> page = service.getDatasetsA(offset, count);
        return Response.ok(page).build();
    }

    @GET
    @Path("/datasets/B")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatasetsB(@QueryParam("cursor") String cursor, @QueryParam("count") int count) {
        CursorPage<Dataset> page = service.getDatasetsB(cursor, count);
        return Response.ok(page).build();
    }

    @GET
    @Path("/datasets/B2")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatasetsD(@QueryParam("cursor") String cursor, @QueryParam("count") int count) {
        CursorPage<Dataset> page = service.getDatasetsB2(cursor, count);
        return Response.ok(page).build();
    }

    @GET
    @Path("/datasets/C")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatasetsC(@QueryParam("offset") int offset, @QueryParam("count") int count) {
        Page<Dataset> page = service.getDatasetsC(offset, count);
        return Response.ok(page).build();
    }

    @GET
    @Path("/datasets/C2")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatasetsC2(@QueryParam("offset") int offset, @QueryParam("count") int count) throws IOException, ParseException {
        Page<Dataset> page = service.getDatasetsC2(offset, count);
        return Response.ok(page).build();
    }



//    @GET
//    @Path("{tenant}/dataset/{datasetId}")
//    @Produces(MediaType.APPLICATION_JSON)
//    public Response getDataset(@PathParam String tenant, @PathParam String datasetId) {
//        LOGGER.debug("Requesting dataset by id {}", datasetId);
//        Optional<Dataset> dataset = service.getDatasetById(tenant, datasetId);
//        if(dataset.isPresent()) {
//            return Response.ok(dataset.get()).build();
//        }
//        return Response.status(Status.NOT_FOUND).build();
//    }
//
//    @GET
//    @Path("{tenant}/datasets")
//    @Produces(MediaType.APPLICATION_JSON)
//    public Response getDatasets(@PathParam String tenant) {
//        LOGGER.info("Requesting datasets for tenant {}", tenant);
//        return Response.ok(service.getDatasetsByTenant(tenant)).build();
//    }
//
//    @GET
//    @Path("{tenant}/datasets/my/{owner}")
//    @Produces(MediaType.APPLICATION_JSON)
//    public Response getDatasetsByOwner(@PathParam String tenant, @PathParam Long owner) {
//        LOGGER.info("Requesting datasets for owner {}", owner);
//        return Response.ok(service.getDatasetsByOwner(tenant, owner)).build();
//    }
//
//    @GET
//    @Path("{tenant}/datasets/find/{tag}")
//    @Produces(MediaType.APPLICATION_JSON)
//    public Response findByTag(@PathParam String tenant, @PathParam String tag) {
//        LOGGER.info("Requesting datasets for tag {}", tag);
//        return Response.ok(service.findByTag(tenant, tag)).build();
//    }


}

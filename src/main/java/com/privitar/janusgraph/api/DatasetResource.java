package com.privitar.janusgraph.api;

import com.privitar.janusgraph.PersistenceService;
import com.privitar.janusgraph.data.GraphGenerator;
import com.privitar.janusgraph.domain.Dataset;
import java.util.Optional;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.jboss.resteasy.annotations.jaxrs.PathParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
@RequestScoped
public class DatasetResource {
    Logger LOGGER = LoggerFactory.getLogger(DatasetResource.class);

    @Inject
    PersistenceService service;


    @GET
    @Path("{tenant}/dataset/{datasetId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDataset(@PathParam String tenant, @PathParam String datasetId) {
        LOGGER.debug("Requesting dataset by id {}", datasetId);
        Optional<Dataset> dataset = service.getDatasetById(tenant, datasetId);
        if(dataset.isPresent()) {
            return Response.ok(dataset.get()).build();
        }
        return Response.status(Status.NOT_FOUND).build();
    }

    @GET
    @Path("{tenant}/datasets")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatasets(@PathParam String tenant) {
        LOGGER.info("Requesting datasets for tenant {}", tenant);
        return Response.ok(service.getDatasetsByTenant(tenant)).build();
    }

    @GET
    @Path("{tenant}/datasets/my/{owner}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatasetsByOwner(@PathParam String tenant, @PathParam Long owner) {
        LOGGER.info("Requesting datasets for owner {}", owner);
        return Response.ok(service.getDatasetsByOwner(tenant, owner)).build();
    }

    @GET
    @Path("{tenant}/datasets/find/{tag}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response findByTag(@PathParam String tenant, @PathParam String tag) {
        LOGGER.info("Requesting datasets for tag {}", tag);
        return Response.ok(service.findByTag(tenant, tag)).build();
    }


}

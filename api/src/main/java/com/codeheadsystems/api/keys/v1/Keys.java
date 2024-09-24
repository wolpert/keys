package com.codeheadsystems.api.keys.v1;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * The interface Keys.
 */
@Path("/v1/keys")
public interface Keys {

  /**
   * Create key. Should return a 201 for the newly created resources unless the user
   * is not authenticated, or an internal error occurred.
   *
   * @return the key in the resource.
   */
  @POST
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  Response create();

  /**
   * Get key.
   *
   * @param uuid the uuid
   * @return the key
   */
  @GET
  @Path("/{uuid}")
  @Produces(MediaType.APPLICATION_JSON)
  Key read(@PathParam("uuid") final String uuid);

  /**
   * Get key.
   *
   * @param uuid the uuid
   * @return the key
   */
  @DELETE
  @Path("/{uuid}")
  Response delete(@PathParam("uuid") final String uuid);

}

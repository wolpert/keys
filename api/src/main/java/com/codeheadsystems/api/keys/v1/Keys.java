package com.codeheadsystems.api.keys.v1;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * The interface Keys.
 */
@Path("/v1/keys")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface Keys {

  /**
   * Create key.
   *
   * @return the key
   */
  @PUT
  @Path("/")
  Key create();

  /**
   * Get key.
   *
   * @param uuid the uuid
   * @return the key
   */
  @GET
  @Path("/{uuid}")
  Key get(@PathParam("uuid") final String uuid);

}

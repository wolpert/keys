package com.codeheadsystems.server.resource;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.UriInfo;
import java.util.List;
import java.util.Optional;
import org.glassfish.jersey.server.internal.process.Endpoint;
import org.glassfish.jersey.server.internal.routing.UriRoutingContext;
import org.glassfish.jersey.server.model.ResourceMethodInvoker;
import org.glassfish.jersey.uri.UriTemplate;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The interface Request decomposition.
 */
@Value.Immutable
public interface RequestDecomposition {

  /**
   * Method string.
   *
   * @return the string
   */
  String method();

  /**
   * Endpoint optional.
   *
   * @return the optional
   */
  String endpoint();

  /**
   * Resource optional.
   *
   * @return the optional
   */
  String resource();

  /**
   * Path string.
   *
   * @return the string
   */
  String path();

  /**
   * The type Factory.
   */
  class Factory {

    private static final Logger LOGGER = LoggerFactory.getLogger(Factory.class.getName());
    private static final String UNKNOWN = "unknown";

    /**
     * Generate request decomposition.
     *
     * @param requestContext the request context
     * @return the request decomposition
     */
    public RequestDecomposition generate(final ContainerRequestContext requestContext) {
      LOGGER.trace("generate({})", requestContext);
      final String method = requestContext.getMethod();
      final Optional<UriRoutingContext> uriRoutingContext = uriRoutingContext(requestContext);
      final String endpoint = uriRoutingContext
          .map(this::endpoint)
          .orElse(UNKNOWN);
      final String resource = uriRoutingContext
          .map(this::resourceMethodInvoker)
          .map(this::resource)
          .orElse(UNKNOWN);
      return ImmutableRequestDecomposition.builder()
          .method(method)
          .endpoint(endpoint)
          .resource(resource)
          .path(requestContext.getUriInfo().getPath())
          .build();
    }

    private Optional<UriRoutingContext> uriRoutingContext(final ContainerRequestContext requestContext) {
      final UriInfo uriInfo = requestContext.getUriInfo();
      if (uriInfo instanceof final UriRoutingContext uriRoutingContext) {
        return Optional.of(uriRoutingContext);
      } else {
        LOGGER.warn("Not a URI routing context: {}:{}", requestContext.getMethod(), requestContext.getUriInfo().getPath());
        return Optional.empty();
      }
    }

    private ResourceMethodInvoker resourceMethodInvoker(final UriRoutingContext uriRoutingContext) {
      final Endpoint endpoint = uriRoutingContext.getEndpoint();
      if (endpoint instanceof final ResourceMethodInvoker resourceMethodInvoker) {
        return resourceMethodInvoker;
      } else {
        LOGGER.warn("Not a ResourceMethodInvoker: {}:{}", uriRoutingContext.getResourceMethod(), uriRoutingContext.getAbsolutePath());
        return null;
      }
    }

    private String endpoint(final UriRoutingContext uriRoutingContext) {
      List<UriTemplate> templates = uriRoutingContext.getMatchedTemplates();
      if (templates.isEmpty()) {
        LOGGER.warn("No matched templates: {}:{}", uriRoutingContext.getResourceMethod(), uriRoutingContext.getAbsolutePath());
        return UNKNOWN;
      } else {
        // The last template represents the most specific path that matches the resource.
        return templates.getLast().getTemplate();
      }
    }

    private String resource(final ResourceMethodInvoker resourceMethodInvoker) {
      return String.format("%s.%s",
          resourceMethodInvoker.getResourceClass().getSimpleName(),
          resourceMethodInvoker.getResourceMethod().getName()
      );
    }
  }

}

/*
 * Copyright (c) 2023. Ned Wolpert
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.codeheadsystems.server.resource;

import com.codeheadsystems.metrics.MetricFactory;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.glassfish.jersey.server.internal.process.Endpoint;
import org.glassfish.jersey.server.internal.routing.UriRoutingContext;
import org.glassfish.jersey.server.model.ResourceMethodInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Used so that we can have set the default tags needed for metrics.
 */
@Singleton
public class MetricTagsResource implements ContainerRequestFilter, ContainerResponseFilter, JerseyResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricTagsResource.class);
  private final MetricFactory metricFactory;

  private final ThreadLocal<MetricFactory.MetricsContext> metricsContextThreadLocal = new ThreadLocal<>();

  /**
   * Default constructor.
   *
   * @param metricFactory metrics object to set the tags.
   */
  @Inject
  public MetricTagsResource(final MetricFactory metricFactory) {
    this.metricFactory = metricFactory;
    LOGGER.info("MetricTagsResource({})", metricFactory);
  }

  private Optional<ResourceMethodInvoker> extract(final ContainerRequestContext requestContext) {
    final UriInfo uriInfo = requestContext.getUriInfo();
    if (!(uriInfo instanceof UriRoutingContext)) {
      LOGGER.warn("Not a URI routing context: {}:{}", requestContext.getMethod(), requestContext.getUriInfo().getPath());
      return Optional.empty();
    }
    final UriRoutingContext routingContext = (UriRoutingContext) requestContext.getUriInfo();
    final Endpoint endpoint = routingContext.getEndpoint();
    if (endpoint instanceof final ResourceMethodInvoker resourceMethodInvoker) {
      return Optional.of(resourceMethodInvoker);
    } else {
      LOGGER.warn("No endpoint: {}:{}", requestContext.getMethod(), requestContext.getUriInfo().getPath());
      return Optional.empty();
    }
  }

  private String getResource(final ResourceMethodInvoker resourceMethodInvoker) {
    return String.format("%s.%s",
        resourceMethodInvoker.getResourceClass().getSimpleName(),
        resourceMethodInvoker.getResourceMethod().getName()
    );
  }

  private String getResource(ContainerRequestContext requestContext) {
    return extract(requestContext)
        .map(this::getResource)
        .orElse("unknown");
  }

  private String endpoint(final ResourceMethodInvoker resourceMethodInvoker) {
    final String p1 = resourceMethodInvoker.getResourceClass().getSimpleName();
    final String p2 = resourceMethodInvoker.getResourceMethod().getName();
    return String.format("%s.%s", p1, p2);
  }

  /**
   * Sets the default tags.
   *
   * @param requestContext request context.
   * @throws IOException if anything goes wrong.
   */
  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {
    final MetricFactory.MetricsContext oldContext = metricsContextThreadLocal.get();
    if (oldContext != null) {
      LOGGER.debug("Metrics context already set, clearing. {}", oldContext);
      metricFactory.disableMetricsContext(oldContext);
    }
    final MetricFactory.MetricsContext context = metricFactory.enableMetricsContext();
    metricsContextThreadLocal.set(context);
    MDC.put("trace", UUID.randomUUID().toString());
    final String path = requestContext.getUriInfo().getPath();
    metricFactory.and("resource", getResource(requestContext));
    LOGGER.trace("MetricTagsResource.filter start:{}", path);
  }

  /**
   * Clears all tags at this point.
   *
   * @param requestContext  request context.
   * @param responseContext response context.
   * @throws IOException if anything goes wrong.
   */
  @Override
  public void filter(final ContainerRequestContext requestContext,
                     final ContainerResponseContext responseContext) throws IOException {
    final String path = requestContext.getUriInfo().getPath();
    LOGGER.trace("MetricTagsResource.filter end:{}", path);
    MDC.clear();
    final MetricFactory.MetricsContext context = metricsContextThreadLocal.get();
    if (context == null) {
      // This can happen if there is no resource found for this endpoint. Not really a problem because we cannot start it.
      LOGGER.debug("No metrics context found for path:{}", path);
    } else {
      // TODO: the metric name really needs to be the path being requested.
      // Wd don't use path directly because we cannot add UUIDs or the like to metric names.
      final String endpoint = extract(requestContext)
          .map(this::endpoint)
          .orElse("unknown");
      metricFactory.publishTime("endpoint-" + endpoint,
          context.duration(), metricFactory.and(
              "status", Integer.toString(responseContext.getStatus()),
              "method", requestContext.getMethod()
          ));
      metricFactory.disableMetricsContext(context);
      metricsContextThreadLocal.remove();
    }
  }
}

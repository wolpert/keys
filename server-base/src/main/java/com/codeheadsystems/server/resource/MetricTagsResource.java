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
import java.io.IOException;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Used so that we can have set the default tags needed for metrics.
 */
@Singleton
public class MetricTagsResource implements ContainerRequestFilter, ContainerResponseFilter, JerseyResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricTagsResource.class);
  private static final String DECOMPOSITION = "decomposition";
  private final MetricFactory metricFactory;
  private final RequestDecomposition.Factory requestDecompositionFactory;

  private final ThreadLocal<MetricFactory.MetricsContext> metricsContextThreadLocal = new ThreadLocal<>();

  /**
   * Default constructor.
   *
   * @param metricFactory               metrics object to set the tags.
   * @param requestDecompositionFactory request decomposition factory.
   */
  @Inject
  public MetricTagsResource(final MetricFactory metricFactory,
                            final RequestDecomposition.Factory requestDecompositionFactory) {
    this.metricFactory = metricFactory;
    this.requestDecompositionFactory = requestDecompositionFactory;
    LOGGER.info("MetricTagsResource({})", metricFactory);
  }

  /**
   * Sets the default tags.
   *
   * @param requestContext request context.
   * @throws IOException if anything goes wrong.
   */
  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {
    final RequestDecomposition decomposition = requestDecompositionFactory.generate(requestContext);
    requestContext.setProperty(DECOMPOSITION, decomposition);
    final MetricFactory.MetricsContext oldContext = metricsContextThreadLocal.get();
    if (oldContext != null) {
      LOGGER.debug("Metrics context already set, clearing. {}", oldContext);
      metricFactory.disableMetricsContext(oldContext);
    }
    final MetricFactory.MetricsContext context = metricFactory.enableMetricsContext();
    metricsContextThreadLocal.set(context);
    MDC.put("trace", UUID.randomUUID().toString());
    metricFactory.and("endpoint", decomposition.method() + " " + decomposition.endpoint());
    LOGGER.trace("MetricTagsResource.filter start:{}", decomposition.path());
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
    RequestDecomposition decomposition = (RequestDecomposition) requestContext.getProperty(DECOMPOSITION);
    if (decomposition == null) {
      LOGGER.warn("No request decomposition found");
      decomposition = requestDecompositionFactory.generate(requestContext);
    }
    LOGGER.trace("MetricTagsResource.filter end:{}", decomposition.path());
    MDC.clear();
    final MetricFactory.MetricsContext context = metricsContextThreadLocal.get();
    if (context == null) {
      // This can happen if there is no resource found for this endpoint. Not really a problem because we cannot start it.
      LOGGER.debug("No metrics context found for path:{}", decomposition.path());
    } else {
      metricFactory.publishTime("resource-" + decomposition.resource(),
          context.duration(), metricFactory.and(
              "status", Integer.toString(responseContext.getStatus())
          ));
      metricFactory.disableMetricsContext(context);
      metricsContextThreadLocal.remove();
    }
  }
}

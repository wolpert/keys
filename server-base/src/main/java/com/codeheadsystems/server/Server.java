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

package com.codeheadsystems.server;

import com.codeheadsystems.metrics.Metrics;
import com.codeheadsystems.metrics.declarative.DeclarativeFactory;
import com.codeheadsystems.server.component.DropWizardComponent;
import com.codeheadsystems.server.module.DropWizardModule;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This is our application itself. Very little here is node specific.
 *
 * @param <T> the type parameter
 */
public abstract class Server<T extends ServerConfiguration> extends Application<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

  /**
   * Default constructor.
   */
  public Server() {
    LOGGER.info("Server()");
  }

  /**
   * Implement this method to return the dropwizard component we will use.
   *
   * @param module for Server created stuff.
   * @return dropwizard component.
   */
  protected abstract DropWizardComponent dropWizardComponent(final DropWizardModule module);

  @DeclarativeFactory
  protected Metrics metrics(DropWizardComponent component) {
    return component.metrics();
  }

  /**
   * Runs the application.
   *
   * @param configuration the parsed object
   * @param environment   the application's {@link Environment}
   * @throws Exception if everything dies.
   */
  @Override
  public void run(final T configuration,
                  final Environment environment) throws Exception {
    MDC.put("trace", "init-" + getName() + "-" + UUID.randomUUID());
    LOGGER.info("run({},{})", configuration, environment);
    LOGGER.info("\n---\n--- Server Setup Starting ---\n---");
    final DropWizardModule module = new DropWizardModule(environment, configuration);
    final DropWizardComponent component = dropWizardComponent(module);
    final Metrics metrics = metrics(component);
    LOGGER.info("Metrics: {}", metrics); // ensures it is created in a declarative fashion.
    component.serverInitializer().initialize();
    LOGGER.info("\n---\n--- Server Setup Complete ---\n---");
    MDC.clear();
  }

}

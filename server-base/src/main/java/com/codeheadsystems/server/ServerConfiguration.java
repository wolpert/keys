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

import io.dropwizard.core.Configuration;

/**
 * A base server configuration for our stuff.
 */
public class ServerConfiguration extends Configuration {

  private String stage = "dev"; // dev, test, alpha, beta, gamma, prod
  private boolean useTestJavaClient = false;

  /**
   * Instantiates a new Server configuration.
   */
  public ServerConfiguration() {
    super();
  }

  /**
   * The stage of deployment.
   *
   * @return value. stage
   */
  public String getStage() {
    return stage;
  }

  /**
   * The stage of deployment.
   *
   * @param stage value.
   */
  public void setStage(final String stage) {
    this.stage = stage;
  }

  /**
   * Is use test java client boolean.
   *
   * @return the boolean
   */
  public boolean isUseTestJavaClient() {
    return useTestJavaClient;
  }

  /**
   * Sets use test java client.
   *
   * @param useTestJavaClient the use test java client
   */
  public void setUseTestJavaClient(final boolean useTestJavaClient) {
    this.useTestJavaClient = useTestJavaClient;
  }
}

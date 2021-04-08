/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.deployment.internal;

import java.io.File;
import java.util.ServiceLoader;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.deployment.internal.exception.ServiceLoadingFailureException;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Factory responsible for loading and holding the singleton instance of the
 * {@link JarDeploymentService}.
 * Use this class to retrieve the instance rather than creating one.
 *
 * @since Geode 1.15
 */
@Experimental
public class JarDeploymentServiceFactory {

  private static final Logger logger = LogService.getLogger();

  @Immutable
  private static final JarDeploymentService jarDeploymentService = createJarDeploymentService();

  private static JarDeploymentService createJarDeploymentService() {
    ServiceLoader<JarDeploymentService> jarDeploymentServices =
        ServiceLoader.load(JarDeploymentService.class);
    if (jarDeploymentServices.iterator().hasNext()) {
      return jarDeploymentServices.iterator().next();
    } else {
      throw new ServiceLoadingFailureException(
          "No implementation of JarDeploymentService could be loaded.");
    }
  }

  /**
   * Gets the current instance of the {@link JarDeploymentService}.
   *
   * @return current instance of the {@link JarDeploymentService}.
   */
  public static JarDeploymentService getJarDeploymentServiceInstance() {
    return jarDeploymentService;
  }

  /**
   * Reconfigures the {@link JarDeploymentService} instance with a new working directory.
   * This cannot be called when there is anything deployed, so it should be called before doing any
   * deployments.
   *
   * @param workingDir a {@link File} representing the new working directory to to deploy jars into.
   * @return the reconfigured instance of the {@link JarDeploymentService}.
   */
  public static JarDeploymentService reinitializeJarDeploymentServiceWithWorkingDirectory(
      File workingDir) {
    jarDeploymentService.reinitializeWithWorkingDirectory(workingDir);
    return jarDeploymentService;
  }

  /**
   * Shuts down the {@link JarDeploymentService}.
   */
  public static void shutdownJarDeploymentService() {
    jarDeploymentService.close();
  }
}

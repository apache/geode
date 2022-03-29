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
package org.apache.geode.deployment.internal.modules.processors;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

import org.apache.logging.log4j.Logger;

import org.apache.geode.deployment.internal.modules.processors.impl.DefaultJarProcessor;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

public class DeploymentResourceProcessor {
  private static final Logger logger = LogService.getLogger();

  public static ServiceResult<List<String>> process(Deployment deployment) {
    List<JarProcessor> jarProcessors = loadProcessors();
    if (jarProcessors.size() > 0) {
      JarProcessor jarProcessor = getJarProcessor(jarProcessors, deployment.getFile());
      return Success.of(jarProcessor.getResourcesFromJarFile(deployment.getFile()));
    }
    return Failure.of("No JarProcessor registered for deployment:" + deployment.getId());
  }

  private static JarProcessor getJarProcessor(List<JarProcessor> jarProcessors, File file) {
    Optional<JarProcessor> firstJarProcessor = jarProcessors.stream()
        .filter(jarProcessor -> !DefaultJarProcessor.DEFAULT_IDENTIFIER
            .equals(jarProcessor.getIdentifier()) && jarProcessor.canProcess(file))
        .findFirst();
    return firstJarProcessor.orElseGet(() -> jarProcessors.stream()
        .filter(jarProcessor -> DefaultJarProcessor.DEFAULT_IDENTIFIER
            .equals(jarProcessor.getIdentifier()))
        .findFirst().orElse(null));
  }

  private static List<JarProcessor> loadProcessors() {
    List<JarProcessor> jarProcessors = new LinkedList<>();
    ServiceLoader<JarProcessor> loadedJarProcessor = ServiceLoader.load(JarProcessor.class);
    loadedJarProcessor.forEach(jarProcessor -> {
      logger.debug("Registering jarProcessor: " + jarProcessor.getIdentifier());
      jarProcessors.add(jarProcessor);
    });
    return jarProcessors;
  }
}

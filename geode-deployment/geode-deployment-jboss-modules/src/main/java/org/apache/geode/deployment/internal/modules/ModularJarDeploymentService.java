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
package org.apache.geode.deployment.internal.modules;

import java.io.File;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.internal.execute.FunctionToFileTracker;
import org.apache.geode.deployment.internal.JarDeploymentService;
import org.apache.geode.deployment.internal.modules.service.DeploymentService;
import org.apache.geode.deployment.internal.modules.service.GeodeJBossDeploymentService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.utils.JarFileUtils;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

/**
 * Implementation of {@link JarDeploymentService} to be used when the system is started using
 * classloader isolation.
 *
 * @since Geode 1.16
 */
public class ModularJarDeploymentService implements JarDeploymentService {

  private static final Logger logger = LogService.getLogger();
  private static final String CORE_MODULE_NAME = "geode";

  private final Map<String, Deployment> deployments = new ConcurrentHashMap<>();
  private final FunctionToFileTracker functionToFileTracker = new FunctionToFileTracker();
  private final DeploymentService geodeJBossDeploymentService;

  public ModularJarDeploymentService() {
    this(new GeodeJBossDeploymentService());
  }

  @VisibleForTesting
  protected ModularJarDeploymentService(DeploymentService deploymentService) {
    this.geodeJBossDeploymentService = deploymentService;
  }

  @Override
  public synchronized ServiceResult<Deployment> deploy(Deployment deployment) {
    logger.debug("Deploying: {}", deployment);

    ServiceResult<String> isDeploymentValidResult = validateDeployment(deployment);
    if (isDeploymentValidResult.isFailure()) {
      return Failure.of(isDeploymentValidResult.getErrorMessage());
    }
    String artifactId = JarFileUtils.getArtifactId(deployment.getFileName());

    Deployment existingDeployment = deployments.get(artifactId);
    if (existingDeployment != null
        && JarFileUtils.hasSameContent(existingDeployment.getFile(), deployment.getFile())) {
      return Success.of(null);
    }

    List<String> moduleDependencies = deployment.getDependencies();
    moduleDependencies.add(CORE_MODULE_NAME);
    moduleDependencies.add("java.se");

    boolean moduleRegistered =
        geodeJBossDeploymentService
            .registerModule(artifactId, deployment.getFilePath(), moduleDependencies);
    logger.debug("Register module result: {} for deployment: {}", moduleRegistered,
        artifactId);

    if (moduleRegistered) {
      return registerFunctions(deployment);
    } else {
      return Failure.of("Module could not be registered");
    }
  }

  private ServiceResult<String> validateDeployment(Deployment deployment) {
    if (deployment == null) {
      return Failure.of("Deployment may not be null");
    }
    if (deployment.getFile() == null) {
      return Failure.of("Cannot deploy Deployment without jar file");
    }
    return Success.of("true");
  }

  private ServiceResult<Deployment> registerFunctions(Deployment deployment) {
    Deployment deploymentCopy = new Deployment(deployment, deployment.getFile());
    String artifactId = JarFileUtils.getArtifactId(deployment.getFileName());
    deploymentCopy.setDeployedTime(Instant.now().toString());
    logger.debug("Deployments before: {}", deployments.size());
    deployments.put(artifactId, deploymentCopy);
    logger.debug("Deployments after: {}", deployments.size());
    try {
      functionToFileTracker.registerFunctionsFromFile(deployment.getFile());
    } catch (Throwable t) {
      undeploy(artifactId);
      return Failure.of(t);
    } finally {
      flushCaches();
    }
    return Success.of(deploymentCopy);
  }

  @Override
  public synchronized ServiceResult<Deployment> deploy(File file) {
    if (file == null) {
      return Failure.of("Jar file may not be null");
    }
    Deployment deployment = new Deployment(file.getName(), "", Instant.now().toString());
    deployment.setFile(file);
    return deploy(deployment);
  }

  @Override
  public List<Deployment> listDeployed() {
    return new LinkedList<>(deployments.values());
  }

  @Override
  public ServiceResult<Deployment> getDeployed(String jarName) {
    String artifactId = JarFileUtils.getArtifactId(jarName);
    if (!deployments.containsKey(artifactId)) {
      return Failure.of("No deployment found for name: " + jarName);
    }

    return Success.of(deployments.get(artifactId));
  }

  @Override
  public void reinitializeWithWorkingDirectory(File workingDirectory) {
    // this is a no-op when using classloader isolation
  }

  @Override
  public void loadJarsFromWorkingDirectory() {
    // this is a no-op when using classloader isolation. Use cluster configuration.
  }

  @Override
  public void close() {
    for (Deployment deployment : deployments.values()) {
      undeploy(deployment.getFileName());
    }
  }

  /**
   * Removes jars from the system by their file name.
   *
   * @param fileName the name of a jar that has previously been deployed.
   * @return a {@link ServiceResult} containing a {@link Deployment} representing the removed jar
   *         when successful and an error message if the file could not be found or undeployed.
   */
  public ServiceResult<Deployment> undeploy(String fileName) {
    String artifactId = JarFileUtils.getArtifactId(fileName);
    if (!deployments.containsKey(artifactId)) {
      return Failure.of(fileName + " not deployed");
    }

    boolean serviceResult = geodeJBossDeploymentService.unregisterModule(artifactId);
    if (serviceResult) {
      Deployment removedDeployment = deployments.remove(artifactId);
      if (removedDeployment != null) {
        functionToFileTracker.unregisterFunctionsForDeployment(removedDeployment.getFileName());
        return Success.of(removedDeployment);
      }
    }
    return Failure.of("Module: " + fileName + "  could not be undeployed");
  }

  /**
   * Flush the type registry after possibly receiving new types or having old types replaced.
   */
  private synchronized void flushCaches() {
    try {
      TypeRegistry typeRegistry = ((InternalCache) CacheFactory.getAnyInstance()).getPdxRegistry();
      if (typeRegistry != null) {
        typeRegistry.flushCache();
      }
    } catch (CacheClosedException ignored) {
      // That's okay, it just means there was nothing to flush to begin with
    }
  }
}

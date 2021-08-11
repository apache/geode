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
package org.apache.geode.deployment.internal.legacy;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.internal.execute.FunctionToFileTracker;
import org.apache.geode.deployment.internal.DeployedJar;
import org.apache.geode.deployment.internal.JarDeployer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.deployment.JarDeploymentService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.utils.JarFileUtils;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

/**
 * Implementation of {@link JarDeploymentService} that encapsulates the standard method of deploying
 * jars by delegating to the {@link JarDeployer}.
 *
 * @since Geode 1.15
 */
@Experimental
public class LegacyJarDeploymentService implements JarDeploymentService {

  private static final Logger logger = LogService.getLogger();

  private final Map<String, Deployment> deployments = new ConcurrentHashMap<>();
  private final FunctionToFileTracker functionToFileTracker = new FunctionToFileTracker();
  private JarDeployer jarDeployer = new JarDeployer();

  @Override
  public synchronized ServiceResult<Deployment> deploy(Deployment deployment) {
    try {
      DeployedJar deployedJar = jarDeployer.deploy(deployment.getFile());
      // This means that the jars are already deployed, so we call it a Success.
      if (deployedJar == null) {
        logger.info("Jar already been deployed: {}", deployment);
        // null is returned when already deployed to satisfy existing public API constraints
        return Success.of(null);
      } else {
        // This will only iterate once since deploy is given a singleton list.
        logger.debug("Adding Deployment: {} with DeployedJar: {}", deployment, deployedJar);
        Deployment deploymentCopy = new Deployment(deployment, deployedJar.getFile());
        deploymentCopy.setDeployedTime(Instant.now().toString());
        deployments.put(JarFileUtils.getArtifactId(deploymentCopy.getFileName()), deploymentCopy);
        logger.debug("Deployments List size after add: {}", deployments.size());
        logger.debug("Deployment copy to return: {}", deploymentCopy);
        try {
          functionToFileTracker.registerFunctionsFromFile(deploymentCopy.getFile());
        } catch (ClassNotFoundException | IOException e) {
          jarDeployer.undeploy(JarFileUtils.getArtifactId(deployment.getFileName()));
          return Failure.of(e);
        }
        return Success.of(deploymentCopy);
      }
    } catch (IOException | ClassNotFoundException e) {
      return Failure.of(e);
    } finally {
      flushCaches();
    }
  }

  @Override
  public ServiceResult<Deployment> deploy(File file) {
    return deploy(createDeployment(file));
  }

  @Override
  public synchronized ServiceResult<Deployment> undeployByFileName(String fileName) {
    try {
      logger.debug("Deployments List size before undeploy: {}", deployments.size());
      logger.debug("File being undeployed: {}", fileName);

      jarDeployer.deleteAllVersionsOfJar(fileName);

      String artifactId = JarFileUtils.getArtifactId(fileName);

      Deployment deployment = deployments.get(artifactId);
      if (deployment == null || !deployment.getFileName().equals(fileName)) {
        return Failure.of(fileName + " not deployed");
      }

      Deployment removedDeployment = deployments.remove(artifactId);
      logger.debug("Removed Deployment: {}", removedDeployment);
      logger.debug("Deployments List size after remove: {}", deployments.size());
      String undeployedPath =
          jarDeployer.undeploy(JarFileUtils.getArtifactId(removedDeployment.getFileName()));
      logger.debug("Jar at path: {} removed by JarDeployer", undeployedPath);
      functionToFileTracker.unregisterFunctionsForDeployment(removedDeployment.getFileName());
      return Success.of(removedDeployment);
    } catch (IOException e) {
      return Failure.of(e);
    } finally {
      flushCaches();
    }
  }

  @Override
  public List<Deployment> listDeployed() {
    return Collections.unmodifiableList(new LinkedList<>(deployments.values()));
  }

  @Override
  public ServiceResult<Deployment> getDeployed(String fileName) {
    logger.debug("Looking up Deployment for name: {}", fileName);
    logger.debug("Deployments keySet: {}", Arrays.toString(deployments.keySet().toArray()));
    String artifactId = JarFileUtils.getArtifactId(fileName);
    if (deployments.containsKey(artifactId)) {
      return Success.of(deployments.get(artifactId));
    } else {
      return Failure.of(fileName + " is not deployed.");
    }
  }

  @Override
  public void reinitializeWithWorkingDirectory(File workingDirectory) {
    logger.info("Reinitializing JarDeploymentService with new working directory: {}",
        workingDirectory);
    if (deployments.isEmpty()) {
      this.jarDeployer = new JarDeployer(workingDirectory);
    } else {
      throw new RuntimeException(
          "Cannot reinitialize working directory with existing deployments. Please undeploy first.");
    }
  }

  @Override
  public void loadJarsFromWorkingDirectory() {
    logger.debug("Loading jars from Working Directory");
    Map<String, DeployedJar> latestVersionOfJarsOnDisk = jarDeployer.getLatestVersionOfJarsOnDisk();
    try {
      List<Deployment> deploymentList = new LinkedList<>();
      for (Map.Entry<String, DeployedJar> entry : latestVersionOfJarsOnDisk.entrySet()) {
        DeployedJar deployedJar = entry.getValue();
        jarDeployer.registerNewVersions(entry.getKey(), deployedJar);
        logger.debug("Deploying DeployedJar: {} from working directory", deployedJar);
        Deployment deployment = createDeployment(deployedJar);
        deploymentList.add(deployment);
        deployments.put(JarFileUtils.toArtifactId(deployment.getFileName()), deployment);
      }
      for (Deployment deployment : deploymentList) {
        functionToFileTracker.registerFunctionsFromFile(deployment.getFile());
      }
    } catch (ClassNotFoundException | IOException e) {
      logger.error(e);
      throw new RuntimeException(e);
    } finally {
      flushCaches();
    }
  }

  @Override
  public void close() {
    String[] jarNames = deployments.keySet().toArray(new String[] {});
    logger.debug(
        "Closing LegacyJarDeploymentService. The following Deployments will be removed: {}",
        Arrays.toString(jarNames));
    for (String jarName : jarNames) {
      undeployByFileName(jarName);
    }
  }

  private Deployment createDeployment(DeployedJar deployedJar) {
    Deployment deployment = new Deployment(deployedJar.getArtifactId(), "",
        Instant.ofEpochMilli(deployedJar.getFile().lastModified()).toString());
    deployment.setFile(deployedJar.getFile());
    return deployment;
  }

  private Deployment createDeployment(File deployedJar) {
    Deployment deployment = new Deployment(deployedJar.getName(), "",
        Instant.ofEpochMilli(deployedJar.lastModified()).toString());
    deployment.setFile(deployedJar);
    return deployment;
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

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
package org.apache.geode.deployment.impl.legacy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.deployment.JarDeploymentService;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

/**
 * Implementation of {@link JarDeploymentService} that encpsulates the standard method of deploying
 * jars by delegating to the {@link JarDeployer}.
 *
 * @since Geode 1.15
 */
@Experimental
public class LegacyJarDeploymentService implements JarDeploymentService {

  private static final Logger logger = LogService.getLogger();

  private final Map<String, Deployment> deployments;
  private JarDeployer jarDeployer;

  public LegacyJarDeploymentService() {
    this.jarDeployer = new JarDeployer();
    this.deployments = new ConcurrentHashMap<>();
  }

  @Override
  public synchronized ServiceResult<Deployment> deploy(Deployment deployment) {
    try {
      Map<String, DeployedJar> deployedJars =
          jarDeployer.deploy(Collections.singleton(deployment.getFile()));
      // This means that the jars are already deployed, so we call it a Success.
      if (deployedJars == null || deployedJars.isEmpty()) {
        // null is returned when already deployed to satisfy existing public API constraints
        return Success.of(null);
      } else {
        // This will only iterate once since deploy is given a singleton list.
        for (Map.Entry<String, DeployedJar> deployedJarEntry : deployedJars.entrySet()) {
          DeployedJar deployedJar = deployedJarEntry.getValue();
          if (deployedJar == null) {
            // null is returned when already deployed to satisfy existing public API constraints
            return Success.of(null);
          }
          Deployment deploymentCopy = new Deployment(deployment, deployedJar.getFile());
          deploymentCopy.setDeployedTime(Instant.now().toString());
          deployments.put(deploymentCopy.getDeploymentName(), deploymentCopy);
          return Success.of(deploymentCopy);
        }
        // null is returned when already deployed to satisfy existing public API constraints
        return Success.of(null);
      }
    } catch (IOException | ClassNotFoundException e) {
      return Failure.of(e);
    }
  }

  @Override
  public ServiceResult<Deployment> deploy(File file) {
    return deploy(createDeployment(file));
  }

  @Override
  public synchronized ServiceResult<Deployment> undeployByDeploymentName(String deploymentName) {
    try {
      Deployment deployment = deployments.get(deploymentName);

      if (deployment == null) {
        return Failure.of("No deployment found for name: " + deploymentName);
      }

      Deployment removedDeployment = deployments.remove(deploymentName);
      jarDeployer.undeploy(deployment);
      return Success.of(removedDeployment);
    } catch (IOException e) {
      return Failure.of(e);
    }
  }

  @Override
  public ServiceResult<Deployment> undeployByFileName(String fileName) {
    List<String> deploymentNamesFromFileName =
        listDeployed().stream()
            .filter(deployment -> deployment.getFileName().equals(fileName))
            .map(Deployment::getDeploymentName)
            .collect(Collectors.toList());
    if (deploymentNamesFromFileName.size() > 1) {
      return Failure.of("There are multiple deployments with file: " + fileName);
    } else if (deploymentNamesFromFileName.isEmpty()) {
      return Failure.of(fileName + " not deployed");
    } else {
      return undeployByDeploymentName(deploymentNamesFromFileName.get(0));
    }
  }

  @Override
  public List<Deployment> listDeployed() {
    return new LinkedList<>(deployments.values());
  }

  @Override
  public ServiceResult<Deployment> getDeployed(String deploymentName) {
    if (deployments.containsKey(deploymentName)) {
      return Success.of(deployments.get(deploymentName));
    } else {
      return Failure.of(deploymentName + " is not deployed.");
    }
  }

  @Override
  public void reinitializeWithWorkingDirectory(File workingDirectory) {
    if (deployments.isEmpty()) {
      this.jarDeployer = new JarDeployer(workingDirectory);
    } else {
      throw new RuntimeException(
          "Cannot reinitialize working directory with existing deployments. Please undeploy first.");
    }
  }

  @Override
  public Map<Path, File> backupJars(Path backupDirectory) throws IOException {
    return jarDeployer.backupJars(backupDirectory);
  }

  @Override
  public void loadJarsFromWorkingDirectory() {
    Map<String, DeployedJar> latestVersionOfJarsOnDisk = jarDeployer.getLatestVersionOfJarsOnDisk();
    try {
      jarDeployer.registerNewVersions(latestVersionOfJarsOnDisk);
      for (DeployedJar deployedJar : latestVersionOfJarsOnDisk.values()) {
        Deployment deployment = createDeployment(deployedJar);
        deployments.put(deployment.getDeploymentName(), deployment);
      }
    } catch (ClassNotFoundException e) {
      logger.error(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    deployments.keySet().forEach(this::undeployByDeploymentName);
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
}

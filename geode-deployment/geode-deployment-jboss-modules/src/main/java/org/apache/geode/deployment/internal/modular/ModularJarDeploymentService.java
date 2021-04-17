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
package org.apache.geode.deployment.internal.modular;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

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
 * Implementation of {@link JarDeploymentService} to be used when the system is started in a modular
 * fashion.
 *
 * @since Geode 1.15
 */
public class ModularJarDeploymentService implements JarDeploymentService {

  private static final String GEODE_CORE_MODULE_NAME = "geode-core";
  private final Logger logger = LogService.getLogger();
  private final Map<String, Deployment> deployments = new ConcurrentHashMap<>();
  private final FunctionToFileTracker functionToFileTracker = new FunctionToFileTracker();
  private final DeploymentService deploymentService;
  private File workingDirectory = new File(System.getProperty("user.dir"));

  public ModularJarDeploymentService() {
    this(new GeodeJBossDeploymentService());
  }

  public ModularJarDeploymentService(DeploymentService deploymentService) {
    this.deploymentService = deploymentService;
  }

  @Override
  public synchronized ServiceResult<Deployment> deploy(Deployment deployment) {
    logger.debug("Deploying: {}", deployment);
    if (deployment == null) {
      return Failure.of("Deployment may not be null");
    }
    if (deployment.getFile() == null) {
      return Failure.of("Cannot deploy Deployment without jar file");
    }

    Deployment existingDeployment = deployments.get(deployment.getDeploymentName());
    if (existingDeployment != null
        && JarFileUtils.hasSameContent(existingDeployment.getFile(), deployment.getFile())) {
      return Success.of(null);
    }

    // Copy the file to the working directory.
    try {
      File deployedFile = new File(workingDirectory, deployment.getFile().getName());
      FileUtils.copyFile(deployment.getFile(), deployedFile);
      deployment.setFile(deployedFile);
    } catch (IOException e) {
      return Failure.of(e);
    }

    List<String> moduleDependencies = new LinkedList<>(deployment.getModuleDependencyNames());
    moduleDependencies.add(GEODE_CORE_MODULE_NAME);

    boolean serviceResult =
        deploymentService
            .registerModule(deployment.getDeploymentName(), deployment.getFilePath(),
                moduleDependencies);
    logger.debug("Register module result: {} for deployment: {}", serviceResult,
        deployment.getDeploymentName());

    if (serviceResult) {
      Deployment deploymentCopy = new Deployment(deployment);
      deploymentCopy.setDeployedTime(Instant.now().toString());
      logger.debug("Deployments before: {}", deployments.size());
      deployments.put(deploymentCopy.getDeploymentName(), deploymentCopy);
      logger.debug("Deployments after: {}", deployments.size());
      try {
        functionToFileTracker.registerFunctionsFromFile(deployment.getDeploymentName(),
            deployment.getFile());
      } catch (ClassNotFoundException | IOException e) {
        undeployByDeploymentName(deployment.getDeploymentName());
        return Failure.of(e);
      } finally {
        flushCaches();
      }
      return Success.of(deploymentCopy);
    } else {
      return Failure.of("Module could not be registered");
    }
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
  public synchronized ServiceResult<Deployment> undeployByDeploymentName(String deploymentName) {
    if (!deployments.containsKey(deploymentName)) {
      return Failure.of("No deployment found for name: " + deploymentName);
    }

    boolean serviceResult =
        deploymentService.unregisterModule(deploymentName);
    if (serviceResult) {
      Deployment removedDeployment = deployments.remove(deploymentName);
      functionToFileTracker.unregisterFunctionsForDeployment(removedDeployment.getDeploymentName());
      return Success.of(removedDeployment);
    } else {
      return Failure.of("Module could not be undeployed");
    }
  }

  @Override
  public List<Deployment> listDeployed() {
    return new LinkedList<>(deployments.values());
  }

  @Override
  public ServiceResult<Deployment> getDeployed(String deploymentName) {
    if (!deployments.containsKey(deploymentName)) {
      return Failure.of("No deployment found for name: " + deploymentName);
    }

    return Success.of(deployments.get(deploymentName));
  }

  @Override
  public void reinitializeWithWorkingDirectory(File workingDirectory) {
    if (deployments.isEmpty()) {
      this.workingDirectory = workingDirectory;
    } else {
      throw new RuntimeException(
          "Cannot reinitialize working directory with existing deployments. Please undeploy first.");
    }
  }

  @Override
  public void loadJarsFromWorkingDirectory() {
    File[] files = workingDirectory.listFiles(
        pathname -> pathname.getAbsolutePath().endsWith(".jar"));

    for (File file : files) {
      ServiceResult<Deployment> serviceResult = deploy(file);
      if (serviceResult.isSuccessful()) {
        Deployment deployment = serviceResult.getMessage();
        logger.info("Registering new version of jar: {}", deployment.getDeploymentName());
      } else {
        logger.error(serviceResult.getErrorMessage());
      }
    }
  }

  @Override
  public void close() {
    deployments.keySet().forEach(this::undeployByDeploymentName);
  }

  /**
   * Removes jars from the system by their file name.
   *
   * @param fileName the name of a jar that has previously been deployed.
   * @return a {@link ServiceResult} containing a {@link Deployment} representing the removed jar
   *         when successful and an error message if the file could not be found or undeployed.
   */
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

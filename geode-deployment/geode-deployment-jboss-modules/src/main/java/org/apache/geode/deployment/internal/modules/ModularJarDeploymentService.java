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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.internal.execute.FunctionToFileTracker;
import org.apache.geode.deployment.internal.JarDeploymentService;
import org.apache.geode.deployment.internal.modules.service.GeodeJBossModuleService;
import org.apache.geode.deployment.internal.modules.service.ModuleService;
import org.apache.geode.deployment.internal.modules.xml.generator.GeodeJBossXmlGenerator;
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
  private static final String defaultApplicationName = "default_application";
  private static final String DEPLOYMENTS_DIR = "deployments";
  private Path deploymentsDirectory = Paths.get(System.getProperty("user.dir"));
  private static final GeodeJBossXmlGenerator generator = new GeodeJBossXmlGenerator();

  private final Map<String, Deployment> deployments = new ConcurrentHashMap<>();
  private final FunctionToFileTracker functionToFileTracker = new FunctionToFileTracker();
  private final ModuleService moduleService;

  public ModularJarDeploymentService() {
    this(new GeodeJBossModuleService());
  }

  @VisibleForTesting
  protected ModularJarDeploymentService(ModuleService moduleService) {
    this.moduleService = moduleService;
  }

  @Override
  public synchronized ServiceResult<Deployment> deploy(Deployment deployment) {
    logger.debug("Deploying: {}", deployment);

    ServiceResult<String> isDeploymentValidResult = validateDeployment(deployment);
    if (isDeploymentValidResult.isFailure()) {
      return Failure.of(isDeploymentValidResult.getErrorMessage());
    }

    String moduleName = JarFileUtils.getArtifactId(deployment.getFileName());
    Deployment existingDeployment = deployments.get(moduleName);
    if (existingDeployment != null
        && JarFileUtils.hasSameContent(existingDeployment.getFile(), deployment.getFile())) {
      return Success.of(null);
    }

    // if no application is specified, assume the default
    String applicationName = deployment.getApplicationName() != null
        ? deployment.getApplicationName() : defaultApplicationName;

    List<String> moduleDependencies = getModuleDependencies(deployment, applicationName);

    ServiceResult<Boolean> dependencyValidationResult = validateDependencies(moduleDependencies);
    if (dependencyValidationResult.isFailure()) {
      return Failure.of(dependencyValidationResult.getErrorMessage());
    }

    if (moduleService.moduleExists(moduleName)) {
      return updateModule(deployment, moduleName, applicationName, moduleDependencies);
    } else {
      return createModule(deployment, moduleName, applicationName, moduleDependencies);
    }
  }

  private List<String> getModuleDependencies(Deployment deployment, String applicationName) {
    List<String> moduleDependencies = deployment.getDependencies();
    moduleDependencies.add(applicationName);
    moduleDependencies.add(CORE_MODULE_NAME);
    moduleDependencies.add("java.se");
    return moduleDependencies;
  }

  private ServiceResult<Deployment> updateModule(Deployment deployment,
      String moduleName,
      String applicationName,
      List<String> moduleDependencies) {
    moduleService.unregisterModule(moduleName);
    return createModule(deployment, moduleName, applicationName, moduleDependencies);
  }

  private ServiceResult<Deployment> createModule(Deployment deployment,
      String moduleName,
      String applicationName,
      List<String> moduleDependencies) {
    ServiceResult<Deployment> deploymentStructureResult =
        createDeploymentStructure(deployment, moduleName, moduleDependencies);
    if (deploymentStructureResult.isFailure()) {
      return deploymentStructureResult;
    }

    boolean moduleRegistered = moduleService.linkModule(moduleName, applicationName, true);

    logger.debug("Register module result: {} for deployment: {}", moduleRegistered,
        moduleName);

    if (moduleRegistered) {
      Deployment deploymentCopy = new Deployment(deployment, deployment.getFile());
      deploymentCopy.setDeployedTime(Instant.now().toString());
      deployments.put(moduleName, deploymentCopy);
      return registerFunctions(deploymentCopy);
    } else {
      return Failure.of("Module could not be registered");
    }
  }

  private ServiceResult<Boolean> validateDependencies(List<String> moduleDependencies) {
    for (String moduleDependency : moduleDependencies) {
      if (!moduleService.moduleExists(moduleDependency)) {
        return Failure.of("Cannot find module: " + moduleDependency);
      }
    }
    return Success.of(true);
  }

  private ServiceResult<Deployment> createDeploymentStructure(Deployment deployment,
      String artifactId,
      List<String> moduleDependencies) {
    // copy jar to lib
    try {
      File jarFileInLibDirectory =
          deploymentsDirectory.resolve(artifactId).resolve("main").resolve("lib")
              .resolve(deployment.getFileName()).toFile();
      FileUtils.copyFile(deployment.getFile(), jarFileInLibDirectory);
      deployment.setFile(jarFileInLibDirectory);
    } catch (IOException e) {
      return Failure.of(e);
    }

    List<String> expandedResources;
    try {
      expandedResources = explodeJarFile(deployment).stream().map(File::getAbsolutePath)
          .collect(Collectors.toList());
    } catch (IOException e) {
      return Failure.of(e);
    }

    // generate module.xml
    generator.generate(deploymentsDirectory,
        artifactId, null, expandedResources, moduleDependencies,
        false);
    return Success.of(deployment);
  }

  private List<File> explodeJarFile(Deployment deployment) throws IOException {
    List<File> resources = new LinkedList<>();
    File deployedFile = deployment.getFile();
    resources.add(deployedFile);

    try (JarFile jarFile = new JarFile(deployedFile)) {
      Enumeration<JarEntry> entries = jarFile.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        if (!entry.isDirectory() && entry.getName().endsWith(".jar")) {
          // extract file into directory and add it as a resource
          File extractedJarFile = extractInnerJarFile(deployedFile, jarFile, entry);
          resources.add(extractedJarFile);
        }
      }
    }
    return resources;
  }

  private File extractInnerJarFile(File deployedFile, JarFile jarFile, JarEntry entry)
      throws IOException {
    File extractedJarFile = deployedFile.getParentFile().toPath().resolve(entry.getName()).toFile();
    if (!extractedJarFile.toPath().normalize().startsWith(deployedFile.getParentFile().toPath())) {
      throw new IOException("Jar entry has invalid path");
    }
    InputStream inputStream = jarFile.getInputStream(entry);
    FileOutputStream outputStream = new FileOutputStream(extractedJarFile);

    while (inputStream.available() > 0) {
      outputStream.write(inputStream.read());
    }

    outputStream.close();
    inputStream.close();

    return extractedJarFile;
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
    String artifactId = JarFileUtils.getArtifactId(deployment.getFileName());
    try {
      functionToFileTracker.registerFunctionsFromFile(deployment.getFile());
    } catch (Throwable t) {
      undeploy(artifactId);
      return Failure.of(t);
    } finally {
      flushCaches();
    }
    return Success.of(deployment);
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
    if (workingDirectory == null) {
      deploymentsDirectory = Paths.get(System.getProperty("user.dir"));
    } else {
      deploymentsDirectory = workingDirectory.toPath().resolve(DEPLOYMENTS_DIR);
    }
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

    boolean serviceResult = moduleService.unregisterModule(artifactId);
    if (serviceResult) {
      Deployment removedDeployment = deployments.remove(artifactId);
      if (removedDeployment != null) {
        functionToFileTracker.unregisterFunctionsForDeployment(removedDeployment.getFileName());
        try {
          Files.walk(deploymentsDirectory.resolve(artifactId))
              .map(Path::toFile)
              .sorted((o1, o2) -> -o1.compareTo(o2))
              .forEach(File::delete);
        } catch (IOException e) {
          return Failure.of(e);
        }
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

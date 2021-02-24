/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.deployment;

import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.utils.JarFileUtils;

public class JarDeployer implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();
  // The pound version scheme predates the sequenced version scheme
  private static final Pattern POUND_VERSION_SCHEME =
      Pattern.compile("^vf\\.gf#(?<artifact>.*)\\.jar#(?<version>\\d+)$");

  @MakeNotStatic
  private static final Lock lock = new ReentrantLock();

  private final Map<String, DeployedJar> deployedJars = new ConcurrentHashMap<>();
  private final File deployDirectory;

  public JarDeployer() {
    this(new File(System.getProperty("user.dir")));
  }

  public JarDeployer(final File deployDirectory) {
    this.deployDirectory = deployDirectory;
  }

  /**
   * Writes the jarBytes for the given jarName to the next version of that jar file (if the bytes do
   * not match the latest deployed version).
   *
   * @return the DeployedJar that was written from jarBytes, or null if those bytes matched the
   *         latest deployed version
   */
  public DeployedJar deployWithoutRegistering(final File stagedJar)
      throws IOException {
    lock.lock();
    String stagedJarName = stagedJar.getName();
    String artifactId = JarFileUtils.getArtifactId(stagedJarName);
    try {
      boolean shouldDeployNewVersion = shouldDeployNewVersion(artifactId, stagedJar);
      if (!shouldDeployNewVersion) {
        logger.debug("No need to deploy a new version of {}", stagedJarName);
        return null;
      }

      verifyWritableDeployDirectory();
      Path deployedFile = getNextVersionedJarFile(stagedJarName).toPath();
      FileUtils.copyFile(stagedJar, deployedFile.toFile());

      return new DeployedJar(deployedFile.toFile());
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get a list of all currently deployed jars.
   *
   * @return The list of DeployedJars
   */
  private List<DeployedJar> findDeployedJars() {
    return new ArrayList<>(getDeployedJars().values());
  }

  /**
   * @param backupDirectory path to the directory to backup jars to.
   * @return a map of paths to the backup to the source file.
   * @throws IOException if the jars cannot be copied to backupDirectory
   */
  public Map<Path, File> backupJars(Path backupDirectory) throws IOException {
    try {
      // Suspend any user deployed jar file updates during this backup.
      lock.lock();

      Map<Path, File> backupDefinition = new HashMap<>();

      List<DeployedJar> jarList = findDeployedJars();
      for (DeployedJar jar : jarList) {
        File source = jar.getFile();
        String sourceFileName = source.getName();
        Path destination = backupDirectory.resolve(sourceFileName);
        Files.copy(source.toPath(), destination, StandardCopyOption.COPY_ATTRIBUTES);
        backupDefinition.put(destination, source);
        logger.debug("File: " + source.getName() + " backed up to: " + destination);
      }

      return backupDefinition;
    } finally {
      lock.unlock();
    }
  }


  protected File getNextVersionedJarFile(String unversionedJarName) {
    int maxVersion = getMaxVersion(JarFileUtils.getArtifactId(unversionedJarName));

    String nextVersionJarName =
        FilenameUtils.getBaseName(unversionedJarName) + ".v" + (maxVersion + 1) + ".jar";

    logger.debug("Next versioned jar name for {} is {}", unversionedJarName, nextVersionJarName);

    return new File(deployDirectory, nextVersionJarName);
  }

  protected int getMaxVersion(String artifactId) {
    return Arrays.stream(deployDirectory.list()).filter(x -> artifactId.equals(
        JarFileUtils.toArtifactId(x)))
        .map(JarFileUtils::extractVersionFromFilename)
        .reduce(Integer::max).orElse(0);
  }

  /**
   * Make sure that the deploy directory is writable.
   *
   * @throws IOException If the directory isn't writable
   */
  public void verifyWritableDeployDirectory() throws IOException {
    try {
      if (this.deployDirectory.canWrite()) {
        return;
      }
    } catch (SecurityException ex) {
      throw new IOException("Unable to write to deploy directory", ex);
    }

    throw new IOException(
        "Unable to write to deploy directory: " + this.deployDirectory.getCanonicalPath());
  }

  /*
   * In Geode 1.1.0, the deployed version of 'myjar.jar' would be named 'vf.gf#myjar.jar#1'. Now it
   * is be named 'myjar.v1.jar'. We need to rename all existing deployed jars to the new convention
   * if this is the first time starting up with the new naming format.
   */
  protected void renameJarsWithOldNamingConvention() throws IOException {
    Set<File> jarsWithOldNamingConvention = findJarsWithOldNamingConvention();

    if (jarsWithOldNamingConvention.isEmpty()) {
      return;
    }

    for (File jar : jarsWithOldNamingConvention) {
      renameJarWithOldNamingConvention(jar);
    }
  }

  protected Set<File> findJarsWithOldNamingConvention() {
    return Stream.of(this.deployDirectory.listFiles())
        .filter((File file) -> isOldNamingConvention(file.getName())).collect(toSet());
  }

  protected boolean isOldNamingConvention(String fileName) {
    return POUND_VERSION_SCHEME.matcher(fileName).matches();
  }

  private void renameJarWithOldNamingConvention(File oldJar) throws IOException {
    Matcher matcher = POUND_VERSION_SCHEME.matcher(oldJar.getName());
    if (!matcher.matches()) {
      throw new IllegalArgumentException("The given jar " + oldJar.getCanonicalPath()
          + " does not match the old naming convention");
    }

    String unversionedJarNameWithoutExtension = matcher.group(1);
    String jarVersion = matcher.group(2);
    String newJarName = unversionedJarNameWithoutExtension + ".v" + jarVersion + ".jar";

    File newJar = new File(this.deployDirectory, newJarName);
    logger.debug("Renaming deployed jar from {} to {}", oldJar.getCanonicalPath(),
        newJar.getCanonicalPath());

    FileUtils.moveFile(oldJar, newJar);
  }

  /**
   * Re-deploy all previously deployed JAR files on disk.
   * It will clean up the old version of deployed jars that are in the deployed directory
   */
  public Map<String, DeployedJar> getLatestVersionOfJarsOnDisk() {
    logger.info("Loading previously deployed jars");
    lock.lock();
    try {
      verifyWritableDeployDirectory();
      renameJarsWithOldNamingConvention();
      // find all the artifacts and its max versions
      Map<String, Integer> artifactToMaxVersion = findArtifactsAndMaxVersion();
      Map<String, DeployedJar> latestVersionOfEachJar = new HashMap<>();

      // clean up the old versions and find the latest version of each jar
      for (File file : deployDirectory.listFiles()) {
        String artifactId = JarFileUtils.toArtifactId(file.getName());
        if (artifactId == null) {
          continue;
        }
        int version = JarFileUtils.extractVersionFromFilename(file.getName());
        if (version < artifactToMaxVersion.get(artifactId)) {
          FileUtils.deleteQuietly(file);
        } else {
          DeployedJar deployedJar = new DeployedJar(file);
          latestVersionOfEachJar.put(deployedJar.getArtifactId(), deployedJar);
        }
      }
      return latestVersionOfEachJar;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  Map<String, Integer> findArtifactsAndMaxVersion() {
    Map<String, Integer> artifactToMaxVersion = new HashMap<>();
    for (String fileName : deployDirectory.list()) {
      String artifactId = JarFileUtils.toArtifactId(fileName);
      if (artifactId == null) {
        continue;
      }
      int version = JarFileUtils.extractVersionFromFilename(fileName);
      Integer maxVersion = artifactToMaxVersion.get(artifactId);
      if (maxVersion == null || maxVersion < version) {
        artifactToMaxVersion.put(artifactId, version);
      }
    }
    return artifactToMaxVersion;
  }


  public void registerNewVersions(Map<String, DeployedJar> deployedJars)
      throws ClassNotFoundException {
    lock.lock();
    try {
      for (Map.Entry<String, DeployedJar> mapEntry : deployedJars.entrySet()) {
        DeployedJar deployedJar = mapEntry.getValue();
        if (deployedJar != null) {
          logger.info("Registering new version of jar: {}", mapEntry);
          DeployedJar oldJar = this.deployedJars.put(deployedJar.getArtifactId(), deployedJar);
          ClassPathLoader.getLatest().chainClassloader(deployedJar.getFile(),
              deployedJar.getArtifactId());
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Deploy the given JAR files.
   *
   * When deploying a jar file, it will always append a sequence number .v<digit> to the end of
   * the file, no matter how the original file is named. This is to allow server on startup to
   * know what's the last version that gets deployed without cluster configuration.
   *
   * @param stagedJarFiles A map of Files which have been staged in another location and are ready
   *        to be deployed as a unit.
   * @return An array of newly created JAR class loaders. Entries will be null for an JARs that were
   *         already deployed.
   * @throws IOException When there's an error saving the JAR file to disk
   */
  public Map<String, DeployedJar> deploy(final Set<File> stagedJarFiles)
      throws IOException, ClassNotFoundException {
    Map<String, DeployedJar> deployedJars = new HashMap<>();

    for (File jar : stagedJarFiles) {
      if (!JarFileUtils.hasValidJarContent(jar)) {
        throw new IllegalArgumentException(
            "File does not contain valid JAR content: " + jar.getName());
      }
    }

    lock.lock();
    try {
      for (File stagedJarFile : stagedJarFiles) {
        DeployedJar deployedJar = deployWithoutRegistering(stagedJarFile);
        deployedJars.put(JarFileUtils.getArtifactId(stagedJarFile.getName()), deployedJar);
      }

      registerNewVersions(deployedJars);
      return deployedJars;
    } finally {
      lock.unlock();
    }
  }

  private boolean shouldDeployNewVersion(String artifactId, File stagedJar) throws IOException {
    DeployedJar oldDeployedJar = this.deployedJars.get(artifactId);

    if (oldDeployedJar == null) {
      return true;
    }

    if (oldDeployedJar.hasSameContentAs(stagedJar)) {
      logger.warn("Jar is identical to the latest deployed version: {}",
          oldDeployedJar.getFileCanonicalPath());

      return false;
    }

    return true;
  }

  public Map<String, DeployedJar> getDeployedJars() {
    return Collections.unmodifiableMap(this.deployedJars);
  }

  /**
   * Undeploy the jar file identified by the given artifact ID.
   *
   * @param deployment The Deployment to undeploy
   * @return The path to the location on disk where the JAR file had been deployed
   * @throws IOException If there's a problem deleting the file
   */
  public String undeploy(final Deployment deployment) throws IOException {
    lock.lock();
    logger.debug("JarDeployer Undeploying Deployment: {}", deployment);

    try {
      String artifactId = JarFileUtils.getArtifactId(deployment.getFileName());
      DeployedJar deployedJar = deployedJars.get(artifactId);
      if (deployedJar == null) {
        throw new IllegalArgumentException(deployment.getDeploymentName() + " not deployed");
      }

      logger.debug("JarDeployer deployedJars list before remove: {}",
          Arrays.toString(deployedJars.keySet().toArray()));

      // remove the deployedJar
      deployedJars.remove(artifactId);
      logger.debug("JarDeployer deployedJars list after remove: {}",
          Arrays.toString(deployedJars.keySet().toArray()));
      ClassPathLoader.getLatest().unloadClassloaderForArtifact(deployment.getDeploymentName());
      deleteAllVersionsOfJar(deployment.getFileName());
      return deployedJar.getFileCanonicalPath();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param jarName a user deployed jar name (abc.jar or abc-1.0.jar)
   */
  public void deleteAllVersionsOfJar(String jarName) {
    lock.lock();
    String artifactId = JarFileUtils.getArtifactId(jarName);
    try {
      for (File file : this.deployDirectory.listFiles()) {
        if (artifactId.equals(JarFileUtils.toArtifactId(file.getName()))) {
          logger.info("Deleting: {}", file.getAbsolutePath());
          FileUtils.deleteQuietly(file);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append('@').append(System.identityHashCode(this)).append('{');
    sb.append("deployDirectory=").append(deployDirectory);
    sb.append('}');
    return sb.toString();
  }
}

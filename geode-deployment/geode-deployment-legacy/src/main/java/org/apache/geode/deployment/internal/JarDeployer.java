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
package org.apache.geode.deployment.internal;

import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.geode.classloader.internal.ClassPathLoader;
import org.apache.geode.logging.internal.log4j.api.LogService;
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
    if (deployDirectory == null) {
      this.deployDirectory = new File(System.getProperty("user.dir"));
    } else {
      this.deployDirectory = deployDirectory;
    }
  }

  /**
   * Writes the jarBytes for the given jarName to the next version of that jar file (if the bytes do
   * not match the latest deployed version).
   *
   * @return the DeployedJar that was written from jarBytes, or null if those bytes matched the
   *         latest deployed version
   */
  public DeployedJar deployWithoutRegistering(String artifactId, final File stagedJar)
      throws IOException {
    lock.lock();
    try {
      boolean shouldDeployNewVersion =
          shouldDeployNewVersion(artifactId, stagedJar);
      if (!shouldDeployNewVersion) {
        logger.debug("No need to deploy a new version of {}", stagedJar.getName());
        return null;
      }

      verifyWritableDeployDirectory();
      Path deployedFile = getNextVersionedJarFile(stagedJar.getName()).toPath();
      FileUtils.copyFile(stagedJar, deployedFile.toFile());

      return new DeployedJar(deployedFile.toFile());
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


  public void registerNewVersions(String artifactId, DeployedJar deployedJar)
      throws ClassNotFoundException {
    lock.lock();
    try {
      if (deployedJar != null) {
        logger.info("Registering new version of jar: {}", deployedJar);
        DeployedJar oldJar = this.deployedJars.put(artifactId, deployedJar);
        ClassPathLoader.getLatest().chainClassloader(deployedJar.getFile(), artifactId);
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
   * @param stagedJarFile A File which have been staged in another location and is ready
   *        to be deployed.
   * @return An array of newly created JAR class loaders. Entries will be null for an JARs that were
   *         already deployed.
   * @throws IOException When there's an error saving the JAR file to disk
   */
  public DeployedJar deploy(String artifactId, final File stagedJarFile)
      throws IOException, ClassNotFoundException {

    if (!JarFileUtils.hasValidJarContent(stagedJarFile)) {
      throw new IllegalArgumentException(
          "File does not contain valid JAR content: " + stagedJarFile.getName());
    }

    lock.lock();
    try {
      DeployedJar deployedJar = deployWithoutRegistering(artifactId, stagedJarFile);
      registerNewVersions(artifactId, deployedJar);

      return deployedJar;
    } finally {
      lock.unlock();
    }
  }

  private boolean shouldDeployNewVersion(String deploymentName, File stagedJar) throws IOException {
    DeployedJar oldDeployedJar = this.deployedJars.get(deploymentName);

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
   * @param artifactId The artifact to undeploy
   * @return The path to the location on disk where the JAR file had been deployed
   * @throws IOException If there's a problem deleting the file
   */
  public String undeploy(String artifactId) throws IOException {
    lock.lock();
    logger.debug("JarDeployer Undeploying artifactId: {}", artifactId);

    try {
      logger.debug("JarDeployer deployedJars list before remove: {}",
          Arrays.toString(deployedJars.keySet().toArray()));

      // remove the deployedJar
      DeployedJar deployedJar = deployedJars.remove(artifactId);
      if (deployedJar == null) {
        throw new IllegalArgumentException(artifactId + " not deployed");
      }
      logger.debug("JarDeployer deployedJars list after remove: {}",
          Arrays.toString(deployedJars.keySet().toArray()));
      ClassPathLoader.getLatest().unloadClassloaderForArtifact(artifactId);
      deleteAllVersionsOfJar(deployedJar.getFile().getName());
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
    logger.info("Deleting all versions of jar: {}", jarName);
    String artifactId = JarFileUtils.toArtifactId(jarName);
    if (artifactId == null) {
      artifactId = JarFileUtils.getArtifactId(jarName);
    }
    logger.debug("ArtifactId to delete: {}", artifactId);
    try {
      for (File file : this.deployDirectory.listFiles()) {
        logger.debug("File in deploy directory: {} with artifactId: {}", file.getName(),
            JarFileUtils.toArtifactId(file.getName()));
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

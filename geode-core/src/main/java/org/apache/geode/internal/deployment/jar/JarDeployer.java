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
package org.apache.geode.internal.deployment.jar;

import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.internal.TypeRegistry;

public class JarDeployer implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();
  // The pound version scheme predates the sequenced version scheme
  private static final Pattern POUND_VERSION_SCHEME =
      Pattern.compile("^vf\\.gf#(?<artifact>.*)\\.jar#(?<version>\\d+)$");

  private final Lock lock = new ReentrantLock();

  private final Map<String, DeployedJar> deployedJars = new ConcurrentHashMap<>();
  private final Map<DeployedJar, List<String>> registeredFunctions = new ConcurrentHashMap<>();
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
    String artifactId = DeployJarFileUtils.getArtifactId(stagedJarName);
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
  public List<DeployedJar> findDeployedJars() {
    return new ArrayList<>(getDeployedJars().values());
  }


  /**
   * Suspend all deploy and undeploy operations. This is done by acquiring and holding the lock
   * needed in order to perform a deploy or undeploy and so it will cause all threads attempting to
   * do one of these to block. This makes it somewhat of a time sensitive call as forcing these
   * other threads to block for an extended period of time may cause other unforeseen problems. It
   * must be followed by a call to {@link #resumeAll()}.
   */
  public void suspendAll() {
    lock.lock();
  }

  /**
   * Release the lock that controls entry into the deploy/undeploy methods which will allow those
   * activities to continue.
   */
  public void resumeAll() {
    lock.unlock();
  }


  protected File getNextVersionedJarFile(String unversionedJarName) {
    int maxVersion = getMaxVersion(DeployJarFileUtils.getArtifactId(unversionedJarName));

    String nextVersionJarName =
        FilenameUtils.getBaseName(unversionedJarName) + ".v" + (maxVersion + 1) + ".jar";

    logger.debug("Next versioned jar name for {} is {}", unversionedJarName, nextVersionJarName);

    return new File(deployDirectory, nextVersionJarName);
  }

  protected int getMaxVersion(String artifactId) {
    return Arrays.stream(deployDirectory.list()).filter(
        x -> artifactId.equals(DeployJarFileUtils.getArtifactIdForSequencedJar(x)))
        .map(DeployJarFileUtils::extractVersionFromFilename)
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
  public void loadPreviouslyDeployedJarsFromDisk() {
    logger.info("Loading previously deployed jars");
    lock.lock();
    try {
      verifyWritableDeployDirectory();
      renameJarsWithOldNamingConvention();
      // find all the artifacts and its max versions
      Map<String, Integer> artifactToMaxVersion = findArtifactsAndMaxVersion();
      List<DeployedJar> latestVersionOfEachJar = new ArrayList<>();

      // clean up the old versions and find the latest version of each jar
      for (File file : deployDirectory.listFiles()) {
        String artifactId = DeployJarFileUtils.getArtifactIdForSequencedJar(file.getName());
        if (artifactId == null) {
          continue;
        }
        int version = DeployJarFileUtils.extractVersionFromFilename(file.getName());
        if (version < artifactToMaxVersion.get(artifactId)) {
          FileUtils.deleteQuietly(file);
        } else {
          latestVersionOfEachJar.add(new DeployedJar(file));
        }
      }

      registerNewVersions(latestVersionOfEachJar);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  Map<String, Integer> findArtifactsAndMaxVersion() {
    Map<String, Integer> artifactToMaxVersion = new HashMap<>();
    for (String fileName : deployDirectory.list()) {
      String artifactId = DeployJarFileUtils.getArtifactIdForSequencedJar(fileName);
      if (artifactId == null) {
        continue;
      }
      int version = DeployJarFileUtils.extractVersionFromFilename(fileName);
      Integer maxVersion = artifactToMaxVersion.get(artifactId);
      if (maxVersion == null || maxVersion < version) {
        artifactToMaxVersion.put(artifactId, version);
      }
    }
    return artifactToMaxVersion;
  }


  public List<DeployedJar> registerNewVersions(List<DeployedJar> deployedJars)
      throws ClassNotFoundException {
    lock.lock();
    try {
      Map<DeployedJar, DeployedJar> newVersionToOldVersion = new HashMap<>();

      for (DeployedJar deployedJar : deployedJars) {
        if (deployedJar != null) {
          logger.info("Registering new version of jar: {}", deployedJar);
          DeployedJar oldJar = this.deployedJars.put(deployedJar.getArtifactId(), deployedJar);
          ClassPathLoader.getLatest().chainClassloader(deployedJar);
          newVersionToOldVersion.put(deployedJar, oldJar);
        }
      }

      // Finally, unregister functions that were removed
      for (Map.Entry<DeployedJar, DeployedJar> entry : newVersionToOldVersion.entrySet()) {
        DeployedJar newjar = entry.getKey();
        DeployedJar oldJar = entry.getValue();

        registeredFunctions.put(newjar,
            DeployJarFunctionUtils.registerFunctions(newjar).stream().map(
                Function::getId).collect(Collectors.toList()));

        if (oldJar != null) {
          unregisterFunctionsFromOldJarNotInNewJar(oldJar, newjar);
        }
      }

    } finally {
      lock.unlock();
    }

    return deployedJars;
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
  public List<DeployedJar> deploy(final Collection<File> stagedJarFiles)
      throws IOException, ClassNotFoundException {
    List<DeployedJar> deployedJars = new ArrayList<>(stagedJarFiles.size());

    for (File jar : stagedJarFiles) {
      if (!DeployJarFileUtils.hasValidJarContent(jar)) {
        throw new IllegalArgumentException(
            "File does not contain valid JAR content: " + jar.getName());
      }
    }

    lock.lock();
    try {
      for (File stagedJarFile : stagedJarFiles) {
        deployedJars.add(deployWithoutRegistering(stagedJarFile));
      }

      return registerNewVersions(deployedJars);
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

  /**
   * Returns the latest registered {@link DeployedJar} for the given JarName
   *
   * @param jarName - the unversioned jar name, e.g. myJar.jar
   */
  @VisibleForTesting
  public DeployedJar getDeployedJar(String jarName) {
    return this.deployedJars.get(DeployJarFileUtils.getArtifactId(jarName));
  }

  @VisibleForTesting
  public DeployedJar deploy(final File stagedJarFile)
      throws IOException, ClassNotFoundException {
    lock.lock();

    Set<File> jarFiles = new HashSet<>();
    jarFiles.add(stagedJarFile);

    try {
      List<DeployedJar> deployedJars = deploy(jarFiles);
      if (deployedJars == null || deployedJars.size() == 0) {
        return null;
      }

      return deployedJars.get(0);
    } finally {
      lock.unlock();
    }
  }

  public Map<String, DeployedJar> getDeployedJars() {
    return Collections.unmodifiableMap(this.deployedJars);
  }

  /**
   * Undeploy the jar file identified by the given artifact ID.
   *
   * @param jarName The jarFile to undeploy
   * @return The path to the location on disk where the JAR file had been deployed
   * @throws IOException If there's a problem deleting the file
   */
  public String undeploy(final String jarName) throws IOException {
    String artifactId = DeployJarFileUtils.getArtifactId(jarName);
    lock.lock();

    try {
      DeployedJar deployedJar = deployedJars.get(artifactId);
      if (deployedJar == null) {
        throw new IllegalArgumentException(jarName + " not deployed");
      }

      if (!deployedJar.getDeployedFileName().equals(jarName)) {
        throw new IllegalArgumentException(jarName + " not deployed");
      }

      // remove the deployedJar
      deployedJars.remove(artifactId);
      ClassPathLoader.getLatest().unloadClassloaderForArtifact(artifactId);

      unregisterFunctionsForJar(deployedJar);

      deleteAllVersionsOfJar(jarName);
      return deployedJar.getFileCanonicalPath();
    } finally {
      lock.unlock();
    }
  }

  /**
   *
   * @param jarName a user deployed jar name (abc.jar or abc-1.0.jar)
   */
  public void deleteAllVersionsOfJar(String jarName) {
    lock.lock();
    String artifactId = DeployJarFileUtils.getArtifactId(jarName);
    try {
      for (File file : this.deployDirectory.listFiles()) {
        if (artifactId.equals(DeployJarFileUtils.getArtifactIdForSequencedJar(file.getName()))) {
          logger.info("Deleting: {}", file.getAbsolutePath());
          FileUtils.deleteQuietly(file);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void unregisterFunctionsFromOldJarNotInNewJar(DeployedJar oldJar, DeployedJar newJar) {
    List<String> oldFunctions = this.registeredFunctions.get(oldJar);
    List<String> newFunctions = this.registeredFunctions.get(newJar);

    oldFunctions.removeAll(newFunctions);

    oldFunctions.forEach(FunctionService::unregisterFunction);

    this.registeredFunctions.put(oldJar, oldFunctions);
  }

  /**
   * Backs up all deployed jars to the provided destination.
   *
   * @param destinationPath The location to backup jars to.
   * @return A map of Paths to backed up file paths and their source files.
   * @throws IOException If the jar cannot be copied
   */
  public Map<Path, File> backup(Path destinationPath) throws IOException {
    Map<Path, File> backedUpJars = new HashMap<>();
    try {
      suspendAll();

      List<DeployedJar> jarList = findDeployedJars();
      for (DeployedJar jar : jarList) {
        File source = new File(jar.getFileCanonicalPath());
        String sourceFileName = source.getName();
        Path destination = destinationPath.resolve(sourceFileName);
        Files.copy(source.toPath(), destination, StandardCopyOption.COPY_ATTRIBUTES);
        backedUpJars.put(destination, source);
      }
    } finally {
      resumeAll();
    }
    return backedUpJars;
  }

  private void unregisterFunctionsForJar(DeployedJar deployedJar) {
    List<String> functionsForJar = this.registeredFunctions.get(deployedJar);
    functionsForJar.forEach(FunctionService::unregisterFunction);
    functionsForJar.clear();
    try {
      TypeRegistry typeRegistry = ((InternalCache) CacheFactory.getAnyInstance()).getPdxRegistry();
      if (typeRegistry != null) {
        typeRegistry.flushCache();
      }
    } catch (CacheClosedException ignored) {
      // That's okay, it just means there was nothing to flush to begin with
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

  public List<DeployedJar> removeOldVersionsAndDeploy(Set<File> stagedJarFiles)
      throws IOException, ClassNotFoundException {
    lock.lock();
    try {
      for (File stagedJarFile : stagedJarFiles) {
        logger.info("Removing old versions of {} in cluster configuration.",
            stagedJarFile.getName());
        deleteAllVersionsOfJar(stagedJarFile.getName());
      }

      return deploy(stagedJarFiles);

    } finally {
      lock.unlock();
    }
  }
}

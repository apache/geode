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
package org.apache.geode.internal;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.logging.LogService;

public class JarDeployer implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();
  public static final String JAR_PREFIX_FOR_REGEX = "";
  @MakeNotStatic
  private static final Lock lock = new ReentrantLock();

  private final Map<String, DeployedJar> deployedJars = new ConcurrentHashMap<>();


  // Split a versioned filename into its name and version
  public static final Pattern versionedPattern =
      Pattern.compile(JAR_PREFIX_FOR_REGEX + "(.*)\\.v(\\d++).jar$");

  private final File deployDirectory;

  public JarDeployer() {
    this(new File(System.getProperty("user.dir")));
  }

  public JarDeployer(final File deployDirectory) {
    this.deployDirectory = deployDirectory;
  }

  public File getDeployDirectory() {
    return this.deployDirectory;
  }

  /**
   * Writes the jarBytes for the given jarName to the next version of that jar file (if the bytes do
   * not match the latest deployed version)
   *
   * @return the DeployedJar that was written from jarBytes, or null if those bytes matched the
   *         latest deployed version
   */
  public DeployedJar deployWithoutRegistering(final String jarName, final File stagedJar)
      throws IOException {
    lock.lock();

    try {
      boolean shouldDeployNewVersion = shouldDeployNewVersion(jarName, stagedJar);
      if (!shouldDeployNewVersion) {
        logger.debug("No need to deploy a new version of {}", jarName);
        return null;
      }

      verifyWritableDeployDirectory();

      File newVersionedJarFile = getNextVersionedJarFile(jarName);
      Files.copy(stagedJar.toPath(), newVersionedJarFile.toPath());

      return new DeployedJar(newVersionedJarFile, jarName);
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
    return getDeployedJars().values().stream().collect(toList());
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
    File[] oldVersions = findSortedOldVersionsOfJar(unversionedJarName);

    String nextVersionedJarName;
    if (oldVersions == null || oldVersions.length == 0) {
      nextVersionedJarName = removeJarExtension(unversionedJarName) + ".v1.jar";
    } else {
      String latestVersionedJarName = oldVersions[0].getName();
      int nextVersion = extractVersionFromFilename(latestVersionedJarName) + 1;
      nextVersionedJarName = removeJarExtension(unversionedJarName) + ".v" + nextVersion + ".jar";
    }

    logger.debug("Next versioned jar name for {} is {}", unversionedJarName, nextVersionedJarName);

    return new File(deployDirectory, nextVersionedJarName);
  }

  /**
   * Find the version number that's embedded in the name of this file
   *
   * @param filename Filename to get the version number from
   * @return The version number embedded in the filename
   */
  public static int extractVersionFromFilename(final String filename) {
    final Matcher matcher = versionedPattern.matcher(filename);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(2));
    } else {
      return 0;
    }
  }

  protected Set<String> findDistinctDeployedJarsOnDisk() {
    // Find all deployed JAR files
    final File[] oldFiles =
        this.deployDirectory.listFiles((file, name) -> versionedPattern.matcher(name).matches());

    // Now add just the original JAR name to the set
    final Set<String> jarNames = new HashSet<>();
    for (File oldFile : oldFiles) {
      Matcher matcher = versionedPattern.matcher(oldFile.getName());
      matcher.find();
      jarNames.add(matcher.group(1) + ".jar");
    }
    return jarNames;
  }

  /**
   * Find all versions of the JAR file that are currently on disk and return them sorted from newest
   * (highest version) to oldest
   *
   * @param unversionedJarName Name of the JAR file that we want old versions of
   * @return Sorted array of files that are older versions of the given JAR
   */
  protected File[] findSortedOldVersionsOfJar(final String unversionedJarName) {
    logger.debug("Finding sorted old versions of {}", unversionedJarName);
    // Find all matching files
    final Pattern pattern = Pattern.compile(
        JAR_PREFIX_FOR_REGEX + removeJarExtension(unversionedJarName) + "\\.v\\d++\\.jar$");
    final File[] oldJarFiles =
        this.deployDirectory.listFiles((file, name) -> (pattern.matcher(name).matches()));

    // Sort them in order from newest (highest version) to oldest
    Arrays.sort(oldJarFiles, (file1, file2) -> {
      int file1Version = extractVersionFromFilename(file1.getName());
      int file2Version = extractVersionFromFilename(file2.getName());
      return file2Version - file1Version;
    });

    logger.debug("Found [{}]",
        Arrays.stream(oldJarFiles).map(File::getAbsolutePath).collect(joining(",")));
    return oldJarFiles;
  }

  protected String removeJarExtension(String jarName) {
    if (jarName != null && jarName.endsWith(".jar")) {
      return jarName.replaceAll("\\.jar$", "");
    } else {
      return jarName;
    }
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

  final Pattern oldNamingPattern = Pattern.compile("^vf\\.gf#(.*)\\.jar#(\\d+)$");

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
    return oldNamingPattern.matcher(fileName).matches();
  }

  private void renameJarWithOldNamingConvention(File oldJar) throws IOException {
    Matcher matcher = oldNamingPattern.matcher(oldJar.getName());
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
   */
  public void loadPreviouslyDeployedJarsFromDisk() {
    logger.info("Loading previously deployed jars");
    lock.lock();
    try {
      verifyWritableDeployDirectory();
      renameJarsWithOldNamingConvention();

      final Set<String> jarNames = findDistinctDeployedJarsOnDisk();
      if (jarNames.isEmpty()) {
        return;
      }

      List<DeployedJar> latestVersionOfEachJar = new ArrayList<>();

      for (String jarName : jarNames) {
        DeployedJar deployedJar = findLatestValidDeployedJarFromDisk(jarName);

        if (deployedJar != null) {
          latestVersionOfEachJar.add(deployedJar);
          deleteOtherVersionsOfJar(deployedJar);
        }
      }

      registerNewVersions(latestVersionOfEachJar);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Deletes all versions of this jar on disk other than the given version
   */
  public void deleteOtherVersionsOfJar(DeployedJar deployedJar) {
    logger.info("Deleting all versions of " + deployedJar.getJarName() + " other than "
        + deployedJar.getFileName());
    final File[] jarFiles = findSortedOldVersionsOfJar(deployedJar.getJarName());

    Stream.of(jarFiles).filter(jarFile -> !jarFile.equals(deployedJar.getFile()))
        .forEach(jarFile -> {
          logger.info("Deleting old version of jar: " + jarFile.getAbsolutePath());
          FileUtils.deleteQuietly(jarFile);
        });
  }

  public DeployedJar findLatestValidDeployedJarFromDisk(String unversionedJarName)
      throws IOException {
    final File[] jarFiles = findSortedOldVersionsOfJar(unversionedJarName);

    Optional<File> latestValidDeployedJarOptional = Arrays.stream(jarFiles).filter(Objects::nonNull)
        .filter(jarFile -> DeployedJar.hasValidJarContent(jarFile)).findFirst();

    if (!latestValidDeployedJarOptional.isPresent()) {
      // No valid version of this jar
      return null;
    }

    File latestValidDeployedJar = latestValidDeployedJarOptional.get();

    return new DeployedJar(latestValidDeployedJar, unversionedJarName);
  }

  public URL[] getDeployedJarURLs() {
    return this.deployedJars.values().stream().map(DeployedJar::getFileURL).toArray(URL[]::new);

  }

  public List<DeployedJar> registerNewVersions(List<DeployedJar> deployedJars)
      throws ClassNotFoundException {
    lock.lock();
    try {
      Map<DeployedJar, DeployedJar> newVersionToOldVersion = new HashMap<>();

      for (DeployedJar deployedJar : deployedJars) {
        if (deployedJar != null) {
          logger.info("Registering new version of jar: {}", deployedJar);
          DeployedJar oldJar = this.deployedJars.put(deployedJar.getJarName(), deployedJar);
          newVersionToOldVersion.put(deployedJar, oldJar);

          addToSystemClasspath(deployedJar);

        }
      }

      ClassPathLoader.getLatest().rebuildClassLoaderForDeployedJars();

      // Finally, unregister functions that were removed
      for (Map.Entry<DeployedJar, DeployedJar> entry : newVersionToOldVersion.entrySet()) {
        DeployedJar newjar = entry.getKey();
        DeployedJar oldJar = entry.getValue();

        newjar.registerFunctions();

        if (oldJar != null) {
          oldJar.cleanUp(newjar);
        }
      }

    } finally {
      lock.unlock();
    }

    return deployedJars;
  }

  private static void addToSystemClasspath(DeployedJar jar) {
    File jarFile = jar.getFile();
    Method method = null;
    try {
      method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[] {URL.class});
      method.setAccessible(true);
      method.invoke(ClassLoader.getSystemClassLoader(), new Object[] {jarFile.toURI().toURL()});
      // Class.forName("org.apache.derby.jdbc.AutoloadedDriver");
      Class driver = ClassLoader.getSystemClassLoader().loadClass("com.mysql.jdbc.Driver");
      DriverManager.registerDriver((Driver) driver.newInstance());
    } catch (NoSuchMethodException | MalformedURLException | IllegalAccessException
        | InvocationTargetException | ClassNotFoundException | InstantiationException
        | SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Deploy the given JAR files.
   *
   * @param stagedJarFiles A map of Files which have been staged in another location and are ready
   *        to be deployed as a unit.
   * @return An array of newly created JAR class loaders. Entries will be null for an JARs that were
   *         already deployed.
   * @throws IOException When there's an error saving the JAR file to disk
   */
  public List<DeployedJar> deploy(final Map<String, File> stagedJarFiles)
      throws IOException, ClassNotFoundException {
    List<DeployedJar> deployedJars = new ArrayList<>(stagedJarFiles.size());

    for (File jar : stagedJarFiles.values()) {
      if (!DeployedJar.hasValidJarContent(jar)) {
        throw new IllegalArgumentException(
            "File does not contain valid JAR content: " + jar.getName());
      }
    }

    lock.lock();
    try {
      for (String fileName : stagedJarFiles.keySet()) {
        deployedJars.add(deployWithoutRegistering(fileName, stagedJarFiles.get(fileName)));
      }

      return registerNewVersions(deployedJars);
    } finally {
      lock.unlock();
    }
  }

  private boolean shouldDeployNewVersion(String jarName, File stagedJar) throws IOException {
    DeployedJar oldDeployedJar = this.deployedJars.get(jarName);

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
  public DeployedJar getDeployedJar(String jarName) {
    return this.deployedJars.get(jarName);
  }

  @VisibleForTesting
  public DeployedJar deploy(final String jarName, final File stagedJarFile)
      throws IOException, ClassNotFoundException {
    lock.lock();

    Map<String, File> jarFiles = new HashMap<>();
    jarFiles.put(jarName, stagedJarFile);

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
   * Undeploy the given JAR file.
   *
   * @param jarName The name of the JAR file to undeploy
   * @return The path to the location on disk where the JAR file had been deployed
   * @throws IOException If there's a problem deleting the file
   */
  public String undeploy(final String jarName) throws IOException {
    lock.lock();

    try {
      DeployedJar deployedJar = deployedJars.remove(jarName);
      if (deployedJar == null) {
        throw new IllegalArgumentException("JAR not deployed");
      }

      ClassPathLoader.getLatest().rebuildClassLoaderForDeployedJars();

      deployedJar.cleanUp(null);

      deleteAllVersionsOfJar(jarName);
      return deployedJar.getFileCanonicalPath();
    } finally {
      lock.unlock();
    }
  }

  public void deleteAllVersionsOfJar(String unversionedJarName) {
    lock.lock();
    try {
      File[] jarFiles = findSortedOldVersionsOfJar(unversionedJarName);
      for (File jarFile : jarFiles) {
        logger.info("Deleting: {}", jarFile.getAbsolutePath());
        FileUtils.deleteQuietly(jarFile);
      }
    } finally {
      lock.unlock();
    }

  }
}

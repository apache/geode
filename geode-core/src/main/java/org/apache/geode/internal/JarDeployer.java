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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.geode.GemFireException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.SystemFailure;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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

public class JarDeployer implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();
  public static final String JAR_PREFIX = "";
  public static final String JAR_PREFIX_FOR_REGEX = "";
  private static final Lock lock = new ReentrantLock();

  private Map<String, DeployedJar> deployedJars = new ConcurrentHashMap<>();


  // Split a versioned filename into its name and version
  public static final Pattern versionedPattern =
      Pattern.compile(JAR_PREFIX_FOR_REGEX + "(.*)\\.v(\\d++).jar$");

  private final File deployDirectory;

  public JarDeployer() {
    this.deployDirectory = new File(System.getProperty("user.dir"));
  }

  public JarDeployer(final File deployDirectory) {
    this.deployDirectory = deployDirectory;
  }

  public File getDeployDirectory() {
    return this.deployDirectory;
  }

  public DeployedJar deployWithoutRegistering(final String jarName, final byte[] jarBytes)
      throws IOException {
    try {
      lock.lock();
      verifyWritableDeployDirectory();

      File newVersionedJarFile = getNextVersionedJarFile(jarName);
      writeJarBytesToFile(newVersionedJarFile, jarBytes);

      return new DeployedJar(newVersionedJarFile, jarName, jarBytes);
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

    return new File(deployDirectory, nextVersionedJarName);
  }

  /**
   * Attempt to write the given bytes to the given file. If this VM is able to successfully write
   * the contents to the file, or another VM writes the exact same contents, then the write is
   * considered to be successful.
   * 
   * @param file File of the JAR file to deploy.
   * @param jarBytes Contents of the JAR file to deploy.
   * @return True if the file was successfully written, false otherwise
   */
  private boolean writeJarBytesToFile(final File file, final byte[] jarBytes) throws IOException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (file.createNewFile()) {
      if (isDebugEnabled) {
        logger.debug("Successfully created new JAR file: {}", file.getAbsolutePath());
      }
      final OutputStream outStream = new FileOutputStream(file);
      outStream.write(jarBytes);
      outStream.close();
      return true;
    }
    return doesFileMatchBytes(file, jarBytes);
  }

  /**
   * Determine if the contents of the file referenced is an exact match for the bytes provided. The
   * method first checks to see if the file is actively being written by checking the length over
   * time. If it appears that the file is actively being written, then it loops waiting for that to
   * complete before doing the comparison.
   * 
   * @param file File to compare
   * @param bytes Bytes to compare
   * @return True if there's an exact match, false otherwise
   * @throws IOException If there's a problem reading the file
   */
  private boolean doesFileMatchBytes(final File file, final byte[] bytes) throws IOException {
    // First check to see if the file is actively being written (if it's not big enough)
    final String absolutePath = file.getAbsolutePath();
    boolean keepTrying = true;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    while (file.length() < bytes.length && keepTrying) {
      if (isDebugEnabled) {
        logger.debug("Loop waiting for another to write file: {}", absolutePath);
      }
      long startingFileLength = file.length();
      try {
        Thread.sleep(500);
      } catch (InterruptedException iex) {
        // Just keep looping
      }
      if (startingFileLength == file.length()) {
        if (isDebugEnabled) {
          logger.debug("Done waiting for another to write file: {}", absolutePath);
        }
        // Assume the other process has finished writing
        keepTrying = false;
      }
    }

    // If they don't have the same number of bytes then nothing to do
    if (file.length() != bytes.length) {
      if (isDebugEnabled) {
        logger.debug("Unmatching file length when waiting for another to write file: {}",
            absolutePath);
      }
      return false;
    }

    // Open the file then loop comparing each byte
    BufferedInputStream inStream = new BufferedInputStream(new FileInputStream(file));
    int index = 0;
    try {
      for (; index < bytes.length; index++) {
        if (((byte) inStream.read()) != bytes[index]) {
          if (isDebugEnabled) {
            logger.debug("Did not find a match when waiting for another to write file: {}",
                absolutePath);
          }
          return false;
        }
      }
    } finally {
      inStream.close();
    }

    return true;
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

  protected Set<String> findDistinctDeployedJars() {
    // Find all deployed JAR files
    final File[] oldFiles = this.deployDirectory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(final File file, final String name) {
        return versionedPattern.matcher(name).matches();
      }
    });

    // Now add just the original JAR name to the set
    final Set<String> jarNames = new HashSet<String>();
    for (File oldFile : oldFiles) {
      Matcher matcher = versionedPattern.matcher(oldFile.getName());
      matcher.find();
      jarNames.add(matcher.group(1));
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
    Exception exception = null;
    int tryCount = 0;
    do {
      try {
        if (this.deployDirectory.canWrite()) {
          return;
        }
      } catch (Exception ex) {
        exception = ex;
        // We'll just ignore exceptions and loop to try again
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException iex) {
        logger.error("Interrupted while testing writable deploy directory", iex);
      }
    } while (tryCount++ < 20);

    if (exception != null) {
      throw new IOException("Unable to write to deploy directory", exception);
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
    logger.debug("Renaming deployed jar from " + oldJar.getCanonicalPath() + " to "
        + newJar.getCanonicalPath());

    FileUtils.moveFile(oldJar, newJar);
    FileUtils.deleteQuietly(oldJar);
  }

  /**
   * Re-deploy all previously deployed JAR files.
   */
  public void loadPreviouslyDeployedJars() {
    try {
      verifyWritableDeployDirectory();
      renameJarsWithOldNamingConvention();

      final Set<String> jarNames = findDistinctDeployedJars();
      if (jarNames.isEmpty()) {
        return;
      }

      Map<String, DeployedJar> latestVersionOfEachJar = new LinkedHashMap<>();

      for (String jarName : jarNames) {
        final File[] jarFiles = findSortedOldVersionsOfJar(jarName);

        Optional<File> latestValidDeployedJarOptional =
            Arrays.stream(jarFiles).filter(Objects::nonNull).filter(jarFile -> {
              try {
                return DeployedJar.isValidJarContent(FileUtils.readFileToByteArray(jarFile));
              } catch (IOException e) {
                return false;
              }
            }).findFirst();

        if (!latestValidDeployedJarOptional.isPresent()) {
          // No valid version of this jar
          continue;
        }

        File latestValidDeployedJar = latestValidDeployedJarOptional.get();
        latestVersionOfEachJar.put(jarName, new DeployedJar(latestValidDeployedJar, jarName));

        // Remove any old left-behind versions of this JAR file
        for (File jarFile : jarFiles) {
          if (!latestValidDeployedJar.equals(jarFile)) {
            FileUtils.deleteQuietly(jarFile);
          }
        }
      }

      registerNewVersions(latestVersionOfEachJar.values().stream().collect(toList()));
      // ClassPathLoader.getLatest().deploy(latestVersionOfEachJar.keySet().toArray(),
      // latestVersionOfEachJar.values().toArray())
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  public URL[] getDeployedJarURLs() {
    return this.deployedJars.values().stream().map(DeployedJar::getFileURL).toArray(URL[]::new);

  }

  public List<DeployedJar> registerNewVersions(List<DeployedJar> deployedJars)
      throws ClassNotFoundException {
    for (DeployedJar deployedJar : deployedJars) {
      if (deployedJar != null) {
        DeployedJar oldJar = this.deployedJars.put(deployedJar.getJarName(), deployedJar);
        if (oldJar != null) {
          oldJar.cleanUp();
        }
      }
    }

    ClassPathLoader.getLatest().rebuildClassLoaderForDeployedJars();

    for (DeployedJar deployedJar : deployedJars) {
      if (deployedJar != null) {
        deployedJar.loadClassesAndRegisterFunctions();
      }
    }

    return deployedJars;
  }

  /**
   * Deploy the given JAR files.
   * 
   * @param jarNames Array of names of the JAR files to deploy.
   * @param jarBytes Array of contents of the JAR files to deploy.
   * @return An array of newly created JAR class loaders. Entries will be null for an JARs that were
   *         already deployed.
   * @throws IOException When there's an error saving the JAR file to disk
   */
  public synchronized List<DeployedJar> deploy(final String jarNames[], final byte[][] jarBytes)
      throws IOException, ClassNotFoundException {
    DeployedJar[] deployedJars = new DeployedJar[jarNames.length];

    for (int i = 0; i < jarNames.length; i++) {
      if (!DeployedJar.isValidJarContent(jarBytes[i])) {
        throw new IllegalArgumentException(
            "File does not contain valid JAR content: " + jarNames[i]);
      }
    }

    for (int i = 0; i < jarNames.length; i++) {
      String jarName = jarNames[i];
      byte[] newJarBytes = jarBytes[i];

      boolean shouldDeployNewVersion = shouldDeployNewVersion(jarName, newJarBytes);

      if (shouldDeployNewVersion) {
        deployedJars[i] = deployWithoutRegistering(jarName, newJarBytes);
      } else {
        deployedJars[i] = null;
      }
    }

    return registerNewVersions(Arrays.asList(deployedJars));
  }

  private boolean shouldDeployNewVersion(String jarName, byte[] newJarBytes) throws IOException {
    DeployedJar oldDeployedJar = this.deployedJars.get(jarName);

    if (oldDeployedJar == null) {
      return true;
    }

    if (oldDeployedJar.hasSameContentAs(newJarBytes)) {
      logger.warn("Jar is identical to the latest deployed version: ",
          oldDeployedJar.getFileCanonicalPath());

      return false;
    }

    return true;
  }

  public DeployedJar findDeployedJar(String jarName) {
    return this.deployedJars.get(jarName);
  }

  public synchronized DeployedJar deploy(final String jarName, final byte[] jarBytes)
      throws IOException, ClassNotFoundException {

    List<DeployedJar> deployedJars = deploy(new String[] {jarName}, new byte[][] {jarBytes});
    if (deployedJars == null || deployedJars.size() == 0) {
      return null;
    }

    return deployedJars.get(0);
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
  public synchronized String undeploy(final String jarName) throws IOException {
    DeployedJar deployedJar = deployedJars.remove(jarName);
    if (deployedJar == null) {
      throw new IllegalArgumentException("JAR not deployed");
    }

    ClassPathLoader.getLatest().rebuildClassLoaderForDeployedJars();

    deployedJar.cleanUp();
    return deployedJar.getFileCanonicalPath();
  }
}

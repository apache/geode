/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.internal.logging.LogService;

public class JarDeployer implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();
  public static final String JAR_PREFIX = "vf.gf#";
  private static final Lock lock = new ReentrantLock();

  // Split a versioned filename into its name and version
  public static final Pattern versionedPattern = Pattern.compile("^(.*)#(\\d++)$");

  private final File deployDirectory;

  public JarDeployer() {
    this.deployDirectory = new File(System.getProperty("user.dir"));
  }

  public JarDeployer(final File deployDirectory) {
    this.deployDirectory = deployDirectory;
  }

  /**
   * Re-deploy all previously deployed JAR files.
   */
  public void loadPreviouslyDeployedJars() {
    List<JarClassLoader> jarClassLoaders = new ArrayList<JarClassLoader>();

    lock.lock();
    try {
      try {
        verifyWritableDeployDirectory();
        final Set<String> jarNames = findDistinctDeployedJars();
        if (!jarNames.isEmpty()) {
          for (String jarName : jarNames) {
            final File[] jarFiles = findSortedOldVersionsOfJar(jarName);

            // It's possible the JARs were deleted by another process
            if (jarFiles.length != 0) {
              JarClassLoader jarClassLoader = findJarClassLoader(jarName);

              try {
                final byte[] jarBytes = getJarContent(jarFiles[0]);
                if (!JarClassLoader.isValidJarContent(jarBytes)) {
                  logger.warn("Invalid JAR file found and deleted: {}", jarFiles[0].getAbsolutePath());
                  jarFiles[0].delete();
                } else {
                  // Test to see if the exact same file is already in use
                  if (jarClassLoader == null || !jarClassLoader.getFileName().equals(jarFiles[0].getName())) {
                    jarClassLoader = new JarClassLoader(jarFiles[0], jarName, jarBytes);
                    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(jarClassLoader);
                    jarClassLoaders.add(jarClassLoader);
                  }
                }
              } catch (IOException ioex) {
                // Another process deleted the file so don't bother doing anything else with it
                if (logger.isDebugEnabled()) {
                  logger.debug("Failed attempt to use JAR to create JarClassLoader for: {}", jarName);
                }
              }

              // Remove any old left-behind versions of this JAR file
              for (File jarFile : jarFiles) {
                if (jarFile.exists() && (jarClassLoader == null || !jarClassLoader.getFileName().equals(jarFile.getName()))) {
                  attemptFileLockAndDelete(jarFile);
                }
              }
            }
          }
        }

        for (JarClassLoader jarClassLoader : jarClassLoaders) {
          jarClassLoader.loadClassesAndRegisterFunctions();
        }
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable th) {
        SystemFailure.checkFailure();
        logger.error("Error when attempting to deploy JAR files on load.", th);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Deploy the given JAR files.
   * 
   * @param jarNames
   *          Array of names of the JAR files to deploy.
   * @param jarBytes
   *          Array of contents of the JAR files to deploy.
   * @return An array of newly created JAR class loaders. Entries will be null for an JARs that were already deployed.
   * @throws IOException
   *           When there's an error saving the JAR file to disk
   */
  public JarClassLoader[] deploy(final String jarNames[], final byte[][] jarBytes) throws IOException, ClassNotFoundException {
    JarClassLoader[] jarClassLoaders = new JarClassLoader[jarNames.length];
    verifyWritableDeployDirectory();

    lock.lock();
    try {
      for (int i = 0; i < jarNames.length; i++) {
        if (!JarClassLoader.isValidJarContent(jarBytes[i])) {
          throw new IllegalArgumentException("File does not contain valid JAR content: " + jarNames[i]);
        }
      }
      
      for (int i = 0; i < jarNames.length; i++) {
        jarClassLoaders[i] = deployWithoutRegistering(jarNames[i], jarBytes[i]);
      }

      for (JarClassLoader jarClassLoader : jarClassLoaders) {
        if (jarClassLoader != null) {
          jarClassLoader.loadClassesAndRegisterFunctions();
        }
      }
    } finally {
      lock.unlock();
    }
    return jarClassLoaders;
  }

  /**
   * Deploy the given JAR file without registering functions.
   * 
   * @param jarName
   *          Name of the JAR file to deploy.
   * @param jarBytes
   *          Contents of the JAR file to deploy.
   * @return The newly created JarClassLoader or null if the JAR was already deployed
   * @throws IOException
   *           When there's an error saving the JAR file to disk
   */
  private JarClassLoader deployWithoutRegistering(final String jarName, final byte[] jarBytes) throws IOException {
    JarClassLoader oldJarClassLoader = findJarClassLoader(jarName);

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Deploying {}: {}", jarName, 
          (oldJarClassLoader == null ? ": not yet deployed" : ": already deployed as " + oldJarClassLoader.getFileCanonicalPath()));
    }

    // Test to see if the exact same file is being deployed
    if (oldJarClassLoader != null && oldJarClassLoader.hasSameContent(jarBytes)) {
      return null;
    }

    JarClassLoader newJarClassLoader = null;

    do {
      File[] oldJarFiles = findSortedOldVersionsOfJar(jarName);

      try {
        // If this is the first version of this JAR file we've seen ...
        if (oldJarFiles.length == 0) {
          if (isDebugEnabled) {
            logger.debug("There were no pre-existing versions for JAR: {}", jarName);
          }
          File nextVersionJarFile = getNextVersionJarFile(jarName);
          if (writeJarBytesToFile(nextVersionJarFile, jarBytes)) {
            newJarClassLoader = new JarClassLoader(nextVersionJarFile, jarName, jarBytes);
            if (isDebugEnabled) {
              logger.debug("Successfully created initial JarClassLoader at file: {}", nextVersionJarFile.getAbsolutePath());
            }
          } else {
            if (isDebugEnabled) {
              logger.debug("Unable to write contents for first version of JAR to file: {}", nextVersionJarFile.getAbsolutePath());
            }
          }

        } else {
          // Most recent is at the beginning of the list, see if this JAR matches what's
          // already on disk.
          if (doesFileMatchBytes(oldJarFiles[0], jarBytes)) {
            if (isDebugEnabled) {
              logger.debug("A version on disk was an exact match for the JAR being deployed: {}", oldJarFiles[0].getAbsolutePath());
            }
            newJarClassLoader = new JarClassLoader(oldJarFiles[0], jarName, jarBytes);
            if (isDebugEnabled) {
              logger.debug("Successfully reused JAR to create JarClassLoader from file: {}", oldJarFiles[0].getAbsolutePath());
            }
          } else {
            // This JAR isn't on disk
            if (isDebugEnabled) {
              logger.debug("Need to create a new version for JAR: {}", jarName);
            }
            File nextVersionJarFile = getNextVersionJarFile(oldJarFiles[0].getName());
            if (writeJarBytesToFile(nextVersionJarFile, jarBytes)) {
              newJarClassLoader = new JarClassLoader(nextVersionJarFile, jarName, jarBytes);
              if (isDebugEnabled) {
                logger.debug("Successfully created next JarClassLoader at file: {}", nextVersionJarFile.getAbsolutePath());
              }
            } else {
              if (isDebugEnabled) {
                logger.debug("Unable to write contents for next version of JAR to file: {}", nextVersionJarFile.getAbsolutePath());
              }
            }
          }
        }
      } catch (IOException ioex) {
        // Another process deleted the file before we could get to it, just start again
        logger.info("Failed attempt to use JAR to create JarClassLoader for: {} : {}", jarName, ioex.getMessage());
      }

      if (isDebugEnabled) {
        if (newJarClassLoader == null) {
          logger.debug("Unable to determine a JAR file location, will loop and try again: {}", jarName);
        } else {
          logger.debug("Exiting loop for JarClassLoader creation using file: {}", newJarClassLoader.getFileName());
        }
      }
    } while (newJarClassLoader == null);

    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(newJarClassLoader);

    // Remove the JAR file that was undeployed as part of this redeploy
    if (oldJarClassLoader != null) {
      attemptFileLockAndDelete(new File(this.deployDirectory, oldJarClassLoader.getFileName()));
    }

    return newJarClassLoader;
  }

  /**
   * Undeploy the given JAR file.
   * 
   * @param jarName
   *          The name of the JAR file to undeploy
   * @return The path to the location on disk where the JAR file had been deployed
   * @throws IOException
   *           If there's a problem deleting the file
   */
  public String undeploy(final String jarName) throws IOException {
    JarClassLoader jarClassLoader = null;
    verifyWritableDeployDirectory();

    lock.lock();
    try {
      jarClassLoader = findJarClassLoader(jarName);
      if (jarClassLoader == null) {
        throw new IllegalArgumentException("JAR not deployed");
      }

      ClassPathLoader.getLatest().removeAndSetLatest(jarClassLoader);
      attemptFileLockAndDelete(new File(this.deployDirectory, jarClassLoader.getFileName()));
      return jarClassLoader.getFileCanonicalPath();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get a list of all currently deployed JarClassLoaders.
   * 
   * @return The list of JarClassLoaders
   */
  public List<JarClassLoader> findJarClassLoaders() {
    List<JarClassLoader> returnList = new ArrayList<JarClassLoader>();
    Collection<ClassLoader> classLoaders = ClassPathLoader.getLatest().getClassLoaders();
    for (ClassLoader classLoader : classLoaders) {
      if (classLoader instanceof JarClassLoader) {
        returnList.add((JarClassLoader) classLoader);
      }
    }

    return returnList;
  }

  /**
   * Suspend all deploy and undeploy operations. This is done by acquiring and holding
   * the lock needed in order to perform a deploy or undeploy and so it will cause all
   * threads attempting to do one of these to block. This makes it somewhat of a time
   * sensitive call as forcing these other threads to block for an extended period of
   * time may cause other unforeseen problems.  It must be followed by a call
   * to {@link #resumeAll()}.
   */
  public void suspendAll() {
    lock.lock();
  }
  
  /**
   * Release the lock that controls entry into the deploy/undeploy methods
   * which will allow those activities to continue.
   */
  public void resumeAll() {
    lock.unlock();
  }
  
  /**
   * Figure out the next version of a JAR file
   * 
   * @param latestJarName
   *          The previous most recent version of the JAR file or original name if there wasn't one
   * @return The file that represents the next version
   */
  private File getNextVersionJarFile(final String latestJarName) {
    String newFileName;
    final Matcher matcher = versionedPattern.matcher(latestJarName);
    if (matcher.find()) {
      newFileName = matcher.group(1) + "#" + (Integer.parseInt(matcher.group(2)) + 1);
    } else {
      newFileName = JAR_PREFIX + latestJarName + "#1";
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Next version file name will be: {}", newFileName);
    }
    return new File(this.deployDirectory, newFileName);
  }

  /**
   * Attempt to write the given bytes to the given file. If this VM is able to successfully write the contents to the
   * file, or another VM writes the exact same contents, then the write is considered to be successful.
   * 
   * @param file
   *          File of the JAR file to deploy.
   * @param jarBytes
   *          Contents of the JAR file to deploy.
   * @return True if the file was successfully written, false otherwise
   */
  private boolean writeJarBytesToFile(final File file, final byte[] jarBytes) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
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

    } catch (IOException ioex) {
      // Another VM clobbered what was happening here, try again
      if (isDebugEnabled) {
        logger.debug("IOException while trying to write JAR content to file: {}", ioex);
      }
      return false;
    }
  }

  /**
   * Determine if the contents of the file referenced is an exact match for the bytes provided. The method first checks
   * to see if the file is actively being written by checking the length over time. If it appears that the file is
   * actively being written, then it loops waiting for that to complete before doing the comparison.
   * 
   * @param file
   *          File to compare
   * @param bytes
   *          Bytes to compare
   * @return True if there's an exact match, false otherwise
   * @throws IOException
   *           If there's a problem reading the file
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
        logger.debug("Unmatching file length when waiting for another to write file: {}", absolutePath);
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
            logger.debug("Did not find a match when waiting for another to write file: {}", absolutePath);
          }
          return false;
        }
      }
    } finally {
      inStream.close();
    }

    return true;
  }

  private void attemptFileLockAndDelete(final File file) throws IOException {
    final String absolutePath = file.getAbsolutePath();
    FileOutputStream fileOutputStream = new FileOutputStream(file, true);
    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      FileLock fileLock = null;
      try {
        fileLock = fileOutputStream.getChannel().tryLock();

        if (fileLock != null) {
          if (isDebugEnabled) {
            logger.debug("Tried and acquired exclusive lock for file: {}, w/ channel {}", absolutePath, fileLock.channel());
          }

          if (file.delete()) {
            if (isDebugEnabled) {
              logger.debug("Deleted file with name: {}", absolutePath);
            }
          } else {
            if (isDebugEnabled) {
              logger.debug("Could not delete file, will truncate instead and delete on exit: {}", absolutePath);
            }
            file.deleteOnExit();

            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            try {
              randomAccessFile.setLength(0);
            } finally {
              try {
                randomAccessFile.close();
              } catch (IOException ioex) {
                logger.error("Could not close file when attempting to set zero length", ioex);
              }
            }
          }
        } else {
          if (isDebugEnabled) {
            logger.debug("Will not delete file since exclusive lock unavailable: {}", absolutePath);
          }
        }

      } finally {
        if (fileLock != null) {
          try {
            fileLock.release();
            fileLock.channel().close();
            if (isDebugEnabled) {
              logger.debug("Released file lock for file: {}, w/ channel: {}", absolutePath, fileLock.channel());
            }
          } catch (IOException ioex) {
            logger.error("Could not close channel on JAR lock file", ioex);
          }
        }
      }
    } finally {
      try {
        fileOutputStream.close();
      } catch (IOException ioex) {
        logger.error("Could not close output stream on JAR file", ioex);
      }
    }
  }

  /**
   * Find the version number that's embedded in the name of this file
   * 
   * @param file
   *          File to get the version number from
   * @return The version number embedded in the filename
   */
  int extractVersionFromFilename(final File file) {
    final Matcher matcher = versionedPattern.matcher(file.getAbsolutePath());
    matcher.find();
    return Integer.parseInt(matcher.group(2));
  }

  private Set<String> findDistinctDeployedJars() {
    final Pattern pattern = Pattern.compile("^" + JAR_PREFIX + "(.*)#\\d++$");

    // Find all deployed JAR files
    final File[] oldFiles = this.deployDirectory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(final File file, final String name) {
        return pattern.matcher(name).matches();
      }
    });

    // Now add just the original JAR name to the set
    final Set<String> jarNames = new HashSet<String>();
    for (File oldFile : oldFiles) {
      Matcher matcher = pattern.matcher(oldFile.getName());
      matcher.find();
      jarNames.add(matcher.group(1));
    }
    return jarNames;
  }

  /**
   * Find all versions of the JAR file that are currently on disk and return them sorted from newest (highest version)
   * to oldest
   * 
   * @param jarFilename
   *          Name of the JAR file that we want old versions of
   * @return Sorted array of files that are older versions of the given JAR
   */
  private File[] findSortedOldVersionsOfJar(final String jarFilename) {
    // Find all matching files
    final Pattern pattern = Pattern.compile("^" + JAR_PREFIX + jarFilename + "#\\d++$");
    final File[] oldJarFiles = this.deployDirectory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(final File file, final String name) {
        return (pattern.matcher(name).matches());
      }
    });

    // Sort them in order from newest (highest version) to oldest
    Arrays.sort(oldJarFiles, new Comparator<File>() {
      @Override
      public int compare(final File file1, final File file2) {
        int file1Version = extractVersionFromFilename(file1);
        int file2Version = extractVersionFromFilename(file2);
        return file2Version - file1Version;
      }
    });

    return oldJarFiles;
  }

  private JarClassLoader findJarClassLoader(final String jarName) {
    Collection<ClassLoader> classLoaders = ClassPathLoader.getLatest().getClassLoaders();
    for (ClassLoader classLoader : classLoaders) {
      if (classLoader instanceof JarClassLoader && ((JarClassLoader) classLoader).getJarName().equals(jarName)) {
        return (JarClassLoader) classLoader;
      }
    }
    return null;
  }

  /**
   * Make sure that the deploy directory is writable.
   * 
   * @throws IOException
   *           If the directory isn't writable
   */
  private void verifyWritableDeployDirectory() throws IOException {
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
    throw new IOException("Unable to write to deploy directory");
  }
  
  private byte[] getJarContent(File jarFile) throws IOException {
    InputStream inputStream = new FileInputStream(jarFile);
    try {
      final ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
      final byte[] bytes = new byte[4096];

      int bytesRead;
      while (((bytesRead = inputStream.read(bytes)) != -1)) {
        byteOutStream.write(bytes, 0, bytesRead);
      }

      return byteOutStream.toByteArray();
    } finally {
      inputStream.close();
    }
  }
}

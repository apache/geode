/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.services.module.internal;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.modules.GeodeModuleFinder;

import org.apache.geode.util.internal.JarFileUtils;

/**
 * Processes the Java classpath into a format that can be consumed by {@link GeodeModuleFinder}.
 * This format lists only the directories/packages that contain class that need to be referenced by
 * the {@link GeodeModuleFinder} as a dependency. This processor also knows how to process manifest
 * files containing "Class-Path" entries and recursively processes each entry.
 *
 * @see GeodeModuleFinder
 * @see JarFile
 * @see Manifest
 * @see JarFileUtils
 *
 */
public final class GeodeJDKPaths {
  public static final Set<String> JDK;
  private static final Logger logger = LogManager.getLogger();

  // Populates the JDK Set<String> from Java classpath.
  static {
    Set<String> result = new TreeSet<>();
    processClassPathItem(System.getProperty("java.class.path"), result);
    if (result.size() == 0)
      throw new IllegalStateException("Something went wrong with system paths set up");
    JDK = Collections.unmodifiableSet(result);
  }

  private GeodeJDKPaths() {}

  /**
   * Iterates over all classpath entries and processes them as either a {@link JarFile} or a source
   * directory.
   *
   * @param classPath a {@link String} representing the classpath.
   * @param pathSet a {@link Set} which will be populated by directories/packages that contain
   *        resources.
   */
  public static void processClassPathItem(String classPath, final Set<String> pathSet) {
    logger.debug("GeodeJDKPaths classpath: " + classPath);
    if (!StringUtils.isBlank(classPath)) {
      String[] classPathEntries = classPath.split(File.pathSeparator);
      for (String classPathEntry : classPathEntries) {
        final File file = new File(classPathEntry);
        try {
          if (file.isDirectory()) {
            final String rootPath = file.getPath();
            logger.debug("Processing directory: " + classPathEntry);
            processDirectory(rootPath, pathSet, file);
          } else {
            try (JarFile jarFile = new JarFile(file)) {
              logger.debug("Processing JarFile: " + classPathEntry);
              processJar(pathSet, jarFile);
            }
          }
        } catch (IOException exception) {
          logger.info(exception);
        }
      }
    }
  }

  /**
   * Processes all entries of the provided {@link JarFile} and adds the directories/packages to the
   * set. Additionally, if a {@link Manifest} file exists, the "Class-Path" entries for the
   * {@link Manifest} are processed as well.
   *
   * @param pathSet a {@link Set} which will be populated by directories/packages that contain
   *        resources.
   * @param jarFile the {@link JarFile} to be processed.
   * @throws IOException if there are errors processing the {@link JarFile}.
   */
  private static void processJar(final Set<String> pathSet, final JarFile jarFile)
      throws IOException {
    Optional<Manifest> manifestFromJar = JarFileUtils.getManifestFromJarFile(jarFile);
    if (manifestFromJar.isPresent()) {
      String rootPath = JarFileUtils.getRootPathFromJarFile(jarFile);
      processClassPathEntriesFromManifest(pathSet, rootPath, manifestFromJar.get());
    }
    for (ZipEntry entry : Collections.list(jarFile.entries())) {
      final String name = entry.getName();
      final int lastSlash = name.lastIndexOf(File.separator);
      if (lastSlash != -1) {
        pathSet.add(name.substring(0, lastSlash));
      }
    }
  }

  /**
   * Processes all entries in the directory represented by the given {@link File}.
   *
   * @param pathSet a {@link Set} which will be populated by directories/packages that contain
   *        resources.
   * @param file the directory to be processed.
   */
  private static void processDirectory(String rootPath, final Set<String> pathSet,
      final File file) {
    Optional.ofNullable(file.listFiles()).ifPresent(listOfFiles -> {
      for (File entry : listOfFiles) {
        if (entry.isDirectory()) {
          processDirectory(rootPath, pathSet, entry);
        } else {
          Optional.ofNullable(entry.getParent()).ifPresent(packagePath -> {
            String relativePath = packagePath.substring(rootPath.length());
            if (!StringUtils.isBlank(relativePath)) {
              if (relativePath.startsWith(File.separator)) {
                relativePath = relativePath.substring(File.pathSeparator.length());
              }
              pathSet.add(relativePath);
            }
          });
        }
      }
    });
  }

  /**
   * Processes the "Class-Path" attribute from the provided {@link Manifest} file.
   *
   * @param pathSet a {@link Set} which will be populated by directories/packages that contain
   *        resources.
   * @param rootPath the path used to resolve the location of {@link Manifest} "Class-Path" entries.
   * @param manifestFromJar the {@link Manifest} to be to retrieve "Class-Path" from.
   * @throws IOException if there is a problem processing the {@link Manifest}.
   */
  private static void processClassPathEntriesFromManifest(Set<String> pathSet, String rootPath,
      Manifest manifestFromJar)
      throws IOException {
    String[] classpathEntries = JarFileUtils.getAttributesFromManifest(manifestFromJar,
        Attributes.Name.CLASS_PATH);

    for (String classpathEntry : classpathEntries) {
      try (JarFile jarFileForClasspathEntry =
          JarFileUtils.getJarFileForPath(rootPath, classpathEntry)) {
        processJar(pathSet, jarFileForClasspathEntry);
      }
    }
  }
}

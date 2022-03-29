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
package org.apache.geode.deployment.internal.modules.processors.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.logging.log4j.Logger;

import org.apache.geode.deployment.internal.modules.processors.JarProcessor;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A {@link JarProcessor} that knows how to process jar-of-jar files. A jar-of-jars is defined to be
 * a jar file
 * that contains jar files on the "root" level of the jar. i.e
 * example.jar ->
 * /META-INF
 * /META-INF/services
 * /innerJar1.jar
 * /innerJar2.jar
 */
public class JarOfJarsProcessor implements JarProcessor {
  private static final String JAR_OF_JARS_IDENTIFIER = "JarOfJars";
  private static final Logger logger = LogService.getLogger();

  @Override
  public String getIdentifier() {
    return JAR_OF_JARS_IDENTIFIER;
  }

  /**
   * Validation method to confirm that there is at least 1 inner jar file on the root level of this
   * shadow jar.
   *
   * @param file - the jar file
   * @return {@literal true} when at least 1 jar file is found on the "root" level of this file.
   *         {@literal false} when no jar file is found on "root" level OR if the file is not a jar
   *         file
   *
   */
  @Override
  public boolean canProcess(File file) {
    try (JarFile jarFile = new JarFile(file)) {
      Enumeration<JarEntry> entries = jarFile.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        if (!entry.isDirectory() && !entry.getName().contains("/")
            && entry.getName().endsWith(".jar")) {
          return true;
        }
      }
    } catch (IOException e) {
      logger.warn(e);
    }
    return false;
  }

  /**
   * Processes the Jar file by extracting/exploding of the jar file.
   *
   * @return List of absolute paths of all inner exploded jars
   */
  @Override
  public List<String> getResourcesFromJarFile(File file) {
    List<String> resourcePaths = new LinkedList<>();
    try (JarFile jarFile = new JarFile(file)) {
      Enumeration<JarEntry> entries = jarFile.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        if (!entry.isDirectory() && !entry.getName().contains("/")
            && entry.getName().endsWith(".jar")) {
          // extract file into directory and add it as a resource
          Path extractedJarFile = extractInnerJarFile(file, jarFile, entry);
          resourcePaths.add(extractedJarFile.toAbsolutePath().toString());
        }
      }
    } catch (IOException e) {
      logger.warn("Could not process jar " + file.getName());
      return Collections.emptyList();
    }
    resourcePaths.add(file.toPath().toAbsolutePath().toString());
    return resourcePaths;
  }

  private Path extractInnerJarFile(File deployedFile, JarFile jarFile, JarEntry entry)
      throws IOException {
    Path extractedJarFile =
        deployedFile.getParentFile().toPath().resolve(entry.getName()).normalize();
    if (!extractedJarFile.startsWith(deployedFile.getParentFile().toPath())) {
      throw new IOException("Jar entry has invalid path");
    }
    try (InputStream inputStream = jarFile.getInputStream(entry)) {
      try (FileOutputStream outputStream = new FileOutputStream(extractedJarFile.toFile())) {
        while (inputStream.available() > 0) {
          outputStream.write(inputStream.read());
        }
      }
    }

    return extractedJarFile;
  }
}

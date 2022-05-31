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
package org.apache.geode.test.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

/**
 * {@code ResourceUtils} is a utility class for tests that use resources and copy them to
 * directories such as {@code TemporaryFolder}.
 */
@SuppressWarnings("unused")
public class ResourceUtils {

  /**
   * Returns the class identified by {@code depth} element of the call stack.
   *
   * @param depth the depth of the call stack to check
   * @return the class
   * @throws RuntimeException wrapping a ClassNotFoundException if the class cannot be located
   */
  public static Class<?> getCallerClass(final int depth) {
    try {
      return Class.forName(getCallerClassName(depth + 1));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the name of the class identified by {@code depth} element of the call stack.
   *
   * @param depth the depth of the call stack to check
   * @return the name of the class
   */
  public static String getCallerClassName(final int depth) {
    return new Throwable().getStackTrace()[depth].getClassName();
  }

  /**
   * Finds {@code resourceName} using the {@code ClassLoader} of the caller class.
   *
   * @param resourceName the resource to find
   * @return the URL of the resource
   *
   * @throws AssertionError if the resource cannot be located
   *
   * @throws RuntimeException wrapping a ClassNotFoundException if the class cannot be located
   */
  public static URL getResource(final String resourceName) {
    URL resource = getCallerClass(2).getResource(resourceName);
    assertThat(resource)
        .as("Resource '" + resourceName + "'")
        .isNotNull();
    return resource;
  }

  /**
   * Finds {@code resourceName} using the {@code ClassLoader} of {@code classInSamePackage}.
   *
   * @param classInSamePackage a class sharing the {@code ClassLoader} from which to retrieve the
   *        resource
   * @param resourceName the resource to find
   * @return the URL of the resource
   *
   * @throws AssertionError if the resource cannot be located
   */
  public static URL getResource(final Class<?> classInSamePackage, final String resourceName) {
    URL resource = classInSamePackage.getResource(resourceName);
    assertThat(resource)
        .as("Resource '" + resourceName + "' associated with Class '" + classInSamePackage.getName()
            + "'")
        .isNotNull();
    return resource;
  }

  /**
   * Finds {@code resourceName} using the specified {@code ClassLoader}.
   *
   * @param classLoader the {@code ClassLoader} from which to retrieve the resource
   * @param resourceName the resource to find
   * @return the URL of the resource
   *
   * @throws AssertionError if the resource cannot be located
   */
  public static URL getResource(final ClassLoader classLoader, final String resourceName) {
    URL resource = classLoader.getResource(resourceName);
    assertThat(resource)
        .as("Resource '" + resourceName + "' associated with ClassLoader '" + classLoader + "'")
        .isNotNull();
    return resource;
  }

  /**
   * Copies a {@code resource} to a {@code file} in {@code targetFolder}.
   *
   * @param resource the resource to copy
   * @param targetFolder the folder in which to create the file
   * @param fileName the name of the file to create
   * @return the newly created file
   *
   * @throws AssertionError if the resource cannot be located
   *
   * @throws UncheckedIOException if an I/O exception occurs or the file exists but is a directory
   *         rather than a regular file, does not exist but cannot be created, or cannot be opened
   *         for any other reason
   */
  public static File createFileFromResource(final URL resource, final File targetFolder,
      final String fileName) {
    File targetFile = new File(targetFolder, fileName);
    copyResourceToFile(resource, targetFile);
    return targetFile;
  }

  /**
   * Copies a {@code resourceName} using the specified {@code ClassLoader} to a {@code file} in
   * {@code targetFolder}.
   *
   * @param classLoader the {@code ClassLoader} from which to retrieve the resource
   * @param resourceName the resource to copy
   * @param targetFolder the folder in which to create the file
   * @param fileName the name of the file to create
   * @return the newly created file
   *
   * @throws AssertionError if the resource cannot be located
   *
   * @throws UncheckedIOException if an I/O exception occurs or the file exists but is a directory
   *         rather than a regular file, does not exist but cannot be created, or cannot be opened
   *         for any other reason
   */
  public static File createFileFromResource(final ClassLoader classLoader,
      final String resourceName, final File targetFolder, final String fileName) {
    URL resource = getResource(classLoader, resourceName);
    return createFileFromResource(resource, targetFolder, fileName);
  }

  /**
   * Copies a {@code resource} to a {@code file} in {@code targetFolder}.
   *
   * @param resource the resource to copy
   * @param fileName the name of the file to create
   * @return the newly created file
   *
   * @throws AssertionError if the resource cannot be located
   *
   * @throws UncheckedIOException if an I/O exception occurs or the file exists but is a directory
   *         rather than a regular file, does not exist but cannot be created, or cannot be opened
   *         for any other reason
   */
  public static File createTempFileFromResource(final URL resource, final String fileName) {
    try {
      File targetFile = Files.createTempFile(fileName, null).toFile();
      targetFile.deleteOnExit();
      copyResourceToFile(resource, targetFile);
      return targetFile;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Copies a {@code resourceName} using the {@code ClassLoader} of {@code classInSamePackage} to a
   * {@code file} in the temporary-file directory.
   *
   * @param classInSamePackage a class sharing the {@code ClassLoader} from which to retrieve the
   *        resource
   * @param resourceName the resource to copy
   * @param fileName the name of the file to create
   * @return the newly created file
   *
   * @throws AssertionError if the resource cannot be located
   *
   * @throws UncheckedIOException if an I/O exception occurs or the file exists but is a directory
   *         rather than a regular file, does not exist but cannot be created, or cannot be opened
   *         for any other reason
   */
  public static File createTempFileFromResource(final Class<?> classInSamePackage,
      final String resourceName, final String fileName) {
    URL resource = getResource(classInSamePackage, resourceName);
    return createTempFileFromResource(resource, fileName);
  }

  /**
   * Copies a {@code resourceName} using the specified {@code ClassLoader} to a {@code file} in
   * the temporary-file directory.
   *
   * @param classLoader the {@code ClassLoader} from which to retrieve the resource
   * @param resourceName the resource to copy
   * @param fileName the name of the file to create
   * @return the newly created file
   *
   * @throws AssertionError if the resource cannot be located
   *
   * @throws UncheckedIOException if an I/O exception occurs or the file exists but is a directory
   *         rather than a regular file, does not exist but cannot be created, or cannot be opened
   *         for any other reason
   */
  public static File createTempFileFromResource(final ClassLoader classLoader,
      final String resourceName, final String fileName) {
    URL resource = getResource(classLoader, resourceName);
    return createTempFileFromResource(resource, fileName);
  }

  /**
   * Copies a {@code resourceName} using the {@code ClassLoader} of {@code classInSamePackage} to a
   * {@code file} in the temporary-file directory.
   *
   * @param classInSamePackage a class sharing the {@code ClassLoader} from which to retrieve the
   *        resource
   * @param resourceName the resource to copy
   * @return the newly created file
   *
   * @throws AssertionError if the resource cannot be located
   *
   * @throws UncheckedIOException if an I/O exception occurs or the file exists but is a directory
   *         rather than a regular file, does not exist but cannot be created, or cannot be opened
   *         for any other reason
   */
  public static File createTempFileFromResource(final Class<?> classInSamePackage,
      final String resourceName) {
    String fileName = resourceName.replaceFirst(".*/", "");
    URL resource = getResource(classInSamePackage, resourceName);
    return createTempFileFromResource(resource, fileName);
  }

  /**
   * Copies a {@code resourceName} using the specified {@code ClassLoader} to a {@code file} in
   * the temporary-file directory.
   *
   * @param classLoader the {@code ClassLoader} from which to retrieve the resource
   * @param resourceName the resource to copy
   * @return the newly created file
   *
   * @throws AssertionError if the resource cannot be located
   *
   * @throws UncheckedIOException if an I/O exception occurs or the file exists but is a directory
   *         rather than a regular file, does not exist but cannot be created, or cannot be opened
   *         for any other reason
   */
  public static File createTempFileFromResource(final ClassLoader classLoader,
      final String resourceName) {
    String fileName = resourceName.replaceFirst(".*/", "");
    URL resource = getResource(classLoader, resourceName);
    return createTempFileFromResource(resource, fileName);
  }

  /**
   * Copies a directory, pointed to by a {@code resource}, to a {@code targetFolder}
   *
   * @param resource a file-based resource referencing a directory
   * @param targetFolder the directory to which to copy the resource and all files within that
   *        resource.
   *
   * @throws AssertionError if the resulting file does not exist
   *
   * @throws UncheckedIOException if an I/O exception occurs or the file exists but is a directory
   *         rather than a regular file, does not exist but cannot be created, or cannot be opened
   *         for any other reason
   */
  public static void copyDirectoryResource(final URL resource, final File targetFolder) {
    try {
      File source = new File(resource.getPath());
      assertThat(source)
          .as("Resource path '" + resource.getPath() + "'")
          .exists();
      FileUtils.copyDirectory(source, targetFolder);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void copyResourceToFile(final URL resource, final File targetFile) {
    try (InputStream inputStream = resource.openStream();
        OutputStream outputStream = new FileOutputStream(targetFile)) {
      IOUtils.copy(inputStream, outputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

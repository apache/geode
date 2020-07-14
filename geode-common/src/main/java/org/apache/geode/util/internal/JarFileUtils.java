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
package org.apache.geode.util.internal;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.lang3.StringUtils;

/**
 * A utility class to consolidate utility methods used by Geode when accessing or processing
 * {@link JarFile}
 *
 * @see JarFile
 * @since 1.14
 */
public class JarFileUtils {

  private static final String[] EMPTY_STRING_ARRAY = {};
  private static final String EMPTY_STRING = "";

  /**
   * Returns a String[] of values from a {@link Manifest} file for a given Attribute name.
   * Returns an empty array if no attribute found.
   * Delegates to {@link #getAttributesFromManifest(Manifest, String, String)} with a single space
   * " " delimiter.
   *
   * @param manifest - a manifest file {@link Manifest}
   * @param attributeName - of type {@link String}
   * @return String[] of values associated to the attribute name. Empty array is no attribute found
   */
  public static String[] getAttributesFromManifest(Manifest manifest, String attributeName) {
    return getAttributesFromManifest(manifest, attributeName, " ");
  }

  /**
   * Returns a String[] of values from a {@link Manifest} file for a given Attribute name.
   * Returns an empty array if no attribute found.
   * Delegates to {@link #getAttributesFromManifest(Manifest, Attributes.Name, String)} with a
   * single space
   * " " delimiter.
   *
   * @param manifest - a manifest file {@link Manifest}
   * @param attributeName - of type {@link Attributes.Name}
   * @return String[] of values associated to the attribute name. Empty array is no attribute found
   */
  public static String[] getAttributesFromManifest(Manifest manifest,
      Attributes.Name attributeName) {
    return getAttributesFromManifest(manifest, attributeName, " ");
  }

  /**
   * Returns a String[] of values from a {@link Manifest} file for a given Attribute name.
   * Returns an empty array if no attribute found.
   * Delegates to {@link #getAttributesFromManifest(Manifest, String, String)} with a specified
   * delimiter.
   *
   * @param manifest - a manifest file {@link Manifest}
   * @param attributeName - of type {@link Attributes.Name}
   * @param entryDelimiter - the delimiter to be used to split the retrieved values
   * @return String[] of values associated to the attribute name. Empty array is no attribute found
   */
  public static String[] getAttributesFromManifest(Manifest manifest, Attributes.Name attributeName,
      String entryDelimiter) {
    return getAttributesFromManifest(manifest, attributeName.toString(), entryDelimiter);
  }

  /**
   * Returns a String[] of values from a {@link Manifest} file for a given Attribute name. Values
   * are split according to the provided delimiter.
   * Returns an empty array if no attribute found.
   *
   * @param manifest - a manifest file {@link Manifest}
   * @param attributeName - of type {@link String}
   * @param entryDelimiter - the delimiter to be used to split the retrieved values
   * @return String[] of values associated to the attribute name. Empty array is no attribute found
   */
  public static String[] getAttributesFromManifest(Manifest manifest, String attributeName,
      String entryDelimiter) {
    if (manifest != null) {
      String value = manifest.getMainAttributes().getValue(attributeName);
      if (!StringUtils.isEmpty(value)) {
        return value.split(entryDelimiter);
      } else {
        return EMPTY_STRING_ARRAY;
      }
    } else {
      return EMPTY_STRING_ARRAY;
    }
  }

  /**
   * Returns an {@link Optional} of a {@link Manifest} file found within the {@link JarFile}.
   *
   * @param jarFile the {@link JarFile} used to extract the {@link Manifest} file from
   * @return {@link Optional<Manifest>}. {@link Optional#empty()} returned if no manifest file found
   */
  public static Optional<Manifest> getManifestFromJarFile(JarFile jarFile) throws IOException {
    if (jarFile != null) {
      return Optional.ofNullable(jarFile.getManifest());
    }
    return Optional.empty();
  }

  /**
   * Returns the rootPath / parent Directory path,{@link String}, for the {@link JarFile}.
   *
   * @param jarFile the {@link JarFile} from which the rootPath is to be extracted
   * @return the rootPath / parentDirectory Path from the {@link JarFile}. Returns empty
   *         {@link String}
   *         when the jarFile is {@link null}
   */
  public static String getRootPathFromJarFile(JarFile jarFile) {
    if (jarFile != null) {
      return jarFile.getName().substring(0, jarFile.getName().lastIndexOf(File.separator));
    }
    return EMPTY_STRING;
  }

  /**
   * Returns the file name,{@link String}, for the {@link JarFile}.
   *
   * @param jarFile the {@link JarFile} from which the fileName is to be extracted
   * @return {@link Optional<String>} optional fileName for the specified {@link JarFile}.
   *         {@link Optional#empty()}
   *         is returned when the {@link JarFile} is null
   */
  public static Optional<String> getFileNameFromJarFile(JarFile jarFile) {
    if (jarFile != null) {
      return Optional
          .of(jarFile.getName().substring(jarFile.getName().lastIndexOf(File.separator)));
    }
    return Optional.empty();
  }

  /**
   * Creates a {@link JarFile} given a name and a path to the containing directory.
   *
   * @param rootPath root path to the {@link JarFile} location.
   * @param fileName the name of {@link JarFile} which may or may not contain the ".jar" extension.
   * @return a {@link JarFile} represented by the provided root path and file name.
   * @throws IOException if the {@link JarFile} cannot be created.
   */
  public static JarFile getJarFileForPath(String rootPath, String fileName) throws IOException {
    if (fileName.endsWith(".jar")) {
      return new JarFile(rootPath + File.separator + fileName);
    } else {
      return new JarFile(rootPath + File.separator + fileName + ".jar");
    }
  }
}

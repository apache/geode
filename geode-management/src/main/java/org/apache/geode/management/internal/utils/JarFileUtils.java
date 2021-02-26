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
package org.apache.geode.management.internal.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.jar.JarInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;


/**
 * Encapsulates functionality that had previously exited in DeployedJar and JarDeployer that needed
 * to be broken out when introducing the JarDeploymentService.
 *
 * Most methods apply a regex to a file name to extract some part of it (artifactId, version, etc.)
 */
public class JarFileUtils {

  // Every deployed file will use this scheme to signify the sequence it's been deployed
  public static final Pattern DEPLOYED_FILE_PATTERN =
      Pattern.compile("(?<baseName>..*)\\.v(?<version>\\d++).jar$");
  private static final Pattern USER_VERSION_PATTERN =
      Pattern.compile("(?<artifact>.*?)[-.]\\d+.*.*?\\.jar$");

  /**
   * get the artifact id from the files deployed by the user. This will recognize files with
   * SEMANTIC_VERSION_PATTERN, it will strip off the version part from the filename. For all other
   * file names, it will just return the basename.
   *
   * @param deployedJarFileName the filename that's deployed by the user. could be in the form of
   *        abc.jar or abc-1.0.0.jar, both should return abc
   * @return the artifact id of the string
   */
  public static String getArtifactId(String deployedJarFileName) {
    Matcher semanticVersionMatcher = USER_VERSION_PATTERN.matcher(deployedJarFileName);
    if (semanticVersionMatcher.matches()) {
      return semanticVersionMatcher.group("artifact");
    } else {
      return FilenameUtils.getBaseName(deployedJarFileName);
    }
  }

  public static boolean isSemanticVersion(String filename) {
    return USER_VERSION_PATTERN.matcher(filename).find();
  }

  /**
   * Peek into the JAR data and make sure that it is valid JAR content.
   *
   * @param jarFile Jar containing data to be validated.
   * @return True if the data has JAR content, false otherwise
   */
  public static boolean hasValidJarContent(File jarFile) {
    boolean valid = false;

    try (FileInputStream fileInputStream = new FileInputStream(jarFile);
        JarInputStream jarInputStream = new JarInputStream(fileInputStream)) {
      valid = jarInputStream.getNextJarEntry() != null;
    } catch (IOException ignore) {
      // Ignore this exception and just return false
    }

    return valid;
  }

  public static boolean isDeployedFile(String filename) {
    return DEPLOYED_FILE_PATTERN.matcher(filename).find();
  }

  /**
   * Find the version number that's embedded in the name of this file
   *
   * @param filename Filename to get the version number from
   * @return The version number embedded in the filename
   */
  public static int extractVersionFromFilename(final String filename) {
    final Matcher matcher = DEPLOYED_FILE_PATTERN.matcher(filename);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(2));
    } else {
      return 0;
    }
  }

  public static String getDeployedFileBaseName(String sequencedJarFileName) {
    Matcher semanticVersionMatcher = DEPLOYED_FILE_PATTERN.matcher(sequencedJarFileName);
    if (semanticVersionMatcher.matches()) {
      return semanticVersionMatcher.group("baseName");
    } else {
      return null;
    }
  }

  /**
   * get the artifact id from the existing files on the server. This will skip files that
   * do not have sequence id appended to them.
   *
   * @param sequencedJarFileName the file names that exists on the server, it should always ends
   *        with a sequence number
   * @return the artifact id. if a file with no sequence number is passed in, this will return null
   */
  public static String toArtifactId(String sequencedJarFileName) {
    String baseName = getDeployedFileBaseName(sequencedJarFileName);
    if (baseName == null) {
      return null;
    }

    return getArtifactId(baseName + ".jar");
  }
}

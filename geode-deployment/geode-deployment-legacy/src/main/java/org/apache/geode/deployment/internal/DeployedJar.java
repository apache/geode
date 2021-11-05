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

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.management.internal.utils.JarFileUtils;


/**
 * ClassLoader for a single JAR file.
 *
 * @since GemFire 7.0
 */
public class DeployedJar {

  @MakeNotStatic("This object gets updated in the production code")
  private static final MessageDigest messageDigest = getMessageDigest();

  private final String artifactId;
  private final File file;
  private final byte[] md5hash;

  private static MessageDigest getMessageDigest() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException ignored) {
      // Failure just means we can't do a simple compare for content equality
    }
    return null;
  }

  public File getFile() {
    return file;
  }

  public int getVersion() {
    return JarFileUtils.extractVersionFromFilename(file.getName());
  }

  /**
   * Writes the given jarBytes to versionedJarFile
   */
  public DeployedJar(File versionedJarFile) {
    String artifactId = JarFileUtils.toArtifactId(versionedJarFile.getName());

    file = versionedJarFile;
    this.artifactId = artifactId;

    if (!JarFileUtils.hasValidJarContent(versionedJarFile)) {
      throw new IllegalArgumentException(
          "File does not contain valid JAR content: " + versionedJarFile.getAbsolutePath());
    }

    byte[] digest = null;
    try {
      if (messageDigest != null) {
        digest = JarFileUtils.fileDigest(file);
      }
    } catch (IOException e) {
      // Ignored
    }
    md5hash = digest;
  }

  /**
   * Get this jar's artifact ID, which is the part of the jar file name that precedes the version
   * information.
   *
   * @return the artifact ID for this jar
   */
  public String getArtifactId() {
    return artifactId;
  }

  /**
   * @return the filename as user deployed, i.e remove the sequence number
   */
  public String getDeployedFileName() {
    String fileBaseName = JarFileUtils.getDeployedFileBaseName(file.getName());
    if (fileBaseName == null) {
      throw new IllegalStateException("file name needs to have a sequence number");
    }
    return fileBaseName + ".jar";
  }

  public String getFileCanonicalPath() throws IOException {
    return file.getCanonicalPath();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (artifactId == null ? 0 : artifactId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DeployedJar other = (DeployedJar) obj;
    if (artifactId == null) {
      return other.artifactId == null;
    } else
      return artifactId.equals(other.artifactId);
  }

  @Override
  public String toString() {
    return getClass().getName() + '@' + System.identityHashCode(this) + '{'
        + "artifactId=" + artifactId
        + ",file=" + file.getAbsolutePath()
        + ",md5hash=" + toHex(md5hash)
        + ",version=" + getVersion()
        + '}';
  }

  private String toHex(byte[] data) {
    StringBuilder result = new StringBuilder();
    for (byte b : data) {
      result.append(String.format("%02x", b));
    }
    return result.toString();
  }
}

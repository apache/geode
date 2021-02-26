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
package org.apache.geode.internal.deployment;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.utils.JarFileUtils;

/**
 * ClassLoader for a single JAR file.
 *
 * @since GemFire 7.0
 */
public class DeployedJar {

  private static final Logger logger = LogService.getLogger();
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
    return this.file;
  }

  public int getVersion() {
    return JarFileUtils.extractVersionFromFilename(this.file.getName());
  }

  /**
   * Writes the given jarBytes to versionedJarFile
   */
  public DeployedJar(File versionedJarFile) {
    String artifactId = JarFileUtils.toArtifactId(versionedJarFile.getName());

    this.file = versionedJarFile;
    this.artifactId = artifactId;

    if (!JarFileUtils.hasValidJarContent(versionedJarFile)) {
      throw new IllegalArgumentException(
          "File does not contain valid JAR content: " + versionedJarFile.getAbsolutePath());
    }

    byte[] digest = null;
    try {
      if (messageDigest != null) {
        digest = fileDigest(this.file);
      }
    } catch (IOException e) {
      // Ignored
    }
    this.md5hash = digest;
  }

  /**
   * Uses MD5 hashes to determine if the original byte content of this DeployedJar is the same as
   * that past in.
   *
   * @param stagedFile File to compare the original content to
   * @return True of the MD5 hash is the same o
   */
  boolean hasSameContentAs(final File stagedFile) {
    // If the MD5 hash can't be calculated then silently return no match
    if (messageDigest == null || this.md5hash == null) {
      return false;
    }

    byte[] compareToMd5;
    try {
      compareToMd5 = fileDigest(stagedFile);
    } catch (IOException ex) {
      return false;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("For JAR file: {}, Comparing MD5 hash {} to {}", this.file.getAbsolutePath(),
          new String(this.md5hash), new String(compareToMd5));
    }
    return Arrays.equals(this.md5hash, compareToMd5);
  }

  private byte[] fileDigest(File file) throws IOException {
    try (BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file))) {
      byte[] data = new byte[8192];
      int read;
      while ((read = fis.read(data)) > 0) {
        messageDigest.update(data, 0, read);
      }
    }

    return messageDigest.digest();
  }

  /**
   * Get this jar's artifact ID, which is the part of the jar file name that precedes the version
   * information.
   *
   * @return the artifact ID for this jar
   */
  public String getArtifactId() {
    return this.artifactId;
  }

  /**
   * @return the filename as user deployed, i.e remove the sequence number
   */
  public String getDeployedFileName() {
    String fileBaseName = JarFileUtils.getDeployedFileBaseName(this.file.getName());
    if (fileBaseName == null) {
      throw new IllegalStateException("file name needs to have a sequence number");
    }
    return fileBaseName + ".jar";
  }

  public String getFileCanonicalPath() throws IOException {
    return this.file.getCanonicalPath();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (this.artifactId == null ? 0 : this.artifactId.hashCode());
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
    if (this.artifactId == null) {
      if (other.artifactId != null) {
        return false;
      }
    } else if (!this.artifactId.equals(other.artifactId)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append('@').append(System.identityHashCode(this)).append('{');
    sb.append("artifactId=").append(this.artifactId);
    sb.append(",file=").append(this.file.getAbsolutePath());
    sb.append(",md5hash=").append(toHex(this.md5hash));
    sb.append(",version=").append(this.getVersion());
    sb.append('}');
    return sb.toString();
  }

  private String toHex(byte[] data) {
    StringBuilder result = new StringBuilder();
    for (byte b : data) {
      result.append(String.format("%02x", b));
    }
    return result.toString();
  }
}

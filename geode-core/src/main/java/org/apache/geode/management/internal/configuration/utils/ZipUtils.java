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
package org.apache.geode.management.internal.configuration.utils;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

/****
 * Utilities class to zip/unzip directory
 *
 */
public class ZipUtils {

  public static void zipDirectory(Path sourceDirectory, Path targetFile) throws IOException {
    Path parentDir = targetFile.getParent();
    if (parentDir != null && !parentDir.toFile().exists()) {
      parentDir.toFile().mkdirs();
    }

    try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(targetFile))
         Stream<Path> stream = Files.walk(sourceDirectory)) {
      stream.filter(path -> !Files.isDirectory(path)).forEach(path -> {
        ZipEntry zipEntry = new ZipEntry(sourceDirectory.relativize(path).toString());
        try {
          zs.putNextEntry(zipEntry);
          zs.write(Files.readAllBytes(path));
          zs.closeEntry();
        } catch (Exception e) {
          throw new RuntimeException("Unable to write zip file", e);
        }
      });
    }
  }

  public static void zipDirectory(String sourceDirectoryPath, String targetFilePath)
      throws IOException {
    Path sourceDirectory = Paths.get(sourceDirectoryPath);
    Path targetFile = Paths.get(targetFilePath);

    zipDirectory(sourceDirectory, targetFile);
  }

  public static void unzip(String zipFilePath, String outputDirectoryPath) throws IOException {
    ZipFile zipFile = new ZipFile(zipFilePath);
    @SuppressWarnings("unchecked")
    Enumeration<ZipEntry> zipEntries = (Enumeration<ZipEntry>) zipFile.entries();

    try {
      while (zipEntries.hasMoreElements()) {
        ZipEntry zipEntry = zipEntries.nextElement();
        File entryDestination = new File(outputDirectoryPath + File.separator + zipEntry.getName());

        if (!entryDestination.toPath().normalize().startsWith(Paths.get(outputDirectoryPath))) {
          throw new IOException("Zip entry contained path traversal");
        }

        if (zipEntry.isDirectory()) {
          FileUtils.forceMkdir(entryDestination);
          continue;
        }
        File parent = entryDestination.getParentFile();
        if (parent != null) {
          FileUtils.forceMkdir(parent);
        }
        if (entryDestination.createNewFile()) {

          InputStream in = zipFile.getInputStream(zipEntry);
          OutputStream out = new FileOutputStream(entryDestination);
          try {
            IOUtils.copy(in, out);
          } finally {
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
          }
        } else {
          throw new IOException("Cannot create file :" + entryDestination.getCanonicalPath());
        }
      }
    } finally {
      zipFile.close();
    }
  }
}

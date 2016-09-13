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
package org.apache.geode.management.internal.configuration.utils;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Enumeration;
import java.util.Stack;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

/****
 * Utilities class to zip/unzip folder
 *
 */
public class ZipUtils {

  public static void zip(String sourceFolderPath , String targetFilePath) throws Exception {
    File sourceFolder = new File(sourceFolderPath);
    File targetFile = new File(targetFilePath);
    
    if (!sourceFolder.exists()) {
      throw new Exception("Source folder does not exist");
    }
    
    FileOutputStream fos = new FileOutputStream(targetFile);
    ZipOutputStream zos = new ZipOutputStream(fos);
    URI baseURI = sourceFolder.toURI();

    Stack<File> fileStack = new Stack<File>();
    fileStack.push(sourceFolder);

    while (!fileStack.isEmpty()) {
      File directory = fileStack.pop();

      for (File child : directory.listFiles()) {
        String name = baseURI.relativize(child.toURI()).getPath();

        if (child.isDirectory()) {
          fileStack.push(child);
          zos.putNextEntry(new ZipEntry(name));
        } else {
          if (!name.endsWith("zip")) {
            ZipEntry zipEntry = new ZipEntry(name);
            zos.putNextEntry(zipEntry);
            InputStream in = new FileInputStream(child);
            IOUtils.copy(in, zos);
            IOUtils.closeQuietly(in);
          }
        }
      }
    }
    IOUtils.closeQuietly(zos);
  }


  public static void unzip (String zipFilePath, String outputFolderPath) throws IOException {
    ZipFile zipFile = new ZipFile(zipFilePath);
    @SuppressWarnings("unchecked")
    Enumeration<ZipEntry> zipEntries = (Enumeration<ZipEntry>) zipFile.entries();

    try {
      while (zipEntries.hasMoreElements()) {
        ZipEntry zipEntry = zipEntries.nextElement();
        String fileName = outputFolderPath + File.separator + zipEntry.getName();

        if (zipEntry.isDirectory()) {
          FileUtils.forceMkdir(new File(fileName));
          continue;
        }
        File entryDestination = new File(fileName);
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



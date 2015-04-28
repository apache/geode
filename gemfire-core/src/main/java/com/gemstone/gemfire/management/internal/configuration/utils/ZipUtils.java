/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.configuration.utils;


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
 * @author bansods
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
          name = name.endsWith(File.separator) ? name : name + File.separator;
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



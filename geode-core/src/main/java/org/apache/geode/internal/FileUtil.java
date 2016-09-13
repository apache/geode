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
package org.apache.geode.internal;

import org.apache.geode.distributed.internal.DistributionConfig;

import java.io.*;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * This class contains static methods for manipulating files and directories,
 * such as recursively copying or deleting files.
 * 
 * TODO A lot of this functionality is probably duplicating apache commons io,
 * maybe we should switch to that.
 * 
 * 
 */
public class FileUtil {
  public static final long MAX_TRANSFER_SIZE = Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "FileUtil.MAX_TRANSFER_SIZE", 1024 * 1024).longValue();
  public static final boolean USE_NIO = !Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "FileUtil.USE_OLD_IO");
  public static final String extSeparator = ".";

  /**
   * Copy a file from the source file to the destination file.
   * If the source is a directory, it will be copied recursively.
   * 
   * Note that unlike unix cp, if the destination is directory,
   * the source *contents* will be copied to the destination *contents*, 
   * not as a subdirectory of dest.
   * 
   * @param source the source file or directory
   * @param dest the destination file or directory.
   * @throws IOException
   */
  public static void copy(File source, File dest) throws IOException {
    if(source.isDirectory()) {
      dest.mkdir();
      for(File child: listFiles(source)) {
        copy(child, new File(dest, child.getName()));
      }
    } else {
      if(source.exists()) {
        long lm = source.lastModified();
        if(dest.isDirectory()) {
          dest = new File(dest, source.getName());
        }
        FileOutputStream fos = new FileOutputStream(dest);
        try {
          FileInputStream fis = new FileInputStream(source);
          try {
            if(USE_NIO) {
              nioCopy(fos, fis);
            } else {
              oioCopy(source, fos, fis);
            }
          } finally {
            fis.close();
          }
        } finally {
          fos.close();
        }
        dest.setExecutable(source.canExecute(), true);
        dest.setLastModified(lm);
      }
    }
  }
  
  /**
   * Basically just like {@link File#listFiles()} but instead of returning null
   * returns an empty array. This fixes bug 43729
   */
  public static File[] listFiles(File dir) {
    File[] result = dir.listFiles();
    if (result == null) {
      result = new File[0];
    }
    return result;
  }
  /**
   * Basically just like {@link File#listFiles(FilenameFilter)} but instead of returning null
   * returns an empty array. This fixes bug 43729
   */
  public static File[] listFiles(File dir, FilenameFilter filter) {
    File[] result = dir.listFiles(filter);
    if (result == null) {
      result = new File[0];
    }
    return result;
  }
  /**
   * Copy a single file using NIO.
   * @throws IOException
   */
  private static void nioCopy(FileOutputStream fos, FileInputStream fis)
      throws IOException {
    FileChannel outChannel = fos.getChannel();
    FileChannel inChannel = fis.getChannel();
    long length = inChannel.size();
    long offset = 0;
    while(true) {
      long remaining = length - offset;
      
      long toTransfer = remaining < MAX_TRANSFER_SIZE ? remaining : MAX_TRANSFER_SIZE;
      long transferredBytes = inChannel.transferTo(offset, toTransfer, outChannel);
      offset += transferredBytes;
      length = inChannel.size();
      if(offset >= length) {
        break;
      }
    }
  }

  /**
   * Copy a single file using the java.io.
   * @throws IOException
   */
  private static void oioCopy(File source, FileOutputStream fos, FileInputStream fis)
  throws IOException {
    int size = (int) (source.length() < MAX_TRANSFER_SIZE ? source.length() : MAX_TRANSFER_SIZE);
    byte[] buffer = new byte[size];
    int read;
    while((read = fis.read(buffer)) > 0) {
      fos.write(buffer, 0, read);
    }

  }

  /**
   * Recursively delete a file or directory.
   * 
   * @throws IOException
   *           if the file or directory couldn't be deleted. Unlike File.delete,
   *           which just returns false.
   */
  public static void delete(File file) throws IOException {
    if(!file.exists())
      return;

    if(file.isDirectory()) {
      for(File child: listFiles(file)) {
        delete(child);
      }
    }

    Files.delete(file.toPath());
  }
  
  /**
   * Recursively delete a file or directory.
   * A description of any files or directories
   * that can not be deleted will be added to failures
   * if failures is non-null.
   * This method tries to delete as much as possible.
   */
  public static void delete(File file, StringBuilder failures) {
    if(!file.exists())
      return;

    if(file.isDirectory()) {
      for(File child: listFiles(file)) {
        delete(child, failures);
      }
    }

    try{
      Files.delete(file.toPath());
    } catch (IOException e) {
      if (failures != null) {
        failures.append("Could not delete ")
            .append(file)
            .append(" due to ")
            .append(e.getMessage())
            .append('\n');
      }
    }
  }

  /**
   * Find the file whose name matches the given regular
   * expression. The regex is matched against the absolute
   * path of the file.
   * 
   * This could probably use a lot of optimization!
   */
  public static File find(File baseFile, String regex) {
    if(baseFile.getAbsolutePath().matches(regex)) {
      return baseFile;
    }
    if(baseFile.exists() && baseFile.isDirectory()) {
      for(File child: listFiles(baseFile)) {
        File foundFile =  find(child, regex);
        if(foundFile != null) {
          return foundFile;
        }
      }
    }
    return null;
  }
  
  /**
   * Find a files in a given base directory that match
   * a the given regex. The regex is matched against the
   * full path of the file.
   */
  public static List<File> findAll(File baseFile, String regex) {
    ArrayList<File> found = new ArrayList<File>();
    findAll(baseFile, regex, found);
    return found;
  }
  
  /**
   * Destroys all files that match the given regex that
   * are in the given directory.
   * If a destroy fails it is ignored and an attempt is
   * made to destroy any other files that match.
   */
  public static void deleteMatching(File baseFile, String regex) {
    if(baseFile.exists() && baseFile.isDirectory()) {
      for(File child: listFiles(baseFile)) {
        if (child.getName().matches(regex)) {
          try {
            delete(child);
          } catch (IOException ignore) {
          }
        }
      }
    }
  }

  /** Implementation of findAll. */
  private static void findAll(File baseFile, String regex, List<File> found) {
    if(baseFile.getAbsolutePath().matches(regex)) {
      found.add(baseFile);
    }
    if(baseFile.exists() && baseFile.isDirectory()) {
      for(File child: listFiles(baseFile)) {
        findAll(child, regex, found);
      }
    }
  }

  /**
   * Convert a file into a relative path from a given parent. This is useful if
   * you want to write out the file name into that parent directory.
   * 
   * @param parent
   *          The parent directory.
   * @param file
   *          The file we want to covert to a relative file.
   * @return A file, such that new File(parent, returnValue) == file. Note that
   *         if file does not have the parent in it's path, an the absolute
   *         version if the file is returned.
   */
  public static File removeParent(File parent, File file) {
    String absolutePath = file.getAbsolutePath();
    String parentAbsolutePath = parent.getAbsolutePath();
    String newPath = absolutePath.replace(parentAbsolutePath + "/", "");
    return new File(newPath);
  }

  /**
   * Copy a URL to a file.
   * @throws IOException
   */
  public static void copy(URL url, File file) throws IOException {
    InputStream is = url.openStream();
    try {
      OutputStream os = new FileOutputStream(file);
      try {
        byte[] buffer = new byte[8192];
        int read;
        while((read = is.read(buffer)) > 0) {
          os.write(buffer, 0, read);
        }
      } finally {
        os.close();
      }
    } finally {
      is.close();
    }
    
  }
  
  /**
   * A safer version of File.mkdirs, which works around
   * a race in the 1.5 JDK where two VMs creating the same 
   * directory chain at the same time could end up in one
   * VM failing to create a subdirectory.
   * @param file
   */
  public static boolean mkdirs(File file) {
    final File parentFile = file.getAbsoluteFile().getParentFile();
    if(! parentFile.exists()) {
      mkdirs(parentFile);
    }
    //As long as someone successfully created the parent file
    //go ahead and create the child directory.
    if(parentFile.exists()) {
      return file.mkdir();
    } else {
      return false;
    }
  }
  
  /**
   * Returns the file name with the extension stripped off (if it has one).
   * 
   * @param fileName the file name
   * @return the file name with the extension stripped off (if it had one)
   */
  public static String stripOffExtension(final String fileName) {
    if (fileName.contains(extSeparator)) {
      // strip off the extension and right-most "."
      return fileName.substring(0, fileName.lastIndexOf(extSeparator));
    }
    return fileName;
  }
}

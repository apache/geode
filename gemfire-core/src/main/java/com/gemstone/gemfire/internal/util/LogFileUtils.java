/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.util;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility class for copying log files
 * 
 * @author gregp
 * @since 6.5
 */
public class LogFileUtils {
  
  

  /**
   * The default buffer size to use.
   */
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;
  
  /**
   * Copy a file preserving the date.
   * 
   * @param srcFile  the validated source file, must not be <code>null</code>
   * @param destFile  the validated destination file, must not be <code>null</code>
   * @throws IOException if an error occurs
   */
  public static void copyFile(File srcFile, File destFile) throws IOException {
      if (destFile.exists() && destFile.isDirectory()) {
          throw new IOException("Destination '" + destFile + "' exists but is a directory");
      }

      FileInputStream input = new FileInputStream(srcFile);
      try {
          FileOutputStream output = new FileOutputStream(destFile);
          try {
              copy(input, output);
          } finally {
              close(output);
          }
      } finally {
          close(input);
      }

      if (srcFile.length() != destFile.length()) {
          throw new IOException("Failed to copy full contents from '" +
                  srcFile + "' to '" + destFile + "'");
      }
      destFile.setLastModified(srcFile.lastModified());
  }
  
  /**
   * Copy bytes from an <code>InputStream</code> to an
   * <code>OutputStream</code>.
   * 
   * @param input  the <code>InputStream</code> to read from
   * @param output  the <code>OutputStream</code> to write to
   * @return the number of bytes copied
   * @throws NullPointerException if the input or output is null
   * @throws IOException if an I/O error occurs
   * @throws ArithmeticException if the byte count is too large
   */
  public static int copy(InputStream input, OutputStream output) throws IOException {
      byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
      long count = 0;
      int n = 0;
      while (-1 != (n = input.read(buffer))) {
          output.write(buffer, 0, n);
          count += n;
      }
      if (count > Integer.MAX_VALUE) {
          return -1;
      }
      return (int) count;
  }
  
  /**
   * Close an <code>InputStream</code> ignoring exceptions.
   * @param input  the InputStream to close, may be null or already closed
   */
  public static void close(InputStream input) {
      try {
          if (input != null) {
              input.close();
          }
      } catch (IOException ioe) {
          // ignore
      }
  }

  /**
   * Close an <code>OutputStream</code> ignoring exceptions.
   * @param output  the OutputStream to close, may be null or already closed
   */
  public static void close(OutputStream output) {
      try {
          if (output != null) {
              output.close();
          }
      } catch (IOException ioe) {
          // ignore
      }
  }

  
  /**
   * Delete a file ignoring exceptions.
   * @param file to delete, can be <code>null</code>
   */
  public static boolean delete(File file) {
      if (file == null) {
          return false;
      }
      try {
          return file.delete();
      } catch (Exception e) {
      	e.printStackTrace();
          return false;
      }
  }


  public static boolean renameAggressively(File oldFile,File newFile) {
    boolean renameOK = oldFile.renameTo(newFile.getAbsoluteFile());
    if(!renameOK) {
      try {
        LogFileUtils.copyFile( oldFile, newFile.getAbsoluteFile() );
        long timeStop = System.currentTimeMillis()+600000;
        boolean noDelete = !oldFile.delete();
        if(noDelete) {
          try {
            RandomAccessFile raf = new RandomAccessFile(oldFile,"rws");
            raf.setLength(0);
            raf.close();
            noDelete = false;
          } catch(Exception e) {
	    // ignore this
          }
        }

        if(noDelete) {
          LogFileUtils.delete(newFile.getAbsoluteFile());
         renameOK = false;
        } else {
          renameOK = true;
        }
      } catch(IOException e) {
        // ignore this and just return failure
      }
    }  
    return renameOK;
  }

}

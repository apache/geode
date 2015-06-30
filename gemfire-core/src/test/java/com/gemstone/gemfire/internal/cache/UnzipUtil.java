/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * Utility methods for unzipping a file. Used for backwards compatibility
 * test for gateway queue to handle checked in test code.
 * @author dsmith
 *
 */
public class UnzipUtil {

  public static final void unzip(InputStream input, String targetDir) throws IOException {
    
    File dir = new File(targetDir);
    if(!dir.exists() && !dir.mkdir()) {
      throw new IOException("Unable to create dir" + dir);
    }
    
    ZipInputStream zipInput;

    zipInput = new ZipInputStream(input);


    ZipEntry entry;
    while((entry = zipInput.getNextEntry()) != null) {

      File newFile = new File(dir,entry.getName());
      if(entry.isDirectory()) {
        if(!newFile.mkdirs()) {
          throw new IOException("Unable to create directory" + newFile);
        }
        continue;
      }

      copyInputStream(zipInput,
          new BufferedOutputStream(new FileOutputStream(newFile)));
      zipInput.closeEntry();
    }

    zipInput.close();
  }
  
  public static final void copyInputStream(InputStream in, OutputStream out)
  throws IOException
  {
    byte[] buffer = new byte[1024];
    int len;

    while((len = in.read(buffer)) >= 0)
      out.write(buffer, 0, len);

    out.close();
  }

  private UnzipUtil() {
    
  }
}

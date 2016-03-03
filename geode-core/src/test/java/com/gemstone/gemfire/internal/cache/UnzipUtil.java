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

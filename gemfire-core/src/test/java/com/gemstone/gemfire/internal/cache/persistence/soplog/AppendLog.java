/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class AppendLog {

  public static AppendLogReader recover(File f) throws IOException {
    throw new RuntimeException("Not implemented");
  }
  
  public static AppendLogWriter create(File f) throws IOException {
    DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
    return new AppendLogWriter(f, dos);
  }
  
  public static class AppendLogReader {
  }
  
  public static class AppendLogWriter implements Closeable {
    private final File file;
    private final DataOutputStream out;
    
    private AppendLogWriter(File f, DataOutputStream out) {
      this.file = f;
      this.out = out;
    }
    
    public synchronized void append(byte[] key, byte[] value) throws IOException {
      out.writeInt(key.length);
      out.writeInt(value.length);
      out.write(key);
      out.write(value);
    }

    @Override
    public void close() throws IOException {
      out.close();
    }
    
    public File getFile() {
      return file;
    }
  }
}

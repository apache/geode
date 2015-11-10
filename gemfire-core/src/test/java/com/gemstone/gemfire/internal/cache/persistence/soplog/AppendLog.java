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

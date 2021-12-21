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
package org.apache.geode.util.internal;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Delegates all operations to the original underlying OutputStream and a mutable branch
 * OutputStream.
 *
 * @since GemFire 7.0
 */
public class TeeOutputStream extends FilterOutputStream {

  private volatile OutputStream branch;

  public TeeOutputStream(OutputStream out) {
    super(out);
  }

  public TeeOutputStream(OutputStream out, OutputStream branch) {
    this(out);
    this.branch = branch;
  }

  public OutputStream getOutputStream() {
    return super.out;
  }

  public OutputStream getBranchOutputStream() {
    return branch;
  }

  public void setBranchOutputStream(OutputStream branch) {
    if (branch == super.out) {
      throw new IllegalArgumentException("TeeOutputStream cannot set branch same as out");
    }
    this.branch = branch;
  }

  @Override
  public void write(int b) throws IOException {
    super.write(b);
    OutputStream os = branch;
    if (os != null) {
      os.write(b);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    super.write(b, off, len);
    OutputStream os = branch;
    if (os != null) {
      os.write(b, off, len);
    }
  }

  @Override
  public void flush() throws IOException {
    super.flush();
    OutputStream os = branch;
    if (os != null) {
      os.flush();
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    OutputStream os = branch;
    if (os != null) {
      os.close();
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("outputStream=").append(super.out);
    sb.append(", branchOutputStream=").append(branch);
    return sb.append("}").toString();
  }
}

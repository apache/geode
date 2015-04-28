/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Delegates all operations to the original underlying OutputStream and a
 * mutable branch OutputStream.
 * 
 * @author Kirk Lund
 * @since 7.0
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
    return this.branch;
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
    OutputStream os = this.branch;
    if (os != null) {
      os.write(b);
    }
  }

  @Override
  public void flush() throws IOException {
    super.flush();
    OutputStream os = this.branch;
    if (os != null) {
      os.flush();
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    OutputStream os = this.branch;
    if (os != null) {
      os.close();
    }
  }
  
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("outputStream=").append(super.out);
    sb.append(", branchOutputStream=").append(this.branch);
    return sb.append("}").toString();
  }
}

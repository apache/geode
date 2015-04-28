/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream;

/**
 * Provides byte stream for testing. Use {@link #getDataInput()} and
 * {@link #getDataOutput()} to get DataInput or DataOutput as needed for
 * testing.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public class ByteArrayData {

  private ByteArrayOutputStream baos;

  public ByteArrayData() {
    this.baos = new ByteArrayOutputStream();
  }

  public int size() {
    return this.baos.size();
  }
  
  public boolean isEmpty() {
    return this.baos.size() == 0;
  }
  
  /**
   * Returns a <code>DataOutput</code> to write to
   */
  public DataOutputStream getDataOutput() {
    return new DataOutputStream(this.baos);
  }

  /**
   * Returns a <code>DataInput</code> to read from
   */
  public DataInput getDataInput() {
    ByteBuffer bb = ByteBuffer.wrap(this.baos.toByteArray());
    ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
    return bbis;
  }

  public DataInputStream getDataInputStream() {
    ByteBuffer bb = ByteBuffer.wrap(this.baos.toByteArray());
    ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
    return new DataInputStream(bbis);
  }

}

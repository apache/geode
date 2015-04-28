/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.junit.UnitTest;

@Category(UnitTest.class)
public class OldVLJUnitTest extends TestCase {
  private ByteArrayOutputStream baos;
  private DataOutputStream dos;

  private DataOutput createDOS() {
    this.baos = new ByteArrayOutputStream(32);
    this.dos = new DataOutputStream(baos);
    return dos;
  }

  private DataInput createDIS() throws IOException {
    this.dos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(this.baos
        .toByteArray());
    return new DataInputStream(bais);
  }

  public void testMinByte() throws IOException {
    InternalDataSerializer.writeVLOld(1, createDOS());
    assertEquals(1, InternalDataSerializer.readVLOld(createDIS()));
  }

  public void testMaxByte() throws IOException {
    InternalDataSerializer.writeVLOld(125, createDOS());
    assertEquals(125, InternalDataSerializer.readVLOld(createDIS()));
  }

  public void testMinShort() throws IOException {
    InternalDataSerializer.writeVLOld(126, createDOS());
    assertEquals(126, InternalDataSerializer.readVLOld(createDIS()));
  }

  public void testMaxShort() throws IOException {
    InternalDataSerializer.writeVLOld(0x7fff, createDOS());
    assertEquals(0x7fff, InternalDataSerializer.readVLOld(createDIS()));
  }

  public void testMinInt() throws IOException {
    InternalDataSerializer.writeVLOld(0x7fff + 1, createDOS());
    assertEquals(0x7fff + 1, InternalDataSerializer.readVLOld(createDIS()));
  }

  public void testMaxInt() throws IOException {
    InternalDataSerializer.writeVLOld(0x7fffffff, createDOS());
    assertEquals(0x7fffffff, InternalDataSerializer.readVLOld(createDIS()));
  }

  public void testMinLong() throws IOException {
    InternalDataSerializer.writeVLOld(0x7fffffffL + 1, createDOS());
    assertEquals(0x7fffffffL + 1, InternalDataSerializer.readVLOld(createDIS()));
  }

  public void testMaxLong() throws IOException {
    InternalDataSerializer.writeVLOld(Long.MAX_VALUE, createDOS());
    assertEquals(Long.MAX_VALUE, InternalDataSerializer.readVLOld(createDIS()));
  }

}

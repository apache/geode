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

/**
 * Test for the new variable length format
 * @author dsmith
 * 
 * TODO these tests need some work. I don't think they really represent
 * edge cases for this variable length value.
 *
 */
@Category(UnitTest.class)
public class VLJUnitTest extends TestCase {
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

  public void testZero() throws IOException {
    InternalDataSerializer.writeUnsignedVL(0, createDOS());
    assertEquals(0, InternalDataSerializer.readUnsignedVL(createDIS()));
  }
  
  public void testOne() throws IOException {
    InternalDataSerializer.writeUnsignedVL(1, createDOS());
    assertEquals(1, InternalDataSerializer.readUnsignedVL(createDIS()));
  }
  
  public void testMinusOne() throws IOException {
    InternalDataSerializer.writeUnsignedVL(-1, createDOS());
    assertEquals(-1, InternalDataSerializer.readUnsignedVL(createDIS()));
  }

  public void testMaxByte() throws IOException {
    InternalDataSerializer.writeUnsignedVL(0x7F, createDOS());
    assertEquals(0x7F, InternalDataSerializer.readUnsignedVL(createDIS()));
  }
  
  public void testMaxNegativeByte() throws IOException {
    InternalDataSerializer.writeUnsignedVL(-0x7F, createDOS());
    assertEquals(-0x7F, InternalDataSerializer.readUnsignedVL(createDIS()));
  }

  public void testMinShort() throws IOException {
    InternalDataSerializer.writeUnsignedVL(0xFF, createDOS());
    assertEquals(0xFF, InternalDataSerializer.readUnsignedVL(createDIS()));
  }
  
  public void testMinNegativeShort() throws IOException {
    InternalDataSerializer.writeUnsignedVL(-0xFF, createDOS());
    assertEquals(-0xFF, InternalDataSerializer.readUnsignedVL(createDIS()));
  }

  public void testMaxShort() throws IOException {
    InternalDataSerializer.writeUnsignedVL(0x7fff, createDOS());
    assertEquals(0x7fff, InternalDataSerializer.readUnsignedVL(createDIS()));
  }
  
  public void testMaxNegativeShort() throws IOException {
    InternalDataSerializer.writeUnsignedVL(-0x7fff, createDOS());
    assertEquals(-0x7fff, InternalDataSerializer.readUnsignedVL(createDIS()));
  }

  public void testMin3Byte() throws IOException {
    InternalDataSerializer.writeUnsignedVL(0xffff, createDOS());
    assertEquals(0xffff, InternalDataSerializer.readUnsignedVL(createDIS()));
  }
  
  public void testMin3Negative() throws IOException {
    InternalDataSerializer.writeUnsignedVL(-0xffff, createDOS());
    assertEquals(-0xffff, InternalDataSerializer.readUnsignedVL(createDIS()));
  }

  public void testMaxInt() throws IOException {
    InternalDataSerializer.writeUnsignedVL(0x7fffffff, createDOS());
    assertEquals(0x7fffffff, InternalDataSerializer.readUnsignedVL(createDIS()));
  }

  public void testMinLong() throws IOException {
    InternalDataSerializer.writeUnsignedVL(0x7fffffffL + 1, createDOS());
    assertEquals(0x7fffffffL + 1, InternalDataSerializer.readUnsignedVL(createDIS()));
  }

  public void testMaxLong() throws IOException {
    InternalDataSerializer.writeUnsignedVL(Long.MAX_VALUE, createDOS());
    assertEquals(Long.MAX_VALUE, InternalDataSerializer.readUnsignedVL(createDIS()));
  }

}

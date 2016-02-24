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
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Test for the new variable length format
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

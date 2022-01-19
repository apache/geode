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
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import org.apache.geode.internal.InternalDataSerializer;

public class OldVLJUnitTest {

  private ByteArrayOutputStream baos;
  private DataOutputStream dos;

  private DataOutput createDOS() {
    baos = new ByteArrayOutputStream(32);
    dos = new DataOutputStream(baos);
    return dos;
  }

  private DataInput createDIS() throws IOException {
    dos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    return new DataInputStream(bais);
  }

  @Test
  public void testMinByte() throws IOException {
    InternalDataSerializer.writeVLOld(1, createDOS());
    assertEquals(1, InternalDataSerializer.readVLOld(createDIS()));
  }

  @Test
  public void testMaxByte() throws IOException {
    InternalDataSerializer.writeVLOld(125, createDOS());
    assertEquals(125, InternalDataSerializer.readVLOld(createDIS()));
  }

  @Test
  public void testMinShort() throws IOException {
    InternalDataSerializer.writeVLOld(126, createDOS());
    assertEquals(126, InternalDataSerializer.readVLOld(createDIS()));
  }

  @Test
  public void testMaxShort() throws IOException {
    InternalDataSerializer.writeVLOld(0x7fff, createDOS());
    assertEquals(0x7fff, InternalDataSerializer.readVLOld(createDIS()));
  }

  @Test
  public void testMinInt() throws IOException {
    InternalDataSerializer.writeVLOld(0x7fff + 1, createDOS());
    assertEquals(0x7fff + 1, InternalDataSerializer.readVLOld(createDIS()));
  }

  @Test
  public void testMaxInt() throws IOException {
    InternalDataSerializer.writeVLOld(0x7fffffff, createDOS());
    assertEquals(0x7fffffff, InternalDataSerializer.readVLOld(createDIS()));
  }

  @Test
  public void testMinLong() throws IOException {
    InternalDataSerializer.writeVLOld(0x7fffffffL + 1, createDOS());
    assertEquals(0x7fffffffL + 1, InternalDataSerializer.readVLOld(createDIS()));
  }

  @Test
  public void testMaxLong() throws IOException {
    InternalDataSerializer.writeVLOld(Long.MAX_VALUE, createDOS());
    assertEquals(Long.MAX_VALUE, InternalDataSerializer.readVLOld(createDIS()));
  }

}

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

package org.apache.geode.serialization;

import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public enum SerializationType implements Serializer, Deserializer {
  STRING(String.class, SerializationType::serializeString, SerializationType::deserializeString),
  BYTE_BLOB(byte[].class, (x) -> x, (x) -> x),
  INT(int.class, SerializationType::serializeInt, SerializationType::deserializeInt),
  BYTE(byte.class, SerializationType::serializeByte, SerializationType::deserializeByte),
  SHORT(short.class, SerializationType::serializeShort, SerializationType::deserializeShort),
  LONG(long.class, SerializationType::serializeLong, SerializationType::deserializeLong),
  JSON(PdxInstance.class, JSONFormatter::toJSONByteArray, JSONFormatter::fromJSON);

  private static final Charset UTF8 = Charset.forName("UTF-8");
  public final Class klass;
  public final Serializer serializer;
  public final Deserializer deserializer;

  <T> SerializationType(Class<T> klass, Serializer<T> serializer, Deserializer<T> deserializer) {
    this.klass = klass;
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  @Override
  public byte[] serialize(Object item) {
    return this.serializer.serialize(item);
  }

  @Override
  public Object deserialize(byte[] bytes) {
    return this.deserializer.deserialize(bytes);
  }

  private static byte[] serializeString(String value) {
    return value.getBytes(UTF8);
  }

  private static String deserializeString(byte[] array) {
    return new String(array, UTF8);
  }

  private static byte[] serializeInt(int i) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
  }

  private static int deserializeInt(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getInt();
  }

  private static byte[] serializeShort(short i) {
    return ByteBuffer.allocate(Short.BYTES).putShort(i).array();
  }

  private static short deserializeShort(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getShort();
  }

  private static byte[] serializeLong(long i) {
    return ByteBuffer.allocate(Long.BYTES).putLong(i).array();
  }

  private static long deserializeLong(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getLong();
  }

  private static byte[] serializeByte(byte i) {
    return ByteBuffer.allocate(Byte.BYTES).put(i).array();
  }

  private static byte deserializeByte(byte[] bytes) {
    return ByteBuffer.wrap(bytes).get();
  }
}


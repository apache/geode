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
package org.apache.geode.internal.util;

import static org.apache.geode.internal.util.BlobHelper.deserializeBlob;
import static org.apache.geode.internal.util.BlobHelper.serializeTo;
import static org.apache.geode.internal.util.BlobHelper.serializeToBlob;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * Unit Tests for {@link BlobHelper}.
 *
 * TODO: add coverage for additional methods:
 * <li>{@link BlobHelper#deserializeBlob(byte[], KnownVersion, ByteArrayDataInput)}
 * <li>{@link BlobHelper#deserializeBuffer(ByteArrayDataInput, int)}
 * <li>{@link BlobHelper#deserializeOffHeapBlob(StoredObject)}
 * <li>{@link BlobHelper#serializeToBlob(Object, KnownVersion)}
 */
public class BlobHelperTest {

  private static final int HDOS_ALLOC_SIZE = 32;

  private static final String CLASS_NOT_FOUND_MESSAGE =
      "ClassNotFoundSerialization.readObject fake exception";

  private Map<Object, Object> mapWithTwoEntries;

  private byte[] bytesOfClassNotFoundSerialization;
  private byte[] bytesOfMap;
  private byte[] bytesOfNull;
  private byte[] zeroBytes;

  @Before
  public void setUp() throws Exception {
    mapWithTwoEntries = new HashMap<>();
    mapWithTwoEntries.put("FOO", "foo");
    mapWithTwoEntries.put("BAR", 7);

    HeapDataOutputStream hdos = createHeapDataOutputStream();
    DataSerializer.writeObject(new ClassNotFoundSerialization(), hdos);
    bytesOfClassNotFoundSerialization = hdos.toByteArray();

    hdos = createHeapDataOutputStream();
    DataSerializer.writeObject(mapWithTwoEntries, hdos);
    bytesOfMap = hdos.toByteArray();

    bytesOfNull = serializeToBlob(null);

    zeroBytes = new byte[0];
  }

  @Test
  public void deserializeBlobOfClassNotFoundSerializationThrowsEOFException() throws Exception {
    assertThatThrownBy(() -> deserializeBlob(bytesOfClassNotFoundSerialization))
        .isExactlyInstanceOf(ClassNotFoundException.class);
  }

  @Test
  public void deserializeBlobOfMapReturnsCopyOfMap() throws Exception {
    final Object object = deserializeBlob(bytesOfMap);

    assertThat(object).isNotNull();
    assertThat(object).isExactlyInstanceOf(HashMap.class);
    assertThat(object).isNotSameAs(mapWithTwoEntries);
    assertThat(object).isEqualTo(mapWithTwoEntries);
  }

  @Test
  public void deserializeBlobOfNullReturnsNull() throws Exception {
    assertThat(deserializeBlob(bytesOfNull)).isNull();
  }

  @Test
  public void deserializeBlobOfZeroBytesThrowsEOFException() throws Exception {
    assertThatThrownBy(() -> deserializeBlob(zeroBytes))
        .isExactlyInstanceOf(EOFException.class);
  }

  @Test
  public void deserializeBlobWithNullThrowsNullPointerException() throws Exception {
    assertThatThrownBy(() -> deserializeBlob(null)).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void serializeMapToStreamWritesMapAsBytes() throws Exception {
    HeapDataOutputStream hdos = createHeapDataOutputStream();

    serializeTo(mapWithTwoEntries, hdos);

    assertThat(hdos.toByteArray()).isNotNull().isEqualTo(bytesOfMap);
  }

  @Test
  public void serializeNullToStreamWritesNullAsBytes() throws Exception {
    HeapDataOutputStream hdos = createHeapDataOutputStream();

    serializeTo(null, hdos);

    assertThat(hdos.toByteArray()).isNotNull().isEqualTo(bytesOfNull);
  }

  @Test
  public void serializeToBlobMapReturnsBytesOfMap() throws Exception {
    byte[] bytes = serializeToBlob(mapWithTwoEntries);

    assertThat(bytes).isNotNull().isEqualTo(bytesOfMap);
  }

  @Test
  public void serializeToBlobUnserializableThrowsNotSerializableException() throws Exception {
    assertThatThrownBy(() -> serializeToBlob(new Object()))
        .isExactlyInstanceOf(NotSerializableException.class).hasMessage(Object.class.getName());
  }

  @Test
  public void serializeToBlobWithNullReturnsBytesOfNull() throws Exception {
    byte[] bytes = serializeToBlob(null);

    assertThat(bytes).isNotNull().isEqualTo(bytesOfNull);
  }

  @Test
  public void serializeToNullNullThrowsNullPointerException() throws Exception {
    assertThatThrownBy(() -> serializeTo(null, null))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void serializeToNullStreamThrowsNullPointerException() throws Exception {
    assertThatThrownBy(() -> serializeTo(mapWithTwoEntries, null))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void serializeUnserializableToStreamThrowsNotSerializableException() throws Exception {
    HeapDataOutputStream hdos = createHeapDataOutputStream();

    assertThatThrownBy(() -> serializeTo(new Object(), hdos))
        .isExactlyInstanceOf(NotSerializableException.class).hasMessage(Object.class.getName());
  }

  private HeapDataOutputStream createHeapDataOutputStream() {
    return new HeapDataOutputStream(HDOS_ALLOC_SIZE, null, true);
  }

  private static class ClassNotFoundSerialization implements Serializable {
    private void readObject(final ObjectInputStream in) throws ClassNotFoundException {
      throw new ClassNotFoundException(CLASS_NOT_FOUND_MESSAGE);
    }
  }
}

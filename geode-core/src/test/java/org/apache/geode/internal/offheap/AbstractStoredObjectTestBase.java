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
package org.apache.geode.internal.offheap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.DataOutput;
import java.io.IOException;

import org.junit.Test;

import org.apache.geode.internal.serialization.DataSerializableFixedID;

public abstract class AbstractStoredObjectTestBase {

  /* Returns Value as an Object Eg: Integer or UserDefinedRegionValue */
  protected abstract Object getValue();

  /* Returns Value as an ByteArray (not serialized) */
  protected abstract byte[] getValueAsByteArray();

  protected abstract Object convertByteArrayToObject(byte[] valueInByteArray);

  protected abstract Object convertSerializedByteArrayToObject(byte[] valueInSerializedByteArray);

  protected abstract StoredObject createValueAsUnserializedStoredObject(Object value);

  protected abstract StoredObject createValueAsSerializedStoredObject(Object value);

  @Test
  public void getValueAsDeserializedHeapObjectShouldReturnDeserializedValueIfValueIsSerialized() {
    Object regionEntryValue = getValue();
    StoredObject storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    Object actualRegionEntryValue = storedObject.getValueAsDeserializedHeapObject();
    assertEquals(regionEntryValue, actualRegionEntryValue);
  }

  @Test
  public void getValueAsDeserializedHeapObjectShouldReturnValueAsIsIfNotSerialized() {
    byte[] regionEntryValue = getValueAsByteArray();
    StoredObject storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    byte[] deserializedValue = (byte[]) storedObject.getValueAsDeserializedHeapObject();
    assertArrayEquals(regionEntryValue, deserializedValue);
  }

  @Test
  public void getValueAsHeapByteArrayShouldReturnSerializedByteArrayIfValueIsSerialized() {
    Object regionEntryValue = getValue();
    StoredObject storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    byte[] valueInSerializedByteArray = storedObject.getValueAsHeapByteArray();
    Object actualRegionEntryValue = convertSerializedByteArrayToObject(valueInSerializedByteArray);

    assertEquals(regionEntryValue, actualRegionEntryValue);
  }

  @Test
  public void getValueAsHeapByteArrayShouldReturnDeserializedByteArrayIfValueIsNotSerialized() {
    Object regionEntryValue = getValue();

    StoredObject storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    byte[] valueInByteArray = storedObject.getValueAsHeapByteArray();

    Object actualRegionEntryValue = convertByteArrayToObject(valueInByteArray);

    assertEquals(regionEntryValue, actualRegionEntryValue);
  }

  @Test
  public void getStringFormShouldReturnStringFromDeserializedValue() {
    Object regionEntryValue = getValue();
    StoredObject storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    String stringForm = storedObject.getStringForm();
    assertEquals(String.valueOf(regionEntryValue), stringForm);
  }

  @Test
  public void getValueShouldReturnSerializedValue() {
    Object regionEntryValue = getValue();
    StoredObject storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    byte[] valueAsSerializedByteArray = (byte[]) storedObject.getValue();

    Object actualValue = convertSerializedByteArrayToObject(valueAsSerializedByteArray);

    assertEquals(regionEntryValue, actualValue);
  }

  @Test(expected = IllegalStateException.class)
  public void getValueShouldThrowExceptionIfValueIsNotSerialized() {
    Object regionEntryValue = getValue();
    StoredObject storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    byte[] deserializedValue = (byte[]) storedObject.getValue();
  }

  @Test
  public void getDeserializedWritableCopyShouldReturnDeserializedValue() {
    byte[] regionEntryValue = getValueAsByteArray();
    StoredObject storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    assertArrayEquals(regionEntryValue,
        (byte[]) storedObject.getDeserializedWritableCopy(null, null));
  }

  @Test
  public void writeValueAsByteArrayWritesToProvidedDataOutput() throws IOException {
    byte[] regionEntryValue = getValueAsByteArray();
    StoredObject storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    DataOutput dataOutput = mock(DataOutput.class);
    storedObject.writeValueAsByteArray(dataOutput);

    verify(dataOutput, times(1)).write(storedObject.getSerializedValue(), 0,
        storedObject.getSerializedValue().length);
  }

  @Test
  public void sendToShouldWriteSerializedValueToDataOutput() throws IOException {
    Object regionEntryValue = getValue();
    StoredObject storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    DataOutput dataOutput = mock(DataOutput.class);
    storedObject.sendTo(dataOutput);

    verify(dataOutput, times(1)).write(storedObject.getSerializedValue());
  }

  @Test
  public void sendToShouldWriteDeserializedObjectToDataOutput() throws IOException {
    byte[] regionEntryValue = getValueAsByteArray();
    StoredObject storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    DataOutput dataOutput = mock(DataOutput.class);
    storedObject.sendTo(dataOutput);

    verify(dataOutput, times(1)).write(regionEntryValue, 0, regionEntryValue.length);
  }

  @Test
  public void sendAsByteArrayShouldWriteSerializedValueToDataOutput() throws IOException {
    Object regionEntryValue = getValue();
    StoredObject storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    DataOutput dataOutput = mock(DataOutput.class);
    storedObject.sendAsByteArray(dataOutput);

    verify(dataOutput, times(1)).write(storedObject.getSerializedValue(), 0,
        storedObject.getSerializedValue().length);
  }

  @Test
  public void sendAsByteArrayShouldWriteDeserializedObjectToDataOutput() throws IOException {
    byte[] regionEntryValue = getValueAsByteArray();
    StoredObject storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    DataOutput dataOutput = mock(DataOutput.class);
    storedObject.sendAsByteArray(dataOutput);

    verify(dataOutput, times(1)).write(regionEntryValue, 0, regionEntryValue.length);
  }

  @Test
  public void sendAsCachedDeserializableShouldWriteSerializedValueToDataOutputAndSetsHeader()
      throws IOException {
    Object regionEntryValue = getValue();
    StoredObject storedObject = createValueAsSerializedStoredObject(regionEntryValue);

    DataOutput dataOutput = mock(DataOutput.class);
    storedObject.sendAsCachedDeserializable(dataOutput);

    verify(dataOutput, times(1)).writeByte((DataSerializableFixedID.VM_CACHED_DESERIALIZABLE));
    verify(dataOutput, times(1)).write(storedObject.getSerializedValue(), 0,
        storedObject.getSerializedValue().length);
  }

  @Test(expected = IllegalStateException.class)
  public void sendAsCachedDeserializableShouldThrowExceptionIfValueIsNotSerialized()
      throws IOException {
    Object regionEntryValue = getValue();
    StoredObject storedObject = createValueAsUnserializedStoredObject(regionEntryValue);

    DataOutput dataOutput = mock(DataOutput.class);
    storedObject.sendAsCachedDeserializable(dataOutput);
  }
}

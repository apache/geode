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
package org.apache.geode.internal.cache.ha;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * This test verifies that eventId, while being sent across the network ( client to server, server
 * to client and peer to peer) , goes as optimized byte-array. For client to server messages, the
 * membership id part of event-id is not need to be sent with each event. Also, the threadId and
 * sequenceId need not be sent as long if their value is small. This is a junit test for testing the
 * methods written in <code>EventID</code> class for the above optimization. For distributed testing
 * for the same , please refer {@link EventIdOptimizationDUnitTest}.
 */
@Category({ClientServerTest.class})
public class EventIdOptimizationJUnitTest {

  /** The long id (threadId or sequenceId) having value equivalent to byte */
  private static final long ID_VALUE_BYTE = Byte.MAX_VALUE;

  /** The long id (threadId or sequenceId) having value equivalent to short */
  private static final long ID_VALUE_SHORT = Short.MAX_VALUE;

  /** The long id (threadId or sequenceId) having value equivalent to int */
  private static final long ID_VALUE_INT = Integer.MAX_VALUE;

  /** The long id (threadId or sequenceId) having value equivalent to long */
  private static final long ID_VALUE_LONG = Long.MAX_VALUE;

  /**
   * Tests the eventId optimization APIs <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for byte-byte combination for
   * threadId and sequenceId values.
   */
  @Test
  public void testOptimizationForByteByte() {
    int expectedLength = 2 + 1 + 1;
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_BYTE, ID_VALUE_BYTE, expectedLength);
  }

  /**
   * Tests the eventId optimization APIs <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for short-short combination for
   * threadId and sequenceId values.
   */
  @Test
  public void testOptimizationForShortShort() {
    int expectedLength = 2 + 2 + 2;
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_SHORT, ID_VALUE_SHORT, expectedLength);
  }

  /**
   * Tests the eventId optimization APIs <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for int-int combination for threadId
   * and sequenceId values.
   */
  @Test
  public void testOptimizationForIntInt() {
    int expectedLength = 2 + 4 + 4;
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_INT, ID_VALUE_INT, expectedLength);
  }

  /**
   * Tests the eventId optimization APIs <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for long-long combination for
   * threadId and sequenceId values.
   */
  @Test
  public void testOptimizationForLongLong() {
    int expectedLength = 2 + 8 + 8;
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_LONG, ID_VALUE_LONG, expectedLength);
  }

  /**
   * Tests the eventId optimization APIs <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for byte-short combinations for
   * threadId and sequenceId values.
   */
  @Test
  public void testOptimizationForByteShort() {
    int expectedLength = 2 + 1 + 2;
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_BYTE, ID_VALUE_SHORT, expectedLength);
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_SHORT, ID_VALUE_BYTE, expectedLength);
  }

  /**
   * Tests the eventId optimization APIs <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for byte-int combinations for
   * threadId and sequenceId values.
   */
  @Test
  public void testOptimizationForByteInt() {
    int expectedLength = 2 + 1 + 4;
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_BYTE, ID_VALUE_INT, expectedLength);
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_INT, ID_VALUE_BYTE, expectedLength);
  }

  /**
   * Tests the eventId optimization APIs <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for byte-long combinations for
   * threadId and sequenceId values.
   */
  @Test
  public void testOptimizationForByteLong() {
    int expectedLength = 2 + 1 + 8;
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_BYTE, ID_VALUE_LONG, expectedLength);
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_LONG, ID_VALUE_BYTE, expectedLength);
  }

  /**
   * Tests the eventId optimization APIs <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for short-int combinations for
   * threadId and sequenceId values.
   */
  @Test
  public void testOptimizationForShortInt() {
    int expectedLength = 2 + 2 + 4;
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_SHORT, ID_VALUE_INT, expectedLength);
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_INT, ID_VALUE_SHORT, expectedLength);
  }

  /**
   * Tests the eventId optimization APIs <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for short-long combinations for
   * threadId and sequenceId values.
   */
  @Test
  public void testOptimizationForShortLong() {
    int expectedLength = 2 + 2 + 8;
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_SHORT, ID_VALUE_LONG, expectedLength);
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_LONG, ID_VALUE_SHORT, expectedLength);
  }

  /**
   * Tests the eventId optimization APIs <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for int-long combinations for
   * threadId and sequenceId values.
   */
  @Test
  public void testOptimizationForIntLong() {
    int expectedLength = 2 + 4 + 8;
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_INT, ID_VALUE_LONG, expectedLength);
    writeReadAndVerifyOptimizedByteArray(ID_VALUE_LONG, ID_VALUE_INT, expectedLength);
  }

  @Test
  public void testEventIDForGEODE100Member() throws IOException, ClassNotFoundException {
    InternalDistributedMember distributedMember = new InternalDistributedMember("localhost", 10999);
    HeapDataOutputStream hdos = new HeapDataOutputStream(256, KnownVersion.CURRENT);
    distributedMember.writeEssentialData(hdos);
    byte[] memberBytes = hdos.toByteArray();
    EventID eventID = new EventID(memberBytes, 1, 1);


    HeapDataOutputStream hdos90 = new HeapDataOutputStream(256, KnownVersion.GFE_90);
    VersionedDataOutputStream dop = new VersionedDataOutputStream(hdos90, KnownVersion.GFE_90);

    eventID.toData(dop, InternalDataSerializer.createSerializationContext(dop));

    ByteArrayInputStream bais = new ByteArrayInputStream(hdos90.toByteArray());


    VersionedDataInputStream dataInputStream =
        new VersionedDataInputStream(bais, KnownVersion.GFE_90);

    EventID eventID2 = new EventID();
    eventID2.fromData(dataInputStream, mock(DeserializationContext.class));

    assertEquals(distributedMember, eventID2.getDistributedMember(KnownVersion.GFE_90));

    assertEquals(memberBytes.length + 17, eventID2.getMembershipID().length);
  }

  /**
   * Creates the optimized byte array using <code>EventID#getOptimizedByteArrayForEventID</code> api
   * with the given threadId and sequenceId and verifies that the length of that byte-array is as
   * expected, then reads the values for eventId and sequenceId from this byte-array using
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> api and verifies that they are
   * decoded properly.
   *
   * @param threadId the long value of threadId
   * @param sequenceId the long value of sequenceId
   * @param expectedArrayLength expected length of the optimized byte-array
   */
  private void writeReadAndVerifyOptimizedByteArray(long threadId, long sequenceId,
      int expectedArrayLength) {
    byte[] array = EventID.getOptimizedByteArrayForEventID(threadId, sequenceId);
    assertEquals("optimized byte-array length not as expected", expectedArrayLength, array.length);
    ByteBuffer buffer = ByteBuffer.wrap(array);
    long threadIdReadFromOptArray = EventID.readEventIdPartsFromOptimizedByteArray(buffer);
    long sequenceIdReadFromOptArray = EventID.readEventIdPartsFromOptimizedByteArray(buffer);
    assertEquals("threadId value read is not same as that written to the byte-buffer", threadId,
        threadIdReadFromOptArray);
    assertEquals("sequenceId value read is not same as that written to the byte-buffer", sequenceId,
        sequenceIdReadFromOptArray);
  }

}

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
package com.gemstone.gemfire.internal.cache.ha;

import java.nio.ByteBuffer;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * This test verifies that eventId, while being sent across the network ( client
 * to server, server to client and peer to peer) , goes as optimized byte-array.
 * For client to server messages, the membership id part of event-id is not need
 * to be sent with each event. Also, the threadId and sequenceId need not be
 * sent as long if their value is small. This is a junit test for testing the
 * methods written in <code>EventID</code> class for the above optmization.
 * For distributed testing for the same , please refer
 * {@link EventIdOptimizationDUnitTest}.
 * 
 * @author Dinesh Patel
 * 
 */
@Category(UnitTest.class)
public class EventIdOptimizationJUnitTest extends TestCase
{
  /** The long id (threadId or sequenceId) having value equivalent to byte */
  private static final long ID_VALUE_BYTE = Byte.MAX_VALUE;

  /** The long id (threadId or sequenceId) having value equivalent to short */
  private static final long ID_VALUE_SHORT = Short.MAX_VALUE;

  /** The long id (threadId or sequenceId) having value equivalent to int */
  private static final long ID_VALUE_INT = Integer.MAX_VALUE;

  /** The long id (threadId or sequenceId) having value equivalent to long */
  private static final long ID_VALUE_LONG = Long.MAX_VALUE;

  /**
   * Constructor
   * 
   * @param arg0 -
   *          name
   */
  public EventIdOptimizationJUnitTest(String arg0) {
    super(arg0);
  }

  /**
   * Tests the eventId optmization APIs
   * <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for byte-byte
   * combination for threadId and sequenceId values.
   * 
   */
  public void testOptmizationForByteByte()
  {
    int expectedLength = 2 + 1 + 1;
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_BYTE, ID_VALUE_BYTE,
        expectedLength);
  }

  /**
   * Tests the eventId optmization APIs
   * <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for
   * short-short combination for threadId and sequenceId values.
   * 
   */
  public void testOptmizationForShortShort()
  {
    int expectedLength = 2 + 2 + 2;
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_SHORT, ID_VALUE_SHORT,
        expectedLength);
  }

  /**
   * Tests the eventId optmization APIs
   * <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for int-int
   * combination for threadId and sequenceId values.
   * 
   */
  public void testOptmizationForIntInt()
  {
    int expectedLength = 2 + 4 + 4;
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_INT, ID_VALUE_INT,
        expectedLength);
  }

  /**
   * Tests the eventId optmization APIs
   * <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for long-long
   * combination for threadId and sequenceId values.
   * 
   */
  public void testOptmizationForLongLong()
  {
    int expectedLength = 2 + 8 + 8;
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_LONG, ID_VALUE_LONG,
        expectedLength);
  }

  /**
   * Tests the eventId optmization APIs
   * <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for byte-short
   * combinations for threadId and sequenceId values.
   * 
   */
  public void testOptmizationForByteShort()
  {
    int expectedLength = 2 + 1 + 2;
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_BYTE, ID_VALUE_SHORT,
        expectedLength);
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_SHORT, ID_VALUE_BYTE,
        expectedLength);
  }

  /**
   * Tests the eventId optmization APIs
   * <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for byte-int
   * combinations for threadId and sequenceId values.
   * 
   */
  public void testOptmizationForByteInt()
  {
    int expectedLength = 2 + 1 + 4;
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_BYTE, ID_VALUE_INT,
        expectedLength);
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_INT, ID_VALUE_BYTE,
        expectedLength);
  }

  /**
   * Tests the eventId optmization APIs
   * <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for byte-long
   * combinations for threadId and sequenceId values.
   * 
   */
  public void testOptmizationForByteLong()
  {
    int expectedLength = 2 + 1 + 8;
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_BYTE, ID_VALUE_LONG,
        expectedLength);
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_LONG, ID_VALUE_BYTE,
        expectedLength);
  }

  /**
   * Tests the eventId optmization APIs
   * <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for short-int
   * combinations for threadId and sequenceId values.
   * 
   */
  public void testOptmizationForShortInt()
  {
    int expectedLength = 2 + 2 + 4;
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_SHORT, ID_VALUE_INT,
        expectedLength);
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_INT, ID_VALUE_SHORT,
        expectedLength);
  }

  /**
   * Tests the eventId optmization APIs
   * <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for short-long
   * combinations for threadId and sequenceId values.
   * 
   */
  public void testOptmizationForShortLong()
  {
    int expectedLength = 2 + 2 + 8;
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_SHORT, ID_VALUE_LONG,
        expectedLength);
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_LONG, ID_VALUE_SHORT,
        expectedLength);
  }

  /**
   * Tests the eventId optmization APIs
   * <code>EventID#getOptimizedByteArrayForEventID</code> and
   * <code>EventID#readEventIdPartsFromOptmizedByteArray</code> for int-long
   * combinations for threadId and sequenceId values.
   * 
   */
  public void testOptmizationForIntLong()
  {
    int expectedLength = 2 + 4 + 8;
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_INT, ID_VALUE_LONG,
        expectedLength);
    writeReadAndVerifyOpmitizedByteArray(ID_VALUE_LONG, ID_VALUE_INT,
        expectedLength);
  }

  /**
   * Creates the optimized byte array using
   * <code>EventID#getOptimizedByteArrayForEventID</code> api with the given
   * threadId and sequenceId and verifies that the length of that byte-array is
   * as expected, then reads the values for eventId and sequenceId from this
   * byte-array using <code>EventID#readEventIdPartsFromOptmizedByteArray</code>
   * api and verifies that they are decoded properly.
   * 
   * @param threadId
   *          the long value of threadId
   * @param sequenceId
   *          the long value of sequenceId
   * @param expectedArrayLength
   *          expected length of the optimized byte-array
   */
  private void writeReadAndVerifyOpmitizedByteArray(long threadId,
      long sequenceId, int expectedArrayLength)
  {
    byte[] array = EventID
        .getOptimizedByteArrayForEventID(threadId, sequenceId);
    assertEquals("optimized byte-array length not as expected",
        expectedArrayLength, array.length);
    ByteBuffer buffer = ByteBuffer.wrap(array);
    long threadIdReadFromOptArray = EventID
        .readEventIdPartsFromOptmizedByteArray(buffer);
    long sequenceIdReadFromOptArray = EventID
        .readEventIdPartsFromOptmizedByteArray(buffer);
    assertEquals(
        "threadId value read is not same as that written to the byte-buffer",
        threadId, threadIdReadFromOptArray);
    assertEquals(
        "sequenceId value read is not same as that written to the byte-buffer",
        sequenceId, sequenceIdReadFromOptArray);
  }

}
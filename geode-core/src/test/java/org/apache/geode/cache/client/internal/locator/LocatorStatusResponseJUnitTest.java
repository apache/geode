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
package org.apache.geode.cache.client.internal.locator;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * The LocatorStatusResponseJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * LocatorStatusResponse class.
 * </p>
 * @see org.apache.geode.cache.client.internal.locator.LocatorStatusResponse
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class LocatorStatusResponseJUnitTest {

  @Test
  public void testSerialize() throws IOException, ClassNotFoundException {
    final int locatorPort = 12345;
    final String locatorHost = "locatorHost";
    final String locatorLogFile = "LocatorStatusResponseJUnitTest.log";
    final String locatorName = "LocatorStatusResponseJUnitTest";
    
    final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    final LocatorStatusResponse expectedResponse = new LocatorStatusResponse()
      .initialize(locatorPort, locatorHost, locatorLogFile, locatorName);

    assertNotNull(expectedResponse);

    expectedResponse.toData(new DataOutputStream(byteStream));

    final byte[] bytes = byteStream.toByteArray();

    assertNotNull(bytes);
    assertFalse(bytes.length == 0);

    final LocatorStatusResponse actualResponse = new LocatorStatusResponse();

    assertNotNull(actualResponse);
    assertNotSame(expectedResponse, actualResponse);
    assertFalse(actualResponse.equals(expectedResponse));

    actualResponse.fromData(new DataInputStream(new ByteArrayInputStream(bytes)));

    assertEquals(expectedResponse, actualResponse);
  }

}

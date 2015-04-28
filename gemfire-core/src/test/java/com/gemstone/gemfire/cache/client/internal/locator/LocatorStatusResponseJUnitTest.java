/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.cache.client.internal.locator;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

/**
 * The LocatorStatusResponseJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * LocatorStatusResponse class.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusResponse
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
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

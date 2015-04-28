/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.util;

import static org.junit.Assert.*;

import java.util.Map;

import com.gemstone.gemfire.management.internal.web.AbstractWebTestCase;
import com.gemstone.junit.UnitTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * The UriUtilsJUnitTest class is a test suite of test cases testing the contract and functionality of the UriUtils class.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.web.AbstractWebTestCase
 * @see com.gemstone.gemfire.management.internal.web.util.UriUtils
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 8.0
 */
@Category(UnitTest.class)
public class UriUtilsJUnitTest extends AbstractWebTestCase {

  @Test
  public void testDecodeNull() {
    assertNull(UriUtils.decode((String) null));
  }

  @Test
  public void testDecodeStringArray() throws Exception {
    final String[] encodedValues = {
      null,
      "123",
      "test",
      encode("Path/Subpath"),
      encode(encode(encode("/Customers/Accounts/Orders/Items")))
    };

    final String[] decodedValues = UriUtils.decode(encodedValues);

    assertSame(encodedValues, decodedValues);
    assertEquals(5, decodedValues.length);
    assertNull(decodedValues[0]);
    assertEquals("123", decodedValues[1]);
    assertEquals("test", decodedValues[2]);
    assertEquals("Path/Subpath", decodedValues[3]);
    assertEquals("/Customers/Accounts/Orders/Items", decodedValues[4]);
  }

  @Test
  public void testDecodeMap() throws Exception {
    final Map<String, Object> encodedForm = createMap(createArray("0", "1", "2", "3", "4"),
      (Object[]) createArray(null, "123", "test", encode("Path/Subpath"), encode(encode(encode("/Customers/Accounts/Orders/Items")))));

    final Map<String, Object> decodedForm = UriUtils.decode(encodedForm);

    assertSame(encodedForm, decodedForm);
    assertEquals(5, decodedForm.size());
    assertNull(decodedForm.get("0"));
    assertEquals("123", decodedForm.get("1"));
    assertEquals("test", decodedForm.get("2"));
    assertEquals("Path/Subpath", decodedForm.get("3"));
    assertEquals("/Customers/Accounts/Orders/Items", decodedForm.get("4"));
  }

  @Test
  public void testEncodeNull() {
    assertNull(UriUtils.encode((String) null));
  }

  @Test
  public void testEncodeStringArray() throws Exception {
    final String[] values = { null, "123", "test", "Path/Subpath", "/Customers/Accounts/Orders/Items" };
    final String[] encodedValues = UriUtils.encode(values);

    assertSame(values, encodedValues);
    assertEquals(5, encodedValues.length);
    assertNull(encodedValues[0]);
    assertEquals("123", encodedValues[1]);
    assertEquals("test", encodedValues[2]);
    assertEquals(encode("Path/Subpath"), encodedValues[3]);
    assertEquals(encode("/Customers/Accounts/Orders/Items"), encodedValues[4]);
  }

  @Test
  public void testEncodeMap() throws Exception {
    final Map<String, Object> form = createMap(createArray("0", "1", "2", "3", "4"),
      (Object[]) createArray(null, "123", "test", "Path/Subpath", "/Customers/Accounts/Orders/Items"));

    final Map<String, Object> encodedForm = UriUtils.encode(form);

    assertSame(form, encodedForm);
    assertEquals(5, encodedForm.size());
    assertNull(encodedForm.get("0"));
    assertEquals("123", encodedForm.get("1"));
    assertEquals("test", encodedForm.get("2"));
    assertEquals(encode("Path/Subpath"), encodedForm.get("3"));
    assertEquals(encode("/Customers/Accounts/Orders/Items"), encodedForm.get("4"));
  }

}

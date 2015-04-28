/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.util.PasswordUtil;
import com.gemstone.junit.UnitTest;

import junit.framework.Assert;
import junit.framework.TestCase;

@Category(UnitTest.class)
public class PasswordUtilJUnitTest extends TestCase {
  public void testPasswordUtil() {
    String x = "password";
    String z = null;

    //System.out.println(x);
    String y = PasswordUtil.encrypt(x);
    //System.out.println(y);
    y = "encrypted(" + y + ")";
    z = PasswordUtil.decrypt(y);
    //System.out.println(z);
    assertEquals(x, z);
  }
}

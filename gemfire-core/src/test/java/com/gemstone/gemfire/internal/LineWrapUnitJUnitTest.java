/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.util.Arrays;
import java.util.List;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class LineWrapUnitJUnitTest extends TestCase {
  
  public void test() {
    String test = new String("aaa aaaaa  aaa aaaa");
    
    assertEquals(list("aaa", "aaaaa", "aaa", "aaaa"), SystemAdmin.lineWrapOut(test, 3));
    assertEquals(list("aaa", "aaaaa", "aaa aaaa"), SystemAdmin.lineWrapOut(test, 8));

    assertEquals(list("aaa aaaaa", "aaa aaaa"), SystemAdmin.lineWrapOut(test, 9));
    assertEquals(list("aaa aaaaa  aaa",  "aaaa"), SystemAdmin.lineWrapOut(test, 14));
    
    String test2 = new String("aaa\n aaaaa  aaa aaaa");
    assertEquals(list("aaa", " aaaaa  aaa",  "aaaa"), SystemAdmin.lineWrapOut(test2, 14));
  }

  private List<String> list(String  ...strings) {
    return Arrays.asList(strings);
  }

}

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import java.io.*;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/** Tests classes of Bug36619 to make sure they are Serializable */
@Category(UnitTest.class)
public class Bug36619JUnitTest extends TestCase {
  
  public Bug36619JUnitTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Assert that MembershipAttributes are serializable.
   */
  public void testMembershipAttributesAreSerializable() throws Exception {
    String[] roles = {"a", "b", "c"};
    MembershipAttributes outMA = new MembershipAttributes(roles);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(outMA);
    
    byte[] data = baos.toByteArray();
    
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bais);
    MembershipAttributes inMA = (MembershipAttributes) ois.readObject();
    assertEquals(outMA, inMA);
  }
  /**
   * Assert that SubscriptionAttributes are serializable.
   */
  public void testSubscriptionAttributesAreSerializable() throws Exception {
    SubscriptionAttributes outSA = new SubscriptionAttributes();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(outSA);
    
    byte[] data = baos.toByteArray();
    
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bais);
    SubscriptionAttributes inSA = (SubscriptionAttributes) ois.readObject();
    assertEquals(outSA, inSA);
  }
}


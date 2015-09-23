/*=========================================================================
 * Copyright (c) 2002-2015 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Test case for Trac <a
 * href="https://svn.gemstone.com/trac/gemfire/ticket/52289">#52289</a>.
 * 
 * Asserts fixes for bug JDK-8076152 in JDK 1.8.0u20 to 1.8.0.u45.
 * http://bugs.java.com/bugdatabase/view_bug.do?bug_id=8076152
 * 
 * The JVM crashes when hotspot compiling a method that uses an array consisting
 * of objects of a base class when different child classes is used as actual
 * instance objects AND when the array is constant (declared final). The crash
 * occurs during process of the aaload byte code.
 * 
 * This test and its corrections can be removed after the release of JDK
 * 1.8.0u60 if we choose to not support 1.8.0u20 - 1.8.0u45 inclusive.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.2
 * 
 */
@Category(UnitTest.class)
public class Bug52289JUnitTest {

  @Test
  public void test() throws IOException, ClassNotFoundException {
    // Iterate enough to cause JIT to compile
    // javax.print.attribute.EnumSyntax::readResolve
    for (int i = 0; i < 100_000; i++) {
      // Must execute two or more subclasses with final static arrays of
      // different types.
      doEvictionAlgorithm();
      doEvictionAction();
    }
  }

  protected void doEvictionAlgorithm() throws IOException, ClassNotFoundException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(EvictionAlgorithm.NONE);
    oos.close();

    final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    final ObjectInputStream ois = new ObjectInputStream(bais);
    ois.readObject();
    ois.close();
  }

  protected void doEvictionAction() throws IOException, ClassNotFoundException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(EvictionAction.NONE);
    oos.close();

    final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    final ObjectInputStream ois = new ObjectInputStream(bais);
    ois.readObject();
    ois.close();
  }

}
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
package org.apache.geode.test.dunit.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;

/**
 * This class tests the functionality of the {@link VM} class.
 */

@SuppressWarnings({"serial", "unused"})
public class VMDistributedTest extends DistributedTestCase {

  private static final AtomicInteger COUNTER = new AtomicInteger();
  private static final boolean BOOLEAN_VALUE = true;
  private static final byte BYTE_VALUE = (byte) 40;
  private static final long LONG_VALUE = 42L;
  private static final String STRING_VALUE = "BLAH BLAH BLAH";

  @Test
  public void testInvokeStaticBoolean() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    assertEquals(BOOLEAN_VALUE, (boolean) vm.invoke(() -> remoteBooleanMethod()));
  }

  @Test
  public void testInvokeStaticByte() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    assertEquals(BYTE_VALUE, (byte) vm.invoke(() -> remoteByteMethod()));
  }

  @Test
  public void testInvokeStaticLong() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    assertEquals(LONG_VALUE, (long) vm.invoke(() -> remoteLongMethod()));
  }

  @Test
  public void testInvokeInstance() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    assertEquals(STRING_VALUE, vm.invoke(new ClassWithString(), "getString"));
  }

  @Test
  public void testInvokeRunnableWithException() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invoke(new InvokeRunnable());
      fail("Should have thrown a BasicTestException");
    } catch (RMIException ex) {
      assertTrue(ex.getCause() instanceof BasicTestException);
    }
  }

  @Test
  public void testReturnValue() throws Exception {
    final Host host = Host.getHost(0);
    final VM vm = host.getVM(0);
    // Assert class static invocation works
    AsyncInvocation a1 = vm.invokeAsync(() -> getAndIncStaticCount());
    a1.join();
    assertEquals(new Integer(0), a1.getReturnValue());
    // Assert class static invocation with args works
    a1 = vm.invokeAsync(() -> incrementStaticCount(new Integer(2)));
    a1.join();
    assertEquals(new Integer(3), a1.getReturnValue());
    // Assert that previous values are not returned when invoking method w/ no return val
    a1 = vm.invokeAsync(() -> incStaticCount());
    a1.join();
    assertNull(a1.getReturnValue());
    // Assert that previous null returns are over-written
    a1 = vm.invokeAsync(() -> getAndIncStaticCount());
    a1.join();
    assertEquals(new Integer(4), a1.getReturnValue());

    // Assert object method invocation works with zero arg method
    final VMTestObject o = new VMTestObject(0);
    a1 = vm.invokeAsync(o, "incrementAndGet", new Object[] {});
    a1.join();
    assertEquals(new Integer(1), a1.getReturnValue());
    // Assert object method invocation works with no return
    a1 = vm.invokeAsync(o, "set", new Object[] {new Integer(3)});
    a1.join();
    assertNull(a1.getReturnValue());
  }

  private static Integer getAndIncStaticCount() {
    return new Integer(COUNTER.getAndIncrement());
  }

  private static Integer incrementStaticCount(Integer inc) {
    return new Integer(COUNTER.addAndGet(inc.intValue()));
  }

  private static void incStaticCount() {
    COUNTER.incrementAndGet();
  }

  /**
   * Accessed via reflection. DO NOT REMOVE
   */
  private static byte remoteByteMethod() {
    return BYTE_VALUE;
  }

  /**
   * Accessed via reflection. DO NOT REMOVE
   */
  private static boolean remoteBooleanMethod() {
    return BOOLEAN_VALUE;
  }

  /**
   * Accessed via reflection. DO NOT REMOVE
   */
  private static long remoteLongMethod() {
    return LONG_VALUE;
  }

  private static class ClassWithLong implements Serializable {
    public long getLong() {
      return LONG_VALUE;
    }
  }

  private static class ClassWithByte implements Serializable {
    public byte getByte() {
      return BYTE_VALUE;
    }
  }

  private static class InvokeRunnable implements SerializableRunnableIF {
    @Override
    public void run() {
      throw new BasicTestException();
    }
  }

  private static class ClassWithString implements Serializable {
    public String getString() {
      return STRING_VALUE;
    }
  }

  private static class BasicTestException extends RuntimeException {
    BasicTestException() {
      this("Test exception.  Please ignore.");
    }

    BasicTestException(String s) {
      super(s);
    }
  }

  private static class VMTestObject implements Serializable {
    private static final long serialVersionUID = 1L;
    private final AtomicInteger val;

    public VMTestObject(int init) {
      val = new AtomicInteger(init);
    }

    public Integer get() {
      return new Integer(val.get());
    }

    public Integer incrementAndGet() {
      return new Integer(val.incrementAndGet());
    }

    public void set(Integer newVal) {
      val.set(newVal.intValue());
    }
  }
}

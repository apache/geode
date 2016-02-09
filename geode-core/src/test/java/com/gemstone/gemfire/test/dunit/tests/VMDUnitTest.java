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
package com.gemstone.gemfire.test.dunit.tests;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This class tests the functionality of the {@link VM} class.
 */
public class VMDUnitTest extends DistributedTestCase {

  private static final boolean BOOLEAN_VALUE = true;
  private static final byte BYTE_VALUE = (byte) 40;
  private static final long LONG_VALUE = 42L;
  private static final String STRING_VALUE = "BLAH BLAH BLAH";

  public VMDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods

  public void notestInvokeNonExistentMethod() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invoke(VMDUnitTest.class, "nonExistentMethod");
      fail("Should have thrown an RMIException");

    } catch (RMIException ex) {
      String s = "Excepted a NoSuchMethodException, got a " +
        ex.getCause();;
      assertTrue(s, ex.getCause() instanceof NoSuchMethodException);
    }
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   * @return
   */
  protected static byte remoteByteMethod() {
    return BYTE_VALUE;
  }

  public void notestInvokeStaticBoolean() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    assertEquals(BOOLEAN_VALUE,
                 vm.invokeBoolean(VMDUnitTest.class, "remoteBooleanMethod")); 
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   * @return
   */
  protected static boolean remoteBooleanMethod() {
    return BOOLEAN_VALUE;
  }

  public void notestInvokeStaticBooleanNotBoolean() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invokeBoolean(VMDUnitTest.class, "remoteByteMethod");
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      
    }
  }

  public void notestInvokeStaticLong() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    assertEquals(LONG_VALUE,
                 vm.invokeLong(VMDUnitTest.class, "remoteLongMethod")); 
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   * @return
   */
  protected static long remoteLongMethod() {
    return LONG_VALUE;
  }

  public void notestInvokeStaticLongNotLong() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invokeLong(VMDUnitTest.class, "remoteByteMethod");
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      
    }
  }

  protected static class ClassWithLong implements Serializable {
    public long getLong() {
      return LONG_VALUE;
    }
  }

  protected static class ClassWithByte implements Serializable {
    public byte getByte() {
      return BYTE_VALUE;
    }
  }

  public void notestInvokeInstanceLong() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    assertEquals(LONG_VALUE,
                 vm.invokeLong(new ClassWithLong(), "getLong"));
  }

  public void notestInvokeInstanceLongNotLong() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invokeLong(new ClassWithByte(), "getByte");
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {

    }
  }

  protected static class InvokeRunnable
    implements SerializableRunnableIF {

    public void run() {
      throw new BasicDUnitTest.BasicTestException();
    }
  }

  protected static class ClassWithString implements Serializable {
    public String getString() {
      return STRING_VALUE;
    }
  }

  public void notestInvokeInstance() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    assertEquals(STRING_VALUE,
                 vm.invoke(new ClassWithString(), "getString"));
  }

  public void notestInvokeRunnable() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invoke(new InvokeRunnable());
      fail("Should have thrown a BasicTestException");

    } catch (RMIException ex) {
      assertTrue(ex.getCause() instanceof BasicDUnitTest.BasicTestException);
    }
  }
  
  private static final AtomicInteger COUNTER = new AtomicInteger();
  public static Integer getAndIncStaticCount() {
    return new Integer(COUNTER.getAndIncrement());
  }
  public static Integer incrementStaticCount(Integer inc) {
    return new Integer(COUNTER.addAndGet(inc.intValue()));
  }
  public static void incStaticCount() {
    COUNTER.incrementAndGet();
  }
  public static class VMTestObject implements Serializable {
    private static final long serialVersionUID = 1L;
    private final AtomicInteger val;
    public VMTestObject(int init) {
      this.val = new AtomicInteger(init);
    }
    public Integer get() {
      return new Integer(this.val.get());
    }
    public Integer incrementAndGet() {
      return new Integer(this.val.incrementAndGet());
    }
    public void set(Integer newVal) {
      this.val.set(newVal.intValue());
    }
  }
  public void testReturnValue() throws Exception {
    final Host host = Host.getHost(0);
    final VM vm = host.getVM(0);
    // Assert class static invocation works
    AsyncInvocation a1 = vm.invokeAsync(getClass(), "getAndIncStaticCount");
    a1.join();
    assertEquals(new Integer(0), a1.getReturnValue());
    // Assert class static invocation with args works
    a1 = vm.invokeAsync(getClass(), "incrementStaticCount", new Object[] {new Integer(2)});
    a1.join();
    assertEquals(new Integer(3), a1.getReturnValue());
    // Assert that previous values are not returned when invoking method w/ no return val
    a1 = vm.invokeAsync(getClass(), "incStaticCount");
    a1.join();
    assertNull(a1.getReturnValue());
    // Assert that previous null returns are over-written 
    a1 = vm.invokeAsync(getClass(), "getAndIncStaticCount");
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
}

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

import java.util.Properties;

import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DUnitEnv;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This class tests the basic functionality of the distributed unit
 * test framework.
 */
public class BasicDUnitTest extends DistributedTestCase {

  public BasicDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods

  /**
   * Tests how the Hydra framework handles an error
   */
  public void _testDontCatchRemoteException() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    vm.invoke(() -> this.remoteThrowException());
  }

  public void testRemoteInvocationWithException() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invoke(() -> this.remoteThrowException());
      fail("Should have thrown a BasicTestException");

    } catch (RMIException ex) {
      assertTrue(ex.getCause() instanceof BasicTestException);
    }
  }
  
  public void testInvokeWithLambda() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    int vm0Num = vm0.invoke(() -> DUnitEnv.get().getVMID());
    int vm1Num = vm1.invoke(() -> DUnitEnv.get().getVMID());
    
    assertEquals(0, vm0Num);
    assertEquals(1, vm1Num);
    
  }
  
  public void testInvokeLambdaAsync() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    AsyncInvocation<Integer> async0 = vm0.invokeAsync(() -> DUnitEnv.get().getVMID());
    int vm0num = async0.getResult();
    
    assertEquals(0, vm0num);
    
  }

  public void testInvokeWithNamedLambda() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    int vm0Num = vm0.invoke("getVMID", () -> DUnitEnv.get().getVMID());
    int vm1Num = vm1.invoke("getVMID", () -> DUnitEnv.get().getVMID());
    
    assertEquals(0, vm0Num);
    assertEquals(1, vm1Num);
    
  }
  
  public void testInvokeNamedLambdaAsync() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    AsyncInvocation<Integer> async0 = vm0.invokeAsync("getVMID", () -> DUnitEnv.get().getVMID());
    int vm0num = async0.getResult();
    
    assertEquals(0, vm0num);
    
  }
  
  static class BasicDUnitException extends RuntimeException {
    public BasicDUnitException() {
    }
  }
  
  public static void throwException() throws BasicDUnitException {
    throw new BasicDUnitException();
  }

  public void testInvokeNamedRunnableLambdaAsync() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    AsyncInvocation<Integer> async0 = vm0.invokeAsync("throwSomething", () -> BasicDUnitTest.throwException());
    try {
      async0.getResult();
      throw new Error("expected an exception to be thrown");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause == null) {
        throw new Error("expected an exception with a cause to be thrown", e);
      }
      if ( !(cause.getCause() instanceof BasicDUnitException) ) {
        throw new Error("expected a BasicDUnitException to be thrown", e.getCause());
      }
    }
  }

  public void testInvokeNamedRunnableLambda() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    try {
      vm0.invoke("throwSomething", () -> BasicDUnitTest.throwException());
      throw new Error("expected an exception to be thrown");
    } catch (Exception e) {
      if ( !(e.getCause() instanceof BasicDUnitException) ) {
        throw new Error("expected a BasicDUnitException to be thrown", e.getCause());
      }
    }
  }

  static class BasicTestException extends RuntimeException {
    BasicTestException() {
      this("Test exception.  Please ignore.");
    }

    BasicTestException(String s) {
      super(s);
    }
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   *
   */
  protected static void remoteThrowException() {
    String s = "Test exception.  Please ignore.";
    throw new BasicTestException(s);
  }

  public void _testRemoteInvocationBoolean() {

  }

  public void testRemoteInvokeAsync() throws InterruptedException {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    String name = this.getUniqueName();
    String value = "Hello";

    AsyncInvocation ai =
      vm.invokeAsync(() -> this.remoteBind( name, value ));
    ai.join();
    // TODO shouldn't we call fail() here?
    if (ai.exceptionOccurred()) {
      Assert.fail("remoteBind failed", ai.getException());
    }

    ai = vm.invokeAsync(() -> this.remoteValidateBind(name, value ));
    ai.join();
    if (ai.exceptionOccurred()) {
      Assert.fail("remoteValidateBind failed", ai.getException());
    }
  }

  private static Properties bindings = new Properties();
  private static void remoteBind(String name, String s) {
    new BasicDUnitTest("bogus").getSystem(); // forces connection
    bindings.setProperty(name, s);
  }

  private static void remoteValidateBind(String name, String expected)
  {
    assertEquals(expected, bindings.getProperty(name));
  }

  public void testRemoteInvokeAsyncWithException() 
    throws InterruptedException {

    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
//    String name = this.getUniqueName();
//    String value = "Hello";

    AsyncInvocation ai =
      vm.invokeAsync(() -> this.remoteThrowException());
    ai.join();
    assertTrue(ai.exceptionOccurred());
    Throwable ex = ai.getException();
    assertTrue(ex instanceof BasicTestException);
  }
}

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
package com.gemstone.gemfire.test.dunit.internal.tests;

import static com.gemstone.gemfire.test.dunit.Invoke.*;

import java.util.Properties;

import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DUnitEnv;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.internal.JUnit3DistributedTestCase;

/**
 * This class tests the basic functionality of the distributed unit
 * test framework.
 */
public class JUnit3BasicDUnitTest extends JUnit3DistributedTestCase {

  private static Properties bindings;

  public JUnit3BasicDUnitTest(String name) {
    super(name);
  }

  @Override
  public void postSetUp() throws Exception {
    bindings = new Properties();
    invokeInEveryVM(() -> bindings = new Properties());
  }

  @Override
  public void postTearDown() throws Exception {
    bindings = null;
    invokeInEveryVM(() -> bindings = null);
  }

  public void testPreconditions() {
    invokeInEveryVM(() -> assertNotNull("getUniqueName() must not return null", getUniqueName()));
    invokeInEveryVM(() -> assertNotNull("bindings must not be null", bindings));
  }

  /**
   * Tests how the Hydra framework handles an error
   */
  public void ignore_testDontCatchRemoteException() throws Exception {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    vm.invoke(() -> remoteThrowException());
  }

  public void testRemoteInvocationWithException() throws Exception {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invoke(() -> remoteThrowException());
      fail("Should have thrown a BasicTestException");

    } catch (RMIException ex) {
      assertTrue(ex.getCause() instanceof BasicTestException);
    }
  }

  public void testInvokeWithLambda() throws Exception {
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

  // Test was never implemented
  public void ignore_testRemoteInvocationBoolean() {
  }

  public void testRemoteInvokeAsync() throws Exception {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    String name = getUniqueName();
    String value = "Hello";

    AsyncInvocation ai = vm.invokeAsync(() -> remoteBind(name, value));
    ai.join();
    // TODO shouldn't we call fail() here?
    if (ai.exceptionOccurred()) {
      Assert.fail("remoteBind failed", ai.getException());
    }

    ai = vm.invokeAsync(() -> remoteValidateBind(name, value ));
    ai.join();
    if (ai.exceptionOccurred()) {
      Assert.fail("remoteValidateBind failed", ai.getException());
    }
  }

  public void testRemoteInvokeAsyncWithException() throws Exception {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);

    AsyncInvocation ai = vm.invokeAsync(() -> remoteThrowException());
    ai.join();
    assertTrue(ai.exceptionOccurred());
    Throwable ex = ai.getException();
    assertTrue(ex instanceof BasicTestException);
  }

  /**
   * Accessed via reflection.  DO NOT REMOVE
   */
  private static void remoteThrowException() {
    String s = "Test exception.  Please ignore.";
    throw new BasicTestException(s);
  }

  private static void remoteBind(String name, String value) {
    assertNotNull("name must not be null", name);
    assertNotNull("value must not be null", value);
    assertNotNull("bindings must not be null", bindings);

    new JUnit3BasicDUnitTest("").getSystem(); // forces connection
    bindings.setProperty(name, value);
  }

  private static void remoteValidateBind(String name, String expected) {
    assertEquals(expected, bindings.getProperty(name));
  }

  private static class BasicTestException extends RuntimeException {
    BasicTestException() {
      this("Test exception.  Please ignore.");
    }
    BasicTestException(String s) {
      super(s);
    }
  }
}

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
package dunit.tests;

import java.util.Properties;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.RMIException;
import dunit.VM;

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
    vm.invoke(this.getClass(), "remoteThrowException");
  }

  public void testRemoteInvocationWithException() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invoke(this.getClass(), "remoteThrowException");
      fail("Should have thrown a BasicTestException");

    } catch (RMIException ex) {
      assertTrue(ex.getCause() instanceof BasicTestException);
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
      vm.invokeAsync(this.getClass(), "remoteBind", 
                     new Object[] { name, value });
    ai.join();
    // TODO shouldn't we call fail() here?
    if (ai.exceptionOccurred()) {
      fail("remoteBind failed", ai.getException());
    }

    ai = vm.invokeAsync(this.getClass(), "remoteValidateBind",
                        new Object[] {name, value });
    ai.join();
    if (ai.exceptionOccurred()) {
      fail("remoteValidateBind failed", ai.getException());
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
      vm.invokeAsync(this.getClass(), "remoteThrowException");
    ai.join();
    assertTrue(ai.exceptionOccurred());
    Throwable ex = ai.getException();
    assertTrue(ex instanceof BasicTestException);
  }
}

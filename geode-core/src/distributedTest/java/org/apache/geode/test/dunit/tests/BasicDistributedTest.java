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

import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DUnitEnv;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.VM;

/**
 * This class tests the basic functionality of the distributed unit test framework.
 */
@SuppressWarnings("serial")
public class BasicDistributedTest extends DistributedTestCase {

  private static final String MESSAGE_FOR_remoteThrowException = "Test exception.  Please ignore.";

  private static Properties bindings;

  private VM vm0;
  private VM vm1;

  @Override
  public final void postSetUp() {
    bindings = new Properties();
    invokeInEveryVM(() -> bindings = new Properties());
    vm0 = Host.getHost(0).getVM(0);
    vm1 = Host.getHost(0).getVM(1);
  }

  @Override
  public final void postTearDown() {
    bindings = null;
    invokeInEveryVM(() -> bindings = null);
  }

  @Test
  public void testPreconditions() {
    invokeInEveryVM(
        () -> assertThat("getUniqueName() must not return null", getUniqueName(), notNullValue()));
    invokeInEveryVM(() -> assertThat("bindings must not be null", bindings, notNullValue()));
  }

  @Test
  public void testInvokeOnClassTargetWithEmptyArgs() {
    assertThat(vm0.invoke(BasicDistributedTest.class, "booleanValue", new Object[] {}), is(true));
  }

  @Test
  public void testInvokeOnObjectTargetWithEmptyArgs() {
    assertThat(vm0.invoke(new BasicDistributedTest(), "booleanValue", new Object[] {}), is(true));
  }

  @Test
  public void testInvokeAsyncOnClassTargetWithEmptyArgs() throws Exception {
    AsyncInvocation<?> async =
        vm0.invokeAsync(BasicDistributedTest.class, "booleanValue", new Object[] {}).join();
    assertThat(async.getResult(), is(true));
  }

  @Test
  public void testInvokeAsyncOnObjectTargetWithEmptyArgs() throws Exception {
    AsyncInvocation<?> async =
        vm0.invokeAsync(new BasicDistributedTest(), "booleanValue", new Object[] {}).join();
    assertThat(async.getResult(), is(true));
  }

  @Test
  public void testInvokeOnClassTargetWithNullArgs() {
    assertThat(vm0.invoke(BasicDistributedTest.class, "booleanValue", null), is(true));
  }

  @Test
  public void testInvokeOnObjectTargetWithNullArgs() {
    assertThat(vm0.invoke(new BasicDistributedTest(), "booleanValue", null), is(true));
  }

  @Test
  public void testInvokeAsyncOnClassTargetWithNullArgs() throws Exception {
    AsyncInvocation<?> async =
        vm0.invokeAsync(BasicDistributedTest.class, "booleanValue", null).join();
    assertThat(async.getResult(), is(true));
  }

  @Test
  public void testInvokeAsyncOnObjectTargetWithNullArgs() throws Exception {
    AsyncInvocation<?> async =
        vm0.invokeAsync(new BasicDistributedTest(), "booleanValue", null).join();
    assertThat(async.getResult(), is(true));
  }

  @Test
  public void testRemoteInvocationWithException() {
    Throwable thrown = catchThrowable(() -> vm0.invoke(() -> remoteThrowException()));

    assertThat(thrown, instanceOf(RMIException.class));
    assertThat(thrown.getCause(), notNullValue());
    assertThat(thrown.getCause(), instanceOf(BasicTestException.class));
    assertThat(thrown.getCause().getMessage(), is(MESSAGE_FOR_remoteThrowException));
  }

  @Test
  public void testInvokeWithLambda() {
    assertThat(vm0.invoke(() -> DUnitEnv.get().getVMID()), is(0));
    assertThat(vm1.invoke(() -> DUnitEnv.get().getVMID()), is(1));
  }

  @Test
  public void testInvokeLambdaAsync() throws Exception {
    assertThat(vm0.invokeAsync(() -> DUnitEnv.get().getVMID()).getResult(), is(0));
  }

  @Test
  public void testInvokeWithNamedLambda() {
    assertThat(vm0.invoke("getVMID", () -> DUnitEnv.get().getVMID()), is(0));
    assertThat(vm1.invoke("getVMID", () -> DUnitEnv.get().getVMID()), is(1));
  }

  @Test
  public void testInvokeNamedLambdaAsync() throws Exception {
    assertThat(vm0.invokeAsync("getVMID", () -> DUnitEnv.get().getVMID()).getResult(), is(0));
  }

  @Test
  public void testRemoteInvokeAsync() throws Exception {
    String name = getUniqueName();
    String value = "Hello";

    vm0.invokeAsync(() -> remoteBind(name, value)).join().checkException();
    vm0.invokeAsync(() -> remoteValidateBind(name, value)).join().checkException();
  }

  @Test
  public void testRemoteInvokeAsyncWithException() throws Exception {
    AsyncInvocation<?> async = vm0.invokeAsync(() -> remoteThrowException()).join();

    assertThat(async.exceptionOccurred(), is(true));
    assertThat(async.getException(), instanceOf(BasicTestException.class));

    Throwable thrown = catchThrowable(() -> async.checkException());

    assertThat(thrown, instanceOf(AssertionError.class));
    assertThat(thrown.getCause(), notNullValue());
    assertThat(thrown.getCause(), instanceOf(BasicTestException.class));
    assertThat(thrown.getCause().getMessage(), is(MESSAGE_FOR_remoteThrowException));
  }

  @Test
  public void testInvokeNamedRunnableLambdaAsync() throws Exception {
    Throwable thrown =
        catchThrowable(() -> vm0.invokeAsync("throwSomething", () -> throwException()).join()
            .checkException());

    assertThat(thrown, notNullValue());
    assertThat(thrown.getCause(), notNullValue());
    assertThat(thrown.getCause(), instanceOf(BasicDUnitException.class));
  }

  @Test
  public void testInvokeNamedRunnableLambda() {
    Throwable thrown = catchThrowable(() -> vm0.invoke("throwSomething", () -> throwException()));

    assertThat(thrown, notNullValue());
    assertThat(thrown.getCause(), notNullValue());
    assertThat(thrown.getCause(), instanceOf(BasicDUnitException.class));
    assertThat(thrown.getCause().getMessage(), nullValue());
  }

  @SuppressWarnings("unused")
  private static boolean booleanValue() { // invoked by reflection
    return true;
  }

  @SuppressWarnings("unused")
  private static boolean booleanValue(final boolean value) { // invoked by reflection
    return value;
  }

  private static void remoteThrowException() {
    throw new BasicTestException(MESSAGE_FOR_remoteThrowException);
  }

  private static void throwException() throws BasicDUnitException {
    throw new BasicDUnitException();
  }

  private static void remoteBind(String name, String value) {
    assertNotNull("name must not be null", name);
    assertNotNull("value must not be null", value);
    assertNotNull("bindings must not be null", bindings);

    new BasicDistributedTest().getSystem(); // forces connection
    bindings.setProperty(name, value);
  }

  private static void remoteValidateBind(String name, String expected) {
    assertEquals(expected, bindings.getProperty(name));
  }

  @SuppressWarnings("unused")
  private static class BasicTestException extends RuntimeException {
    BasicTestException() {
      this("Test exception.  Please ignore.");
    }

    BasicTestException(String s) {
      super(s);
    }
  }

  private static class BasicDUnitException extends RuntimeException {
    BasicDUnitException() {
      // nothing
    }
  }
}

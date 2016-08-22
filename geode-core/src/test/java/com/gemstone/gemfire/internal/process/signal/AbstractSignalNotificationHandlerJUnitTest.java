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
package com.gemstone.gemfire.internal.process.signal;

import static org.junit.Assert.*;

import java.util.Set;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The AbstractSignalNotificationHandlerJUnitTest class is a test suite of test cases testing the contract
 * and functionality of the AbstractSignalNotificationHandler.
 * </p>
 * @see com.gemstone.gemfire.internal.process.signal.AbstractSignalNotificationHandler
 * @see com.gemstone.gemfire.internal.process.signal.Signal
 * @see com.gemstone.gemfire.internal.process.signal.SignalEvent
 * @see com.gemstone.gemfire.internal.process.signal.SignalListener
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class AbstractSignalNotificationHandlerJUnitTest {

  private Mockery mockContext;

  @Before
  public void setup() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  private AbstractSignalNotificationHandler createSignalNotificationHandler() {
    return new TestSignalNotificationHandler();
  }

  @Test
  public void testAssertNotNullWithNonNullValue() {
    AbstractSignalNotificationHandler.assertNotNull(new Object(), "TEST");
  }

  @Test(expected = NullPointerException.class)
  public void testAssertNotNullWithNullValue() {
    try {
      AbstractSignalNotificationHandler.assertNotNull(null, "Expected %1$s message!", "test");
    }
    catch (NullPointerException expected) {
      assertEquals("Expected test message!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testAssertStateWithValidState() {
    AbstractSignalNotificationHandler.assertState(true, "TEST");
  }

  @Test(expected = IllegalStateException.class)
  public void testAssertStateWithInvalidState() {
    try {
      AbstractSignalNotificationHandler.assertState(false, "Expected %1$s message!", "test");
    }
    catch (IllegalStateException expected) {
      assertEquals("Expected test message!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testAssertValidArgumentWithLegalArgument() {
    AbstractSignalNotificationHandler.assertValidArgument(true, "TEST");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAssertValidArgmentWithIllegalArgument() {
    try {
      AbstractSignalNotificationHandler.assertValidArgument(false, "Expected %1$s message!", "test");
    }
    catch (IllegalArgumentException expected) {
      assertEquals("Expected test message!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testRegisterListener() {
    final AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();

    final SignalListener mockListenerOne = mockContext.mock(SignalListener.class, "SIGALL1");
    final SignalListener mockListenerTwo = mockContext.mock(SignalListener.class, "SIGALL2");

    assertFalse(signalHandler.isListening(mockListenerOne));
    assertFalse(signalHandler.isListening(mockListenerTwo));

    for (final Signal signal : Signal.values()) {
      assertFalse(signalHandler.hasListeners(signal));
      assertFalse(signalHandler.isListening(mockListenerOne, signal));
      assertFalse(signalHandler.isListening(mockListenerTwo, signal));
    }

    assertTrue(signalHandler.registerListener(mockListenerOne));
    assertTrue(signalHandler.registerListener(mockListenerTwo));
    assertFalse(signalHandler.registerListener(mockListenerTwo));
    assertTrue(signalHandler.isListening(mockListenerOne));
    assertTrue(signalHandler.isListening(mockListenerTwo));

    for (final Signal signal : Signal.values()) {
      assertTrue(signalHandler.hasListeners(signal));
      assertTrue(signalHandler.isListening(mockListenerOne, signal));
      assertTrue(signalHandler.isListening(mockListenerTwo, signal));
    }
  }

  @Test(expected = NullPointerException.class)
  public void testRegisterListenerWithNullSignalListener() {
    try {
      createSignalNotificationHandler().registerListener(null);
    }
    catch (NullPointerException expected) {
      assertEquals("The SignalListener to register, listening for all signals cannot be null!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testRegisterListenerWithSignal() {
    final AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();

    final SignalListener mockSigIntListener = mockContext.mock(SignalListener.class, "SIGINT");
    final SignalListener mockSigIntTermListener = mockContext.mock(SignalListener.class, "SIGINT + SIGTERM");

    assertFalse(signalHandler.isListening(mockSigIntListener));
    assertFalse(signalHandler.isListening(mockSigIntTermListener));

    for (final Signal signal : Signal.values()) {
      assertFalse(signalHandler.hasListeners(signal));
      assertFalse(signalHandler.isListening(mockSigIntListener, signal));
      assertFalse(signalHandler.isListening(mockSigIntTermListener, signal));
    }

    assertTrue(signalHandler.registerListener(mockSigIntListener, Signal.SIGINT));
    assertTrue(signalHandler.registerListener(mockSigIntTermListener, Signal.SIGINT));
    assertTrue(signalHandler.registerListener(mockSigIntTermListener, Signal.SIGTERM));
    assertFalse(signalHandler.registerListener(mockSigIntTermListener, Signal.SIGINT));
    assertTrue(signalHandler.isListening(mockSigIntListener));
    assertTrue(signalHandler.isListening(mockSigIntTermListener));

    final Set<Signal> expectedSignals = CollectionUtils.asSet(Signal.SIGINT, Signal.SIGTERM);

    for (final Signal signal : Signal.values()) {
      assertEquals(expectedSignals.contains(signal), signalHandler.hasListeners(signal));
      switch (signal) {
        case SIGINT:
          assertTrue(signalHandler.isListening(mockSigIntListener, signal));
          assertTrue(signalHandler.isListening(mockSigIntTermListener, signal));
          break;
        case SIGTERM:
          assertFalse(signalHandler.isListening(mockSigIntListener, signal));
          assertTrue(signalHandler.isListening(mockSigIntTermListener, signal));
          break;
        default:
          assertFalse(signalHandler.isListening(mockSigIntListener, signal));
          assertFalse(signalHandler.isListening(mockSigIntTermListener, signal));
      }
    }
  }

  @Test(expected = NullPointerException.class)
  public void testRegisterListenerWithNullSignal() {
    try {
      createSignalNotificationHandler().registerListener(mockContext.mock(SignalListener.class, "SIGALL"), null);
    }
    catch (NullPointerException expected) {
      assertEquals("The signal to register the listener for cannot be null!", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = NullPointerException.class)
  public void testRegisterListenerWithSignalAndNullSignalListener() {
    try {
      createSignalNotificationHandler().registerListener(null, Signal.SIGQUIT);
    }
    catch (NullPointerException expected) {
      assertEquals(String.format("The SignalListener being registered to listen for '%1$s' signals cannot be null!",
        Signal.SIGQUIT.getName()), expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testUnregisterListener() {
    final AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();
    final SignalListener mockSignalListener = mockContext.mock(SignalListener.class, "SIGALL");

    assertFalse(signalHandler.isListening(mockSignalListener));
    assertTrue(signalHandler.registerListener(mockSignalListener));
    assertTrue(signalHandler.isListening(mockSignalListener));

    for (final Signal signal : Signal.values()) {
      assertTrue(signalHandler.hasListeners(signal));
    }

    assertTrue(signalHandler.unregisterListener(mockSignalListener));
    assertFalse(signalHandler.isListening(mockSignalListener));

    for (final Signal signal : Signal.values()) {
      assertFalse(signalHandler.hasListeners(signal));
    }

    assertFalse(signalHandler.unregisterListener(mockSignalListener));
  }

  @Test
  public void testUnregisterListenerWithSignalListenerAndAllSignals() {
    final AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();
    final SignalListener mockSignalListener = mockContext.mock(SignalListener.class, "SIGALL");

    assertFalse(signalHandler.isListening(mockSignalListener));
    assertTrue(signalHandler.registerListener(mockSignalListener));
    assertTrue(signalHandler.isListening(mockSignalListener));

    for (final Signal signal : Signal.values()) {
      assertTrue(signalHandler.hasListeners(signal));
      assertTrue(signalHandler.isListening(mockSignalListener, signal));
      assertTrue(signalHandler.unregisterListener(mockSignalListener, signal));
      assertFalse(signalHandler.isListening(mockSignalListener, signal));
      assertFalse(signalHandler.hasListeners(signal));
    }

    assertFalse(signalHandler.unregisterListener(mockSignalListener));
  }

  @Test
  public void testUnregisterListenerWithSignalListenerAndSigint() {
    final AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();
    final SignalListener mockSignalListener = mockContext.mock(SignalListener.class, "SIGALL");

    assertFalse(signalHandler.isListening(mockSignalListener));
    assertTrue(signalHandler.registerListener(mockSignalListener, Signal.SIGINT));
    assertTrue(signalHandler.isListening(mockSignalListener));
    assertTrue(signalHandler.isListening(mockSignalListener, Signal.SIGINT));

    for (final Signal signal : Signal.values()) {
      if (!Signal.SIGINT.equals(signal)) {
        assertFalse(signalHandler.hasListeners(signal));
        assertFalse(signalHandler.isListening(mockSignalListener, signal));
      }
    }

    assertTrue(signalHandler.isListening(mockSignalListener));
    assertTrue(signalHandler.isListening(mockSignalListener, Signal.SIGINT));
    assertTrue(signalHandler.unregisterListener(mockSignalListener, Signal.SIGINT));
    assertFalse(signalHandler.isListening(mockSignalListener, Signal.SIGINT));
    assertFalse(signalHandler.isListening(mockSignalListener));

    for (final Signal signal : Signal.values()) {
      assertFalse(signalHandler.hasListeners(signal));
    }

    assertFalse(signalHandler.unregisterListener(mockSignalListener));
  }

  @Test(expected = NullPointerException.class)
  public void testUnregisterListenerWithSignalListenerAndNullSignal() {
    try {
      createSignalNotificationHandler().unregisterListener(mockContext.mock(SignalListener.class, "SIGALL"), null);
    }
    catch (NullPointerException expected) {
      assertEquals("The signal from which to unregister the listener cannot be null!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testUnregisterListeners() {
    final AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();

    final SignalListener mockSigQuitListener = mockContext.mock(SignalListener.class, "SIGQUIT");
    final SignalListener mockSigTermListener = mockContext.mock(SignalListener.class, "SIGTERM");
    final SignalListener mockSigTermQuitListener = mockContext.mock(SignalListener.class, "SIGTERM + SIGQUIT");

    assertFalse(signalHandler.isListening(mockSigQuitListener));
    assertFalse(signalHandler.isListening(mockSigTermListener));
    assertFalse(signalHandler.isListening(mockSigTermQuitListener));

    for (final Signal signal : Signal.values()) {
      assertFalse(signalHandler.hasListeners(signal));
    }

    // register sigquit and sigterm listeners...
    assertTrue(signalHandler.registerListener(mockSigQuitListener, Signal.SIGQUIT));
    assertTrue(signalHandler.registerListener(mockSigTermListener, Signal.SIGTERM));
    assertTrue(signalHandler.registerListener(mockSigTermQuitListener, Signal.SIGQUIT));
    assertTrue(signalHandler.registerListener(mockSigTermQuitListener, Signal.SIGTERM));

    assertTrue(signalHandler.isListening(mockSigQuitListener));
    assertFalse(signalHandler.isListening(mockSigQuitListener, Signal.SIGINT));
    assertTrue(signalHandler.isListening(mockSigQuitListener, Signal.SIGQUIT));
    assertFalse(signalHandler.isListening(mockSigQuitListener, Signal.SIGTERM));
    assertTrue(signalHandler.isListening(mockSigTermListener));
    assertFalse(signalHandler.isListening(mockSigTermListener, Signal.SIGINT));
    assertFalse(signalHandler.isListening(mockSigTermListener, Signal.SIGQUIT));
    assertTrue(signalHandler.isListening(mockSigTermListener, Signal.SIGTERM));
    assertTrue(signalHandler.isListening(mockSigTermQuitListener));
    assertFalse(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGINT));
    assertTrue(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGQUIT));
    assertTrue(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGTERM));
    assertFalse(signalHandler.hasListeners(Signal.SIGINT));
    assertTrue(signalHandler.hasListeners(Signal.SIGQUIT));
    assertTrue(signalHandler.hasListeners(Signal.SIGTERM));

    // unregister all sigterm listeners...
    assertTrue(signalHandler.unregisterListeners(Signal.SIGTERM));

    assertTrue(signalHandler.isListening(mockSigQuitListener));
    assertFalse(signalHandler.isListening(mockSigQuitListener, Signal.SIGINT));
    assertTrue(signalHandler.isListening(mockSigQuitListener, Signal.SIGQUIT));
    assertFalse(signalHandler.isListening(mockSigQuitListener, Signal.SIGTERM));
    assertFalse(signalHandler.isListening(mockSigTermListener));
    assertFalse(signalHandler.isListening(mockSigTermListener, Signal.SIGINT));
    assertFalse(signalHandler.isListening(mockSigTermListener, Signal.SIGQUIT));
    assertFalse(signalHandler.isListening(mockSigTermListener, Signal.SIGTERM));
    assertTrue(signalHandler.isListening(mockSigTermQuitListener));
    assertFalse(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGINT));
    assertTrue(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGQUIT));
    assertFalse(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGTERM));
    assertFalse(signalHandler.hasListeners(Signal.SIGINT));
    assertTrue(signalHandler.hasListeners(Signal.SIGQUIT));
    assertFalse(signalHandler.hasListeners(Signal.SIGTERM));
  }

  @Test(expected = NullPointerException.class)
  public void testUnregisterListenersWithNullSignal() {
    try {
      createSignalNotificationHandler().unregisterListeners(null);
    }
    catch (NullPointerException expected) {
      assertEquals("The signal from which to unregister all listeners cannot be null!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testNotifyListeners() {
    final AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();

    final SignalListener mockSigAllListener = mockContext.mock(SignalListener.class, "SIGALL");
    final SignalListener mockSigIntListener = mockContext.mock(SignalListener.class, "SIGINT");
    final SignalListener mockSigQuitListener = mockContext.mock(SignalListener.class, "SIGQUIT");
    final SignalListener mockSigQuitTermListener = mockContext.mock(SignalListener.class, "SIGQUIT + SIGTERM");

    final SignalEvent sigintEvent = new SignalEvent(this, Signal.SIGINT);
    final SignalEvent sigioEvent = new SignalEvent(this, Signal.SIGIO);
    final SignalEvent sigquitEvent = new SignalEvent(this, Signal.SIGQUIT);
    final SignalEvent sigtermEvent = new SignalEvent(this, Signal.SIGTERM);

    mockContext.checking(new Expectations() {{
      oneOf(mockSigAllListener).handle(with(equal(sigintEvent)));
      oneOf(mockSigAllListener).handle(with(equal(sigioEvent)));
      oneOf(mockSigAllListener).handle(with(equal(sigquitEvent)));
      oneOf(mockSigAllListener).handle(with(equal(sigtermEvent)));
      oneOf(mockSigIntListener).handle(with(equal(sigintEvent)));
      oneOf(mockSigQuitListener).handle(with(equal(sigquitEvent)));
      oneOf(mockSigQuitTermListener).handle(with(equal(sigquitEvent)));
      oneOf(mockSigQuitTermListener).handle(with(equal(sigtermEvent)));
    }});

    assertFalse(signalHandler.isListening(mockSigAllListener));
    assertFalse(signalHandler.isListening(mockSigIntListener));
    assertFalse(signalHandler.isListening(mockSigQuitListener));
    assertFalse(signalHandler.isListening(mockSigQuitTermListener));

    for (final Signal signal : Signal.values()) {
      assertFalse(signalHandler.hasListeners(signal));
    }

    assertTrue(signalHandler.registerListener(mockSigAllListener));
    assertTrue(signalHandler.registerListener(mockSigIntListener, Signal.SIGINT));
    assertTrue(signalHandler.registerListener(mockSigQuitListener, Signal.SIGQUIT));
    assertTrue(signalHandler.registerListener(mockSigQuitTermListener, Signal.SIGQUIT));
    assertTrue(signalHandler.registerListener(mockSigQuitTermListener, Signal.SIGTERM));
    assertTrue(signalHandler.isListening(mockSigAllListener));
    assertTrue(signalHandler.isListening(mockSigIntListener));
    assertTrue(signalHandler.isListening(mockSigQuitListener));
    assertTrue(signalHandler.isListening(mockSigQuitTermListener));

    for (final Signal signal : Signal.values()) {
      assertTrue(signalHandler.hasListeners(signal));
      assertTrue(signalHandler.isListening(mockSigAllListener, signal));

      switch (signal) {
        case SIGINT:
          assertTrue(signalHandler.isListening(mockSigIntListener, signal));
          assertFalse(signalHandler.isListening(mockSigQuitListener, signal));
          assertFalse(signalHandler.isListening(mockSigQuitTermListener, signal));
          break;
        case SIGQUIT:
          assertFalse(signalHandler.isListening(mockSigIntListener, signal));
          assertTrue(signalHandler.isListening(mockSigQuitListener, signal));
          assertTrue(signalHandler.isListening(mockSigQuitTermListener, signal));
          break;
        case SIGTERM:
          assertFalse(signalHandler.isListening(mockSigIntListener, signal));
          assertFalse(signalHandler.isListening(mockSigQuitListener, signal));
          assertTrue(signalHandler.isListening(mockSigQuitTermListener, signal));
          break;
        default:
          assertFalse(signalHandler.isListening(mockSigIntListener, signal));
          assertFalse(signalHandler.isListening(mockSigQuitListener, signal));
          assertFalse(signalHandler.isListening(mockSigQuitTermListener, signal));
      }
    }

    signalHandler.notifyListeners(sigintEvent);
    signalHandler.notifyListeners(sigioEvent);
    signalHandler.notifyListeners(sigquitEvent);
    signalHandler.notifyListeners(sigtermEvent);

    // notification verification handled by mockContext.assertIsSatisfied in tearDown()
  }

  private static final class TestSignalNotificationHandler extends AbstractSignalNotificationHandler {
  }

}

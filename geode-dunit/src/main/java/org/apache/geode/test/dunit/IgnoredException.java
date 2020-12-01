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
package org.apache.geode.test.dunit;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * {@code IgnoredException} provides static utility methods that will log messages to add or
 * remove {@code IgnoredException}s. Each {@code IgnoredException} allows you to specify a
 * suspect string that will be ignored by the {@code GrepLogs} utility which is run after each
 * {@code DistributedTest} test method.
 *
 * <p>
 * These methods can be used directly: {@code IgnoredException.addIgnoredException(...)},
 * however, they are intended to be referenced through static import:
 *
 * <pre>
 * import static org.apache.geode.test.dunit.IgnoredException.*;
 *    ...
 *    addIgnoredException(...);
 * </pre>
 *
 * <p>
 * A test should use {@code addIgnoredException(...)} before executing the code that will
 * potentially log the suspect string. The test should then {@code remove()} the
 * {@code IgnoredException} immediately after. Note that
 * {@code DistributedTestCase.tearDown()} will automatically remove all current
 * {@code IgnoredException}s by invoking {@code removeAllIgnoredExceptions}.
 *
 * <p>
 * A suspect string is typically an Exception class and/or message string.
 *
 * <p>
 * The {@code GrepLogs} utility is part of Hydra which is not included in Apache Geode. The
 * Hydra class which consumes logs and reports suspect strings is
 * {@code batterytest.greplogs.GrepLogs}.
 *
 * <p>
 * Extracted from DistributedTestCase.
 *
 * @since GemFire 5.7bugfix
 */
@SuppressWarnings("serial")
public class IgnoredException implements Serializable, AutoCloseable {

  private static final Logger logger = LogService.getLogger();

  private static final ConcurrentLinkedQueue<IgnoredException> IGNORED_EXCEPTIONS =
      new ConcurrentLinkedQueue<>();

  private final String suspectString;

  private final transient VM vm;

  /**
   * Log in all VMs, in both the test logger and the GemFire logger the ignored exception string to
   * prevent grep logs from complaining. The suspect string is used by the GrepLogs utility and so
   * can contain regular expression characters.
   *
   * <p>
   * If you do not remove the ignored exception, it will be removed at the end of your test case
   * automatically.
   *
   * @since GemFire 5.7bugfix
   * @param suspectString the exception string to expect
   * @return an IgnoredException instance for removal
   */
  public static IgnoredException addIgnoredException(final String suspectString) {
    return addIgnoredException(suspectString, null);
  }

  public static IgnoredException addIgnoredException(final Class exceptionClass) {
    return addIgnoredException(exceptionClass.getName(), null);
  }

  /**
   * Log in all VMs, in both the test logger and the GemFire logger the ignored exception string to
   * prevent grep logs from complaining. The suspect string is used by the GrepLogs utility and so
   * can contain regular expression characters.
   *
   * @since GemFire 5.7bugfix
   * @param suspectString the exception string to expect
   * @param vm the VM on which to log the expected exception or null for all VMs
   * @return an IgnoredException instance for removal purposes
   */
  public static IgnoredException addIgnoredException(final String suspectString, final VM vm) {
    IgnoredException ignoredException = new IgnoredException(suspectString, vm);
    SerializableRunnableIF addRunnable = runnableForAdd(ignoredException.getAddMessage());

    try {
      addRunnable.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (vm != null) {
      vm.invoke(addRunnable);
    } else {
      Invoke.invokeInEveryVM(addRunnable);
      if (Host.getLocator() != null) {
        Invoke.invokeInLocator(addRunnable);
      }
    }

    IGNORED_EXCEPTIONS.add(ignoredException);
    return ignoredException;
  }

  public static void removeAllExpectedExceptions() {
    IgnoredException ignoredException;
    while ((ignoredException = IGNORED_EXCEPTIONS.poll()) != null) {
      ignoredException.remove();
    }
  }

  public IgnoredException(final String suspectString) {
    this.suspectString = suspectString;
    vm = null;
  }

  IgnoredException(final String suspectString, final VM vm) {
    this.suspectString = suspectString;
    this.vm = vm;
  }

  public String getRemoveMessage() {
    return "<ExpectedException action=remove>" + suspectString + "</ExpectedException>";
  }

  public String getAddMessage() {
    return "<ExpectedException action=add>" + suspectString + "</ExpectedException>";
  }

  public void remove() {
    SerializableRunnableIF removeRunnable = runnableForRemove(getRemoveMessage());

    try {
      removeRunnable.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (vm != null) {
      vm.invoke(removeRunnable);
    } else {
      Invoke.invokeInEveryVM(removeRunnable);
    }
  }

  @Override
  public void close() {
    remove();
  }

  String suspectString() {
    return suspectString;
  }

  VM vm() {
    return vm;
  }

  private static SerializableRunnableIF runnableForAdd(String message) {
    return createRunnable(message, "addIgnoredException");
  }

  private static SerializableRunnableIF runnableForRemove(String message) {
    return createRunnable(message, "remove");
  }

  private static SerializableRunnableIF createRunnable(String message, String action) {
    return new SerializableRunnable(IgnoredException.class.getSimpleName() + " " + action) {
      @Override
      public void run() {
        logger.info(message);
      }
    };
  }
}

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

package org.apache.geode.internal;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.distributed.internal.DistributionConfig;

/**
 * Support for correctness assertions. "An assertion is a statement containing a boolean expression
 * that the programmer believes to be true at the time the statement is executed. For example, after
 * unmarshalling all of the arguments from a data buffer, a programmer might assert that the number
 * of bytes of data remaining in the buffer is zero. The system executes the assertion by evaluating
 * the boolean expression and reporting an error if it evaluates to false. By verifying that the
 * boolean expression is indeed true, the system corroborates the programmer's knowledge of the
 * program and increases their confidence that the program is free of bugs.Assertion checking may be
 * disabled for increased performance. Typically, assertion-checking is enabled during program
 * development and testing, and disabled during deployment." - from
 * http://java.sun.com/aboutJava/communityprocess/jsr/asrt_prop.html
 *
 * This class does not provide a way to disable assertions for increased performance, since we
 * cannot prevent the arguments from being evaluated in any case without changes to the Java
 * language itself.
 *
 * The interface to this class was designed for easy migration to the new assert facility that is
 * currently proposed for the Java language in JSR-000041
 *
 * @see <a href="http://java.sun.com/aboutJava/communityprocess/jsr/jsr_041_asrt.html">JSR-000041 A
 *      Simple Assertion Facility</a>
 * @see <a href="http://java.sun.com/aboutJava/communityprocess/jsr/asrt_prop.html">Proposal: A
 *      Simple Assertion Facility For the Java[tm] Programming Language</a>
 */

public class Assert {
  /**
   * Assert that a boolean value is true.
   *
   * @param b the boolean value to check
   * @throws InternalGemFireError if false
   */
  public static void assertTrue(boolean b) {
    if (!b) {
      throwError(null);
    }
  }

  private static boolean debug =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "haltOnAssertFailure");

  public static void fail(Object message) {
    throwError(message);
  }

  private static void throwError(Object message) {
    if (debug) {
      System.out.flush();
      System.err.println("Assertion failure: " + message);
      try {
        throw new Exception("get Stack trace");
      } catch (Exception ex) {
        ex.printStackTrace();
      }
      System.out.println();
      System.out.flush();
      System.err.println("Waiting for debugger to attach");
      System.err.flush();
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignore) {
          Thread.currentThread().interrupt();
        }
      }
    } else {
      InternalGemFireError ex = null;
      if (message != null) {
        ex = new InternalGemFireError(message);
      } else {
        ex = new InternalGemFireError();
      }
      throw ex;
    }
  }

  /**
   * Assert that a boolean value is true.The message object will be sent toString() and used for an
   * error message.
   *
   * @param b the boolean value to check
   * @param message used for error message
   * @throws InternalGemFireError if false
   */
  public static void assertTrue(boolean b, Object message) {
    if (!b) {
      throwError(message);
    }
  }

  public static void assertTrue(boolean b, boolean message) {
    if (!b) {
      throwError(Boolean.valueOf(message));
    }
  }

  public static void assertTrue(boolean b, char message) {
    if (!b) {
      throwError(new Character(message));
    }
  }

  public static void assertTrue(boolean b, int message) {
    if (!b) {
      throwError(Integer.valueOf(message));
    }
  }

  public static void assertTrue(boolean b, long message) {
    if (!b) {
      throwError(Long.valueOf(message));
    }
  }

  public static void assertTrue(boolean b, float message) {
    if (!b) {
      throwError(new Float(message));
    }
  }

  public static void assertTrue(boolean b, double message) {
    if (!b) {
      throwError(Double.valueOf(message));
    }
  }

  /**
   * This is a workaround for X bug 38288. JRockit can throw a NullPointerException from
   * Thread.holdsLock, so we catch the NullPointerException if it happens.
   *
   * This method returns true, unless it throws an exception. This is so we can disable these tests
   * for performance reasons with a java assertion, eg
   * <code>assert Assert.assertHoldLock(lock, true);</code>
   *
   * @param lock The lock to test
   * @param shouldBeHeld true if this thread should hold this lock.
   * @return true, unless the method throws an exception.
   */
  public static boolean assertHoldsLock(Object lock, boolean shouldBeHeld) {
    try {
      if (Thread.holdsLock(lock) != shouldBeHeld) {
        throwError(null);
      }
    } catch (NullPointerException jrockitSucks) {
      assertTrue(lock != null);
    }

    return true;
  }

  public static void assertArgument(final boolean valid, final String message,
      final Object... args) {
    if (!valid) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

  public static void assertNotNull(final Object obj, final String message, final Object... args) {
    if (obj == null) {
      throw new NullPointerException(String.format(message, args));
    }
  }

  public static void assertState(final boolean valid, final String message, final Object... args) {
    if (!valid) {
      throw new IllegalStateException(String.format(message, args));
    }
  }
}

/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.lang;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.Thread.State;

/**
 * The ThreadUtils class is an abstract utility class for working with and invoking methods on Threads.
 * <p/>
 * @author John Blum
 * @see java.lang.Thread
 * @since 7.0
 */
public abstract class ThreadUtils {

  /**
   * Gets the name of the particular Thread or null if the Thread object reference is null.
   * <p/>
   * @param thread the Thread object whose name is returned.
   * @return a String value indicating the name of the Thread or null if the Thread object reference is null.
   * @see java.lang.Thread#getName()
   */
  public static String getThreadName(final Thread thread) {
    return (thread == null ? null : thread.getName());
  }

  /**
   * Interrupts the specified Thread, guarding against null.
   * <p/>
   * @param thread the Thread to interrupt.
   * @see java.lang.Thread#interrupt()
   */
  public static void interrupt(final Thread thread) {
    if (thread != null) {
      thread.interrupt();
    }
  }

  /**
   * Determines whether the specified Thread is alive, guarding against null Object references.
   * <p/>
   * @param thread the Thread to determine for aliveness.
   * @return a boolean value indicating whether the specified Thread is alive.  Will return false if the Thread Object
   * references is null.
   * @see java.lang.Thread#isAlive()
   */
  public static boolean isAlive(final Thread thread) {
    return (thread != null && thread.isAlive());
  }

  /**
   * Determines whether the specified Thread is in a waiting state, guarding against null Object references
   * <p/>
   * @param thread the Thread to access it's state.
   * @return a boolean value indicating whether the Thread is in a waiting state.  If the Thread Object reference
   * is null, then this method return false, as no Thread is clearly not waiting for anything.
   * @see java.lang.Thread#getState()
   * @see java.lang.Thread.State#WAITING
   */
  public static boolean isWaiting(final Thread thread) {
    return (thread != null && thread.getState().equals(State.WAITING));
  }

  /**
   * Causes the current Thread to sleep for the specified number of milliseconds.  If the current Thread is interrupted
   * during sleep, the interrupt flag on the current Thread will remain set and the duration, in milliseconds, of completed sleep is returned.
   * <p/>
   * @param milliseconds an integer value specifying the number of milliseconds the current Thread should sleep.
   * @return a long value indicating duration in milliseconds of completed sleep by the current Thread.
   * @see java.lang.System#nanoTime()
   * @see java.lang.Thread#sleep(long)
   */
  public static long sleep(final long milliseconds) {
    final long t0 = System.nanoTime();

    try {
      Thread.sleep(milliseconds);
    }
    catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }

    return (System.nanoTime() - t0) / 1000;
  }

  /**
   * Returns a stack trace of the {@code Throwable} as a {@code String}.
   * 
   * @param throwable
   *          The throwable for which to create the stack trace.
   * @param expectNull
   *          True if null should be returned when {@code throwable} is null or
   *          false to return "" when {@code throwable} is null
   * @return null if {@code throwable} is null and {@code expectNull} is true,
   *         "" if {@code throwable} is null and {@code expectNull} is false,
   *         otherwise the stack trace for {@code throwable}
   */
  public static String stackTraceToString(final Throwable throwable, final boolean expectNull) {
    if (throwable == null) {
      if (expectNull == true) {
        return null;
      }
      
      return "";
    }
    
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    throwable.printStackTrace(printWriter);
    printWriter.close();
    return stringWriter.toString();
  }
}

/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.internal.process.signal;

/**
 * The SignalType class...
 * </p>
 * @author John Blum
 * @since 7.0
 */
public enum SignalType {
  CONTROL("Other signals that are used by the JVM for control purposes."),
  ERROR("The JVM raises a SIGABRT if it detects a condition from which it cannot recover."),
  EXCEPTION("The operating system synchronously raises an appropriate exception signal whenever an unrecoverable condition occurs."),
  INTERRUPT("Interrupt signals are raised asynchronously, from outside a JVM process, to request shut down."),
  UNKNOWN("Unknown");

  private final String description;

  SignalType(final String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return this.description;
  }

}

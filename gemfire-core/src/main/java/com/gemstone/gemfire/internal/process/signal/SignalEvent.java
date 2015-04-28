/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.internal.process.signal;

import java.util.EventObject;

/**
 * The SignalEvent class...
 * </p>
 * @author John Blum
 * @see java.util.EventObject
 * @since 7.0
 */
@SuppressWarnings("unused")
public class SignalEvent extends EventObject {

  private final Signal signal;

  public SignalEvent(final Object source, final Signal signal) {
    super(source);
    assert signal != null : "The Signal generating this event cannot be null!";
    this.signal = signal;
  }

  public Signal getSignal() {
    return this.signal;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());
    buffer.append("{ signal = ").append(getSignal());
    buffer.append(", source = ").append(getSource());
    buffer.append("}");
    return buffer.toString();
  }

}

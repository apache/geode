/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.distributed.internal.unsafe;

/**
 * The RegisterSignalHandlerSupport class is an ugly hack!
 * </p>
 * @author John Blum
 * @since 7.0
 */
public abstract class RegisterSignalHandlerSupport {

  public static void registerSignalHandlers() {
    sun.misc.Signal.handle(new sun.misc.Signal("INT"), new sun.misc.SignalHandler() {
      public void handle(final sun.misc.Signal sig) {
      }
    });
  }

}

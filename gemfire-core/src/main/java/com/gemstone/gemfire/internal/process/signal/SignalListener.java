/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.internal.process.signal;

import java.util.EventListener;

/**
 * The SignalListener class...
 * </p>
 * @author John Blum
 * @see java.util.EventListener
 * @since 7.0
 */
@SuppressWarnings("unused")
public interface SignalListener extends EventListener {

  public void handle(SignalEvent event);

}

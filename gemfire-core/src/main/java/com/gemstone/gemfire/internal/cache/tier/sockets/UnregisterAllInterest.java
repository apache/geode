/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.io.*;

/**
 * Singleton token indicating that all interest registrations should be removed.
 *
 * @author Darrel Schneider
 *
 */
public class UnregisterAllInterest implements Serializable {
  private static final long serialVersionUID = 5026160621257178459L;
  private static final UnregisterAllInterest SINGLETON = new UnregisterAllInterest();
  /**
   * Return the only instance of this class.
   */
  public static final UnregisterAllInterest singleton() {
    return SINGLETON;
  }
  
  /** Creates a new instance of UnregisterAllInterest */
  private UnregisterAllInterest() {
  }
  
  @Override
  public String toString() {
    return "UNREGISTER ALL INTEREST";
  }
  
  private Object readResolve() throws ObjectStreamException {
    return SINGLETON;
  }
}

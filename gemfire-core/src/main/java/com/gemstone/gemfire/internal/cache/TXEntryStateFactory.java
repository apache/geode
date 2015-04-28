/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.internal.cache.RegionEntry;

/**
 * 
 * @author Asif
 *
 */
public interface TXEntryStateFactory
{

  /**
   * Creates an instance of TXEntryState.
   * 
   * @return the created entry
   */
  public TXEntryState createEntry();

  public TXEntryState createEntry(RegionEntry re, Object vId, Object pendingValue, Object entryKey,TXRegionState txrs);

}

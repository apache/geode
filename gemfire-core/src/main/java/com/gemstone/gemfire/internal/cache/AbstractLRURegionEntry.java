/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;


import com.gemstone.gemfire.internal.cache.lru.LRUEntry;

/**
 * Abstract implementation class of RegionEntry interface.
 * This adds LRU support behaviour
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */
public abstract class AbstractLRURegionEntry
  extends AbstractRegionEntry
  implements LRUEntry
{
  protected AbstractLRURegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }
  
  /////////////////////////////////////////////////////////////////////
  ////////////////////////// instance methods /////////////////////////
  /////////////////////////////////////////////////////////////////////

  // Do not add any instance fields to this class.
  // Instead add them to the LRU section of LeafRegionEntry.cpp.
}

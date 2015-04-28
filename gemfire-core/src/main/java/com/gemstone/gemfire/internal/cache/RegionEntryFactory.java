/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;


/**
 * A factory that produces RegionEntry instances.
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */
public interface RegionEntryFactory {
  /**
   * Creates an instance of RegionEntry.
   * @return the created entry
   */
  public RegionEntry createEntry(RegionEntryContext context, Object key, Object value);
  /**
   * @return the Class that each entry, of this factory, is an instance of
   */
  public Class getEntryClass();
  /**
   * @return return the versioned equivalent of this RegionEntryFactory
   */
  public RegionEntryFactory makeVersioned();
}

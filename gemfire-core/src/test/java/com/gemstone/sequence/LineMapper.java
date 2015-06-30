/*=========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.sequence;

/**
 * An interface for mapping a lifeline name to a shorter version of the same
 * line. This could also consolodate multiple lifelines onto a single line.
 * 
 * The most common case for this is that a lifeline represents a VM that is
 * restarted several times. Eg time, the line name changes, but we want to put
 * all of the states for that "logical" vm on the same line.
 * @author dsmith
 *
 */
public interface LineMapper {
  
  /**
   * Return the short name for this lifeline.
   */
  public String getShortNameForLine(String lineName);

}

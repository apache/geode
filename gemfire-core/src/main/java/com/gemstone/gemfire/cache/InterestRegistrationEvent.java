/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

import java.util.Set;

import com.gemstone.gemfire.internal.cache.tier.InterestType;

/**
 * Interface <code>InterestRegistrationEvent</code> encapsulated interest
 * event information like region and keys of interest.
 *
 * @author Barry Oglesby
 * @since 6.0
 */
public interface InterestRegistrationEvent {

  /**
   * Returns the name of the region to which this interest event belongs.
   *
   * @return the name of the region to which this interest event belongs
   */
  public String getRegionName();
  
  /**
   * Returns the region to which this interest belongs.
   * 
   * @return the region to which this interest belongs
   */
  public Region<?,?> getRegion(); 

  /**
   * Returns a <code>Set</code> of keys of interest.
   * 
   * @return a <code>Set</code> of keys of interest
   */
  public Set<?> getKeysOfInterest();

  /**
   * Returns this event's interest type.
   *
   * @return this event's interest type
   */
  public int getInterestType();

  /**
   * Returns whether this event represents a register interest.
   *
   * @return whether this event represents a register interest
   */
  public boolean isRegister();

  /**
   * Returns whether this event's interest type is
   * {@link InterestType#KEY}.
   *
   * @return whether this event's interest type is
   *         {@link InterestType#KEY}
   */
  public boolean isKey();

  /**
   * Returns whether this event's interest type is
   * {@link InterestType#REGULAR_EXPRESSION}.
   *
   * @return whether this event's interest type is
   *         {@link InterestType#REGULAR_EXPRESSION}
   */
  public boolean isRegularExpression();
  
  /** 
   * Returns the {@link ClientSession} that initiated this event 
   *  
   * @return the {@link ClientSession} that initiated this event 
   */ 
  public ClientSession getClientSession(); 
}

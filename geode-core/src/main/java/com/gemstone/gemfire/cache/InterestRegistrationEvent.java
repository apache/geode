/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.cache;

import java.util.Set;

import com.gemstone.gemfire.internal.cache.tier.InterestType;

/**
 * Interface <code>InterestRegistrationEvent</code> encapsulated interest
 * event information like region and keys of interest.
 *
 * @since GemFire 6.0
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

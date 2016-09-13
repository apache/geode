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
package com.gemstone.gemfire.admin;

/**
 * Adminitrative interface for monitoring a statistic resource in a GemFire
 * system member.  A resource is comprised of one or many 
 * <code>Statistics</code>.
 *
 * @since GemFire     3.5
 *
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 */
public interface StatisticResource {
  
  /**
   * Gets the identifying name of this resource.
   *
   * @return the identifying name of this resource
   */
  public String getName();

  /**
   * Gets the full description of this resource.
   *
   * @return the full description of this resource
   */
  public String getDescription();
  
  /**
   * Gets the classification type of this resource.
   *
   * @return the classification type of this resource
   * @since GemFire 5.0
   */
  public String getType();
  
  /**
   * Returns a display string of the {@link SystemMember} owning this 
   * resource.
   *
   * @return a display string of the owning {@link SystemMember}
   */
  public String getOwner();
  
  /**
   * Returns an ID that uniquely identifies the resource within the
   * {@link SystemMember} it belongs to.
   *
   * @return unique id within the owning {@link SystemMember}
   */
  public long getUniqueId();
  
  /**
   * Returns a read-only array of every {@link Statistic} in this resource.
   *
   * @return read-only array of every {@link Statistic} in this resource
   */
  public Statistic[] getStatistics();
  
  /**
   * Refreshes the values of every {@link Statistic} in this resource by
   * retrieving them from the member's VM.
   *
   * @throws com.gemstone.gemfire.admin.AdminException 
   *         if unable to refresh statistic values
   */
  public void refresh() throws com.gemstone.gemfire.admin.AdminException;
  
}


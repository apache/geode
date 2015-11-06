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
package com.gemstone.gemfire.distributed.internal.membership;

import java.net.InetAddress;

/**
 * This is the SPI for the basic element of membership provided in the
 * GemFire system.
 * 
 * @author jpenney
 *
 */
public interface NetMember
{

  public abstract void setAttributes(MemberAttributes args);

  public abstract MemberAttributes getAttributes();

  public abstract InetAddress getIpAddress();

  public abstract int getPort();
  
  public abstract void setPort(int p);

  public abstract boolean isMulticastAddress();
  
  /**
   * return a flag stating whether the member has network partition detection enabled
   * @since 5.6
   */
  public abstract boolean splitBrainEnabled();
  
  /**
   * return a flag stating whether the member can be the membership coordinator
   * @since 5.6
   */
  public abstract boolean canBeCoordinator();

  /**
   * Establishes an order between 2 addresses. Assumes other contains non-null IpAddress.
   * Excludes channel_name from comparison.
   * @return 0 for equality, value less than 0 if smaller, greater than 0 if greater.
   */
  public abstract int compare(NetMember other);

  /**
   * implements the java.lang.Comparable interface
   * @see java.lang.Comparable
   * @param o - the Object to be compared
   * @return a negative integer, zero, or a positive integer as this object is less than,
   *         equal to, or greater than the specified object.
   * @exception java.lang.ClassCastException - if the specified object's type prevents it
   *            from being compared to this Object.
   */
  public abstract int compareTo(Object o);

  public abstract boolean equals(Object obj);

  public abstract int hashCode();

  public abstract String toString();

 }

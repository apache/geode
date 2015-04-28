/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

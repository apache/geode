/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;

/**
 * This is the SPI for the basic element of membership provided in the
 * GemFire system.
 * 
 * @author jpenney
 *
 */
public interface NetMember extends Comparable<NetMember>
{

  public void setAttributes(MemberAttributes args);

  public MemberAttributes getAttributes();

  public InetAddress getInetAddress();

  public int getPort();
  
  public void setPort(int p);

  public boolean isMulticastAddress();
  
  public short getVersionOrdinal();
  
  /**
   * return a flag stating whether the member has network partition detection enabled
   * @since 5.6
   */
  public boolean splitBrainEnabled();
  
  public void setSplitBrainEnabled(boolean enabled);
  
  /**
   * return a flag stating whether the member can be the membership coordinator
   * @since 5.6
   */
  public boolean preferredForCoordinator();
  
  /**
   * Set whether this member ID is preferred for coordinator.  This
   * is mostly useful for unit tests because it does not distribute
   * this status to other members in the distributed system. 
   * @param preferred
   */
  public void setPreferredForCoordinator(boolean preferred);
  
  public byte getMemberWeight();

  /** write identity information not known by DistributedMember instances */
  public void writeAdditionalData(DataOutput out) throws IOException;
  
  /** read identity information not known by DistributedMember instances */
  public void readAdditionalData(DataInput in) throws ClassNotFoundException, IOException;

 }

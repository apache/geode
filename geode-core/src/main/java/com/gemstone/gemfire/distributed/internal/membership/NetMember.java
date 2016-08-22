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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;

import com.gemstone.gemfire.internal.Version;

/**
 * This is the SPI for the basic element of membership provided in the
 * GemFire system.
 * 
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
   * @since GemFire 5.6
   */
  public boolean isNetworkPartitionDetectionEnabled();
  
  public void setNetworkPartitionDetectionEnabled(boolean enabled);
  
  /**
   * return a flag stating whether the member can be the membership coordinator
   * @since GemFire 5.6
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
  
  public void setVersion(Version v);

  /** write identity information not known by DistributedMember instances */
  public void writeAdditionalData(DataOutput out) throws IOException;
  
  /** read identity information not known by DistributedMember instances */
  public void readAdditionalData(DataInput in) throws ClassNotFoundException, IOException;

 }

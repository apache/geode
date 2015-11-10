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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.Vector;


/**
 * The NetView class tracks the 'leader'
 * process for split-brain detection/handling.
 * 
 * @author bruce
 * @since 5.5 
 */
public class NetView extends Vector<InternalDistributedMember> {
  private static final long serialVersionUID = -8888347937416039434L;
  /**
   * The lead member is used in jgroups to determine which
   * members survive a network partitioning event. 
   */
  private transient NetMember leadmember;
  
  private transient NetMember creator;
  
  private long viewNumber;
  
  /**
   * @return the view number
   */
  public long getViewNumber() {
    return this.viewNumber;
  }
  
  /** crashed members removed in this view change */
  private Set suspectedMembers;
  public NetView(int size, long viewNumber) {
    super(size);
    this.viewNumber = viewNumber;
  }
  
  public NetView(Collection mbrs) {
    throw new UnsupportedOperationException(); // must have a view number
  }
  
  public NetView(NetView mbrs, long viewNumber) {
    super(mbrs);
    this.creator = mbrs.creator;
    this.viewNumber = viewNumber;
  }
  public NetView() {
    super();
  }
  
  public NetMember getCreator() {
    return this.creator;
  }
  public void setCreator(NetMember mbr) {
    this.creator = mbr;
  }
  
  public void setLeadMember(NetMember lead) {
    this.leadmember = lead;
  }
  public NetMember getLeadMember() {
    return this.leadmember;
  }
  public synchronized Set getCrashedMembers() {
    if (this.suspectedMembers == null) {
      return Collections.EMPTY_SET;
    }
    return this.suspectedMembers;
  }
  public synchronized void setCrashedMembers(Set mbrs) {
    this.suspectedMembers = mbrs;
  }
  @Override
  public synchronized boolean equals(Object arg0) {
    return super.equals(arg0);
  }
  @Override
  public synchronized int hashCode() {
    return super.hashCode();
  }
  @Override
  public String toString() {
    // this string is displayed in the product-use log file
    return "View(creator="+creator+", viewId=" + this.viewNumber + ", " + super.toString() + ")";
  }
}


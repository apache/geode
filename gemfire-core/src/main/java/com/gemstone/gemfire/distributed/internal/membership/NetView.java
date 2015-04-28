/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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


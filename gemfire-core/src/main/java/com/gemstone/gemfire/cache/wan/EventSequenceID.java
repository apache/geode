/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.wan;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Arrays;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * This class wraps 1) DistributedMembershipID 2) ThreadID 3) SequenceID
 * attributes which are used to uniquely identify any Region Operation like
 * create, update, destroy etc. This helps in sequencing the events belonging to
 * a unique producer. As an example, the EventSequenceID can be used to track 
 * the events received by <code>AsyncEventListener</code>. 
 * If the event has already been seen, <code>AsyncEventListener</code> can 
 * choose to ignore it.
 */
public final class EventSequenceID {
  /**
   * Uniquely identifies the distributed member VM in which the Event is
   * produced
   */
  private String membershipID;

  /**
   * Unqiuely identifies the thread producing the event
   */
  private long threadID;

  /**
   * Uniquely identifies individual event produced by a given thread
   */
  private long sequenceID;

  public EventSequenceID(byte[] membershipID, long threadID, long sequenceID) {
    // convert the byte array of membershipID to a readable string
    Object mbr;
    try {
      mbr = InternalDistributedMember.readEssentialData(new DataInputStream(
          new ByteArrayInputStream(membershipID)));
    }
    catch (Exception e) {
      mbr = Arrays.toString(membershipID); // punt and use the bytes
    }
    this.membershipID = mbr.toString();
    this.threadID = threadID;
    this.sequenceID = sequenceID;
  }

  public String getMembershipID() {
    return this.membershipID;
  }

  public long getThreadID() {
    return this.threadID;
  }

  public long getSequenceID() {
    return this.sequenceID;
  }
  
  public boolean equals(Object obj) {
    if (!(obj instanceof EventSequenceID))
      return false;

    EventSequenceID obj2 = (EventSequenceID)obj;
    return (this.membershipID.equals(obj2.getMembershipID())
        && this.threadID == obj2.getThreadID() 
        && this.sequenceID == obj2.getSequenceID());
  }
  
  public int hashCode() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.membershipID);
    builder.append(this.threadID);
    builder.append(this.sequenceID);
    return builder.toString().hashCode();
  }
  
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("membershipID: " + membershipID);
    builder.append("; threadID: " + threadID);
    builder.append("; sequenceID: " + sequenceID);
    return builder.toString();
  }

}

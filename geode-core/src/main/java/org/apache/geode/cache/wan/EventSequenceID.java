/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.wan;

import java.util.Arrays;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.ByteArrayDataInput;

/**
 * This class wraps 1) DistributedMembershipID 2) ThreadID 3) SequenceID attributes which are used
 * to uniquely identify any Region Operation like create, update, destroy etc. This helps in
 * sequencing the events belonging to a unique producer. As an example, the EventSequenceID can be
 * used to track the events received by <code>AsyncEventListener</code>. If the event has already
 * been seen, <code>AsyncEventListener</code> can choose to ignore it.
 */
public class EventSequenceID {
  /**
   * Uniquely identifies the distributed member VM in which the Event is produced
   */
  private final String membershipID;

  /**
   * Unqiuely identifies the thread producing the event
   */
  private final long threadID;

  /**
   * Uniquely identifies individual event produced by a given thread
   */
  private final long sequenceID;

  public EventSequenceID(byte[] membershipID, long threadID, long sequenceID) {
    // convert the byte array of membershipID to a readable string
    Object mbr;
    try (ByteArrayDataInput byteArrayDataInput = new ByteArrayDataInput(membershipID)) {
      mbr = InternalDistributedMember.readEssentialData(byteArrayDataInput);
    } catch (Exception e) {
      mbr = Arrays.toString(membershipID); // punt and use the bytes
    }
    this.membershipID = mbr.toString();
    this.threadID = threadID;
    this.sequenceID = sequenceID;
  }

  public String getMembershipID() {
    return membershipID;
  }

  public long getThreadID() {
    return threadID;
  }

  public long getSequenceID() {
    return sequenceID;
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof EventSequenceID)) {
      return false;
    }

    EventSequenceID obj2 = (EventSequenceID) obj;
    return (membershipID.equals(obj2.getMembershipID()) && threadID == obj2.getThreadID()
        && sequenceID == obj2.getSequenceID());
  }

  public int hashCode() {
    final String builder = membershipID
        + threadID
        + sequenceID;
    return builder.hashCode();
  }

  public String toString() {
    return "membershipID: " + membershipID
        + "; threadID: " + threadID
        + "; sequenceID: " + sequenceID;
  }

}

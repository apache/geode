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
package org.apache.geode.distributed.internal;

import java.util.Collection;
import java.util.Collections;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * A reply processor optimized for direct ack responses (It skips synchronization,
 * doesn't register with the processor keeper, etc.)
 *
 */
public class DirectReplyProcessor extends ReplyProcessor21 {

  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from
   * a single member of a distributed system.
   * 
   * @param system the DistributedSystem connection
   * @param member the member this processor wants a reply from
   */
  public DirectReplyProcessor(InternalDistributedSystem system,
                          InternalDistributedMember member) {
    this(system, Collections.singleton(member));
  }

  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from
   * a single member of a distributed system.
   * 
   * @param system the DistributedSystem connection
   * @param member the member this processor wants a reply from
   * @param cancelCriterion optional CancelCriterion to use; will use the 
   *  DistributionManager if null
   */
  public DirectReplyProcessor(InternalDistributedSystem system,
                          InternalDistributedMember member,
                          CancelCriterion cancelCriterion) {
    this(system, Collections.singleton(member), cancelCriterion);
  }

  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from
   * a single member of a distributed system.
   * 
   * @param dm the DistributionManager to use for messaging and membership
   * @param member the member this processor wants a reply from
   */
  public DirectReplyProcessor(DM dm, 
                          InternalDistributedMember member) {
    this(dm, Collections.singleton(member));
  }
  
  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from
   * some number of members of a distributed system. Call this method
   * with {@link DistributionManager#getDistributionManagerIds} if
   * you want replies from all DMs including the one hosted in this
   * VM.
   * 
   * @param dm the DistributionManager to use for messaging and membership
   * @param initMembers the Set of members this processor wants replies from
   */
  public DirectReplyProcessor(DM dm,
                          Collection initMembers) {
    this(dm, dm.getSystem(), initMembers, null);
  }
  
  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from
   * some number of members of a distributed system. Call this method
   * with {@link DistributionManager#getDistributionManagerIds} if
   * you want replies from all DMs including the one hosted in this
   * VM.
   * 
   * @param system the DistributedSystem connection
   * @param initMembers the Set of members this processor wants replies from
   */
  public DirectReplyProcessor(InternalDistributedSystem system,
                          Collection initMembers) {
    this(system.getDistributionManager(), system, initMembers, null);
  }
  
  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from
   * some number of members of a distributed system. Call this method
   * with {@link DistributionManager#getDistributionManagerIds} if
   * you want replies from all DMs including the one hosted in this
   * VM.
   * 
   * @param system the DistributedSystem connection
   * @param initMembers the Set of members this processor wants replies from
   * @param cancelCriterion optional CancelCriterion to use; will use the 
   * DistributedSystem's DistributionManager if null
   */
  public DirectReplyProcessor(InternalDistributedSystem system,
                          Collection initMembers,
                          CancelCriterion cancelCriterion) {
    this(system.getDistributionManager(), system, initMembers, cancelCriterion);
  }
  
  /**
   * @param dm
   * @param system
   * @param initMembers
   * @param cancelCriterion
   */
  public DirectReplyProcessor(DM dm, InternalDistributedSystem system,
      Collection initMembers, CancelCriterion cancelCriterion) {
    super(dm, system, initMembers, cancelCriterion, false);
  }
  
  @Override
  public int register() {
    if(processorId != 0) {
      return processorId;
    }
    return super.register();
  }

  @Override
  protected boolean removeMember(InternalDistributedMember m, boolean departed) {
    if(isExpectingDirectReply()) {
      return true;
    } else {
      return super.removeMember(m, departed);
    }
  }

  public boolean isExpectingDirectReply() {
    return processorId == 0;
  }

  @Override
  protected boolean stillWaiting() {
    if(isExpectingDirectReply()) {
      return false;
    } else {
      return super.stillWaiting();
    }
  }
  
  @Override
  protected void checkIfDone() {
    if(processorId != 0) {
      super.checkIfDone();
    }
  }
  
  
  
  

}

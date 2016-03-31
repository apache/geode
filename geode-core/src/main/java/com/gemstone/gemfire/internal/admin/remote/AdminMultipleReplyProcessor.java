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
package com.gemstone.gemfire.internal.admin.remote;

import java.util.Collection;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;


/**
 * TODO prpersist. This code really needs to be merged with the 
 * AdminReplyProcessor. However, we're getting close to the release
 * and I don't want to mess with all of the admin code right now. We
 * need this class to handle failures from admin messages that expect
 * replies from multiple members.
 *
 */
public class AdminMultipleReplyProcessor extends ReplyProcessor21 {

  public AdminMultipleReplyProcessor(DM dm, Collection initMembers) {
    super(dm, initMembers);
  }

  public AdminMultipleReplyProcessor(DM dm, InternalDistributedMember member) {
    super(dm, member);
  }

  public AdminMultipleReplyProcessor(DM dm, InternalDistributedSystem system,
      Collection initMembers, CancelCriterion cancelCriterion, boolean register) {
    super(dm, system, initMembers, cancelCriterion, register);
  }

  public AdminMultipleReplyProcessor(InternalDistributedSystem system,
      Collection initMembers, CancelCriterion cancelCriterion) {
    super(system, initMembers, cancelCriterion);
  }

  public AdminMultipleReplyProcessor(InternalDistributedSystem system,
      Collection initMembers) {
    super(system, initMembers);
  }

  public AdminMultipleReplyProcessor(InternalDistributedSystem system,
      InternalDistributedMember member, CancelCriterion cancelCriterion) {
    super(system, member, cancelCriterion);
  }

  public AdminMultipleReplyProcessor(InternalDistributedSystem system,
      InternalDistributedMember member) {
    super(system, member);
  }

  @Override
  protected void process(DistributionMessage msg, boolean warn) {
    if (msg instanceof AdminFailureResponse) {
      Exception ex = ((AdminFailureResponse)msg).getCause();
      if (ex != null) {
        ReplyException rex = new ReplyException(ex);
        rex.setSenderIfNull(msg.getSender());
        processException(msg, rex);
      }
    }
    super.process(msg, warn);
  }

  
  

}

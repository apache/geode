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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * Used to force a current member of the distributed system to disconnect.
 * @deprecated starting with Ganges, use MembeshipManager.requestMemberRemoval instead
 * @author Darrel
 * @since 4.2.1
 */
@Deprecated
public class ForceDisconnectOperation  {
//  protected DM dm;
//  protected ReplyProcessor21 processor;
//  protected ForceDisconnectMessage msg;

  public ForceDisconnectOperation(DM dm, InternalDistributedMember destination) {
    throw new UnsupportedOperationException("ForceDisconnectOperation is no longer supported - use MembershipManager.requestMemberRemoval instead");
//    this.dm = dm;
//    this.processor = new ReplyProcessor21(dm, destination);
//    this.msg = new ForceDisconnectMessage();
//    msg.setRecipient(destination);
//    msg.setProcessorId(this.processor.getProcessorId());
  }

}

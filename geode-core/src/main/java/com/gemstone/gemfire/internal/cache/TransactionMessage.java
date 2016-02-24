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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * Messages that carry transaction information will implement this interface
 * 
 */
public interface TransactionMessage {
  /**
   * Returns the transaction id on the sender this message belongs to
   * @return the unique transaction id per sender
   */
  public int getTXUniqId();
  
  /**
   * Returns the member id of the originating member for this transaction
   * @return memberId of tx originator
   */
  public InternalDistributedMember getMemberToMasqueradeAs();
  

  /**
   * We do not want all the messages to start a remote transaction. e.g. SizeMessage.
   * If this method returns true, a transaction will be created if none exists
   * @return true if this message can start a remote transaction, false otherwise
   */
  public boolean canStartRemoteTransaction();
  
  /**
   * @see DistributionMessage#getSender()
   * @return the sender of this message
   */
  public InternalDistributedMember getSender();
  
  /**
   * Gets the MemberId of the originating member of the transaction that this message is a part of.
   * @return the memberId of the client that started this transaction, or null if not from client
   */
  public InternalDistributedMember getTXOriginatorClient();
  
  
  /**
   * Messages that do not want to participate in transactions return false.  
   * e.g. <code>ManageBucketMessage</code>
   */
  public boolean canParticipateInTransaction();
  
  /**
   * Messages that participate in distributed transaction return true,
   * others return false
   */
  public boolean isTransactionDistributed();
}

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author sbawaska
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
}

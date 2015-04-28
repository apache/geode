/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.*;

/**
 * Represents one or more distributed operations that can be reliably distributed.
 * This interface allows the data to be queued and checked for reliable distribution.
 * 
 * @author Darrel Schneider
 * @since 5.0
 */
public interface ReliableDistributionData  {
//  /**
//   * Returns a set of the recipients that this data was sent to successfully.
//   * @param processor the reply processor used for responses to this data.
//   */
//  public Set getSuccessfulRecipients(ReliableReplyProcessor21 processor);
  /**
   * Returns the number of logical operations this data contains.
   */
  public int getOperationCount();
  /**
   * Returns a list of QueuedOperation instances one for each logical
   * operation done by this data instance.
   */
  public List getOperations();
}

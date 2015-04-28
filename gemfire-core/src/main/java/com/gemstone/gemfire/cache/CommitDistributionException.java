/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

import java.util.*;

/**
 * Indicates that an attempt to notify required participants of a transaction 
 * involving one or more regions that are configured with {@link 
 * MembershipAttributes} may have failed. The commit itself was completed but
 * one or more regions affected by the transaction have one or more required
 * roles that were not successfully notified. Failure may be caused by 
 * departure of one or more required roles while sending the operation to
 * them. This exception will contain one {@link RegionDistributionException}
 * for every region that had a reliability failure. Details of the failed
 * {@link MembershipAttributes#getRequiredRoles required roles} are provided
 *  in each RegionDistributionException.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class CommitDistributionException extends TransactionException {
  private static final long serialVersionUID = -3517820638706581823L;
  /** 
   * The RegionDistributionExceptions for every region that had a reliability
   * failure during distribution of the operation.
   */
  private Set regionDistributionExceptions = Collections.EMPTY_SET;
  
  /** 
   * Constructs a <code>CommitDistributionException</code> with a message.
   *
   * @param s the String message
   */
  public CommitDistributionException(String s) {
    super(s);
  }
  
  /** 
   * Constructs a <code>CommitDistributionException</code> with a message and
   * a cause.
   *
   * @param s the String message
   * @param regionDistributionExceptions set of RegionDistributionExceptions
   * for each region that had a reliability failure
   */
  public CommitDistributionException(String s, Set regionDistributionExceptions) {
    super(s);
    this.regionDistributionExceptions = regionDistributionExceptions;
    if (this.regionDistributionExceptions == null) {
      this.regionDistributionExceptions = Collections.EMPTY_SET;
    }
  }
  
  /** 
   * Returns set of RegionDistributionExceptions for each region that had a 
   * reliability failure during distribution of the operation.
   *
   * @return set of RegionDistributionExceptions for each region that had a 
   * reliability failure during distribution of the operation
   */
  public Set getRegionDistributionExceptions() {
    return this.regionDistributionExceptions;
  }
  
}


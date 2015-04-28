/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

/***
 *Data class used for sending back subscription-queue-size for a client or a cq
 * @author bansods
 *
 */
public class SubscriptionQueueSizeResult extends MemberResult {

  private static final long serialVersionUID = 1L;
  private long subscriptionQueueSize;

  public SubscriptionQueueSizeResult(String memberNameOrId) {
    super(memberNameOrId);
  }

  public long getSubscriptionQueueSize() {
    return subscriptionQueueSize;
  }

  public void setSubscriptionQueueSize(long queueSize) {
    this.subscriptionQueueSize = queueSize;
    super.isSuccessful = true;
    super.opPossible = true;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(super.toString());
    sb.append("\nsubscription-queue-size : ");
    sb.append(this.subscriptionQueueSize);
    return sb.toString();
  }

}

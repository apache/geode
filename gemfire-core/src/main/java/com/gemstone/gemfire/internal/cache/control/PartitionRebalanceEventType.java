/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.control;


/**
 * Defines the type of a {@link PartitionRebalanceEvent}.
 * 
 * @since 6.0
 */
public enum PartitionRebalanceEventType {
  REBALANCE_STARTING,
  REBALANCE_STARTED,
  PARTITIONEDREGION_STARTING,
  PARTITIONEDREGION_STARTED,
  BUCKET_CREATED,
  BUCKET_DESTROYED,
  BUCKET_TRANSFERRED,
  PRIMARY_TRANSFERRED,
  PARTITIONEDREGION_CANCELLED,
  PARTITIONEDREGION_FINISHED,
  REBALANCE_CANCELLED,
  REBALANCE_FINISHED
}

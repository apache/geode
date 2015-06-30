/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.modules.session.common;

/**
 * Used to define cache properties
 */
public enum CacheProperty {

  ENABLE_DEBUG_LISTENER(Boolean.class),

  ENABLE_GATEWAY_REPLICATION(Boolean.class),

  ENABLE_GATEWAY_DELTA_REPLICATION(Boolean.class),

  ENABLE_LOCAL_CACHE(Boolean.class),

  REGION_NAME(String.class),

  REGION_ATTRIBUTES_ID(String.class),

  STATISTICS_NAME(String.class),

  /**
   * This parameter can take the following values which match the respective
   * attribute container classes
   * <p/>
   * delta_queued     : QueuedDeltaSessionAttributes delta_immediate  :
   * DeltaSessionAttributes immediate        : ImmediateSessionAttributes queued
   * : QueuedSessionAttributes
   */
  SESSION_DELTA_POLICY(String.class),

  /**
   * This parameter can take the following values:
   * <p/>
   * set (default) set_and_get
   */
  REPLICATION_TRIGGER(String.class);

  Class clazz;

  CacheProperty(Class clazz) {
    this.clazz = clazz;
  }

  public Class getClazz() {
    return clazz;
  }
}

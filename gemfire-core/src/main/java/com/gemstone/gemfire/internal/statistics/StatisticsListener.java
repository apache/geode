/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public interface StatisticsListener {
  
  public void handleNotification(StatisticsNotification notification);
  
}
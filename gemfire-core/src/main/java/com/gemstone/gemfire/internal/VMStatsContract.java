/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal;

/**
 * Describes the contract a VMStats implementation must implement.
 * <p> I named this VMStatsContract because an implementation named
 *     VMStats already exists and I didn't want to rename it because
 *     of the svn merge issues.
 * @see VMStatsContractFactory
 */
public interface VMStatsContract {
  /**
   * Called by sampler when it wants the VMStats statistics values to be
   * refetched from the system.
   */
  public void refresh();
  /**
   * Called by sampler when it wants the VMStats to go away.
   */
  public void close();
}

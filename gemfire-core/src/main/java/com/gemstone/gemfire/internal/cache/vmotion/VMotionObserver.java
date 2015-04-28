/*
 * =========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved. This product is
 * protected by U.S. and international copyright and intellectual property laws.
 * Pivotal products are covered by one or more patents listed at
 * http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache.vmotion;

import java.util.Set;

import com.gemstone.gemfire.internal.cache.LocalRegion;

/**
 * This interface is used by testing/debugging code to be notified of different
 * events.
 */

public interface VMotionObserver {

  /**
   * This callback is called just before CQ registration on the server
   */
  public void vMotionBeforeCQRegistration();

  /**
   * This callback is called just before register Interest on the server
   */
  public void vMotionBeforeRegisterInterest();

  /**
   * This callback is called before a request for GII is sent.
   */
  public void vMotionDuringGII(Set recipientSet, LocalRegion region);
}

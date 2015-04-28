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
 * This class provides 'do-nothing' implementations of all of the methods of
 * interface VMotionObserver.
 */

public class VMotionObserverAdapter implements VMotionObserver {

  /**
   * This callback is called just before CQ registration on the server
   */

  public void vMotionBeforeCQRegistration() {
  }

  /**
   * This callback is called just before register Interset on the server
   */

  public void vMotionBeforeRegisterInterest() {
  }

  /**
   * This callback is called before a request for GII is sent.
   */
  public void vMotionDuringGII(Set recipientSet, LocalRegion region){
  }
}

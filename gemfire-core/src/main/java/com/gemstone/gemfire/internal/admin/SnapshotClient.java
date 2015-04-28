/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin;

import java.util.List;

/**
 * This interface is implemented by clients of a {@link
 * CacheCollector} who want to be notified of updated snapshot views.
 */
public interface SnapshotClient {

  /**
   * Called when a snapshot view is ready for use
   *
   * @param snapshot 
   *        a set of {@link CacheSnapshot}s.
   * @param responders
   *        All of the members ({@link GemFireVM}s) that have
   *        responded to the snapshot request. 
   */
  public void updateSnapshot(CacheSnapshot snapshot, List responders);
}

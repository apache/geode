/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.Statistics;

/**
 * Abstracts the process statistics that are common on all platforms.
 * This is necessary for monitoring the health of GemFire components.
 *
 * @author David Whitlock
 *
 * @since 3.5
 * */
public abstract class ProcessStats {

  /** The underlying statistics */
  private final Statistics stats;

  /**
   * Creates a new <code>ProcessStats</code> that wraps the given
   * <code>Statistics</code>. 
   */
  ProcessStats(Statistics stats) {
    this.stats = stats;
  }

  /**
   * Closes these process stats
   *
   * @see Statistics#close
   */
  public final void close() {
    this.stats.close();
  }

  public final Statistics getStatistics() {
    return this.stats;
  }
  
  /**
   * Returns the size of this process (resident set on UNIX or working
   * set on Windows) in megabytes
   */
  public abstract long getProcessSize();

}

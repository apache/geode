/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import java.io.File;

/**
 * Defines the contract enabling the {@link StatArchiveHandler} to retrieve
 * configuration details (some of which may change at runtime).
 * <p/>
 * Implemented by {@link com.gemstone.gemfire.internal.HostStatSampler}.

 * @author Kirk Lund
 * @since 7.0
 * @see com.gemstone.gemfire.distributed.internal.RuntimeDistributionConfigImpl
 */
public interface StatArchiveHandlerConfig {

  /**
   * Gets the name of the archive file.
   */
  public File getArchiveFileName();
  
  /**
   * Gets the archive size limit in bytes.
   */
  public long getArchiveFileSizeLimit();
  
  /**
   * Gets the archive disk space limit in bytes.
   */
  public long getArchiveDiskSpaceLimit();

  /**
   * Returns a unique id for the sampler's system.
   */
  public long getSystemId();

  /**
   * Returns the time this sampler's system was started.
   */
  public long getSystemStartTime();

  /**
   * Returns the path to this sampler's system directory; if it has one.
   */
  public String getSystemDirectoryPath();
  
  /**
   * Returns a description of the product that the stats are on
   */
  public String getProductDescription();
}

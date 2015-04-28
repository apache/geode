/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.compression.Compressor;

/**
 * Provides important contextual information that allows a {@link RegionEntry} to manage its state.
 * @author rholmes
 * @since 8.0
 */
public interface RegionEntryContext extends HasCachePerfStats {
  public static final String DEFAULT_COMPRESSION_PROVIDER="com.gemstone.gemfire.compression.SnappyCompressor";
  
  /**
   * Returns the compressor to be used by this region entry when storing the
   * entry value.
   * 
   * @return null if no compressor is assigned or available for the entry.
   */
  public Compressor getCompressor();
}

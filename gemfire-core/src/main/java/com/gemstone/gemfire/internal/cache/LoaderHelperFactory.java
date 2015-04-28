/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * The LoaderHelperFactory inspiration came 
 * from a need to allow Partitioned Regions to generate a LoaderHelper that was
 * outside the context of the Region the loader invoked from.
 * @since 5.0
 * @author Mitch Thomas
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.LoaderHelper;

/**
 * The LoaderHelperFactory creates a LoaderHelper class which is used by a 
 * {@link com.gemstone.gemfire.cache.CacheLoader}.
 *  
 * @since 5.0
 * @author Mitch Thomas
 */
public interface LoaderHelperFactory
{
  public LoaderHelper createLoaderHelper(Object key, Object callbackArgument, boolean netSearchAllowed, boolean netLoadAllowed,
      SearchLoadAndWriteProcessor searcher);
  
}

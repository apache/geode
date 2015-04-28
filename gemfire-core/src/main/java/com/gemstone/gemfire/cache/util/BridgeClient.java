/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import java.util.Properties;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;

/**
 * This class combines the BridgeWriter and BridgeLoader functionality into one
 * class, sharing BridgeServer connections, optimizing the number
 * of connections required when using a BridgeWriter and BridgeLoader separately.
 * <p>
 * When a BridgeClient is declared in cache.xml
 * it can be installed as either a cache-loader or as a cache-writer
 * and it will automatically be installed as both the loader and writer
 * for that region.
 * This allows a single instance to be declared in XML and used as both
 * the cache-loader and cache-writer thus reducing the number of connections to the server.
 * 
 * <p>
 * For configuration details please see the {@link com.gemstone.gemfire.cache.util.BridgeWriter} and 
 * the {@link com.gemstone.gemfire.cache.util.BridgeLoader}.
 * 
 * @author Mitch Thomas
 * @since 5.0.1
 * @see com.gemstone.gemfire.cache.util.BridgeLoader
 * @see com.gemstone.gemfire.cache.util.BridgeWriter
 * @see com.gemstone.gemfire.cache.util.BridgeServer
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */
@Deprecated
public class BridgeClient extends BridgeWriter implements CacheLoader
{

  private final BridgeLoader loader = new BridgeLoader();
  
  public Object load(LoaderHelper helper) throws CacheLoaderException
  {
    return this.loader.load(helper);
  }

  /**
   * Ensure that the BridgeLoader class gets loaded.
   * 
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    BridgeLoader.loadEmergencyClasses();
  }
  
  @Override
  public void close()
  {
    try {
      this.loader.close();
    } finally {
      super.close();
    }
  }

  /**
   * Returns true if this <code>BridgeClient</code> has been closed.
   */
  @Override
  public boolean isClosed() {
    return super.isClosed();
  }

  /**
   * Notify the BridgeClient that the given Region will begin delivering events to this BridgeClient.
   * This method effects the behavior of {@link #close()} and allows a single instance of BridgeClient 
   * to be safely shared with multiple Regions.
   *
   * This is called internally when the BridgeClient is added to a Region
   * via {@link AttributesFactory#setCacheWriter(CacheWriter)}}
   *
   * @param r
   *          the Region which will begin use this BridgeWriter.
   *
   * @see #detach(Region)
   * @see #close()
   */
  @Override
  public void attach(Region r)
  {
    try {
      this.loader.attach(r);
    } finally {
      super.attach(r);
    }
  }

  /**
   * Notify the BridgeClient that the given region is no longer relevant.
   * This method is used internally during Region {@link Region#destroyRegion() destruction} and {@link Region#close() closure}.
   * This method effects the behavor of {@link #close()} and allows a single instance of BridgeClient 
   * to be safely shared with multiple Regions.
   *
   * @see #attach(Region)
   * @see #close()
   * @param r
   *          the Region which will no longer use this BridgeWriter
   */
  @Override
  public void detach(Region r)
  {
    try {
      this.loader.detach(r);
    } finally {
      super.detach(r);
    }
  }

  @Override
  public void init(BridgeWriter bridgeWriter)
  {
    super.init(bridgeWriter);
    this.loader.init(this);
  }

  @Override
  public void init(Properties p)
  {
    super.init(p);
    this.loader.init(this);
  }

  /**
   * Return the internally maintained BridgeLoader 
   * @return the internal BridgeLoader
   */
  public BridgeLoader getBridgeLoader() {
    return this.loader;
  }
  
  /**
   * Returns a string description of the BridgeClient
   */
  @Override
  public String toString()
  {
    return "BridgeClient#" + System.identityHashCode(this) +  " connected to " + this.proxy;
  }
}

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import java.io.*;
//import java.util.Set;
import com.gemstone.org.jgroups.util.StringId;

/** Provides a set of APIs to help the
 * implementation of the <code>CacheLoader</code> load method. An instance of <code>LoaderHelper</code>
 * is only valid within the {@link CacheLoader#load(LoaderHelper) load}
 * method.
 *
 * @author Sudhir Menon
 *
 * @see CacheLoader#load(LoaderHelper) load
 * @since 2.0
 */
public class LoaderHelperImpl implements LoaderHelper
{
  /** The message issued when the user attempts to netSearch on a
   * LOCAL Region.  It is public for testing purposes only. */
  public static final StringId NET_SEARCH_LOCAL = LocalizedStrings.LoaderHelperImpl_CANNOT_NETSEARCH_FOR_A_SCOPELOCAL_OBJECT;

  private final Object key;
  private final boolean netSearchAllowed;
  private final boolean netLoadAllowed;
  private final Region region;
  private final Object aCallbackArgument;
  private SearchLoadAndWriteProcessor searcher = null;


  public LoaderHelperImpl(Region region, Object key, Object aCallbackArgument,boolean netSearchAllowed,
                          SearchLoadAndWriteProcessor searcher) {
    this.region=region;
    this.key=key;
    this.aCallbackArgument = aCallbackArgument;
    this.netSearchAllowed=netSearchAllowed;
    this.netLoadAllowed = true;
    this.searcher = searcher;
  }

  public LoaderHelperImpl(Region region, Object key, Object aCallbackArgument,boolean netSearchAllowed, boolean netLoadAllowed,
                          SearchLoadAndWriteProcessor searcher) {
    this.region=region;
    this.key=key;
    this.aCallbackArgument = aCallbackArgument;
    this.netSearchAllowed=netSearchAllowed;
    this.netLoadAllowed = netLoadAllowed;
    this.searcher = searcher;
  }



  /** Searchs other caches for the value to be loaded. If the cache is part of
   * a distributed caching system, <code>netSearch</code> will try to locate the requested
   * value in any other cache within the system.
   * If the search is successful, a reference to a local copy of the
   * value is returned. If there is no value for this entry present in the
   * system, and doNetLoad is true, GemFire looks for and invokes
   * <code>CacheLoaders</code> in other nodes in the system.  The net load
   * will invoke one loader at a time until a loader either returns a
   * non-null value, or throws an exception.  If the object
   * is not found <code>null</code> is returned.
   *
   * @param doNetLoad if true, and there is no valid value found
   * for this entry in the system, then look for and invoke loaders
   * on other nodes.
   * @return the requested value or null if not found
   * @throws TimeoutException if the netSearch times out before getting a response from another cache
   */
  public Object netSearch(final boolean doNetLoad) throws CacheLoaderException, TimeoutException{

    if (this.region.getAttributes().getScope().isLocal()) {
      throw new CacheLoaderException(NET_SEARCH_LOCAL.toLocalizedString());
    }

    boolean removeSearcher = false;
    if (searcher == null) {
      searcher = SearchLoadAndWriteProcessor.getProcessor();
      removeSearcher = true;
    }

    try {
      if (removeSearcher) {
        searcher.initialize((LocalRegion)this.region,this.key, this.aCallbackArgument);
      }
      Object obj = null;

      if (this.netSearchAllowed) {
        obj = searcher.doNetSearch();
        if (searcher.resultIsSerialized()) {
          obj = EntryEventImpl.deserialize((byte[])obj);
        }
      }
      if (doNetLoad && obj == null && this.netLoadAllowed) {
        obj = searcher.doNetLoad();
        if (searcher.resultIsSerialized()) {
          obj = EntryEventImpl.deserialize((byte[])obj);
        }
      }
      // Note it is possible for netsearch to not be allowed
      // but netload to be allowed.
      // For example on replicated regions we say don't bother netsearching
      // but we do need to check for netLoaders
      return obj;
    }
    finally {
      if (removeSearcher) {
        searcher.remove();
      }
    }
  }

  /**
   * Returns the key for the value being loaded.
   *
   * @return The name Object for the object being loaded.
   * @see CacheLoader#load(LoaderHelper) load
   */
  public Object getKey(){
    return this.key;
  }

  /**
   * Returns the region to which the entry belongs.
   *
   * @return The name of the region for the object being loaded.
   * @see CacheLoader#load(LoaderHelper) load
   */
  public Region getRegion(){
    return region;
  }

  /** Return the argument object for the load method that was passed in from
   * application code. This object is passed in as <i>aLoaderArgument</i> in
   * {@link Region#get(Object, Object) get}.
   * @return the argument or null if one was not supplied
   */
  public Object getArgument(){
    return aCallbackArgument;
  }

  @Override
  public String toString() {
    return "LoaderHelper region: " + getRegion()
      + " key: " + getKey()
      + " argument: " + getArgument();
  }
}


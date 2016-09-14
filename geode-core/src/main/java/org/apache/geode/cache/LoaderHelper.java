/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.cache;

/** Provides a set of APIs to help the
 * implementation of the <code>CacheLoader</code> load method. An instance of <code>LoaderHelper</code>
 * is only valid within the {@link CacheLoader#load(LoaderHelper) load}
 * method.
 *
 *
 * @see CacheLoader#load(LoaderHelper) load
 * @since GemFire 2.0
 */
public interface LoaderHelper<K,V>
{
  /** Searches other caches for the value to be loaded. If the cache
   * is part of a distributed caching system, <code>netSearch</code>
   * will try to locate the requested value in any other cache within
   * the system.  If the search is successful, a reference to a local
   * copy of the value is returned. If there is no value for this
   * entry present in the system, and doNetLoad is true, GemFire looks
   * for and invokes <code>CacheLoaders</code> in other nodes in the
   * system.  The net load will invoke one loader at a time until a
   * loader either returns a non-null value or throws an exception.
   * If the object is not found, <code>null</code> is returned.
   *
   * @param doNetLoad 
   *        if true, and there is no valid value found for this entry
   *        in the system, then look for and invoke loaders on other
   *        nodes
   *
   * @return the requested value or null if not found
   *
   * @throws TimeoutException if the netSearch times out before
   *         getting a response from another cache
   * @throws CacheLoaderException
   *         If <code>netSearch</code> is attempted on a {@linkplain
   *         org.apache.geode.cache.Scope#LOCAL local} region. 
   */
  public V netSearch(boolean doNetLoad)
    throws CacheLoaderException, TimeoutException;

  /**
   * Returns the key for the value being loaded.
   *
   * @return the name Object for the object being loaded
   * @see CacheLoader#load(LoaderHelper) load
   */
  public K getKey();

  /**
   * Returns the region to which the entry belongs.
   *
   * @return the name of the region for the object being loaded
   * @see CacheLoader#load(LoaderHelper) load
   */
  public Region<K,V> getRegion();

  /** Return the argument object for the load method that was passed in from
   * application code. This object is passed in as <i>aLoaderArgument</i> in
   * {@link Region#get(Object, Object) get}.
   * @return the argument or null if one was not supplied
   */
  public Object getArgument();
}

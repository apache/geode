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

/**
 * Allows data from outside of the VM to be placed into a region.  
 * When {@link org.apache.geode.cache.Region#get(Object)} is called for a region
 * entry that has a <code>null</code> value, the 
 * {@link CacheLoader#load load} method of the
 * region's cache loader is invoked.  The <code>load</code> method
 * creates the value for the desired key by performing an operation such
 * as a database query.  The <code>load</code> may also perform a
 * {@linkplain LoaderHelper#netSearch net search}
 * that will look for the value in a cache instance hosted by
 * another member of the distributed system.</p>
 *
 *
 *
 * @see AttributesFactory#setCacheLoader
 * @see RegionAttributes#getCacheLoader
 * @see AttributesMutator#setCacheLoader
 * @since GemFire 2.0
 */
public interface CacheLoader<K,V> extends CacheCallback {
  /**
   * Loads a value. Application writers should implement this
   * method to customize the loading of a value. This method is called
   * by the caching service when the requested value is not in the cache.
   * Any exception (including an unchecked exception) thrown by this
   * method is propagated back to and thrown by the invocation of
   * {@link Region#get(Object, Object)} that triggered this load.
   * <p>
   *
   * @param helper a LoaderHelper object that is passed in from cache service
   *   and provides access to the key, region, argument, and <code>netSearch</code>.
   * @return the value supplied for this key, or null if no value can be
   *    supplied.  A local loader will always be invoked if one exists.
   *    Otherwise one remote loader is invoked.
   *    Returning <code>null</code> causes
   *    {@link Region#get(Object, Object)} to return <code>null</code>.
   * @throws CacheLoaderException, if an error occurs. This exception or any
   * other exception thrown by this method will be propagated back to the
   * application from the get method.
   *
   * @see   Region#get(Object, Object) Region.get
   */
  public V load(LoaderHelper<K,V> helper)
  throws CacheLoaderException;
}

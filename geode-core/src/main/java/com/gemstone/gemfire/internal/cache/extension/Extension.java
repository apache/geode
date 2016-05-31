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

package com.gemstone.gemfire.internal.cache.extension;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

/**
 * Interface used for objects wishing to extend and {@link Extensible} object.
 * 
 *
 * @since GemFire 8.1
 */
public interface Extension<T> {

  /**
   * Get {@link XmlGenerator} capable of serializing this object's
   * configuration.
   * 
   * @return {@link XmlGenerator} for this object's configuration.
   * @since GemFire 8.1
   */
  XmlGenerator<T> getXmlGenerator();

  /**
   * Called by {@link CacheXml} objects that are {@link Extensible} before
   * creating this extension.
   *
   * @param source
   *          source object this extension is currently attached to.
   * @param cache
   *          target object to attach any created extensions to.
   * @since Geode 1.0.0
   */
  void beforeCreate(Extensible<T> source, Cache cache);

  /**
   * Called by {@link CacheXml} objects that are {@link Extensible} to create
   * this extension.
   *
   * @param source
   *          source object this extension is currently attached to.
   * @param target
   *          target object to attach any created extensions to.
   * @since GemFire 8.1
   */
  void onCreate(Extensible<T> source, Extensible<T> target);

}

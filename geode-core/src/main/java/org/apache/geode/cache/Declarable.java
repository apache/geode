/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache;

import java.util.Properties;


/**
 * An object that can be described in a declarative caching XML file.
 *
 * <p>
 *
 * Any user-defined object in the declarative caching xml file should implement this interface in
 * order to be constructed.
 *
 * <p>
 *
 * For example, the user can declare a <code>CacheLoader</code> in a declarative XML file as
 * follows:
 *
 * <pre>
 *        &lt;cache-loader&gt;
 *          &lt;class-name&gt;com.company.app.DBLoader&lt;/class-name&gt;
 *          &lt;parameter name="URL"&gt;
 *            &lt;string&gt;jdbc://12.34.56.78/mydb&lt;/string&gt;
 *          &lt;/parameter&gt;
 *        &lt;/cache-loader&gt;
 * </pre>
 *
 * <p>
 *
 * In this case, <code>com.company.app.DBLoader</code> must implement both the {@link CacheLoader}
 * and <code>Declarable</code> interfaces. The cache service will construct a
 * <code>com.company.app.DBLoader</code> object by invoking the loader's zero-argument constructor
 * and then calling the {@link #init} method to pass in the parameters.
 *
 * <P>
 *
 * See <a href="package-summary.html#declarative">package introduction</a>.
 *
 *
 *
 * @since GemFire 2.0
 */
public interface Declarable {

  /**
   * Initializes a user-defined object using the given properties. Note that any uncaught exception
   * thrown by this method will cause the <code>Cache</code> initialization to fail.
   *
   * @param props Contains the parameters declared in the declarative xml file.
   *
   * @throws IllegalArgumentException If one of the configuration options in <code>props</code> is
   *         illegal or malformed.
   * @deprecated as of Geode 1.5 implement initialize instead.
   */
  default void init(Properties props) {};

  /**
   * Initializes a user-defined object, owned by the given cache, using the given properties.
   * Note that any uncaught exception
   * thrown by this method will cause the <code>Cache</code> initialization to fail.
   * Note that if this method is implemented then the deprecated init method should not be
   * implemented.
   * The product will call both methods assuming that only one will have a non-default
   * implementation.
   *
   * @param cache the cache that owns this declarable
   * @param properties Contains the parameters declared in the declarative xml file.
   *
   * @throws IllegalArgumentException should be thrown if properties contains something unexpected.
   *
   * @since Geode 1.5
   */
  default void initialize(Cache cache, Properties properties) {};
}

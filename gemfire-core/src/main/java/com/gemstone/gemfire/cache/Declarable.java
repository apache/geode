/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

import java.util.Properties;

/** 
 * An object that can be described in a declarative caching XML file.
 *
 * <p>
 *
 * Any user-defined object in the declarative caching xml file
 * should implement this interface in order to be constructed.
 *
 * <p>
 *
 * For example, the user can declare a <code>CacheLoader</code> in a declarative
 * XML file as follows:
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
 * In this case, <code>com.company.app.DBLoader</code> must 
 * implement both the {@link CacheLoader} and <code>Declarable</code>
 * interfaces. The cache service will construct a
 * <code>com.company.app.DBLoader</code> object by invoking the loader's
 * zero-argument constructor and then calling the {@link #init} method
 * to pass in the parameters.
 *
 * <P>
 *
 * See <a href="package-summary.html#declarative">package introduction</a>.
 *
 * @author Darrel Schneider
 *
 * 
 * @since 2.0
 */
public interface Declarable {

  /**
   * Initializes a user-defined object using the given properties.
   * Note that any uncaught exception thrown by this method will cause
   * the <code>Cache</code> initialization to fail.
   *
   * @param props 
   *        Contains the parameters declared in the declarative xml
   *        file.
   *
   * @throws IllegalArgumentException
   *         If one of the configuration options in <code>props</code>
   *         is illegal or malformed.
   */
  public void init(Properties props);
}

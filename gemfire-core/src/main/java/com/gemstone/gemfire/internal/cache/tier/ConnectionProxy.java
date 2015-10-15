/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier;

import com.gemstone.gemfire.internal.Version;

/**
 * Provides the version of the client.
 *
 * @author Sudhir Menon
 * @since 2.0.2
 */
@SuppressWarnings("deprecation")
public interface ConnectionProxy {

/**
   * The GFE version of the client.
   * @since 5.7
   */
  public static final Version VERSION = Version.CURRENT.getGemFireVersion();
}

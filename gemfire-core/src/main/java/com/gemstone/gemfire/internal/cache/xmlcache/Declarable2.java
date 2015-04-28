/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import com.gemstone.gemfire.cache.Declarable;
import java.util.Properties;

/**
 * An extension of {@link Declarable} that allows a
 * <code>Declarable</code> to provides its configuration (as a {@link
 * Properties}).  This allows us to convert a <code>Declarable</code>
 * into XML.
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public interface Declarable2 extends Declarable {

  /**
   * Returns the current configuration of this {@link Declarable}
   */
  public Properties getConfig();

}

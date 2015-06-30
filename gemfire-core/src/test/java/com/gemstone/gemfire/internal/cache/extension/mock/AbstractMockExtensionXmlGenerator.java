/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension.mock;

import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

/**
 * Base class for Mock Extension XML Generators.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public abstract class AbstractMockExtensionXmlGenerator<T> implements XmlGenerator<T> {
  public final AbstractMockExtension<T> extension;

  public AbstractMockExtensionXmlGenerator(AbstractMockExtension<T> extension) {
    this.extension = extension;
  }

  @Override
  public String getNamspaceUri() {
    return MockExtensionXmlParser.NAMESPACE;
  }

}

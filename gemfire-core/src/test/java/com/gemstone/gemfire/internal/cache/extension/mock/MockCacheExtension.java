/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension.mock;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.cache.extension.Extensible;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

/**
 * Mock Extension for {@link Cache}.
 * 
 * <dl>
 * <dt>Uses</dt>
 * <dd>{@link ClusterConfigurationDUnitTest}</dd>
 * <dd>{@link CacheXml81DUnitTest}</dd>
 * </dl>
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public final class MockCacheExtension extends AbstractMockExtension<Cache> {
  public MockCacheExtension(final String value) {
    super(value);
  }

  @Override
  public void onCreate(Extensible<Cache> source, Extensible<Cache> target) {
    super.onCreate(source, target);
    target.getExtensionPoint().addExtension(MockCacheExtension.class, this);
  }

  @Override
  public XmlGenerator<Cache> getXmlGenerator() {
    super.getXmlGenerator();
    return new MockCacheExtensionXmlGenerator(this);
  }
}

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension.mock;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.extension.Extensible;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

/**
 * Mock Extension for {@link Region}.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public final class MockRegionExtension extends AbstractMockExtension<Region<?, ?>> {
  public MockRegionExtension(final String value) {
    super(value);
  }

  @Override
  public void onCreate(Extensible<Region<?, ?>> source, Extensible<Region<?, ?>> target) {
    super.onCreate(source, target);
    target.getExtensionPoint().addExtension(this);
  }

  @Override
  public XmlGenerator<Region<?, ?>> getXmlGenerator() {
    super.getXmlGenerator();
    return new MockRegionExtensionXmlGenerator(this);
  }
}

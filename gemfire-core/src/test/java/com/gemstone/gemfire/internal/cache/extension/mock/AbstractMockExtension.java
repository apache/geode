/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension.mock;

import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.internal.cache.extension.Extensible;
import com.gemstone.gemfire.internal.cache.extension.Extension;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

/**
 * Base class for Mock Extension.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public abstract class AbstractMockExtension<T> implements Extension<T> {
  public AtomicInteger onCreateCounter = new AtomicInteger();
  public AtomicInteger getXmlGeneratorCounter = new AtomicInteger();

  public String value;

  public AbstractMockExtension(final String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
  
  public void setValue(final String value) {
    this.value = value;
  }

  @Override
  public void onCreate(Extensible<T> source, Extensible<T> target) {
    onCreateCounter.incrementAndGet();
  }

  @Override
  public XmlGenerator<T> getXmlGenerator() {
    getXmlGeneratorCounter.incrementAndGet();
    return null;
  }
}

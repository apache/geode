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

package org.apache.geode.internal.cache.extension.mock;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;

/**
 * Base class for Mock Extension.
 *
 *
 * @since GemFire 8.1
 */
public abstract class AbstractMockExtension<T> implements Extension<T> {
  public AtomicInteger beforeCreateCounter = new AtomicInteger();
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
  public void beforeCreate(Extensible<T> source, Cache cache) {
    beforeCreateCounter.incrementAndGet();
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

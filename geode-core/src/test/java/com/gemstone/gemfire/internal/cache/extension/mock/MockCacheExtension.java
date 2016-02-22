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

package com.gemstone.gemfire.internal.cache.extension.mock;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.cache.extension.Extensible;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

/**
 * Mock Extension for {@link Cache}.
 * 
 * <dl>
 * <dt>Uses</dt>
 * <dd>com.gemstone.gemfire.management.internal.configuration.ClusterConfigurationDUnitTest</dd>
 * <dd>{@link com.gemstone.gemfire.cache30.CacheXml81DUnitTest}</dd>
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
    target.getExtensionPoint().addExtension(this);
  }

  @Override
  public XmlGenerator<Cache> getXmlGenerator() {
    super.getXmlGenerator();
    return new MockCacheExtensionXmlGenerator(this);
  }
}

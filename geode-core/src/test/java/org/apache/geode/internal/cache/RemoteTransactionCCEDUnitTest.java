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
package org.apache.geode.internal.cache;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class RemoteTransactionCCEDUnitTest extends RemoteTransactionDUnitTest {

  private static final long serialVersionUID = 5960292521068781262L;

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    String pattern = (String) properties.get(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER);
    pattern += ";org.apache.geode.internal.cache.RemoteTransactionCCEDUnitTest";
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER, pattern);
    return properties;
  }

  @Override
  protected boolean getConcurrencyChecksEnabled() {
    return true;
  }
}

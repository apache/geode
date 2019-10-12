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
package org.apache.geode.internal.cache.wan.concurrent;

import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.WanTest;

@SuppressWarnings("serial")
@Category(WanTest.class)
public class ConcurrentSerialGatewaySenderOperationsOffHeapDistributedTest
    extends ConcurrentSerialGatewaySenderOperationsDistributedTest {

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = new Properties();
    props.setProperty(OFF_HEAP_MEMORY_SIZE, "300m");
    return props;
  }

  @Override
  protected boolean isOffHeap() {
    return true;
  }
}

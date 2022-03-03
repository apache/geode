/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import org.junit.rules.ExternalResource;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;

public class LocalCacheRule extends ExternalResource {
  private InternalCache cache;

  @Override
  protected void before() {
    cache = (InternalCache) new CacheFactory().set(MCAST_PORT, "0").create();
  }

  @Override
  protected void after() {
    cache.close();
  }

  public InternalCache getCache() {
    return cache;
  }
}

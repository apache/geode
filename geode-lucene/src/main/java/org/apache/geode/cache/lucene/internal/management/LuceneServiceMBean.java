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
package org.apache.geode.cache.lucene.internal.management;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

import javax.management.NotificationBroadcasterSupport;

public class LuceneServiceMBean extends NotificationBroadcasterSupport
    implements LuceneServiceMXBean, CacheServiceMBeanBase {

  private LuceneServiceBridge bridge;

  public LuceneServiceMBean(LuceneService service) {
    this.bridge = new LuceneServiceBridge(service);
  }

  @Override
  public LuceneIndexMetrics[] listIndexMetrics() {
    return this.bridge.listIndexMetrics();
  }

  @Override
  public LuceneIndexMetrics[] listIndexMetrics(String regionPath) {
    return this.bridge.listIndexMetrics(regionPath);
  }

  @Override
  public LuceneIndexMetrics listIndexMetrics(String regionPath, String indexName) {
    return this.bridge.listIndexMetrics(regionPath, indexName);
  }

  @Override
  public String getId() {
    return "LuceneService";
  }

  @Override
  public Class getInterfaceClass() {
    return LuceneServiceMXBean.class;
  }

  public void addIndex(LuceneIndex index) {
    this.bridge.addIndex(index);
  }
}

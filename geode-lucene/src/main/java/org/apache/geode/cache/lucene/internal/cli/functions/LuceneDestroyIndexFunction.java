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
package org.apache.geode.cache.lucene.internal.cli.functions;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class LuceneDestroyIndexFunction implements Function, InternalEntity {

  public void execute(final FunctionContext context) {
    String memberId = getCache().getDistributedSystem().getDistributedMember().getId();
    try {
      LuceneIndexInfo indexInfo = (LuceneIndexInfo) context.getArguments();
      String indexName = indexInfo.getIndexName();
      String regionPath = indexInfo.getRegionPath();
      LuceneService service = LuceneServiceProvider.get(getCache());
      if (indexName == null) {
        service.destroyIndexes(regionPath);
      } else {
        service.destroyIndex(indexName, regionPath);
      }
      context.getResultSender()
          .lastResult(new CliFunctionResult(memberId, getXmlEntity(regionPath)));
    } catch (Exception e) {
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, e.getMessage()));
    }
  }

  protected XmlEntity getXmlEntity(String regionPath) {
    return new XmlEntity(CacheXml.REGION, "name", regionPath);
  }

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }
}

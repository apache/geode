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

import org.apache.commons.lang.StringUtils;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneDestroyIndexInfo;
import org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class LuceneDestroyIndexFunction implements Function, InternalEntity {

  public void execute(final FunctionContext context) {
    CliFunctionResult result = null;
    String memberId = context.getCache().getDistributedSystem().getDistributedMember().getId();
    try {
      LuceneDestroyIndexInfo indexInfo = (LuceneDestroyIndexInfo) context.getArguments();
      String indexName = indexInfo.getIndexName();
      String regionPath = indexInfo.getRegionPath();
      LuceneService service = LuceneServiceProvider.get(context.getCache());
      if (indexName == null) {
        if (indexInfo.isDefinedDestroyOnly()) {
          ((LuceneServiceImpl) service).destroyDefinedIndexes(regionPath);
          result = new CliFunctionResult(memberId);
        } else {
          service.destroyIndexes(regionPath);
          result = new CliFunctionResult(memberId, getXmlEntity(indexName, regionPath));
        }
      } else {
        if (indexInfo.isDefinedDestroyOnly()) {
          ((LuceneServiceImpl) service).destroyDefinedIndex(indexName, regionPath);
          result = new CliFunctionResult(memberId);
        } else {
          service.destroyIndex(indexName, regionPath);
          result = new CliFunctionResult(memberId, getXmlEntity(indexName, regionPath));
        }
      }
    } catch (Exception e) {
      result = new CliFunctionResult(memberId, e, e.getMessage());
    }
    context.getResultSender().lastResult(result);
  }

  protected XmlEntity getXmlEntity(String indexName, String regionPath) {
    String regionName = StringUtils.stripStart(regionPath, "/");
    return new XmlEntity(CacheXml.REGION, "name", regionName, LuceneXmlConstants.PREFIX,
        LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, "name", indexName);
  }
}

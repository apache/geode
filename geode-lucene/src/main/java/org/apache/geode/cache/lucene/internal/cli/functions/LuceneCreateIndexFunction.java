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

package org.apache.geode.cache.lucene.internal.cli.functions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;


/**
 * The LuceneCreateIndexFunction class is a function used to create Lucene indexes.
 * </p>
 * @see Cache
 * @see org.apache.geode.cache.execute.Function
 * @see FunctionAdapter
 * @see FunctionContext
 * @see InternalEntity
 * @see LuceneIndexDetails
 */
@SuppressWarnings("unused")
public class LuceneCreateIndexFunction extends FunctionAdapter implements InternalEntity {

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  public String getId() {
    return LuceneListIndexFunction.class.getName();
  }

  public void execute(final FunctionContext context) {
    String memberId = null;
    try {
      final LuceneIndexInfo indexInfo = (LuceneIndexInfo) context.getArguments();
      final Cache cache = getCache();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();
      LuceneService service = LuceneServiceProvider.get(cache);

      String[] fields = indexInfo.getSearchableFieldNames();
      String[] analyzerName = indexInfo.getFieldAnalyzers();

      if (analyzerName == null || analyzerName.length == 0) {
        service.createIndex(indexInfo.getIndexName(), indexInfo.getRegionPath(), fields);
      }
      else {
        if (analyzerName.length != fields.length) throw new Exception("Mismatch in lengths of fields and analyzers");
        Map<String, Analyzer> fieldAnalyzer = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
          Analyzer analyzer = toAnalyzer(analyzerName[i]);
          fieldAnalyzer.put(fields[i], analyzer);
        }
        service.createIndex(indexInfo.getIndexName(), indexInfo.getRegionPath(), fieldAnalyzer);
      }

      //TODO - update cluster configuration by returning a valid XmlEntity
      XmlEntity xmlEntity = null;
      context.getResultSender().lastResult(new CliFunctionResult(memberId, xmlEntity));
    }
    catch (Exception e) {
      String exceptionMessage = CliStrings.format(CliStrings.EXCEPTION_CLASS_AND_MESSAGE, e.getClass().getName(),
        e.getMessage());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, e.getMessage()));
    }
  }

  private Analyzer toAnalyzer(String className)
  {
    if (className==null)
      className=StandardAnalyzer.class.getCanonicalName();
    else if (StringUtils.trim(className).equals("") | StringUtils.trim(className).equals("null") )
      className = StandardAnalyzer.class.getCanonicalName();

    Class<? extends Analyzer> clazz = CliUtil.forName(className, LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER);
    return CliUtil.newInstance(clazz, LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER);
  }

}

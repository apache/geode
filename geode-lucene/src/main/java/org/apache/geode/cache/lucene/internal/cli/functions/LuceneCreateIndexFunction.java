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

import static org.apache.geode.cache.lucene.internal.LuceneServiceImpl.validateCommandParameters.INDEX_NAME;
import static org.apache.geode.cache.lucene.internal.LuceneServiceImpl.validateCommandParameters.REGION_PATH;

import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneIndexFactoryImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneCliStrings;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.cache.lucene.internal.security.LucenePermission;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;


/**
 * The LuceneCreateIndexFunction class is a function used to create Lucene indexes.
 * </p>
 *
 * @see Cache
 * @see org.apache.geode.cache.execute.Function
 * @see Function
 * @see FunctionContext
 * @see InternalEntity
 * @see LuceneIndexDetails
 */
@SuppressWarnings("unused")
public class LuceneCreateIndexFunction implements InternalEntity, Function {

  private static final long serialVersionUID = 3061443846664615818L;

  public String getId() {
    return LuceneCreateIndexFunction.class.getName();
  }

  public void execute(final FunctionContext context) {
    String memberId = null;
    try {
      final LuceneIndexInfo indexInfo = (LuceneIndexInfo) context.getArguments();
      final Cache cache = context.getCache();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();
      LuceneService service = LuceneServiceProvider.get(cache);

      INDEX_NAME.validateName(indexInfo.getIndexName());

      String[] fields = indexInfo.getSearchableFieldNames();
      String[] analyzerName = indexInfo.getFieldAnalyzers();
      String serializerName = indexInfo.getSerializer();

      final LuceneIndexFactoryImpl indexFactory =
          (LuceneIndexFactoryImpl) service.createIndexFactory();
      if (analyzerName == null || analyzerName.length == 0) {
        for (String field : fields) {
          indexFactory.addField(field);
        }
      } else {
        if (analyzerName.length != fields.length)
          throw new Exception("Mismatch in lengths of fields and analyzers");
        for (int i = 0; i < fields.length; i++) {
          Analyzer analyzer = toAnalyzer(analyzerName[i]);
          indexFactory.addField(fields[i], analyzer);
        }
      }

      if (serializerName != null && !serializerName.equals("")) {
        indexFactory.setLuceneSerializer(toSerializer(serializerName));
      }

      REGION_PATH.validateName(indexInfo.getRegionPath());
      if (LuceneServiceImpl.LUCENE_REINDEX) {
        indexFactory.create(indexInfo.getIndexName(), indexInfo.getRegionPath(), true);
      } else {
        indexFactory.create(indexInfo.getIndexName(), indexInfo.getRegionPath(), false);
      }

      // TODO - update cluster configuration by returning a valid XmlEntity
      XmlEntity xmlEntity = null;
      context.getResultSender().lastResult(new CliFunctionResult(memberId, xmlEntity));
    } catch (Exception e) {
      String exceptionMessage = CliStrings.format(CliStrings.EXCEPTION_CLASS_AND_MESSAGE,
          e.getClass().getName(), e.getMessage());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, e.getMessage()));
    }
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singleton(
        new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, LucenePermission.TARGET));
  }

  private LuceneSerializer toSerializer(String serializerName)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    String trimmedName = StringUtils.trim(serializerName);
    if (trimmedName == "") {
      return null;
    }
    Class<? extends LuceneSerializer> clazz =
        CliUtil.forName(serializerName, LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER);
    return CliUtil.newInstance(clazz, LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER);
  }

  private Analyzer toAnalyzer(String className) {
    if (className == null)
      className = StandardAnalyzer.class.getCanonicalName();
    else {
      String trimmedClassName = StringUtils.trim(className);
      if (trimmedClassName.equals("") || trimmedClassName.equals("DEFAULT"))
        className = StandardAnalyzer.class.getCanonicalName();
      else
        className = trimmedClassName;
    }

    Class<? extends Analyzer> clazz =
        CliUtil.forName(className, LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER);
    return CliUtil.newInstance(clazz, LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER);
  }
}

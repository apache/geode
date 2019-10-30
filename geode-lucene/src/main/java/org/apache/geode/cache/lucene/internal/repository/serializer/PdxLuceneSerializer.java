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

package org.apache.geode.cache.lucene.internal.repository.serializer;

import java.util.Collection;
import java.util.Collections;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.PdxInstance;

/**
 * LuceneSerializer which can handle any PdxInstance
 */
class PdxLuceneSerializer implements LuceneSerializer {

  private static final Logger logger = LogService.getLogger();

  public PdxLuceneSerializer() {}

  @Override
  public Collection<Document> toDocuments(LuceneIndex index, Object value) {
    Document doc = new Document();
    PdxInstance pdx = (PdxInstance) value;
    for (String field : index.getFieldNames()) {
      if (pdx.hasField(field)) {
        Object fieldValue = pdx.getField(field);
        if (fieldValue == null) {
          continue;
        }
        SerializerUtil.addField(doc, field, fieldValue);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("PdxLuceneSerializer.toDocument:" + doc);
    }
    return Collections.singleton(doc);
  }
}

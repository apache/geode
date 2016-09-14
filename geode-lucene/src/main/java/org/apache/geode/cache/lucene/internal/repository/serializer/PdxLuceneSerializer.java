/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.cache.lucene.internal.repository.serializer;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.PdxInstance;

/**
 * LuceneSerializer which can handle any PdxInstance
 */
class PdxLuceneSerializer implements LuceneSerializer {

  private String[] indexedFields;

  private static final Logger logger = LogService.getLogger();

  public PdxLuceneSerializer(String[] indexedFields) {
    this.indexedFields = indexedFields;
  }

  @Override
  public void toDocument(Object value, Document doc) {
    PdxInstance pdx = (PdxInstance) value;
    for(String field : indexedFields) {
      if(pdx.hasField(field)) {
        Object fieldValue = pdx.getField(field);
        if (fieldValue == null) {
          continue;
        }
        SerializerUtil.addField(doc, field, fieldValue);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("PdxLuceneSerializer.toDocument:"+doc);
    }
  }
}
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

package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

import org.apache.lucene.document.Document;

import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * LuceneSerializer which can handle any PdxInstance
 */
class PdxLuceneSerializer implements LuceneSerializer {

  private String[] indexedFields;

  public PdxLuceneSerializer(String[] indexedFields) {
    this.indexedFields = indexedFields;
  }

  @Override
  public void toDocument(Object value, Document doc) {
    PdxInstance pdx = (PdxInstance) value;
    for(String field : indexedFields) {
      if(pdx.hasField(field)) {
        Object fieldValue = pdx.getField(field);
        SerializerUtil.addField(doc, field, fieldValue);
      }
    }
  }
}
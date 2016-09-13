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

import org.apache.geode.cache.lucene.LuceneService;

import org.apache.lucene.document.Document;

/**
 * A LuceneSerializer that can serialize a primitive value (String, int, long, double)
 * by creating a document with a special field containing the value
 */
public class PrimitiveSerializer implements LuceneSerializer {

  @Override
  public void toDocument(final Object value, final Document doc) {
    SerializerUtil.addField(doc, LuceneService.REGION_VALUE_FIELD, value);
  }
}

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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.internal.logging.LogService;

/**
 * A lucene serializer that handles a single class and can map an instance of that class to a
 * document using reflection.
 */
class ReflectionLuceneSerializer implements LuceneSerializer {

  private Field[] fields;

  private static final Logger logger = LogService.getLogger();

  public ReflectionLuceneSerializer(Class<? extends Object> clazz, String[] indexedFields) {
    Set<String> fieldSet = new HashSet<String>();
    fieldSet.addAll(Arrays.asList(indexedFields));

    // Iterate through all declared fields and save them
    // in a list if they are an indexed field and have the correct
    // type.
    ArrayList<Field> foundFields = new ArrayList<Field>();
    while (clazz != Object.class) {
      for (Field field : clazz.getDeclaredFields()) {
        Class<?> type = field.getType();
        if (fieldSet.contains(field.getName()) && SerializerUtil.isSupported(type)) {
          field.setAccessible(true);
          foundFields.add(field);
        }
      }

      clazz = clazz.getSuperclass();
    }

    this.fields = foundFields.toArray(new Field[foundFields.size()]);
  }

  @Override
  public Collection<Document> toDocuments(LuceneIndex index, Object value) {
    Document doc = new Document();
    for (Field field : fields) {
      try {
        Object fieldValue = field.get(value);
        if (fieldValue == null) {
          continue;
        }
        SerializerUtil.addField(doc, field.getName(), fieldValue);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        // TODO - what to do if we can't read a field?
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("ReflectionLuceneSerializer.toDocument:" + doc);
    }
    return Collections.singleton(doc);
  }

  public Field[] getFields() {
    return fields;
  }
}

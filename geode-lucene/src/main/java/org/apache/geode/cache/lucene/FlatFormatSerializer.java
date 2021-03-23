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
package org.apache.geode.cache.lucene;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;

import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil;
import org.apache.geode.internal.lang.utils.JavaWorkarounds;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.PdxInstance;

/**
 * A built-in {@link LuceneSerializer} to parse user's nested object into a flat format, i.e. a
 * single document. Each nested object will become a set of fields, with field name in format of
 * contacts.name, contacts.homepage.title.
 *
 * Here is a example of usage:
 *
 * User needs to explicitly setLuceneSerializer with an instance of this class, and specify nested
 * objects' indexed fields in following format:
 *
 * luceneService.createIndexFactory().setLuceneSerializer(new FlatFormatSerializer())
 * .addField("name").addField("contacts.name").addField("contacts.email", new KeywordAnalyzer())
 * .addField("contacts.address").addField("contacts.homepage.content") .create(INDEX_NAME,
 * REGION_NAME);
 *
 * Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
 *
 * When querying, use the same dot-separated index field name, such as contacts.homepage.content
 *
 * LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
 * "contacts.homepage.content:Hello*", "name");
 *
 * results = query.findPages();
 */
public class FlatFormatSerializer implements LuceneSerializer {

  private final ConcurrentMap<String, List<String>> tokenizedFieldCache = new ConcurrentHashMap<>();

  private static final Logger logger = LogService.getLogger();

  /**
   * Recursively serialize each indexed field's value into a field of lucene document. The field
   * name will be in the same format as its indexed, such as contacts.homepage.content
   *
   * @param index lucene index
   * @param value user object to be serialized into index
   */
  @Override
  public Collection<Document> toDocuments(LuceneIndex index, Object value) {
    String[] fields = index.getFieldNames();

    Document doc = new Document();
    for (String indexedFieldName : fields) {
      List<String> tokenizedFields = tokenizeField(indexedFieldName);
      addFieldValue(doc, indexedFieldName, value, tokenizedFields);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("FlatFormatSerializer.toDocuments: " + doc);
    }
    return Collections.singleton(doc);
  }

  private List<String> tokenizeField(String indexedFieldName) {
    List<String> tokenizedFields =
        JavaWorkarounds.computeIfAbsent(tokenizedFieldCache, indexedFieldName,
            field -> Arrays.asList(indexedFieldName.split("\\.")));
    return tokenizedFields;
  }

  private void addFieldValue(Document doc, String indexedFieldName, Object value,
      List<String> tokenizedFields) {
    String currentLevelField = tokenizedFields.get(0);

    Object fieldValue = getFieldValue(value, currentLevelField);

    if (fieldValue == null) {
      return;
    }

    if (fieldValue.getClass().isArray()) {
      for (int i = 0; i < Array.getLength(fieldValue); i++) {
        Object item = Array.get(fieldValue, i);
        addFieldValueForNonCollectionObject(doc, indexedFieldName, item, tokenizedFields);
      }
    } else if (fieldValue instanceof Collection) {
      Collection collection = (Collection) fieldValue;
      for (Object item : collection) {
        addFieldValueForNonCollectionObject(doc, indexedFieldName, item, tokenizedFields);
      }
    } else {
      addFieldValueForNonCollectionObject(doc, indexedFieldName, fieldValue, tokenizedFields);
    }
  }

  private void addFieldValueForNonCollectionObject(Document doc, String indexedFieldName,
      Object fieldValue, List<String> tokenizedFields) {
    if (tokenizedFields.size() == 1) {
      SerializerUtil.addField(doc, indexedFieldName, fieldValue);
    } else {
      addFieldValue(doc, indexedFieldName, fieldValue,
          tokenizedFields.subList(1, tokenizedFields.size()));
    }
  }

  private Object getFieldValue(Object value, String fieldName) {
    if (value instanceof PdxInstance) {
      PdxInstance pdx = (PdxInstance) value;
      Object fieldValue = null;
      if (pdx.hasField(fieldName)) {
        fieldValue = pdx.getField(fieldName);
      }
      return fieldValue;
    } else {
      Class<?> clazz = value.getClass();
      if (fieldName.equals(LuceneService.REGION_VALUE_FIELD)
          && SerializerUtil.supportedPrimitiveTypes().contains(clazz)) {
        return value;
      }

      do {
        try {
          Field field = clazz.getDeclaredField(fieldName);
          field.setAccessible(true);
          return field.get(value);
        } catch (Exception e) {
          clazz = clazz.getSuperclass();
        }
      } while (clazz != null && !clazz.equals(Object.class));
      return null;
    }
  }
}

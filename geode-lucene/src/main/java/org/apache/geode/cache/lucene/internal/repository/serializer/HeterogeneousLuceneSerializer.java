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
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;
import org.apache.geode.pdx.PdxInstance;

/**
 * An implementation of LuceneSerializer that reads the fields of a given object using reflection or
 * from a PDX instance and writes them to a lucene document.
 */
public class HeterogeneousLuceneSerializer implements LuceneSerializer {
  /**
   * A mapper for converting a PDX object into a document
   */
  private LuceneSerializer pdxMapper;

  /**
   * Mappers for each individual class type that this class has seen.
   *
   * Weak so that entry will be removed if a class is garbage collected.
   */
  private Map<Class<?>, LuceneSerializer> mappers =
      new CopyOnWriteWeakHashMap<Class<?>, LuceneSerializer>();

  private Map<String, PointsConfig> pointsConfigMap = new HashMap();

  private static final Logger logger = LogService.getLogger();

  public HeterogeneousLuceneSerializer() {
    final PrimitiveSerializer primitiveSerializer = new PrimitiveSerializer();
    SerializerUtil.supportedPrimitiveTypes().stream()
        .forEach(type -> mappers.put(type, primitiveSerializer));

    pdxMapper = new PdxLuceneSerializer();
  }

  @Override
  public Collection<Document> toDocuments(LuceneIndex index, Object value) {

    if (value == null) {
      return Collections.emptyList();
    }

    LuceneSerializer mapper = getFieldMapper(value, index.getFieldNames());

    Collection<Document> docs = mapper.toDocuments(index, value);
    if (logger.isDebugEnabled()) {
      logger.debug("HeterogeneousLuceneSerializer.toDocuments:" + docs);
    }

    return docs;
  }

  /**
   * Get the field mapper based on the type of the given object.
   */
  private LuceneSerializer getFieldMapper(Object value, String[] indexedFields) {
    if (value instanceof PdxInstance) {
      return pdxMapper;
    } else {
      Class<?> clazz = value.getClass();
      LuceneSerializer mapper = mappers.get(clazz);
      if (mapper == null) {
        mapper = new ReflectionLuceneSerializer(clazz, indexedFields);
        mappers.put(clazz, mapper);
      }
      return mapper;
    }
  }

  // TODO need a compute method to recalculate pointsConfigMap
  public Map<String, PointsConfig> getPointsConfigMap() {
    for (LuceneSerializer serializer : mappers.values()) {
      if (serializer instanceof ReflectionLuceneSerializer) {
        ReflectionLuceneSerializer reflectionSerializer = (ReflectionLuceneSerializer) serializer;
        Field[] fields = reflectionSerializer.getFields();
        for (Field field : fields) {
          Class<?> type = field.getType();
          if (type == int.class || type == Integer.class) {
            pointsConfigMap.put(field.getName(),
                new PointsConfig(NumberFormat.getInstance(), Integer.class));
          } else if (type == float.class || type == Float.class) {
            pointsConfigMap.put(field.getName(),
                new PointsConfig(NumberFormat.getInstance(), Float.class));
          } else if (type == long.class || type == Long.class) {
            pointsConfigMap.put(field.getName(),
                new PointsConfig(NumberFormat.getInstance(), Long.class));
          } else if (type == double.class || type == Double.class) {
            pointsConfigMap.put(field.getName(),
                new PointsConfig(NumberFormat.getInstance(), Double.class));
          }
        }
      }
    }
    return pointsConfigMap;
  }

}

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

import java.util.Arrays;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;
import org.apache.geode.pdx.PdxInstance;

/**
 * An implementation of LuceneSerializer that reads the fields
 * of a given object using reflection or from a PDX instance and
 * writes them to a lucene document.
 */
public class HeterogeneousLuceneSerializer implements LuceneSerializer {
  /**
   * The set of indexed fields for this mapper
   */
  private String[] indexedFields;
  
  /**
   * A mapper for converting a PDX object into a document
   */
  private LuceneSerializer pdxMapper;

  /**
   * Mappers for each individual class type that this class has seen.
   * 
   * Weak so that entry will be removed if a class is garbage collected.
   */
  private Map<Class<?>, LuceneSerializer> mappers = new CopyOnWriteWeakHashMap<Class<?>, LuceneSerializer>();
  
  private static final Logger logger = LogService.getLogger();
  
  public HeterogeneousLuceneSerializer(String[] indexedFields) {
    this.indexedFields = indexedFields;
    pdxMapper = new PdxLuceneSerializer(indexedFields);


    addSerializersForPrimitiveValues();
  }

  /**
   * Add serializers for the primitive value types (String, Number, etc.)
   * if the user has requested that the whole value be serialized
   */
  private void addSerializersForPrimitiveValues() {
    if(Arrays.asList(indexedFields).contains(LuceneService.REGION_VALUE_FIELD)) {
      final PrimitiveSerializer primitiveSerializer = new PrimitiveSerializer();
      SerializerUtil.supportedPrimitiveTypes().stream()
        .forEach(type -> mappers.put(type, primitiveSerializer));
    }
  }

  @Override
  public void toDocument(Object value, Document doc) {
    
    LuceneSerializer mapper = getFieldMapper(value);
    
    mapper.toDocument(value, doc);
    if (logger.isDebugEnabled()) {
      logger.debug("HeterogeneousLuceneSerializer.toDocument:"+doc);
    }
  }

  /**
   * Get the field mapper based on the type of the given object.
   */
  private LuceneSerializer getFieldMapper(Object value) {
    if(value instanceof PdxInstance) {
      return pdxMapper;
    } else {
      Class<?> clazz = value.getClass();
      LuceneSerializer mapper = mappers.get(clazz);
      if(mapper == null) {
        mapper = new ReflectionLuceneSerializer(clazz, indexedFields);
        mappers.put(clazz, mapper);
      }
      return mapper;
    }
  }
  

}

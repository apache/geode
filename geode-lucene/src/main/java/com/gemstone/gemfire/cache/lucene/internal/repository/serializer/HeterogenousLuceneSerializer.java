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

import java.util.Map;

import org.apache.lucene.document.Document;

import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteWeakHashMap;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * An implementation of LuceneSerializer that reads the fields
 * of a given object using reflection or from a PDX instance and
 * writes them to a lucene document.
 */
public class HeterogenousLuceneSerializer implements LuceneSerializer {
  /**
   * The set of indexed fiels for this mapper
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
  
  public HeterogenousLuceneSerializer(String[] indexedFields) {
    this.indexedFields = indexedFields;
    pdxMapper = new PdxLuceneSerializer(indexedFields);
  }
  
  @Override
  public void toDocument(Object value, Document doc) {
    
    LuceneSerializer mapper = getFieldMapper(value);
    
    mapper.toDocument(value, doc);
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

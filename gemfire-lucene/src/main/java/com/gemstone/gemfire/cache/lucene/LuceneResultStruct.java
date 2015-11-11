/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.lucene;

import com.gemstone.gemfire.annotations.Experimental;

/**
 * <p>
 * Abstract data structure for one item in query result.
 * 
 * @author Xiaojian Zhou
 */
@Experimental
public interface LuceneResultStruct<K, V> {
  /**
   * Return the value associated with the given field name
   *
   * @param fieldName the String name of the field
   * @return the value associated with the specified field
   * @throws IllegalArgumentException If this struct does not have a field named fieldName
   */
  public Object getProjectedField(String fieldName);
  
  /**
   * Return key of the entry
   *
   * @return key
   * @throws IllegalArgumentException If this struct does not contain key
   */
  public K getKey();
  
  /**
   * Return value of the entry
   *
   * @return value the whole domain object
   * @throws IllegalArgumentException If this struct does not contain value
   */
  public V getValue();
  
  /**
   * Return score of the query 
   *
   * @return score
   * @throws IllegalArgumentException If this struct does not contain score
   */
  public float getScore();
}


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
package com.gemstone.gemfire.cache.query.internal;

import java.util.Iterator;

import com.gemstone.gemfire.cache.query.types.CollectionType;

/**
 * This interface is to be implemented by all SelectResults implementation which
 * can hold struct using field values array ( Object[]) 
 * 
 * @see SortedStructSet
 * @see StructSet
 * @see StructBag
 * @see SortedStructBag
 * 
 *
 */
public interface StructFields {

  public boolean addFieldValues(Object[] fieldValues);
  public boolean removeFieldValues(Object[] fieldValues);
  public Iterator fieldValuesIterator();
  public CollectionType getCollectionType();
  public boolean containsFieldValues(Object[] fieldValues);
}

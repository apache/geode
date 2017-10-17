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

import java.util.Collection;

import org.apache.geode.cache.Declarable;
import org.apache.lucene.document.Document;

import org.apache.geode.annotations.Experimental;

/**
 * An interface for writing the fields of an object into a lucene document
 */
@Experimental
public interface LuceneSerializer extends Declarable {

  /**
   * Add the fields of the given value to a set of documents
   * 
   * @param index lucene index
   * @param value user object to be serialized into index
   */
  Collection<Document> toDocuments(LuceneIndex index, Object value);
}

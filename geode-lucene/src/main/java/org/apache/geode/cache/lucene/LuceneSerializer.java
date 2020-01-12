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

import org.apache.lucene.document.Document;

import org.apache.geode.cache.Declarable;

/**
 * An interface for writing the fields of an object into a lucene document
 *
 * @param <T> The type of object supported by this lucene serializer
 */
public interface LuceneSerializer<T> extends Declarable {

  /**
   * Add the fields of the given value to a set of documents
   *
   * Added fields should be marked with {@link org.apache.lucene.document.Field.Store#NO}. These
   * fields are only used for searches. When doing a query, geode will return the value from the
   * region, not any fields that are stored on the returned Documents.
   *
   * @param index lucene index
   * @param value user object to be serialized into index
   */
  Collection<Document> toDocuments(LuceneIndex index, T value);
}

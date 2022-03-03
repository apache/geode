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

import org.apache.geode.GemFireException;

/**
 * A LuceneIndexDestroyedException is thrown if a Lucene index is attempted to be used while it is
 * being destroyed or after it has been destroyed.
 */
public class LuceneIndexDestroyedException extends GemFireException {

  private final String indexName;

  private final String regionPath;

  public LuceneIndexDestroyedException(String indexName, String regionPath) {
    super("Lucene index " + indexName + " on region " + regionPath + " has been destroyed");
    this.indexName = indexName;
    this.regionPath = regionPath;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getRegionPath() {
    return regionPath;
  }
}

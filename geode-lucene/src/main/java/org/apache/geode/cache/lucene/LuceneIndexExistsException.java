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
 * A LuceneIndexExistsException is thrown if a Lucene index that already exists is attempted to be
 * created.
 */
public class LuceneIndexExistsException extends GemFireException {

  private final String indexName;

  private final String regionPath;

  public LuceneIndexExistsException(String indexName, String regionPath) {
    super();
    this.indexName = indexName;
    this.regionPath = regionPath;
  }

  @Override
  public String getMessage() {
    return String.format("Lucene index %s on region %s already exists.",
        this.indexName, this.regionPath);
  }

  public String getIndexName() {
    return this.indexName;
  }

  public String getRegionPath() {
    return this.regionPath;
  }
}

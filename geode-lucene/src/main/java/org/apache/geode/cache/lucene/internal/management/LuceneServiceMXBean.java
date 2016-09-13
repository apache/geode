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
package com.gemstone.gemfire.cache.lucene.internal.management;

import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to the {@link com.gemstone.gemfire.cache.lucene.LuceneService}.
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface LuceneServiceMXBean {

  /**
   * Returns an array of {@link LuceneIndexMetrics} for the {@link com.gemstone.gemfire.cache.lucene.LuceneIndex}
   * instances defined in this member
   *
   * @return an array of LuceneIndexMetrics for the LuceneIndexes defined in this member
   */
  public LuceneIndexMetrics[] listIndexMetrics();

  /**
   * Returns an array of {@link LuceneIndexMetrics} for the {@link com.gemstone.gemfire.cache.lucene.LuceneIndex}
   * instances defined on the input region in this member
   *
   * @param regionPath The full path of the region to retrieve
   *
   * @return an array of LuceneIndexMetrics for the LuceneIndex instances defined on the input region
   * in this member
   */
  public LuceneIndexMetrics[] listIndexMetrics(String regionPath);

  /**
   * Returns a {@link LuceneIndexMetrics} for the {@link com.gemstone.gemfire.cache.lucene.LuceneIndex}
   * with the input index name defined on the input region in this member.
   *
   * @param regionPath The full path of the region to retrieve
   * @param indexName The name of the index to retrieve
   *
   * @return a LuceneIndexMetrics for the LuceneIndex with the input index name defined on the input region
   * in this member.
   */
  public LuceneIndexMetrics listIndexMetrics(String regionPath, String indexName);
}

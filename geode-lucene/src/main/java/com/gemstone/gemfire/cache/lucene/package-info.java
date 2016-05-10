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
/**
 * This package provides an integration with Apache Lucene that allows Geode regions to be indexed in a distributed
 * Lucene index and queries using Lucene queries.
 * <p>
 * All indexing and query operations are performed through the {@link com.gemstone.gemfire.cache.lucene.LuceneService} class.
 * See {@link com.gemstone.gemfire.cache.lucene.LuceneService} for an example of how to add a lucene index to a geode region.
 * <p>
 *
 * The Lucene indexes created using this API are stored in geode and colocated with the indexed region, which means they
 * have the same availability guarantees as the underlying region. The indexes are maintained asynchronously, so changes
 * to the region may not be immediately visible in the lucene index.
 */

package com.gemstone.gemfire.cache.lucene;
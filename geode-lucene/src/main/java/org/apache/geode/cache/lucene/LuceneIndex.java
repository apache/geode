/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;



/**
 * <p>
 * LuceneIndex represents the Lucene index created over the data stored in Apache Geode regions. The
 * Lucene indexes are maintained automatically by Apache Geode whenever the entries are updated in
 * the associated regions. Lucene Indexes are created using {@link LuceneService#createIndexFactory}
 * by specifying the Lucene index name, the region associated with the Lucene index and the fields
 * on which the Lucene index is to be created.
 * </p>
 *
 * <p>
 * Every Lucene index is uniquely identified by the index name and the name of the region associated
 * with it. To obtain the LuceneIndex created over a region use
 * {@link LuceneService#getIndex(String, String)}
 * </p>
 *
 * <p>
 * LuceneIndexes are created using gfsh, xml, or the Java API using LuceneService
 * {@link LuceneService#createIndexFactory}. More information about LuceneIndex can be found at
 * {@link LuceneService}
 * </p>
 */
public interface LuceneIndex {

  /**
   * Returns the name of the LuceneIndex object. This name is provided while creating the
   * LuceneIndex using {@link LuceneService#createIndexFactory()} create method
   *
   * @return Name of the LuceneIndex
   */
  String getName();

  /**
   * Returns the path of the region on which the LuceneIndex was created. The region name is
   * provided while creating the LuceneIndex using {@link LuceneService#createIndexFactory()}
   *
   * @return Path of the region
   */
  String getRegionPath();

  /**
   * Returns a string array containing the fields on which the LuceneIndex was created. These fields
   * are assigned using the addField method while creating the LuceneIndex using
   * {@link LuceneService#createIndexFactory()}
   *
   * @return String array containing the field names
   */
  String[] getFieldNames();

  /**
   * Returns a map containing the field name and the {@link Analyzer} used to tokenize the field.
   * The analyzer to be used on a particular field is set in the addField method while creating the
   * LuceneIndex using {@link LuceneService#createIndexFactory()}
   *
   * @return a map containing pairs of the indexed field name and the corresponding {@link Analyzer}
   *         being used on each indexed field.
   */
  Map<String, Analyzer> getFieldAnalyzers();

  /**
   * Return the {@link LuceneSerializer} associated with this index
   */
  LuceneSerializer getLuceneSerializer();

  /**
   * Returns a boolean value to indicate if reindexing is in progress.
   *
   * @return a boolean value indicating indexing progress
   */

  boolean isIndexingInProgress();

}

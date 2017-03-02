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

import org.apache.geode.annotations.Experimental;


/**
 * <p>
 * An Lucene index is built over the data stored in a GemFire Region.
 * </p>
 * <p>
 * An index is specified using a index name, field names, region name.
 * </p>
 * The index name and region name together uniquely identifies the Lucene index.
 * 
 */
@Experimental
public interface LuceneIndex {

  /**
   * @return the name of this index
   */
  public String getName();

  /**
   * @return the name of the region that is being indexed
   */
  public String getRegionPath();

  /**
   * @return the indexed field names
   */
  public String[] getFieldNames();

  /**
   * @return a map of what {@link Analyzer} is being used for each indexed field.
   */
  public Map<String, Analyzer> getFieldAnalyzers();

}

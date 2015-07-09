/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.lucene;

import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;


/**
 * An lucene index is built over the data stored in a GemFire Region.
 * <p>
 * An index is specified using a index name, field names, region name.
 * <p>
 * The index name and region name together uniquely identifies the lucene index.
 * <p>
 * 
 * @author Xiaojian Zhou
 * @since 8.5
 */
public interface LuceneIndex {

  /**
   * @return the index name of this index
   */
  public String getName();

  /**
   * @return the region name for this index
   */
  public String getRegionName();
      
  /**
   * @return the indexed field names in a Set
   */
  public String[] getFieldNames();
  
  /**
   * @return the field to analyzer map
   */
  public Map<String, Analyzer> getFieldAnalyzerMap();
  
}

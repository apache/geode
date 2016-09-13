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

package org.apache.geode.cache.query;

//import java.util.*;
import org.apache.geode.cache.*;

/**
 * An index that is built over the data stored in a GemFire {@link
 * Region}.
 * <p>
 * For a description of the types of indexes that GemFire currently supports,
 * see {@linkplain IndexType}.
 * <p>
 * An index is specified using a name, indexedExpression, fromClause, and
 * optionally a projectionAttributes.
 * <p>
 * The name uniquely identifies the index to for the statistics services in GemFire.
 * <p>
 * The indexedExpression is the lookup value for the index. The way that an
 * indexedExpression is specified and used varies depending on the type of
 * index. For more information, see {@linkplain IndexType}.
 * <p>
 * The fromClause specifies the collection(s) of objects that the index ranges
 * over, and must contain one and only one region path.
 * <p>
 * The optional projectAttributes specifies a tranformation that is done on
 * the values and is used for pre-computing a corresponding projection as
 * defined in a query.
 * 
 * @see QueryService#createIndex(String, IndexType, String, String)
 * @see IndexType
 *
 * @since GemFire 4.0
 */
public interface Index {

  /**
   * Returns the unique name of this index
   */
  public String getName();

  /**
   * Get the index type
   * @return the type of index
   */
  public IndexType getType();
      
  /**
   * The Region this index is on
   * @return the Region for this index
   */
  public Region<?,?> getRegion();
  
  /**
   * Get statistics information for this index.
   * 
   * @throws UnsupportedOperationException
   *           for indexes created on Maps. Example: Index on Maps with
   *           expression p['key']. <br>
   *           On Map type indexes the stats are created at individual key level
   *           that can be viewed in VSD stats.
   */
  public IndexStatistics getStatistics();
  
  /**
   * Get the original fromClause for this index.
   */
  public String getFromClause();
  /**
   * Get the canonicalized fromClause for this index.
   */
  public String getCanonicalizedFromClause();
  
  /**
   * Get the original indexedExpression for this index.
   */
  public String getIndexedExpression();
  
    /**
   * Get the canonicalized indexedExpression for this index.
   */
  public String getCanonicalizedIndexedExpression();
  /**
   * Get the original projectionAttributes for this expression.
   * @return the projectionAttributes, or "*" if there were none
   * specified at index creation.
   */
  public String getProjectionAttributes();
  /**
   * Get the canonicalized projectionAttributes for this expression.
   * @return the projectionAttributes, or "*" if there were none
   * specified at index creation.
   */
  public String getCanonicalizedProjectionAttributes();
}

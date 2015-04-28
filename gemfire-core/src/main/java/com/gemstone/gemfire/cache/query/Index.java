/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

//import java.util.*;
import com.gemstone.gemfire.cache.*;

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
 * @author Eric Zoerner
 * @since 4.0
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

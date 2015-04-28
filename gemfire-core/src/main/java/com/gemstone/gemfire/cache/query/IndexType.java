/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.query;

/**
 * @deprecated As of 6.6.1. Check {@link QueryService} for changes.
 * 
 * Enumerated type for types of {@linkplain Index indexes}
 *
 * @author Eric Zoerner
 * @since 4.0
 */
public class IndexType {

  /**
   * The index type of a functional index.
   * A functional index is used for the comparison of some function
   * of a region value with a constant, using a relational operator.
   * The indexedExpression yields a value that is a Comparable. The "constant"
   * that it is to be compared to is any expression that is not dependent
   * on a value in the region. A simple example is an index on the
   * indexedExpression "age". This would be used for a query that has the
   * where clause "age < 55".
   * <p>
   * The indexedExpression for a functional index can be any Comparable
   * or any of the following primitive types:<br>
   * <code>long int short byte char float double</code>
   *
   * @see QueryService#createIndex(String, IndexType, String, String)
   */
  public static final IndexType FUNCTIONAL
    = new IndexType("FUNCTIONAL");
  
  /**
   * The index type of a hash index.
   * A hash index is used for the comparison of some function
   * of a region value with a constant, using a relational operator.
   * The indexedExpression yields a value that is a Comparable. The "constant"
   * that it is to be compared to is any expression that is not dependent
   * on a value in the region. A simple example is an index on the
   * indexedExpression "age". This would be used for a query that has the
   * where clause "age = 55".
   * <p>
   * The indexedExpression for a hash index can be any Comparable
   * or any of the following primitive types:<br>
   * <code>long int short byte char float double</code>
   *
   * @see QueryService#createIndex(String, IndexType, String, String)
   */
  public static final IndexType HASH
  = new IndexType("HASH");
  
  
  /**
   * The index type of a primary key index. A primary key index uses the
   * keys in the region itself. By creating a primary key index, you make
   * the query service aware of the relationship between the values in the
   * region and the keys in the region and enable the relationship to be
   * used to optimize the execution of queries. For example, if the values
   * in a region are employee objects and the keys in the region is the
   * attribute empId on those employees, then you can create a primary key
   * index on that region with the indexedExpression "empId".
   * <p>
   * The type of the indexedExpression for a primary key index can be any
   * object type. Just as in a Region, the lookup is based on the implementation
   * of the <code>equals</code> and <code>hashCode</code> methods in the
   * object.
   *
   * @see QueryService#createIndex(String, IndexType, String, String)
   */
  public static final IndexType PRIMARY_KEY = new IndexType("PRIMARY_KEY");
  
  //public static final IndexType MAP_INDEX = new IndexType("MAP_INDEX");
  
  private String name;
  
  /** Creates a new instance of IndexType */
  private IndexType(String name) {
    this.name = name;
  }
  
  /**
   * Return the index type as a String
   * @return the String representation of this IndexType
   */
  @Override
  public String toString() {
    return this.name;
  }
  
}

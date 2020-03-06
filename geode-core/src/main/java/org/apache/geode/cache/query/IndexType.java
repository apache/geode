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

package org.apache.geode.cache.query;

/**
 * @deprecated As of 6.6.1. Check {@link QueryService} for changes.
 *
 *             Enumerated type for types of {@linkplain Index indexes}
 *
 * @since GemFire 4.0
 */
@Deprecated
public enum IndexType {
  /**
   * The index type of a functional index. A functional index is used for the comparison of some
   * function of a region value with a constant, using a relational operator. The indexedExpression
   * yields a value that is a Comparable. The "constant" that it is to be compared to is any
   * expression that is not dependent on a value in the region. A simple example is an index on the
   * indexedExpression "age". This would be used for a query that has the where clause "age < 55".
   * <p>
   * The indexedExpression for a functional index can be any Comparable or any of the following
   * primitive types:<br>
   * <code>long int short byte char float double</code>
   *
   * @see QueryService#createIndex(String, IndexType, String, String)
   */
  FUNCTIONAL("RANGE"),

  /**
   * The index type of a hash index. A hash index is used for the comparison of some function of a
   * region value with a constant, using a relational operator. The indexedExpression yields a value
   * that is a Comparable. The "constant" that it is to be compared to is any expression that is not
   * dependent on a value in the region. A simple example is an index on the indexedExpression
   * "age". This would be used for a query that has the where clause "age = 55".
   * <p>
   * The indexedExpression for a hash index can be any Comparable or any of the following primitive
   * types:<br>
   * <code>long int short byte char float double</code>
   *
   * @see QueryService#createIndex(String, IndexType, String, String)
   * @deprecated Due to the overhead caused by rehashing while expanding the backing array, hash
   *             index has been deprecated since Apache Geode 1.4.0. Use
   *             {@link IndexType#FUNCTIONAL} instead.
   */
  @Deprecated
  HASH("HASH"),


  /**
   * The index type of a primary key index. A primary key index uses the keys in the region itself.
   * By creating a primary key index, you make the query service aware of the relationship between
   * the values in the region and the keys in the region and enable the relationship to be used to
   * optimize the execution of queries. For example, if the values in a region are employee objects
   * and the keys in the region is the attribute empId on those employees, then you can create a
   * primary key index on that region with the indexedExpression "empId".
   * <p>
   * The type of the indexedExpression for a primary key index can be any object type. Just as in a
   * Region, the lookup is based on the implementation of the <code>equals</code> and
   * <code>hashCode</code> methods in the object.
   *
   * @see QueryService#createIndex(String, IndexType, String, String)
   */
  PRIMARY_KEY("KEY");

  private String name;

  IndexType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static IndexType valueOfSynonym(String name) {
    name = name.toUpperCase();

    switch (name) {
      case "KEY":
        return valueOf("PRIMARY_KEY");
      case "RANGE":
        return valueOf("FUNCTIONAL");
    }

    return valueOf(name);
  }
}

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal.index;

/**
 * 
 * @author Asif
 * 
 * Object of this class contains information of various attributes of Range
 * Index for a filter evaluatable condition , from the perspective of the query
 * from clause. It identifies the match level & the maping of Iterators of the
 * Group to the index result fields.
 */
public class IndexData  {

  /**
   * Index assosciated with the condition
   */
  IndexProtocol _index;
  /**
   * int idenifying the match level. A match level of zero means exact match. A
   * match level greater than 0 means the query from clauses have extra
   * iterators as compared to Index resultset ( This does not neccessarily mean
   * that Index resultset is not having extra fields. It is just that the
   * criteria for match level is the absence or presence of extra iterators. The
   * order of preference will be 0 , <0 , > 0 for first cut.
   */
  int _matchLevel;
  /**
   * An int array of size equal to the number of Iterators present in the Group
   * representing the Iterators for that region. It identifies the index result
   * field position which maps to the RuntimeIterator for teh Group. The mapping
   * of index result field is 1 based ( not zero). For example the second
   * Iterator of the group ( 0 indxe based , hence 1 ) will map to the field
   * position in the Index Result given by the value maping[1]. If an iterator
   * has no mapping , the value will be 0. *
   *  
   */
  int[] mapping = null;

  IndexData(IndexProtocol index, int matchLevel, int[] mapping) {
    this.mapping = mapping;
    _index = index;
    _matchLevel = matchLevel;
  }

  public IndexProtocol getIndex() {
    return this._index;
  }

  public int getMatchLevel() {
    return this._matchLevel;
  }

  public int[] getMapping() {
    return this.mapping;
  }
}

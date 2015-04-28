/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

/**
 * Interface implemented by CompiledComparision and CompiledUndefibed to
 * indicate that index can be created on such CompiledValues.It indicates that
 * they are filter evaluatable at the atomic level.
 * 
 * @author asif
 * 
 */
public interface Indexable {
  /**
   * Returns the IndexInfo object, if any, associated with the CompiledValue
   * 
   * @param context
   *                ExecutionContext object
   * @return IndexInfo object , if any, associated with the CompiledValue
   * @throws TypeMismatchException
   * @throws AmbiguousNameException
   * @throws NameResolutionException
   */
  IndexInfo[] getIndexInfo(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException,
      NameResolutionException;

  /**
   * 
   * @return boolean indicating whether the CompiledValue is RangeEvaluatable or
   *         not. Presently CompiledUndefined is assumed to be not range
   *         evaluatable while a CompiledComparison is assumed to be range
   *         evaluatable ( though we do not club compiled comparison having null
   *         as RHS or LHS field along with other CompiledComparison objects)
   */
  boolean isRangeEvaluatable();
}

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
package org.apache.geode.cache.query.internal;

import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.TypeMismatchException;

/**
 * Interface implemented by CompiledComparision and CompiledUndefibed to indicate that index can be
 * created on such CompiledValues.It indicates that they are filter evaluatable at the atomic level.
 *
 *
 */
public interface Indexable {
  /**
   * Returns the IndexInfo object, if any, associated with the CompiledValue
   *
   * @param context ExecutionContext object
   * @return IndexInfo object , if any, associated with the CompiledValue
   */
  IndexInfo[] getIndexInfo(ExecutionContext context)
      throws TypeMismatchException, NameResolutionException;

  /**
   *
   * @return boolean indicating whether the CompiledValue is RangeEvaluatable or not. Presently
   *         CompiledUndefined is assumed to be not range evaluatable while a CompiledComparison is
   *         assumed to be range evaluatable ( though we do not club compiled comparison having null
   *         as RHS or LHS field along with other CompiledComparison objects)
   */
  boolean isRangeEvaluatable();
}

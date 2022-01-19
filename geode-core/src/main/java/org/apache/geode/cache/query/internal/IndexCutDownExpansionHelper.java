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

import java.util.List;

import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;

/**
 * This is a helper class which contains informaion on how to expand / cutdown index results for
 * making it compatible with the query.
 */
class IndexCutDownExpansionHelper {

  /**
   * booelan which identifies if a cutdown of index results is needed or not.
   */
  boolean cutDownNeeded = false;

  /**
   * A SelectResults ( ResultBag or StructBag) object used to prevent unnecessary expansion of index
   * results as described in IndexConditionalHelper class.
   */
  SelectResults checkSet = null;

  /**
   * ObjectType for the checkSet object ( An ObjectType for a ResultBag & StructType for a
   * StructBag)
   */
  ObjectType checkType = null;

  int checkSize = -1;

  IndexCutDownExpansionHelper(List checkList, ExecutionContext context) {
    cutDownNeeded = checkList != null && (checkSize = checkList.size()) > 0;
    if (cutDownNeeded) {
      Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
      boolean useLinkedDataStructure = false;
      boolean nullValuesAtStart = true;
      if (orderByClause != null && orderByClause) {
        List orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
        useLinkedDataStructure = orderByAttrs.size() == 1;
        nullValuesAtStart = !((CompiledSortCriterion) orderByAttrs.get(0)).getCriterion();
      }
      if (checkSize > 1) {

        checkType = QueryUtils.createStructTypeForRuntimeIterators(checkList);
        if (useLinkedDataStructure) {
          checkSet = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) checkType)
              : new SortedResultsBag<>(checkType, nullValuesAtStart);
        } else {
          checkSet = QueryUtils.createStructCollection(context, (StructTypeImpl) checkType);
        }
      } else {
        checkType = ((RuntimeIterator) checkList.get(0)).getElementType();
        if (useLinkedDataStructure) {
          checkSet = context.isDistinct() ? new LinkedResultSet(checkType)
              : new SortedResultsBag(checkType, nullValuesAtStart);
        } else {
          checkSet = QueryUtils.createResultCollection(context, checkType);
        }
      }
    }
  }
}

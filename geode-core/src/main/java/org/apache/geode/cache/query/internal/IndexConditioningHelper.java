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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.query.types.StructType;

/**
 * This is a helper class which provides information on how an index data be used so as to make it
 * compatible with the query.
 */
class IndexConditioningHelper {

  /**
   * boolean if true implies that the index results need to be iterated so as to make it compatible
   * with from clause. Shuffling may be needed for any of the following reasons: 1) Match level not
   * zero ( implying index result expansion or cutdown) 2) Match level zero , but the relative
   * positions of iterators in the List of iterators for the group not matching the positions in the
   * index result StructBag 3) Iter operand is not null. *
   *
   */
  // If shuffling is not needed , then it can be bcoz of two reasons
  // 1) The Index results is a ResultSet & match level is zero ( in that case we
  // don't have to do anything)
  // 2) The Index results is a StructBag with match level as zero & inddex
  // fields matching
  // the order of RuntimeIterators. In that case we just have to change the
  // StructType of the StructBag
  boolean shufflingNeeded = true;

  /**
   * An arary of RuntimeIterators whose size is equal to the number of fields in the Index results.
   * It identifies the RuntimeIterator for the field in the Index Results. Thus the Runtime Iterator
   * at position 0 will be that for field 0 in the index result & so on. For those index fields
   * which do not have a Runtime Iterator assosciated , the value is null (This is the case if index
   * results require cut down)
   */
  RuntimeIterator[] indexFieldToItrsMapping = null;

  /**
   * The List containing RuntimeIterators to which the index results need to be expanded This will
   * usually be Final List of RuntimeIterators - RuntimeIteratosr already accounted for in the index
   * results
   */
  // The default is initialized as empty List rather than null to avoid
  // Null Pointer Exception in the function
  // getconditionedRelationshipIndexResults
  List expansionList = Collections.emptyList();

  /**
   * The List containing RuntimeIterators which define the final SelectResults after the relevant
   * expansion/cutdown of index results
   */
  // Though in case of single index usage , if no shuffling is needed (
  // exact match) we
  // do not need finalList , but it is used in relation ship index , even if
  // match level is zero.
  // So we should never leave it as null
  List finalList = null;

  /**
   * This is the List of RuntimeIterators which gets created only if the index resulst require a
   * cutdown. In such cases , it identifies those Runtime Iterators of Index Results which will be
   * selected to form the result tuple. The RuntimeIterators in this List will have corresponding
   * fields in the resultset obtained from Index usage. This List will be populated only if there
   * exists fields in index resultset which will not be selected.If all the fields of index
   * resultset will be used , then this List should be null or empty. It is used in preventing
   * unnecessary expansion of same type, when a similar expansion has already occured. as for eg
   *
   * consider a index result containing 3 fields field1 field2 & field3 . Assume that field3 is for
   * cutdown. Since the expansion iterators can either be independent of all the fields in the index
   * result or at the max be dependent on field1 & field2, we should expand for a given combination
   * of field1 & field2 , only once ( as we have resulst as Set, we can only have unique entries)
   * ie. suppose a index result tuple has values ( 1,2 , 3 ) & ( 1,2,4) , we should expand only once
   * ( as field with value 3 & 4 are to be discarded).
   */
  /*
   * Below Can be null or empty collections if the match level is exact & no shuffling needed
   */
  List checkList = null;

  /**
   * This field is meaningful iff the match level is zero, no shuffling needed & there exists a
   * StructBag (& not a ResultBag)
   */
  StructType structType = null;

  /**
   * Independent Iterator for the Group to which the Path expression belongs to
   */
  RuntimeIterator indpndntItr = null;

  /**
   * Indexnfo object for the path expression
   */
  IndexInfo indxInfo = null;

  IndexConditioningHelper(IndexInfo indexInfo, ExecutionContext context, int indexFieldsSize,
      boolean completeExpansion, CompiledValue iterOperands, RuntimeIterator grpIndpndntItr) {
    /*
     * First obtain the match level of index resultset. If the match level happens to be zero , this
     * implies that we just have to change the StructType ( again if only the Index resultset is a
     * StructBag). If the match level is zero & expand to to top level flag is true & iff the total
     * no. of iterators in current scope is greater than the no. of fields in StructBag , then only
     * we need to do any expansion. The grpIndpndtItr passed can be null if the where clause
     * comprises of just this condition. However if it is invoked from GroupJunction , it will be
     * not null
     *
     */
    this.indxInfo = indexInfo;
    List grpItrs = null;
    int size = indexInfo.mapping.length;
    this.indpndntItr = grpIndpndntItr;
    this.indexFieldToItrsMapping = new RuntimeIterator[indexFieldsSize];
    // Obtain the grpIndpndt iterator if it is passed as null
    if (this.indpndntItr == null) {
      Set set1 = new HashSet();
      context.computeUltimateDependencies(indexInfo._path, set1);
      Support.Assert(set1.size() == 1,
          " Since we are in Indexed Evaluate that means there has to be exactly one independent iterator for this compiled comparison");
      // The ultimate independent RuntimeIterator
      this.indpndntItr = (RuntimeIterator) set1.iterator().next();
      Support.Assert(
          this.indpndntItr.getScopeID() == context.currentScope()
              .getScopeID()/* context.getScopeCount() */,
          " Since we are in Indexed Evaluate that means the current scope count & indpenedent iterator's scope count should match");
    }
    if (indexInfo._matchLevel == 0
        && (!completeExpansion || context.getCurrentIterators().size() == size)) {
      // Don't do anything , just change the StructType if the set is
      // structset.
      if (size > 1) {
        // The Index resultset is a structType.
        Support.Assert(indexInfo._index.getResultSetType() instanceof StructType,
            " If the match level is zero & the size of mapping array is 1 then Index is surely ResultBag else StructBag");
        // The independent iterator is added as the first element
        grpItrs = context.getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(this.indpndntItr);
        // Check if reshuffling is needed or just changing the struct
        // type will suffice
        boolean isReshufflingNeeded = false;
        int pos = -1;
        for (int i = 0; i < size; ++i) {
          pos = indexInfo.mapping[i];
          isReshufflingNeeded = isReshufflingNeeded || (pos != (i + 1));
          this.indexFieldToItrsMapping[pos - 1] = (RuntimeIterator) grpItrs.get(i);
        }
        this.finalList = grpItrs;
        // Even if Reshuffle is not need but if the iter conditions are
        // present we need to do evaluation
        // We can avoid iterating over the set iff reshuffling is not needed &
        // there is no iter eval condition
        if (isReshufflingNeeded || iterOperands != null) {
          // this.expansionList = Collections.EMPTY_LIST;
          this.checkList = null;
          // indexReults = QueryUtils.cutDownAndExpandIndexResults(indexReults,
          // indexFieldToItrsMapping, Collections.EMPTY_LIST, grpItrs,
          // context, Collections.EMPTY_LIST, iterOperands);
        } else {
          this.structType = QueryUtils.createStructTypeForRuntimeIterators(grpItrs);
          // indexReults.setElementType(structType);
          // Shuffling is not needed. Index results is a StructBag
          // with match level zero & no expansion needed & index fields map
          // with the RuntimeIterators. But we need to change the StructType
          // of the StructBag
          this.shufflingNeeded = false;
        }
      } else {
        // The finalList should not be left uninitialized, & if the match
        // level is zero
        // & the Index Results is a ResultBag ( & not an StructBag ) implying
        // indexFieldsSize of
        // 1 , then the final List should contain only the independent iterator
        this.finalList = new ArrayList();
        this.finalList.add(this.indpndntItr);
        Support.Assert(this.indexFieldToItrsMapping.length == 1,
            "In this else block , it should be guaranteed that there exists only one iterator in query as well as index from clause & that should be nothing but the independent RuntimeIterator of the group  ");
        this.indexFieldToItrsMapping[0] = this.indpndntItr;
        // Shuffling is needed if iter operand is not null even if index results is a
        // ResultSet
        // with match level zero & no expansion needed
        this.shufflingNeeded = (iterOperands != null);
      }
    } else {
      // There is some expansion or truncation needed on the data
      // obtained from index.Identify a the iterators belonging to this group
      // The independent iterator is added as the first element
      grpItrs = context.getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(this.indpndntItr);
      // Create an array of RuntimeIterators which map to the fields of the
      // Index set.
      // For those fields which do not have corresponding RuntimeIterator , keep
      // it as null;
      int pos = -1;
      this.finalList = completeExpansion ? context.getCurrentIterators() : grpItrs;
      // This is the List of runtimeIterators which have corresponding fields
      // in the resultset obtained from Index usage. This List will be populated
      // only if there exists fields in index resultset which will not be
      // selected
      // If all the fields of index resultset will be used , then this List
      // should
      // be null or empty
      this.checkList = new ArrayList();
      // This List contains the RuntimeIterators which are missing from
      // index resultset but are present in the final iterators
      this.expansionList = new LinkedList(finalList);
      RuntimeIterator tempItr = null;
      // boolean cutDownNeeded = false;
      int unMappedFields = indexFieldsSize;
      for (int i = 0; i < size; ++i) {
        pos = indexInfo.mapping[i];
        if (pos > 0) {
          tempItr = (RuntimeIterator) grpItrs.get(i);
          this.indexFieldToItrsMapping[pos - 1] = tempItr;
          this.expansionList.remove(tempItr);
          this.checkList.add(tempItr);
          --unMappedFields;
        }
      }
      boolean cutDownNeeded = unMappedFields > 0;
      if (!cutDownNeeded) {
        this.checkList = null;
      }
      /*
       * indexReults = QueryUtils.cutDownAndExpandIndexResults(indexReults, indexFieldToItrsMapping,
       * expansionList, finalList, context, checkList, iterOperands);
       */
    }
  }
}

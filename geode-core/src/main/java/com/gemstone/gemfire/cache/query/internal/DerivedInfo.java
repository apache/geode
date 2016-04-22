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

package com.gemstone.gemfire.cache.query.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.index.AbstractIndex;
import com.gemstone.gemfire.cache.query.internal.index.IndexProtocol;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;

public class DerivedInfo {
  public Map<String, SelectResults> derivedResults;
  public List<Object[]> newDerivatives;
  public List successfulOps = new LinkedList();
  public List originalOps;
  public CompiledValue currentOp;
  private List expansionList;

  public DerivedInfo() {
    derivedResults = new HashMap<String, SelectResults>();
    newDerivatives = new ArrayList<Object[]>();
  }

  public List getExpansionList() {
    return expansionList;
  }

  public void setExpansionList(List expansionList) {
    this.expansionList = expansionList;
  }

  public void setOriginalOps(List opsList) {
    originalOps = new LinkedList(opsList);
  }

  public List getRemainingOps() {
    List remainingOps = new LinkedList(originalOps);
    remainingOps.removeAll(successfulOps);
    return remainingOps;
  }

  public void addDerivedResults(IndexInfo indexInfo, SelectResults sr) {
    IndexProtocol index = indexInfo._index;
    String key = QueryUtils.getCompiledIdFromPath(indexInfo._path).getId() + ":" + index.getCanonicalizedIteratorDefinitions()[0];
    // String key = index.getCanonicalizedIteratorDefinitions()[0];
    if (derivedResults.containsKey(key)) {
      derivedResults.get(key).addAll(sr);
    } else {
      derivedResults.put(key, sr);
    }
    newDerivatives.add(new Object[] { QueryUtils.getCompiledIdFromPath(indexInfo._path).getId(), sr });
    successfulOps.add(currentOp);
  }

  public void addDerivedResults(IndexInfo indexInfo, SelectResults[] srs) {
    addDerivedResults(indexInfo, srs[0]);
    // Nested / range index is not supported at this time due to the way we cross the results
    // This solution would have duplicates. The problem is the way we doNestedIteration. The map would
    // have all values be associated with the current nested level object which is not what the values would represent
    // IndexProtocol index = indexInfo._index;
    // String[] definitions = index.getCanonicalizedIteratorDefinitions();
    // for (int i = 0 ; i < definitions.length; i++) {
    // String key = QueryUtils.getCompiledIdFromPath(indexInfo._path).getId() + ":" + definitions[i];
    // if (derivedResults.containsKey(key)) {
    // derivedResults.get(key).addAll(srs[i]);
    // }
    // else {
    // derivedResults.put(key, srs[i]);
    // }
    // }
    //
    // int indexToIterateOn = QueryUtils.figureOutWhichStructIndexToExtract(index);
    // newDerivatives.add(new Object[]{getCompiledIdFromPath(indexInfo._path).getId(), srs[indexToIterateOn]});
    // successfulOps.add(currentOp);

  }

  public void computeDerivedJoinResults(IndexInfo theCallingIndex, ExecutionContext context, CompiledValue iterOps) throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // Call this computeDerivedResults()
    // We are looking for join conditions so we can filter eval instead of iterate eval
    // Then we can apply the rest of the ops on the results
    if (theCallingIndex != null && iterOps != null) {
      if (iterOps instanceof CompiledJunction) {
        List opsList = ((CompiledJunction) iterOps).getOperands();
        this.setOriginalOps(opsList);
        createDerivedJoinResultsFromOpsList((QueryUtils.getCompiledIdFromPath(theCallingIndex._path)).getId(), context, opsList);
      } else if (iterOps.getType() == CompiledValue.COMPARISON) {
        createDerivedJoinResultsFromCC((QueryUtils.getCompiledIdFromPath(theCallingIndex._path)).getId(), (CompiledComparison) iterOps, context);
      }
    }
  }

  private void createDerivedJoinResultsFromOpsList(String theCallingIndexId, ExecutionContext context, List opsList) throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    Iterator iter = opsList.iterator();
    while (iter.hasNext()) {
      CompiledValue cv = (CompiledValue) iter.next();
      this.currentOp = cv;

      if (cv.getType() == CompiledValue.COMPARISON) {
        createDerivedJoinResultsFromCC(theCallingIndexId, (CompiledComparison) cv, context);
      }
    }
    // Now let's derive from our derivatives (for multiple join clauses that can be chained, such as a.id = 1 and a.id = b.id and b.id = c.id
    List<Object[]> newDerivatives = new ArrayList<Object[]>(this.newDerivatives);
    this.newDerivatives.clear();
    if (newDerivatives.size() > 0) {
      Iterator<Object[]> iterator = newDerivatives.iterator();
      while (iterator.hasNext()) {
        Object[] idDerivedAndResults = iterator.next();
        derivedDerivative(idDerivedAndResults, context, this.getExpansionList());
      }
    }
  }

  private void derivedDerivative(Object[] idDerivedAndResults, ExecutionContext context, List expansionList) throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {

    String idDerived = (String) idDerivedAndResults[0];
    SelectResults results = (SelectResults) idDerivedAndResults[1];
    RuntimeIterator ritr = getMatchingRuntimeIterator(idDerived, expansionList);
    List remainingOps = this.getRemainingOps();
    Iterator iterator = results.iterator();
    while (iterator.hasNext()) {
      Object val = iterator.next();
      ritr.setCurrent(val);
      createDerivedJoinResultsFromOpsList(idDerived, context, remainingOps);
    }

  }

  private RuntimeIterator getMatchingRuntimeIterator(String receiverId, List expansionList) throws QueryInvocationTargetException {
    Iterator iterator = expansionList.iterator();
    while (iterator.hasNext()) {
      RuntimeIterator ritr = (RuntimeIterator) iterator.next();
      if (ritr.getCmpIteratorDefn().getName().equals(receiverId)) {
        return ritr;
      }
    }
    throw new QueryInvocationTargetException("Unable to locate correct iterator for " + receiverId);
  }

  /*
   Example query : "Select * from /region1 r, /region2 s where r.id = 1 and r.id = s.id"
   Up until this point we have evaluated the r.id portion
   We determine if the path (r) matches any of the paths in the current cc (r.id = s.id)
   If so we figure out which side it matches (in this case the left side and create a new compiled comparison
   This new cc will set the left side as s.id and the right side as the evaluated value, in this case it happens to be 1 but
   it could be another field from the object instead.
   */
  private void createDerivedJoinResultsFromCC(String theCallingIndexReceiverId, CompiledComparison cc, ExecutionContext context) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    if (isCompiledPath(cc._right) && matchingPathIds(theCallingIndexReceiverId, cc._left)) {
      evaluateDerivedJoin(context, cc._right, new CompiledLiteral(cc._left.evaluate(context)), cc.getOperator());
    } else if (isCompiledPath(cc._left) && matchingPathIds(theCallingIndexReceiverId, cc._right)) {
      evaluateDerivedJoin(context, cc._left, new CompiledLiteral(cc._right.evaluate(context)), cc.getOperator());
    }
  }
 
  /*
   Called by createDerivedJoinResultsFromCCa
   Creates the new cc, executes the cc and releases any newly obtain index locks
  */ 
  private void evaluateDerivedJoin(ExecutionContext context, CompiledValue newLeftSide, CompiledValue newRightSide, int operator) 
   throws TypeMismatchException, FunctionDomainException, NameResolutionException, QueryInvocationTargetException {
   CompiledComparison dcc = createDerivedJoin(context, newLeftSide, newRightSide, operator);
   IndexInfo[] indexInfos = (IndexInfo[]) dcc.getIndexInfo(context);
   try {
     if (indexInfos != null && isValidIndexTypeToDerive(indexInfos[0]._getIndex())) {
       populateDerivedResultsFromDerivedJoin(context, dcc, indexInfos[0]);
     }
   } finally {
     if (indexInfos != null) {
       Index index = (Index) indexInfos[0]._index;
       Index prIndex = ((AbstractIndex) index).getPRIndex();
       if (prIndex != null) {
         ((PartitionedIndex) prIndex).releaseIndexReadLockForRemove();
       } else {
         ((AbstractIndex) index).releaseIndexReadLockForRemove();
       }
     }
   }
  }

  /*
   Does the evaluation/execution of the cc and stores them into our map
   We prevent limit and order by to be conducted by the index at this time as we do not those applied
   We have no idea what the other operands are and do not want to limit results as the first X results may not 
   fulfill all operands.
   */
  private void populateDerivedResultsFromDerivedJoin(ExecutionContext context, CompiledComparison dcc, IndexInfo indexInfo) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    // overwrite context values to disable limit, order by etc that should not be done by a derived join
    // If we apply limit at this point, we cannot guarantee that after we iterate, the we do not continue to
    // reduce the count below the limited amount
    Boolean originalCanApplyLimit = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX);
    context.cachePut(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX, Boolean.FALSE);
    Boolean originalCanApplyOrderBy = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
    context.cachePut(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX, Boolean.FALSE);

    SelectResults sr = dcc.filterEvaluate(context, null, false, null, null, false, false, false);

    context.cachePut(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX, originalCanApplyLimit);
    context.cachePut(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX, originalCanApplyOrderBy);
    ObjectType ot = indexInfo._index.getResultSetType();
    //The following if block is not currently used other than the else
    //This would be needed once we figure out how to handle nested object indexes (range, map, etc)
    //The issue we have right now with these indexes is the results will come back as a tuple, if we use those as is, we end up 
    //reusing the evaluated values even if they did not come from the top level object leading to duplicate results or incorrect tupling
    if (ot.isStructType()) {
      //createObjectResultsFromStructResults(indexInfo, sr);
    } else if (ot.isMapType()) {

    } else if (ot.isCollectionType()) {

    } else {
      this.addDerivedResults(dcc.getIndexInfo(context)[0], sr);
    }
  }

  //Not used at this time.  Was left over from attempt to speed up Range Indexes
  /*
  private void createObjectResultsFromStructResults(IndexInfo indexInfo, SelectResults sr) {
    Iterator srIterator = sr.iterator();
    SelectResults[] newSrs = null;

    while (srIterator.hasNext()) {
      Struct struct = (Struct) srIterator.next();
      Object[] fieldValues = struct.getFieldValues();
      int structLength = struct.getFieldValues().length;
      if (newSrs == null) {
        newSrs = new FakeSelectResults[structLength];
        for (int x = 0; x < structLength; x++) {
          newSrs[x] = new FakeSelectResults();
        }
      }
      for (int i = 0; i < structLength; i++) {
        newSrs[i].add(fieldValues[i]);
      }
    }

    if (newSrs != null) {
      this.addDerivedResults(indexInfo, newSrs);
    }
  }
  */

  private boolean isValidIndexTypeToDerive(IndexProtocol index) {
    ObjectType type = index.getResultSetType();
    return !(type.isCollectionType() || type.isMapType() || type.isStructType());
  }

  private CompiledComparison createDerivedJoin(ExecutionContext context, CompiledValue newLeft, CompiledValue newRight, int op)
    throws TypeMismatchException, NameResolutionException {
    CompiledComparison cc = new CompiledComparison(newLeft, newRight, op);
    cc.computeDependencies(context);
    return cc;
  }

  //Given a compiled value, we check to see if the receiver id of a CompiledPath matches the receiverId passed in
  private boolean matchingPathIds(String receiverId, CompiledValue cv) {
    if (isCompiledPath(cv)) {
      CompiledPath path = (CompiledPath)cv;
      return receiverId.equals(QueryUtils.getCompiledIdFromPath(path).getId());
    }
    return false;
  }

  private boolean isCompiledPath(CompiledValue cv) {
    return cv.getType() == CompiledValue.PATH;
  }

}

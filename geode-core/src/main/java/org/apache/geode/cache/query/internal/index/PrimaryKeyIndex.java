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
/*
 * PrimaryKeyIndex.java
 *
 * Created on March 20, 2005, 6:47 PM
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.ExecutionContext;
import com.gemstone.gemfire.cache.query.internal.QueryMonitor;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.QueryUtils;
import com.gemstone.gemfire.cache.query.internal.RuntimeIterator;
import com.gemstone.gemfire.cache.query.internal.Support;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.pdx.internal.PdxString;

/**
 * 
 */
public class PrimaryKeyIndex extends AbstractIndex  {

  protected long numUses = 0;
  ObjectType indexResultType;
 
  public PrimaryKeyIndex(String indexName, Region region,String fromClause,
      String indexedExpression,  String projectionAttributes,
      String origFromClause, String origIndxExpr, String[] defintions, IndexStatistics indexStatistics) {
    super(indexName, region, fromClause, indexedExpression,
        projectionAttributes, origFromClause, origIndxExpr, defintions, indexStatistics);
    //TODO : Asif Check if the below is correct
    Class constr = region.getAttributes().getValueConstraint();
    if (constr == null) constr = Object.class;
    this.indexResultType = new ObjectTypeImpl(constr);
    markValid(true);
  }

  public IndexType getType() {
    return IndexType.PRIMARY_KEY;
  }
  
  @Override
  protected boolean isCompactRangeIndex() {
    return false;
  }
  
  public ObjectType getResultSetType()  {
    return this.indexResultType;
  }

  void removeMapping(RegionEntry entry, int opCode) {
  }

  @Override
  void addMapping(RegionEntry entry) throws IMQException {
  }
  
  @Override
  void instantiateEvaluator(IndexCreationHelper ich) {    
  }
 
  @Override
  void lockedQuery(Object key, int operator, Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException {
    assert keysToRemove == null;
    //System.out.println("PrimaryKeyIndex.lockedQuery");
    //System.out.println(" key="+key);
    //System.out.println(" key.class="+(key != null ? key.getClass().getName()
    // : "null"));
    //    if(key == null){
    //      numUses++;
    //      return;
    //    }
    //key = TypeUtils.indexKeyFor(key);
    int limit = -1;
    
    //Key cannot be PdxString in a region
    if(key instanceof PdxString){
      key = key.toString();
    }

    Boolean applyLimit = (Boolean)context.cacheGet(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX); 
    if(applyLimit != null && applyLimit.booleanValue()) {
      limit = ((Integer)context.cacheGet(CompiledValue.RESULT_LIMIT)).intValue();
    }
    QueryObserver observer = QueryObserverHolder.getInstance();
    if (limit != -1 &&  results.size() == limit) {
      observer.limitAppliedAtIndexLevel(this, limit,results);
      return;
    }
    switch (operator) {
      case OQLLexerTokenTypes.TOK_EQ: {
        if (key != null && key != QueryService.UNDEFINED) {
          Region.Entry entry = ((LocalRegion)getRegion()).accessEntry(key, false);
          if (entry != null) {
            Object value = entry.getValue();
            if (value != null) {
              results.add(value);
            }
          }
        }
        break;
      }
      case OQLLexerTokenTypes.TOK_NE_ALT:
      case OQLLexerTokenTypes.TOK_NE: { // add all btree values
        Set values = (Set) getRegion().values();
        //Add data one more than the limit
        if(limit != -1) {
          ++limit;
        }
        // results.addAll(values);
        Iterator iter = values.iterator();
        while (iter.hasNext()) {
          // Check if query execution on this thread is canceled.
          QueryMonitor.isQueryExecutionCanceled();
          results.add(iter.next());
          if (limit != -1 &&  results.size() == limit) {
            observer.limitAppliedAtIndexLevel(this, limit,results);
            return;
          }
        }

        boolean removeOneRow = limit != -1;
        if (key != null && key != QueryService.UNDEFINED) {
          Region.Entry entry = ((LocalRegion)getRegion()).accessEntry(key, false);
          if (entry != null) {
            if (entry.getValue() != null) {
              results.remove(entry.getValue());
              removeOneRow = false;
            }
          }
        }
        if(removeOneRow) {
          Iterator itr = results.iterator();
          if(itr.hasNext()) {
            itr.next();
            itr.remove();
          }
        }
        break;
      }
      default: {
        throw new IllegalArgumentException(LocalizedStrings.PrimaryKeyIndex_INVALID_OPERATOR.toLocalizedString());
      }
    } // end switch
    numUses++;
  }

  void lockedQuery(Object key,
                   int operator,
                   Collection results,
                   CompiledValue iterOps,
                   RuntimeIterator runtimeItr,
                   ExecutionContext context,
                   List  projAttrib, SelectResults intermediateResults, boolean isIntersection)
  throws TypeMismatchException,
         FunctionDomainException,
         NameResolutionException,
         QueryInvocationTargetException
  {
   
    QueryObserver observer = QueryObserverHolder.getInstance();
    int limit = -1;    
    
   Boolean applyLimit = (Boolean)context.cacheGet(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX);
    
    if(applyLimit != null && applyLimit.booleanValue()) {
      limit = ((Integer)context.cacheGet(CompiledValue.RESULT_LIMIT)).intValue();
    }
    if(limit != -1 && results.size()== limit) {
      observer.limitAppliedAtIndexLevel(this, limit,results);
      return;
    }
    //Key cannot be PdxString in a region
    if(key instanceof PdxString){
      key = key.toString();
    }
    switch (operator) {
    case OQLLexerTokenTypes.TOK_EQ: {
      if (key != null && key != QueryService.UNDEFINED) {
        Region.Entry entry = ((LocalRegion)getRegion()).accessEntry(key, false);
        if (entry != null) {
          Object value = entry.getValue();
          if (value != null) {
            boolean ok = true;
            if (runtimeItr != null) {
              runtimeItr.setCurrent(value);
              ok = QueryUtils.applyCondition(iterOps, context);
            }
            if (ok) {
              applyProjection(projAttrib, context, results,value,intermediateResults,isIntersection);
            }
          }
        }
      }
      break;
    }
   
    case OQLLexerTokenTypes.TOK_NE_ALT:
    case OQLLexerTokenTypes.TOK_NE: { // add all btree values
      Set entries = (Set)getRegion().entrySet();
      Iterator itr = entries.iterator();
      while (itr.hasNext()) {       
        Map.Entry entry = (Map.Entry)itr.next();
            
        if(key != null && key != QueryService.UNDEFINED && key.equals(entry.getKey())) {
          continue;
        }
        Object val = entry.getValue();
        // TODO:Asif: is this correct. What should be the behaviour of null
        // values?
        if (val != null) {
          boolean ok = true;
          if (runtimeItr != null) {
            runtimeItr.setCurrent(val);
            ok = QueryUtils.applyCondition(iterOps, context);
          }
          if (ok) {
            applyProjection(projAttrib, context, results,val,intermediateResults,isIntersection);
          }
          if (limit != -1 && results.size() == limit){            
             observer.limitAppliedAtIndexLevel(this, limit,results);            
            break;
          }
        }
      }
//      if (key != null && key != QueryService.UNDEFINED) {
//        Region.Entry entry = getRegion().getEntry(key);
//        if (entry != null) {
//          Object val = entry.getValue();
//          if (val != null) {
//            boolean ok = true;
//            if (runtimeItr != null) {
//              runtimeItr.setCurrent(val);
//              ok = QueryUtils.applyCondition(iterOps, context);
//            }
//            if (ok) {
//              applyProjection(projAttrib, context, results,val,intermediateResults,isIntersection);
//            }
//          }
//        }
//      }
      break;
    }
    default: {
      throw new IllegalArgumentException("Invalid Operator");
    }
    } // end switch
    numUses++;
  }

  void recreateIndexData() throws IMQException {
    Support
        .Assert(
            false,
            "PrimaryKeyIndex::recreateIndexData: This method should not have got invoked at all");
  }

  public boolean clear() throws QueryException {
    return true;
  }
  

  protected InternalIndexStatistics createStats(String indexName) {
    return new PrimaryKeyIndexStatistics();
  }

  class PrimaryKeyIndexStatistics extends InternalIndexStatistics {
    /**
     * Returns the total number of times this index has been accessed by a
     * query.
     */
    public long getTotalUses() {
      return numUses;
    }

    /**
     * Returns the number of keys in this index.
     */
    public long getNumberOfKeys() {
      return getRegion().keys().size();
    }

    /**
     * Returns the number of values in this index.
     */
    public long getNumberOfValues() {
      return getRegion().values().size();
    }

    /**
     * Return the number of values for the specified key in this index.
     */
    public long getNumberOfValues(Object key) {
      if (getRegion().containsValueForKey(key)) return 1;
      return 0;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("No Keys = ").append(getNumberOfKeys()).append("\n");
      sb.append("No Values = ").append(getNumberOfValues()).append("\n");
      sb.append("No Uses = ").append(getTotalUses()).append("\n");
      sb.append("No Updates = ").append(getNumUpdates()).append("\n");
      sb.append("Total Update time = ").append(getTotalUpdateTime()).append(
          "\n");
      return sb.toString();
    }
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.cache.query.internal.index.AbstractIndex#lockedQuery(java.lang.Object,
   *      int, java.lang.Object, int, java.util.Collection, java.util.Set)
   */

  @Override
  void lockedQuery(Object lowerBoundKey, int lowerBoundOperator,
      Object upperBoundKey, int upperBoundOperator, Collection results,
      Set keysToRemove, ExecutionContext context) throws TypeMismatchException {
    throw new UnsupportedOperationException(
        LocalizedStrings.PrimaryKeyIndex_FOR_A_PRIMARYKEY_INDEX_A_RANGE_HAS_NO_MEANING
          .toLocalizedString());

  }  
  
  public int getSizeEstimate(Object key, int op, int matchLevel) {
    return 1;
  }

  @Override
  void addMapping(Object key, Object value, RegionEntry entry)
      throws IMQException
  {
    // TODO Auto-generated method stub
    
  }  

  @Override
  void saveMapping(Object key, Object value, RegionEntry entry)
      throws IMQException
  {
    // Do Nothing; We are not going to call this for PrimaryKeyIndex ever.
  }


  public boolean isEmpty() {
    return createStats("primaryKeyIndex").getNumberOfKeys() == 0 ? true : false;
  }
}

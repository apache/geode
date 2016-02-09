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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.NameNotFoundException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.index.AbstractIndex;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.TypeUtils;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.internal.PdxString;

/**
 * Class Description
 * 
 * @version $Revision: 1.2 $
 * @author ericz
 * @author asif
 */
public class CompiledSelect extends AbstractCompiledValue {
  
  protected List<CompiledSortCriterion> orderByAttrs; //order by attributes: list of CompiledValue
  private CompiledValue whereClause; // can be null if there isn't one
  private List iterators; // fromClause: list of CompiledIteratorDefs
  protected List projAttrs; //projection attributes: list of Object[2]:
     // 0 is projection name, 1 is the CompiledValue for the expression 
  private boolean distinct;
  private boolean count;
  //Asif: limits the SelectResults by the number specified.
  private CompiledValue limit;
  //Shobhit: counts the no of results satisfying where condition for
  // count(*) non-distinct queries where no indexes are used.
  private int countStartQueryResult = 0;
  
  protected List<CompiledValue> groupBy = null;
  //Are not serialized and are recreated when compiling the query
  private List<String> hints;
  protected boolean transformationDone = false;
  protected ObjectType cachedElementTypeForOrderBy = null;
  private boolean hasUnmappedOrderByCols = false; 

  //used as a key in a context to identify the scope of this CompiledSelect 
  private Object scopeID = new Object(); 
  
  /*
   * Set in context for the where clause to signify that it has been evaluated at least one time
   * for any other CompiledValue that may use precalculated indexes
   * we want to mark this as Evaluated so that we don't unlock locks
   * that don't belong to this iteration of evaluate.
   * This is similar to how CompiledComparisons store their IndexInfo in the context
   * but for example a CompiledJunction that uses 2 Comparisons
   * would have unlocked the readlocks because we check to see if the clause has a mapped value
   * in the context. Because CompiledJunctions did not, we unlocked the read locks.
   * Now we set a value so that it will not do this. See where we use this value to see how unlock is determined
   */
  private final static String CLAUSE_EVALUATED = "Evaluated";

  public CompiledSelect(boolean distinct, boolean count, CompiledValue whereClause,
                        List iterators, List projAttrs,List<CompiledSortCriterion> orderByAttrs, CompiledValue limit,
                        List<String> hints, List<CompiledValue> groupByClause) {
    this.orderByAttrs = orderByAttrs;
    this.whereClause = whereClause;
    this.iterators = iterators;
    this.projAttrs = projAttrs;
    this.distinct = distinct;
    this.count = count;
    this.limit = limit;
    this.hints = hints;
    this.groupBy = groupByClause;
     
  }

  @Override
  public List getChildren() {
    List list = new ArrayList();
    if (this.whereClause != null) {
      list.add(this.whereClause);
    }
    
    list.addAll(iterators);
    
    if (this.projAttrs != null) {
      // extract the CompiledValues out of the projAttrs (each of which are Object[2])
      for (Iterator itr = this.projAttrs.iterator(); itr.hasNext(); ) {
        list.add(((Object[])itr.next())[1]);
      }
    }
    
    if (this.orderByAttrs != null) {
      list.addAll(this.orderByAttrs);
    }
    
    return list;
  }
  
  public boolean isDistinct() { 
    return this.distinct;
  }
  
  public boolean isGroupBy() { 
    return this.groupBy != null;
  }
  
  public boolean isOrderBy() { 
    return this.orderByAttrs != null;
  }
    
  public void setDistinct(boolean distinct) {
    this.distinct = distinct;
  }

  public boolean isCount() { 
    return this.count;
  }
    
  public void setCount(boolean count) {
    this.count = count;
  }
  public int getType() {
    return LITERAL_select;
  }

  public CompiledValue getWhereClause() {
    return this.whereClause;
  }
  
  public List getIterators() {
    return this.iterators;
  }
  
  public List getProjectionAttributes() {
    return this.projAttrs;
  }
  
  public List<CompiledSortCriterion> getOrderByAttrs() {
    return this.orderByAttrs;
  }
  
  @Override
  public Set computeDependencies(ExecutionContext context)
  throws TypeMismatchException,
         AmbiguousNameException,
         NameResolutionException {
    // bind iterators in new scope in order to determine dependencies
    context.cachePut(scopeID, context.assosciateScopeID());
    context.newScope((Integer)context.cacheGet(scopeID));
    context.pushExecCache((Integer)context.cacheGet(scopeID));
    try {
      Iterator iter = this.iterators.iterator();
      while (iter.hasNext()) {
        
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        // compute dependencies on this iter first before adding its
        // RuntimeIterator to the current scope.
        // this makes sure it doesn't bind attributes to itself
        context.addDependencies(this, iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);    
        context.addToIndependentRuntimeItrMap(iterDef);
        context.bindIterator(rIter);
      }
      
      //    is the where clause dependent on itr?
      if (this.whereClause != null) {
        context.addDependencies(this, this.whereClause.computeDependencies(context));
      }
      // are any projections dependent on itr?
      if (this.projAttrs != null) {
        Set totalDependencySet = null;
        for (iter = this.projAttrs.iterator(); iter.hasNext();) {
          // unwrap the projection expressions, they are in 2-element Object[]
          // with first element the fieldName and second the projection
          // expression
          Object[] prj = (Object[]) TypeUtils.checkCast(iter.next(), Object[].class);
          CompiledValue prjExpr = (CompiledValue) TypeUtils.checkCast(prj[1], CompiledValue.class);
          totalDependencySet  = context.addDependencies(this, prjExpr.computeDependencies(context));
        }
        this.doTreeTransformation(context);
        
        return totalDependencySet;
      }else {
        this.doTreeTransformation(context);
        return context.getDependencySet(this, true);
      }
      // is the where clause dependent on itr?
      /*
      if (this.whereClause != null) {
        return context.addDependencies(this, this.whereClause.computeDependencies(context));
      }
      else {
        return context.getDependencySet(this, true);
      }*/
    }
    finally {
      context.popExecCache();
      context.popScope();
    }
  }
  
  protected void doTreeTransformation(ExecutionContext context) throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    if(!this.transformationDone) {
      this.cachedElementTypeForOrderBy = prepareResultType(context);
      this.mapOrderByColumns(context);
      this.transformGroupByIfPossible(context);         
    }
    this.transformationDone = true;
  }
  
  /**
   * Transforms the group by clause into distinct order by clause, if possible
   * @param context
   * @throws AmbiguousNameException
   * @throws TypeMismatchException
   * @throws NameResolutionException
   */
  private void transformGroupByIfPossible(ExecutionContext context) 
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException  {
    //for time being assume that the group by cols are explicitly mentioned in proj
    if(this.groupBy != null) {
      List projAttribs = this.projAttrs;
      if(projAttribs == null) {
        projAttribs = new ArrayList();
        List currentIters = context.getCurrentIterators();
        for(Object o: currentIters) {
          RuntimeIterator rIter = (RuntimeIterator)o;
          String name = rIter.getName();
          projAttribs.add(new Object[]{name, rIter});
        }
      }
      
      if(projAttribs != null && projAttribs.size() != this.groupBy.size()) {
        throw new QueryInvalidException(
            LocalizedStrings.DefaultQuery_PROJ_COL_ABSENT_IN_GROUP_BY.toLocalizedString() +" or "
            + LocalizedStrings.DefaultQuery_GROUP_BY_COL_ABSENT_IN_PROJ.toLocalizedString());
      }
     
      boolean shouldTransform = true;
      StringBuffer lhsBuffer = new StringBuffer();
      StringBuffer rhsBuffer = new StringBuffer();
      
      outer :for(int i = 0; i < projAttribs.size() ; ++i) {
        Object[] prj = (Object[]) TypeUtils.checkCast(projAttribs.get(i), Object[].class);
        CompiledValue groupByAttr = this.groupBy.get(i);
        if(prj[0] != null) {
          if( groupByAttr instanceof CompiledID ) {
            if (prj[0].equals(((CompiledID)groupByAttr).getId())) {              
              lhsBuffer.delete(0,lhsBuffer.length());
              rhsBuffer.delete(0,rhsBuffer.length());
              continue;  
            }
          }
        }
        CompiledValue cvProj = (CompiledValue) TypeUtils.checkCast(prj[1], CompiledValue.class);
        cvProj.generateCanonicalizedExpression(lhsBuffer, context);
        groupByAttr.generateCanonicalizedExpression(rhsBuffer, context);
        if(lhsBuffer.length() == rhsBuffer.length()) {          
          for(int indx = 0; indx < lhsBuffer.length() ; ++indx) {
            if(lhsBuffer.charAt(indx) != rhsBuffer.charAt(indx)) {
              shouldTransform = false;
              break outer;
            }
          }
        }else {
          shouldTransform = false;
          break;
        }     
       
        lhsBuffer.delete(0,lhsBuffer.length());
        rhsBuffer.delete(0,rhsBuffer.length());
        
      }
      //check if the order by clause is null or order by clause is same as proj.
      //for now check if order by is null
      if(shouldTransform && this.orderByAttrs == null) {
        this.modifyGroupByToOrderBy(true, context);
      }else {
        throw new QueryInvalidException(
            LocalizedStrings.DefaultQuery_PROJ_COL_ABSENT_IN_GROUP_BY.toLocalizedString() +" or "
            + LocalizedStrings.DefaultQuery_GROUP_BY_COL_ABSENT_IN_PROJ.toLocalizedString());
      }
    }
  }
  
  protected void modifyGroupByToOrderBy(boolean setDistinct, ExecutionContext context) throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    if(setDistinct) {
      this.distinct = setDistinct;
    }
    this.orderByAttrs = new ArrayList<CompiledSortCriterion>(this.groupBy.size());
    int colIndex = 0;
    for(CompiledValue cv : this.groupBy) {      
      CompiledSortCriterion csc = new CompiledSortCriterion(false, cv);      
      csc.mapExpressionToProjectionField(projAttrs, context) ;       
      this.orderByAttrs.add(csc);
    }
    this.groupBy = null;
  }
  
  private void mapOrderByColumns(ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException,
      NameResolutionException {
    if (this.orderByAttrs != null) {
      Iterator<CompiledSortCriterion> iter = this.orderByAttrs.iterator();
      while (iter.hasNext()) {
        CompiledSortCriterion csc = iter.next();

        // Asif: Ideally for replicated regions, the requirement that
        // projected columns should
        // contain order by fields ( directly or derivable on it),
        // is not needed. But for PR , the query gathers only projected
        // columns, so applying order by on the query node
        // will need order by values ( which we dont send). So this
        // restriction is needed.
        // Also if this restriction is assumed to be correct, then the order
        // by comparator can be optimized as
        // it does not need to keep the mapping of evaluated order by clause,
        // for comparison
        if (!csc.mapExpressionToProjectionField(this.projAttrs, context)) {          
          this.hasUnmappedOrderByCols = true;
        }
      }
    }
  }
  
  
  private void evalCanonicalizedExpressionForCSC(CompiledSortCriterion csc, ExecutionContext context,
      StringBuffer buffer) throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    csc.getExpr().generateCanonicalizedExpression(buffer, context);
  }

  /* Gets the appropriate empty results set when outside of actual query evalutaion.
   *
   * @param parameters the parameters that will be passed into the query when evaluated
   * @param cache the cache the query will be executed in the context of
   * @return the empty result set of the appropriate type
   */
  public SelectResults getEmptyResultSet(Object[] parameters, Cache cache, Query query)
  throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    ExecutionContext context = new QueryExecutionContext(parameters, cache, query);
    computeDependencies(context);
    context.newScope((Integer)context.cacheGet(scopeID));
    context.pushExecCache((Integer)context.cacheGet(scopeID));
    SelectResults results = null;
    try {
      Iterator iter = iterators.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);      
      }
      results = prepareEmptyResultSet(context,false);
    }
    finally {
      context.popScope();
      context.popExecCache();
    }
    return results;
  }
  
  public ObjectType getElementTypeForOrderByQueries() {
    return this.cachedElementTypeForOrderBy;
  }
  
  public SelectResults evaluate(ExecutionContext context) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    context.newScope((Integer)context.cacheGet(scopeID));
    context.pushExecCache((Integer)context.cacheGet(scopeID));
    context.setDistinct(this.distinct);
    if(this.hasUnmappedOrderByCols && context.getBucketList() != null) {
      throw new QueryInvalidException(LocalizedStrings.DefaultQuery_ORDER_BY_ATTRIBS_NOT_PRESENT_IN_PROJ.toLocalizedString()); 
    }
    if (hints != null) {
      context.cachePut(QUERY_INDEX_HINTS, hints);
    }

    try {
      //set flag to keep objects serialized for "select *" queries
      if((DefaultQuery)context.getQuery() != null){
        ((DefaultQuery)context.getQuery()).keepResultsSerialized(this, context);
      }
      Iterator iter = iterators.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        //Asif . Ideally the function below should always be called after binding has occured
        //So that the interal ID gets set during binding to the scope. If not so then chances
        // are that internal_id is still null causing index_interanl_id to be null.
        //Though in our case it may not be an issue as the compute depedency phase must have
        //already set the index id
      }
      Integer limitValue = evaluateLimitValue(context, this.limit);
      SelectResults result = null;
      boolean evalAsFilters = false;
      if (this.whereClause == null) {
        result = doIterationEvaluate(context, false);
      }
      else {
        if (!this.whereClause.isDependentOnCurrentScope(context)) { // independent
                                                                    // where
                                                                    // @todo
                                                                    // check
                                                                    // for
                                                                    // dependency
                                                                    // on
                                                                    // current
                                                                    // scope
                                                                    // only?
          // clause
          Object b = this.whereClause.evaluate(context);
          if (b == null || b == QueryService.UNDEFINED) {
            // treat as if all elements are undefined
            result = prepareEmptyResultSet(context,false);
            //ResultsSet.emptyResultsSet(resultSet, 0);
            //return result;
          }
          else if (!(b instanceof Boolean)) {
            throw new TypeMismatchException(LocalizedStrings.CompiledSelect_THE_WHERE_CLAUSE_WAS_TYPE_0_INSTEAD_OF_BOOLEAN.toLocalizedString(b.getClass().getName()));
          }
          else if (((Boolean) b).booleanValue()) {
            result = doIterationEvaluate(context, false);
          }
          else {
            result = prepareEmptyResultSet(context,false);
            //ResultsSet.emptyResultsSet(resultSet, 0);
            //return result;
          }
        }
        else {
          //Check the numer of independent iterators
          int numInd = context.getAllIndependentIteratorsOfCurrentScope().size();
          //If order by clause is defined, then the first column should be the preferred index 
          if(this.orderByAttrs != null && numInd == 1) {
            CompiledSortCriterion csc = (CompiledSortCriterion)orderByAttrs.get(0);
            StringBuffer preferredIndexCondn = new StringBuffer();
            this.evalCanonicalizedExpressionForCSC(csc, context, preferredIndexCondn);
            context.cachePut(PREF_INDEX_COND, preferredIndexCondn.toString());        
          }
          boolean unlock = true;
          Object obj = context.cacheGet(this.whereClause);
          if(obj != null && (obj instanceof IndexInfo[] || obj.equals(CLAUSE_EVALUATED))) {
            // if indexinfo is cached means the read lock 
            // is not being taken this time, so releasing 
            // the lock is not required
            unlock = false;
          }
          // see if we should evaluate as filters,
          // and count how many actual index lookups will be performed
          PlanInfo planInfo = this.whereClause.getPlanInfo(context);
          if (context.cacheGet(this.whereClause) == null) {
            context.cachePut(this.whereClause, CLAUSE_EVALUATED);
          }
          try {
            evalAsFilters = planInfo.evalAsFilter;
            // let context know if there is exactly one index lookup
            context.setOneIndexLookup(planInfo.indexes.size() == 1);
            if (evalAsFilters) {
              ((QueryExecutionContext)context).setIndexUsed(true);
              // Ignore order by attribs for a while

              boolean canApplyOrderByAtIndex = false;
              if (limitValue >= 0
                  && numInd == 1
                  && ((Filter) this.whereClause)
                      .isLimitApplicableAtIndexLevel(context)) {
                context.cachePut(CAN_APPLY_LIMIT_AT_INDEX, Boolean.TRUE);
              }
              StringBuffer temp = null;
              if (this.orderByAttrs != null) {
                temp = new StringBuffer();
                CompiledSortCriterion csc = (CompiledSortCriterion) this.orderByAttrs.get(0);
                this.evalCanonicalizedExpressionForCSC(csc, context, temp);
              }

              boolean needsTopLevelOrdering = true;
              if (temp != null
                  && numInd == 1
                  && ((Filter) this.whereClause)
                      .isOrderByApplicableAtIndexLevel(context, temp.toString())) {
                context.cachePut(CAN_APPLY_ORDER_BY_AT_INDEX, Boolean.TRUE);
                context.cachePut(ORDERBY_ATTRIB, this.orderByAttrs);
                canApplyOrderByAtIndex = true;
                if (this.orderByAttrs.size() == 1) {
                  needsTopLevelOrdering = false;
                  //If there is a limit present and we are executing on a partitioned region
                  //we should use a sorted set
                  if (this.limit != null) {
                    //Currently check bucket list to determine if it's a pr query
                    if (context.getBucketList() != null && context.getBucketList().size() > 0) {
                      needsTopLevelOrdering = true;
                    } 
                  }
                }
              } else if (temp != null) {
                // If order by is present but cannot be applied at index level,
                // then limit also cannot be applied
                // at index level
                context.cachePut(CAN_APPLY_LIMIT_AT_INDEX, Boolean.FALSE);
              }

              context.cachePut(RESULT_LIMIT, limitValue);
              if (numInd == 1
                  && ((Filter) this.whereClause)
                      .isProjectionEvaluationAPossibility(context)
                  && (this.orderByAttrs == null || (canApplyOrderByAtIndex && !needsTopLevelOrdering))
                  && this.projAttrs != null) {
                // Possibility of evaluating the resultset as filter itself
                ObjectType resultType = this.cachedElementTypeForOrderBy != null? this.cachedElementTypeForOrderBy :this.prepareResultType(context);
                context.cachePut(RESULT_TYPE, resultType);
                context.cachePut(PROJ_ATTRIB, this.projAttrs);
              }
              
              
              result = ((Filter) this.whereClause)
                  .filterEvaluate(context, null);
              if (!(context.cacheGet(RESULT_TYPE) instanceof Boolean)) {
                QueryObserverHolder.getInstance()
                    .beforeApplyingProjectionOnFilterEvaluatedResults(result);
                result = applyProjectionOnCollection( result, context,
                    !needsTopLevelOrdering);
              }
            } else {
              // otherwise iterate over the single from var to evaluate
              result = doIterationEvaluate(context, true);
            }
          } finally {
            // The Read lock is acquired in {@link
            // IndexManager#getBestMatchIndex()},
            // because we need to select index which can be read-locked.
            if(unlock) {
              releaseReadLockOnUsedIndex(planInfo);
            }
          }
        }
      }
      //TODO:Asif: It does not appear that results would be null ever. 
      //if (result == null) { return QueryService.UNDEFINED; }
      assert result != null;
      // drop duplicates if this is DISTINCT
      if (result instanceof SelectResults) {
        SelectResults sr = (SelectResults) result;
        CollectionType colnType = sr.getCollectionType();
        //if (this.distinct && colnType.allowsDuplicates()) {
        if (this.distinct) {
          Collection r;
          //Set s = sr.asSet();
          if (colnType.allowsDuplicates()) {
          // don't just convert to a ResultsSet (or StructSet), since
          // the bags can convert themselves to a Set more efficiently
            r = sr.asSet();
          }else {
            r = sr;
          }
          
          result = new ResultsCollectionWrapper(colnType.getElementType(), r, limitValue);
          if (r instanceof Bag.SetView) {
            ((ResultsCollectionWrapper)result).setModifiable(false);
          }
        }
        else {
          // SelectResults is of type
          if (limitValue > -1) {
            ((Bag)sr).applyLimit(limitValue);
          }
        }

        /*
         * We still have to get size of SelectResults in some cases like,
         * if index was used OR query is a distinct query.
         * 
         * If SelectResult size is zero then we need to put Integer for 0
         * count.
         */
        if (this.count) {
          SelectResults res = (SelectResults) result;
          
          if ((this.distinct || evalAsFilters || countStartQueryResult == 0)) {
            // Retrun results as it is as distinct is applied
            // at coordinator node for PR queries.
            if (context.getBucketList() != null && this.distinct) {
              return result;
            }
            //Take size and empty the results
            int resultCount = res.size();
            res.clear();
            
            ResultsBag countResult = new ResultsBag(new ObjectTypeImpl(Integer.class), context.getCachePerfStats());
            countResult.addAndGetOccurence(resultCount);
            result = countResult;

          } else {
            ((ResultsBag)res).addAndGetOccurence(countStartQueryResult);
          }
        }
      }
      return result;
    }
    finally {
      context.popScope();
      context.popExecCache();
    }
  }
  
  /**
   * The index is locked during query to prevent it from being
   * removed by another thread. So we have to release the lock only after
   * whole query is finished as one query can use an index multiple times.
   * 
   * 
   * @param planInfo
   */
  private void releaseReadLockOnUsedIndex(PlanInfo planInfo) {
    List inds = planInfo.indexes;
    for (Object obj : inds) {
      Index index = (Index) obj;
      Index prIndex = ((AbstractIndex) index).getPRIndex();
      if (prIndex != null) {
        ((PartitionedIndex) prIndex).releaseIndexReadLockForRemove();
      } else {      
         ((AbstractIndex) index).releaseIndexReadLockForRemove();
      }
    }
  }


  /**
   * Retruns the size of region iterator for count(*) on a region without
   * whereclause.
   * @param collExpr 
   * @throws RegionNotFoundException 
   * @since 6.6.2
   */
  private int getRegionIteratorSize(ExecutionContext context,
      CompiledValue collExpr) throws RegionNotFoundException {
    Region region;
    String regionPath = ((CompiledRegion) collExpr).getRegionPath();
    if (context.getBucketRegion() == null) {
      region = context.getCache().getRegion(regionPath);
    } else {
      region = context.getBucketRegion();
    }
    if (region != null) {
      return region.size();
    } else {
      // if we couldn't find the region because the cache is closed, throw
      // a CacheClosedException
      Cache cache = context.getCache();
      if (cache.isClosed()) {
        throw new CacheClosedException();
      }
      throw new RegionNotFoundException(
          LocalizedStrings.CompiledRegion_REGION_NOT_FOUND_0
              .toLocalizedString(regionPath));
    }
  }

  public int getLimitValue(Object[] bindArguments) throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    //if evaluation of the limit fails, we default to no limit
    return evaluateLimitValue(bindArguments);
  }
  
  // returns null if result is UNDEFINED
  private SelectResults doIterationEvaluate(ExecutionContext context, boolean evaluateWhereClause)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    
    SelectResults results = prepareEmptyResultSet(context,false);
    
    //TODO:Asif: SELF : Work on limit implementation on bulk get    
    // check for bulk get optimization
    if (evaluateWhereClause) {
      List tmpResults = optimizeBulkGet(context);
      if (tmpResults != null) {
        // (has only one iterator)
        RuntimeIterator rIter = (RuntimeIterator)context.getCurrentIterators().get(0); 
        for (Iterator itr = tmpResults.iterator(); itr.hasNext(); ) {
          Object currObj = itr.next();
          rIter.setCurrent(currObj);
          QueryObserver observer = QueryObserverHolder.getInstance();
          observer.beforeIterationEvaluation(rIter, currObj);
          applyProjectionAndAddToResultSet(context, results, this.orderByAttrs ==null);
        }
        return results;
      }
    }
    int numElementsInResult = 0;
    try {
      doNestedIterations(0, results, context, evaluateWhereClause,numElementsInResult);
    }catch(CompiledSelect.NullIteratorException cnie) {
      return null;
    }
    return results;
  }
  
  // @todo make this more general to work for any kind of map, not just regions
  /**
   * Check for the bulk-get pattern and if it applies do an optimized execution.
   * The pattern is: SELECT ?? FROM <Region>.entrySet e WHERE e.key IN <Collection>.
   *
   * @return a List of entries if optimization was executed,
   * or null if it wasn't because the optimization pattern didn't match
   */
  private List optimizeBulkGet(ExecutionContext context)
  throws TypeMismatchException, FunctionDomainException, NameResolutionException,
  QueryInvocationTargetException {
    List iterList = context.getCurrentIterators();
    // must be one iterator
    if (iterList.size() != 1) {
      return null;
    }
    
    // where clause must be an IN operation
    if (!(this.whereClause instanceof CompiledIn)) {
      return null;
    }
    
    RuntimeIterator rIter = (RuntimeIterator)iterList.get(0);
    CompiledIteratorDef cIterDef = rIter.getCmpIteratorDefn();
    CompiledValue colnExpr = cIterDef.getCollectionExpr();
    
    // check for region.entrySet or region.entrySet()
    boolean match = false;
    CompiledRegion rgn = null;
    if (colnExpr instanceof CompiledPath) {
      CompiledPath cPath = (CompiledPath)colnExpr;
      CompiledValue rcvr = cPath.getReceiver();
      if (rcvr instanceof CompiledRegion) {
        rgn = (CompiledRegion)rcvr;
        String attr = cPath.getTailID();
        match = attr.equals("entrySet");
      }
    }
    if (!match && (colnExpr instanceof CompiledOperation)) {
      CompiledOperation cOp = (CompiledOperation)colnExpr;
      CompiledValue rcvr = cOp.getReceiver(context);
      if (rcvr instanceof CompiledRegion) {
        rgn = (CompiledRegion)rcvr;
        match = cOp.getMethodName().equals("entrySet");
      }
    }
    if (!match) {
      return null;
    }
    
    // check for IN expression
    CompiledIn cIn = (CompiledIn)this.whereClause;
    // defer to the CompiledIn for rest of pattern match and
    // evaluation
    return cIn.optimizeBulkGet(rgn, context);
  }

  // returns the number of elements added in the return ResultSet
  private int doNestedIterations(int level, SelectResults results, ExecutionContext context,
      boolean evaluateWhereClause, int numElementsInResult) throws TypeMismatchException, AmbiguousNameException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException, CompiledSelect.NullIteratorException {
    List iterList = context.getCurrentIterators();
    if (level == iterList.size()) {
      boolean addToResults = true;
      if (evaluateWhereClause) {
        Object result = this.whereClause.evaluate(context);
        QueryObserver observer = QueryObserverHolder.getInstance();
        observer.afterIterationEvaluation(result);
        if (result == null) {
          addToResults = false;
        }
        else if (result instanceof Boolean) {
          addToResults = ((Boolean)result).booleanValue();
        }
        else if (result == QueryService.UNDEFINED) {
          // add UNDEFINED to results only for NOT EQUALS queries
          if (this.whereClause.getType() == COMPARISON) {
            int operator = ((CompiledComparison) this.whereClause).getOperator();
            if ((operator != TOK_NE && operator != TOK_NE_ALT)) {
              addToResults = false;
            } 
          } else {
            addToResults = false;
          }
        }
        else {
          throw new TypeMismatchException(LocalizedStrings.CompiledSelect_THE_WHERE_CLAUSE_WAS_TYPE_0_INSTEAD_OF_BOOLEAN.toLocalizedString(result.getClass().getName()));
        }
      }
      if (addToResults) {
        int occurence = applyProjectionAndAddToResultSet(context, results, this.orderByAttrs == null);
        // Asif: If the occurence is greater than 1, then only in case of
        // non distinct query should it be treated as contributing to size
        // else duplication will be eliminated when making it distinct using
        // ResultsCollectionWrapper and we will fall short of limit
        if (occurence == 1 || (occurence > 1 && !this.distinct)) {
          // Asif: (Unique i.e first time occurence) or subsequent occurence
          // for non distinct query
          ++numElementsInResult;
        }
      }     
    }
    else {
      RuntimeIterator rIter = (RuntimeIterator) iterList.get(level);
      SelectResults sr = rIter.evaluateCollection(context);
      if (sr == null) {
        return 0; //continue iteration if a collection evaluates to UNDEFINED
      }
      
      // Check if its a non-distinct count(*) query without where clause, in that case, 
      // we can size directly on the region.
      if (this.whereClause == null && iterators.size() == 1 && isCount() && !isDistinct() && 
          sr instanceof QRegion) {
        QRegion qr = (QRegion)sr;
        countStartQueryResult = qr.getRegion().size();
        return 1;
      }

      // #44807: select * query should not deserialize objects
      // In case of "select *" queries we can keep the results in serialized
      // form and send it to the client.
      if ((DefaultQuery) context.getQuery() != null
          && ((DefaultQuery) context.getQuery()).isKeepSerialized()
          && sr instanceof QRegion) {
        ((QRegion) sr).setKeepSerialized(true);
      }

      // Iterate through the data set.
      Iterator cIter = sr.iterator();
      while (cIter.hasNext()) {
        // Check if query execution on this thread is canceled.
        QueryMonitor.isQueryExecutionCanceled();

        Object currObj = cIter.next();
        rIter.setCurrent(currObj);
        QueryObserver observer = QueryObserverHolder.getInstance();
        observer.beforeIterationEvaluation(rIter, currObj);
        numElementsInResult = doNestedIterations(level + 1, results, context,
            evaluateWhereClause, numElementsInResult);
        Integer limitValue = evaluateLimitValue(context, this.limit);
        if (this.orderByAttrs == null && limitValue > -1 && numElementsInResult == limitValue) {
          break;
        }        
      }     
    }
    return numElementsInResult;
  }

  private SelectResults applyProjectionOnCollection(SelectResults resultSet,
      ExecutionContext context, boolean ignoreOrderBy) throws TypeMismatchException,
      AmbiguousNameException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    List iterators = context.getCurrentIterators();
    if (projAttrs == null && (this.orderByAttrs == null || ignoreOrderBy )) {
      // Asif : If the projection attribute is null( ie.e specified as *) &
      // there is only one
      // Runtime Iteratir we can return the set as it is.But if the proejction
      // attribute is null & multiple Iterators are defined we need to rectify
      // the
      // the StructBag that is returned. It is to be noted that in case of
      // single from clause
      // where the from clause itself is defined as nested select query with
      // multiple
      // from clauses , the resultset returned will be a StructBag which we
      // have to return
      // as it is.
      if (iterators.size() > 1) {
        StructType type = createStructTypeForNullProjection(iterators, context);
        ((SelectResults)resultSet).setElementType(type);
      }

      return resultSet;
    }
    else {      
      int numElementsAdded = 0;
      SelectResults pResultSet = prepareEmptyResultSet(context,ignoreOrderBy);
      boolean isStructType = resultSet.getCollectionType().getElementType() != null 
          && resultSet.getCollectionType().getElementType().isStructType();  
      if (isStructType) {
        Iterator resultsIter = resultSet.iterator();
        // Apply limit if there is no order by.
        Integer limitValue = evaluateLimitValue(context, this.limit);
        while (( (this.orderByAttrs != null && !ignoreOrderBy)|| limitValue < 0 || (numElementsAdded < limitValue))
            && resultsIter.hasNext()) {
          // Check if query execution on this thread is canceled.
          QueryMonitor.isQueryExecutionCanceled();

          Object values[] = ((Struct)resultsIter.next()).getFieldValues();
          for (int i = 0; i < values.length; i++) {
            ((RuntimeIterator)iterators.get(i)).setCurrent(values[i]);
          }
          int occurence = applyProjectionAndAddToResultSet(context, pResultSet,ignoreOrderBy);
          if (occurence == 1 || (occurence > 1 && !this.distinct)) {
            // Asif: (Unique i.e first time occurence) or subsequent occurence
            // for non distinct query
            ++numElementsAdded;
          }
        }
        // return pResultSet;
      }
      else if (iterators.size() == 1) {
        RuntimeIterator rIter = (RuntimeIterator)iterators.get(0);
        Iterator resultsIter = ((SelectResults)resultSet).iterator();
        // Apply limit if there is no order by.
        Integer limitValue = evaluateLimitValue(context, this.limit);
        while (( (this.orderByAttrs != null && !ignoreOrderBy) || limitValue < 0 || (numElementsAdded < limitValue))
            && resultsIter.hasNext()) {
          rIter.setCurrent(resultsIter.next());
          int occurence = applyProjectionAndAddToResultSet(context, pResultSet,ignoreOrderBy);
          if (occurence == 1 || (occurence > 1 && !this.distinct)) {
            // Asif: (Unique i.e first time occurence) or subsequent occurence
            // for non distinct query
            ++numElementsAdded;
          }
        }
      }
      else {
        throw new RuntimeException(LocalizedStrings.CompiledSelect_RESULT_SET_DOES_NOT_MATCH_WITH_ITERATOR_DEFINITIONS_IN_FROM_CLAUSE.toLocalizedString());
      }
      return pResultSet;
    }
  }

  private SelectResults prepareEmptyResultSet(ExecutionContext context, boolean ignoreOrderBy)
      throws TypeMismatchException, AmbiguousNameException
 {
    // Asif:if no projection attributes or '*'as projection attribute
    // & more than one/RunTimeIterator then create a StrcutSet.
    // If attribute is null or '*' & only one RuntimeIterator then create a
    // ResultSet.
    // If single attribute is present without alias name , then create
    // ResultSet
    // Else if more than on attribute or single attribute with alias is
    // present then return a StrcutSet
    // create StructSet which will contain root objects of all iterators in
    // from clause

    ObjectType elementType = this.cachedElementTypeForOrderBy != null ? this.cachedElementTypeForOrderBy
        : prepareResultType(context);
    SelectResults results;
    if (this.distinct || !this.count) {
      if (this.orderByAttrs != null) {
        boolean nullValuesAtStart = !((CompiledSortCriterion)orderByAttrs.get(0)).getCriterion();
        if (elementType.isStructType()) {
          if (ignoreOrderBy) {
            results = this.distinct ? new LinkedStructSet(
                (StructTypeImpl) elementType) : new SortedResultsBag(
                elementType, nullValuesAtStart);

          } else {
            OrderByComparator comparator = this.hasUnmappedOrderByCols ? new OrderByComparatorUnmapped(
                this.orderByAttrs, (StructTypeImpl) elementType, context)
                : new OrderByComparator(this.orderByAttrs,
                    (StructTypeImpl) elementType, context);
            results = this.distinct ? new SortedStructSet(comparator,
                (StructTypeImpl) elementType) : new SortedStructBag(comparator,
                (StructTypeImpl) elementType, nullValuesAtStart);

          }
        } else {
          if (ignoreOrderBy) {
            results = this.distinct ? new LinkedResultSet()
                : new SortedResultsBag(nullValuesAtStart);

          } else {
            OrderByComparator comparator = this.hasUnmappedOrderByCols ? new OrderByComparatorUnmapped(
                this.orderByAttrs, elementType, context)
                : new OrderByComparator(this.orderByAttrs, elementType, context);
            results = this.distinct ? new SortedResultSet(comparator)
                : new SortedResultsBag(comparator, nullValuesAtStart);
          }
          results.setElementType(elementType);
        }
      } else {
        if (this.distinct) {
          if (elementType.isStructType()) {
            results = new StructSet((StructType) elementType);
          } else {
            results = new ResultsSet(elementType);
          }
        } else {
          if (elementType.isStructType()) {
            results = new StructBag((StructType) elementType,
                context.getCachePerfStats());
          } else {
            results = new ResultsBag(elementType, context.getCachePerfStats());
          }
        }
      }
    } else {
      // Shobhit: If its a 'COUNT' query and no End processing required Like for
      // 'DISTINCT'
      // we can directly keep count in ResultSet and ResultBag is good enough
      // for that.
      results = new ResultsBag(new ObjectTypeImpl(Integer.class), 1 /*
                                                                     * initial
                                                                     * capacity
                                                                     * for count
                                                                     * value
                                                                     */,
          context.getCachePerfStats());
      countStartQueryResult = 0;

    }
   return results;   
  }
  
  protected ObjectType prepareResultType(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException
  {
    // Asif:if no projection attributes or '*'as projection attribute
    // & more than one/RunTimeIterator then create a StrcutSet.
    // If attribute is null or '*' & only one RuntimeIterator then create a
    // ResultSet.
    // If single attribute is present without alias name , then create
    // ResultSet
    // Else if more than on attribute or single attribute with alias is
    // present then return a StrcutSet
    // create StructSet which will contain root objects of all iterators in
    // from clause

    ObjectType elementType = null;
    SelectResults sr = null;

    List currentIterators = context.getCurrentIterators();
    if (projAttrs == null) {
      if (currentIterators.size() == 1) {
        RuntimeIterator iter = (RuntimeIterator)currentIterators.get(0);
        elementType = iter.getElementType();
      }
      else {
        elementType = createStructTypeForNullProjection(currentIterators,
            context);
      }
    }
    else {
      // Create StructType for projection attributes
      int projCount = projAttrs.size();
      String fieldNames[] = new String[projCount];
      ObjectType fieldTypes[] = new ObjectType[projCount];
      boolean createStructSet = false;
      String fldName = null;
      for (int i = 0; i < projCount; i++) {
        Object[] projDef = (Object[])projAttrs.get(i);
        fldName = (String)projDef[0];
        if (!createStructSet) {
          if (fldName != null || projCount > 1) {
            createStructSet = true;
          }
        }
        fieldNames[i] = (fldName == null && createStructSet) ? generateProjectionName(
            (CompiledValue)projDef[1], context)
            : fldName;
        fieldTypes[i] = getFieldTypeOfProjAttrib(context,
            (CompiledValue)projDef[1]);
        // fieldTypes[i] = TypeUtils.OBJECT_TYPE;
      }
      if (createStructSet) {
        elementType = new StructTypeImpl(fieldNames, fieldTypes);
      }
      else {
        elementType = fieldTypes[0];
      }
    }
    return elementType;
  }

  /*
  private SelectResults prepareEmptySelectResults(ObjectType elementType,
                                                  boolean isSorted,
                                                  ExecutionContext context) {
    if (elementType.isStructType()) {
      if (isSorted) { // sorted struct
        return prepareEmptySortedStructSet((StructTypeImpl)elementType);
  }
      else { // unsorted struct
        return new StructBag((StructType)elementType,  context.getCachePerfStats());
  }
    }
    else { // non-struct
      if (isSorted) { // sorted non-struct
          return prepareEmptySortedResultSet(elementType);
      }
      else { // unsorted non-struct
        return new ResultsBag(elementType, context.getCachePerfStats());
      }
    }
  }
  */
 
 
    
 /**
   * Asif: This function should be used to create a StructType for those
   * queries which have * as projection attribute (implying null projection
   * attribute) & multiple from clauses
   *  
   */
  private StructTypeImpl createStructTypeForNullProjection(List currentIterators, ExecutionContext context) {
    int len = currentIterators.size();
    String fieldNames[] = new String[len];
    ObjectType fieldTypes[] = new ObjectType[len];
    String fldName = null;
    for (int i = 0; i < len; i++) {
      RuntimeIterator iter = (RuntimeIterator) currentIterators.get(i);
      //fieldNames[i] = iter.getName();
      if ((fldName = iter.getName()) == null) {
        fldName = generateProjectionName(iter, context);
      }
      fieldNames[i] = fldName;
      fieldTypes[i] = iter.getElementType();
    }
    return new StructTypeImpl(fieldNames, fieldTypes);
  }

  private ObjectType getFieldTypeOfProjAttrib(ExecutionContext context, CompiledValue cv) throws TypeMismatchException,
      AmbiguousNameException {
    // Identify the RuntimeIterator for the compiled value
    ObjectType retType = TypeUtils.OBJECT_TYPE;
    try {
      RuntimeIterator rit = context.findRuntimeIterator(cv);
      List pathOnItr = cv.getPathOnIterator(rit, context);
      if (pathOnItr != null) {
        String path[] = (String[]) pathOnItr.toArray(new String[0]);
        ObjectType ot[] = PathUtils.calculateTypesAlongPath(rit.getElementType(), path);
        retType = ot[ot.length - 1];
      }
    }
    catch (NameNotFoundException e) {
      // Unable to determine the type Of attribute.It will default to
      // ObjectType
    }
    return retType;
  }

 
  // resultSet could be a set or a bag (we have set constructor, or there
  // could be a distinct subquery)
  // in future, it would be good to simplify this to always work with a bag
  // (converting all sets to bags) until the end when we enforce distinct
  // Asif: The number returned indicates the occurence of the data in the SelectResults
  // Thus if the SelectResults is of type ResultsSet or StructSet
  // then 1 will indicate that data was added to the results & that was the 
  // first occurence. For this 0 will indicate that the data was not added
  // because it was a duplicate
  // If the SelectResults is an instance  ResultsBag or StructsBag , the number will
  // indicate the occurence. Thus 1 will indicate it being added for first time
  // Currently orderBy is present only for StructSet & ResultSet which are
  // unique object holders. So the occurence for them can be either 0 or 1 only
  
  
  
  
  private int applyProjectionAndAddToResultSet(ExecutionContext context,
      SelectResults resultSet, boolean ignoreOrderBy) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    List currrentRuntimeIters = context.getCurrentIterators();

    int occurence = 0;
    ObjectType elementType = resultSet.getCollectionType().getElementType();
    boolean isStruct = elementType != null && elementType.isStructType();

    //TODO : Asif : Optimize this condition in some clean way
    boolean isLinkedStructure = resultSet instanceof Ordered && ((Ordered)resultSet).dataPreordered();
   
    ArrayList evaluatedOrderByClause = null;
    OrderByComparator comparator = null;
    boolean applyOrderBy = false;
    if (this.orderByAttrs != null && !ignoreOrderBy) {
      // In case PR order-by will get applied on the coordinator node
      // on the cumulative results. Apply the order-by on PR only if
      // limit is specified.
      Integer limitValue = evaluateLimitValue(context, this.limit);
      if (context.getPartitionedRegion() != null && limitValue < 0) {
        applyOrderBy = false;
      }
      applyOrderBy = true;
    }

    if (this.orderByAttrs != null && !ignoreOrderBy) {
      comparator = (OrderByComparator) ((Ordered) resultSet).comparator();
    }
    if (projAttrs == null) {
      int len = currrentRuntimeIters.size();
      Object values[] = new Object[len];
      for (int i = 0; i < len; i++) {
        RuntimeIterator iter = (RuntimeIterator) currrentRuntimeIters.get(i);
        values[i] = iter.evaluate(context);
        // For local queries with distinct, deserialize all PdxInstances
        // as we do not have a way to compare Pdx and non Pdx objects in case
        // the cache has a mix of pdx and non pdx objects.
        // We still have to honor the cache level readserialized flag in
        // case of all Pdx objects in cache
        if (this.distinct
            && !((DefaultQuery) context.getQuery()).isRemoteQuery()
            && !context.getCache().getPdxReadSerialized()
            && (values[i] instanceof PdxInstance)) {
          values[i] = ((PdxInstance) values[i]).getObject();
        }
      }

      // Shobhit: Add count value to the counter for this select expression.
      // Don't care about Order By for count(*).
      if (isCount() && !this.distinct) {
        // Counter is local to CompileSelect and not available in ResultSet
        // until
        // the end of evaluate call to this CompiledSelect object.
        this.countStartQueryResult++;
        occurence = 1;
      } else {
        // if order by is present
        if (applyOrderBy) {
          StructImpl structImpl;
          if (this.distinct) {
            if (isStruct) {
              if (values.length == 1 && values[0] instanceof StructImpl) {
                structImpl = (StructImpl) values[0];
                comparator.addEvaluatedSortCriteria(
                    structImpl.getFieldValues(), context);
                occurence =  resultSet.add(structImpl) ? 1
                    : 0;
              } else {
                comparator.addEvaluatedSortCriteria(values, context);
                occurence = ((StructFields) resultSet).addFieldValues(values) ? 1
                    : 0;
              }
              // Asif: TODO:Instead of a normal Map containing which holds
              // StructImpl object
              // use a THashObject with Object[] array hashing stragtegy as we
              // are unnnecessarily
              // creating objects of type Object[]
            } else {
              comparator.addEvaluatedSortCriteria(values[0], context);
              occurence =  resultSet.add(values[0]) ? 1 : 0;
            }
          } else {
            if (isStruct) {
              if (values.length == 1 && values[0] instanceof StructImpl) {
                structImpl = (StructImpl) values[0];
                comparator.addEvaluatedSortCriteria(
                    structImpl.getFieldValues(), context);
                occurence = ((Bag) resultSet).addAndGetOccurence(structImpl.getFieldValues());
              } else {
                comparator.addEvaluatedSortCriteria(values, context);
                occurence = ((Bag) resultSet).addAndGetOccurence(values) ;
                    
              }
            } else {
              comparator.addEvaluatedSortCriteria(values[0], context);
              occurence = ((Bag) resultSet)
                  .addAndGetOccurence(values[0]);
            }
          }
        } else {
          if(isLinkedStructure) {
            if(isStruct) {
              StructImpl structImpl;
              if (values.length == 1 && values[0] instanceof StructImpl) {
                structImpl = (StructImpl) values[0];
              } else {
                structImpl = new StructImpl(
                    (StructTypeImpl)elementType, values);
              }
              if(this.distinct) {
                occurence = resultSet.add(structImpl) ? 1 : 0;
              }else {
                occurence = ((Bag)resultSet).addAndGetOccurence(structImpl);
              } 
            }else {
              if(this.distinct) {
                occurence = resultSet.add(values[0]) ? 1 : 0;  
              }else {
                occurence = ((Bag)resultSet).addAndGetOccurence(values[0]);
              }
              
            }
          }
          else {
            if (this.distinct) {
              if (isStruct) {
                occurence = ((StructFields) resultSet).addFieldValues(values) ? 1
                    : 0;
              } else {
                occurence =  resultSet.add(values[0]) ? 1 : 0;
              }
            } else {
              if (isStruct) {
                occurence = ((Bag) resultSet)
                    .addAndGetOccurence(values);
              } else {
                boolean add = true;
                if (context.isCqQueryContext()) {
                  if (values[0] instanceof Region.Entry) {
                    Region.Entry e = (Region.Entry) values[0];
                    if (!e.isDestroyed()) {
                      try {
                        values[0] = new CqEntry(e.getKey(), e.getValue());
                      } catch (EntryDestroyedException ede) {
                        // Even though isDestory() check is made, the entry
                        // could
                        // throw EntryDestroyedException if the value becomes
                        // null.
                        add = false;
                      }
                    } else {
                      add = false;
                    }
                  }
                }
                if (add) {
                  occurence = ((Bag) resultSet)
                      .addAndGetOccurence(values[0]);
                }
              }
            }
          }
        }
      }
    } else { // One or more projection attributes
      int projCount = projAttrs.size();
      Object[] values = new Object[projCount];
      for (int i = 0; i < projCount; i++) {
        Object projDef[] = (Object[]) projAttrs.get(i);
        values[i] = ((CompiledValue) projDef[1]).evaluate(context);
        // For local queries with distinct, deserialize all PdxInstances
        // as we do not have a way to compare Pdx and non Pdx objects in case
        // the cache has a mix of pdx and non pdx objects.
        // We still have to honor the cache level readserialized flag in
        // case of all Pdx objects in cache.
        // Also always convert PdxString to String before adding to resultset
        // for remote queries
        if (!((DefaultQuery) context.getQuery()).isRemoteQuery()) {
          if (this.distinct && values[i] instanceof PdxInstance
              && !context.getCache().getPdxReadSerialized()) {
            values[i] = ((PdxInstance) values[i]).getObject();
          } else if (values[i] instanceof PdxString) {
            values[i] = ((PdxString) values[i]).toString();
          }
        }
      }
      // if order by is present
      if (applyOrderBy) {
        if (distinct) {
          if (isStruct) {
            comparator.addEvaluatedSortCriteria(values, context);
            // Asif: Occurence field is used to identify the corrcet number of
            // iterations
            // required to implement the limit based on the presence or absence
            // of distinct clause
            occurence = ((StructFields) resultSet).addFieldValues(values) ? 1
                : 0;
          } else {
            comparator.addEvaluatedSortCriteria(values[0], context);
            occurence =  resultSet.add(values[0]) ? 1 : 0;
          }
        } else {
          if (isStruct) {
            comparator.addEvaluatedSortCriteria(values, context);
            occurence = ((Bag) resultSet).addAndGetOccurence(values);
          } else {
            comparator.addEvaluatedSortCriteria(values[0], context);
            occurence = ((Bag) resultSet)
                .addAndGetOccurence(values[0]);
          }
        }
      } else {
        if(isLinkedStructure) {
          if(isStruct) {
            StructImpl structImpl = new StructImpl((StructTypeImpl)elementType, values);
            if(this.distinct) {
              occurence = resultSet.add(structImpl) ? 1 : 0;
            }else {
              occurence = ((Bag)resultSet).addAndGetOccurence(structImpl);
            }
   
          }else {
            if(this.distinct) {
              occurence =  resultSet.add(values[0]) ? 1 : 0;
            }else {
              occurence = ((Bag)resultSet).addAndGetOccurence(values[0]);
            }
          }
        }else {
          if (this.distinct) {
            if (isStruct) {
              occurence = ((StructFields) resultSet).addFieldValues(values) ? 1
                  : 0;
            } else {
              occurence =  resultSet.add(values[0]) ? 1 : 0;
            }
          } else {
            if (isStruct) {
              occurence = ((Bag) resultSet)
                  .addAndGetOccurence(values);
            } else {
              occurence = ((Bag) resultSet)
                  .addAndGetOccurence(values[0]);
            }
          }
        }
      }
    }
    return occurence;
  }

  private String generateProjectionName(CompiledValue projExpr, ExecutionContext context) {
    String name = null;
    if (projExpr instanceof RuntimeIterator) {
      RuntimeIterator rIter = (RuntimeIterator) projExpr;
      name = rIter.getDefinition();
      int index = name.lastIndexOf('.');
      if (index > 0) {
        name = name.substring(index + 1);
      }
      else if (name.charAt(0) == '/') {
        index = name.lastIndexOf('/');
        name = name.substring(index + 1);
      }
      else {
        name = rIter.getInternalId();
      }
    }
    else {
      int type = projExpr.getType();
      if (type == PATH) {
        name = ((CompiledPath) projExpr).getTailID();
      }
      else if (type == Identifier) {
        name = ((CompiledID) projExpr).getId();
      }
      else if (type == LITERAL) {
        name = (((CompiledLiteral) projExpr)._obj).toString();
      }
      else if (type == METHOD_INV) {
        name = ((CompiledOperation) projExpr).getMethodName();
      }
      else {
        name = new StringBuffer("field$").append(context.nextFieldNum()).toString();
        // name = projExpr.toString();
      }
    }
    return name;
  }
 

  /**
   * Optimized evaluate for CQ execution.
   * @param context
   * @return boolean
   * @throws FunctionDomainException
   * @throws TypeMismatchException
   * @throws NameResolutionException
   * @throws QueryInvocationTargetException
   */
  public boolean evaluateCq(ExecutionContext context) throws FunctionDomainException, TypeMismatchException,
  NameResolutionException, QueryInvocationTargetException {
    if (this.whereClause == null) {
      return true;
    }

    context.newScope((Integer)context.cacheGet(scopeID));
    context.pushExecCache((Integer)context.cacheGet(scopeID));
    try {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) iterators.get(0);
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      context.bindIterator(rIter);


      Collection coll;
      {
        Object evalResult = iterDef.getCollectionExpr().evaluate(context);
        if (evalResult == null || evalResult == QueryService.UNDEFINED) {
          return false;
        }
        coll = (Collection)evalResult;
      }
      if (coll.isEmpty()) {
        return false;
      }

      if (this.whereClause.isDependentOnCurrentScope(context)) { 
        Iterator cIter = coll.iterator();
        Object currObj = cIter.next();
        rIter.setCurrent(currObj);
      }
      Object b = this.whereClause.evaluate(context);
      if (b == null) {
        return false;
      } else if (b == QueryService.UNDEFINED) {
        // add UNDEFINED to results only for NOT EQUALS queries
        if (this.whereClause.getType() == COMPARISON) {
          int operator = ((CompiledComparison) this.whereClause).getOperator();
          if ((operator != TOK_NE && operator != TOK_NE_ALT)) {
            return false;
          } else {
            return true;
          }
        } else {
          return false;
        }
      } else {
        return(((Boolean) b).booleanValue());
      }
    }
    finally {
      context.popExecCache();
      context.popScope();
    }
  }
  
  /*
   * A special evaluation of limit for when limit needs to be evaluated before
   * an execution context is created.
   * 
   * It assumes the limit is either a CompiledBindArgument or a CompiledLiteral
   * @param bindArguments
   * @return
   * @throws FunctionDomainException
   * @throws TypeMismatchException
   * @throws NameResolutionException
   * @throws QueryInvocationTargetException
   */
  private Integer evaluateLimitValue(Object[] bindArguments) throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    Integer limitValue = -1;
    if (this.limit != null) {
      if (this.limit instanceof CompiledBindArgument) {
        limitValue = (Integer)((CompiledBindArgument)this.limit).evaluate(bindArguments);
      }
      else {
        //Assume limit is a compiled literal which does not need a context
        limitValue =  (Integer)this.limit.evaluate(null);
      }
    }
    return limitValue;
  }
  
  protected static Integer evaluateLimitValue(ExecutionContext context, CompiledValue limit) throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    Integer limitValue = -1;
    if (limit != null) {
        limitValue =  (Integer)limit.evaluate(context);
        if (limitValue == null) {
          //This is incase an object array was passed in but no param was set for the limit 
          limitValue = -1;
        }
    }
    return limitValue;
  }
  
  private static class NullIteratorException extends Exception {
    
  }  

}





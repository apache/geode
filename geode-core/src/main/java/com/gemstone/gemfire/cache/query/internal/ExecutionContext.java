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

import java.util.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.internal.types.*;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.cache.query.internal.index.IndexUtils;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.pdx.internal.PdxString;

/**
 * This is used to carry the state of a query or index update operation. A state
 * of a query is a set of flags to be applied during query execution life cycle.
 * Also, carries the dependencies of where clauses or index expressions to from
 * clause iterators.
 *
 * @see QueryExecutionContext for extended version of this ONLY for querying.
 * 
 */
public class ExecutionContext {

  protected Object[] bindArguments;
  private final Stack scopes = new Stack();
  private final Cache cache;

  /**
   * a Sequentially increasing number identifying a scope & also indicating
   * whether a given scope came prior or later to another scope. It is needed
   * to distiguish between two scopes having same nesting level relative to 
   * the top scope
   */
  private int scopeNum =0;
  
  /**
   * Dependency graph. Maps CompiledValues in tree to the RuntimeIterators each
   * node is dependent on. This information is computed just before the query is
   * evaluated. The information is good for only one execution, since regions
   * can be destroyed and re-created with different type constraints. Type of
   * this map: map <CompiledValue, set <RuntimeIterator>>
   */
  Map dependencyGraph = new HashMap();
  /**
   * Map which stores the CompiledIteratorDef as the key & the value is the set
   * of Independent RuntimeIterators on which it is dependent upon. The idea is
   * that this Map will identify the final Independent RuntimeIterator or
   * Iterators , ie. those refering to a Region or BindArgument, on which the
   * CompiledIteratorDef depends upon . TODO:Asif: For a single vale , should we
   * still use a Set?
   *  
   */
  private final Map itrDefToIndpndtRuntimeItrMap = new HashMap();
  /**
   * Asif : This Map will store its Region Path String against an Independent
   * RuntimeIterator An entry in this Map will be only for those
   * RuntimeIteartors which have an underlying Region as its Collection
   * Expression
   *  
   */
  private final Map indpndtItrToRgnMap = new HashMap();  
  
  //used when querying on a PR: Substitute reference to PartitionedRegion with BucketRegion
  private BucketRegion bukRgn = null;
  private PartitionedRegion pr = null;
  private boolean distinct = false;
  private Object currentProjectionField = null;
  private boolean isPRQueryNode = false;
  /**
   * Param specialIteratorVar name of special variable to use to denote the
   * current iteration element. Used to implement the "this" var in the query
   * shortcut methods
   * 
   * @see com.gemstone.gemfire.cache.Region#query
   */
  public ExecutionContext(Object[] bindArguments, Cache cache) {
    this.bindArguments = bindArguments;
    this.cache = cache;
  }

  public ExecutionContext(Object[] bindArguments, Cache cache, SelectResults results) {
    this.bindArguments = bindArguments;
    this.cache = cache;
  }

  public ExecutionContext(Object[] bindArguments, Cache cache, Query query) {
    this.bindArguments = bindArguments;
    this.cache = cache;
  }

  public CachePerfStats getCachePerfStats() {
    return ((GemFireCacheImpl)this.cache).getCachePerfStats();
  }

  /**
   * Add RuntimeIterator as a dependency of a CompiledValue. ASSUMPTION:
   * unsynchronized, assumed to be single-threaded.
   * 
   * @return the dependency set as a shortcut
   */
  Set addDependency(CompiledValue cv, RuntimeIterator itr) {
    Set ds = getDependencySet(cv, false);
    ds.add(itr);
    return ds;
  }

  /** @return the dependency set as a shortcut */
 public Set addDependencies(CompiledValue cv, Set set /* <RuntimeIterator> */) {
    if (set.isEmpty()) return getDependencySet(cv, true);
    Set ds = getDependencySet(cv, false);
    ds.addAll(set);
    return ds;
  }
  //TODO:ASIF:QUERY
  /**
   * Return true if given CompiledValue is dependent on any RuntimeIterator in
   * current scope
   * 
   */
  boolean isDependentOnCurrentScope(CompiledValue cv) {
    //return !getDependencySet(cv, true).isEmpty();
    Set setRItr = getDependencySet(cv, true);
    boolean isDependent = false;
    if (!setRItr.isEmpty()) {
      //int currScopeID = this.scopes.size();
      int currScopeID = currentScope().getScopeID();
      for (Iterator itr = setRItr.iterator(); itr.hasNext();) {
        RuntimeIterator ritr = (RuntimeIterator) itr.next();
        if (currScopeID == ritr.getScopeID()) {
          isDependent = true;
          break;
        }
      }
    }
    return isDependent;
  }

  /**
   * Return true if given CompiledValue is dependent on any RuntimeIterator in
   * all of the scopes
   */
  boolean isDependentOnAnyIterator(CompiledValue cv) {
    return !getDependencySet(cv, true).isEmpty();
  }

  /**
   * Return true if given CompiledValue is dependent on specified
   * RuntimeIterator
   */
  boolean isDependentOn(CompiledValue cv, RuntimeIterator itr) {
    return getDependencySet(cv, true).contains(itr);
  }

  Set getDependencySet(CompiledValue cv, boolean readOnly) {
    Set set = (Set) this.dependencyGraph.get(cv);
    if (set == null) {
      if (readOnly) return Collections.EMPTY_SET;
      set = new HashSet(1);
      this.dependencyGraph.put(cv, set);
    }
    return set;
  }

  /**
   * Returns all dependencies in from this context which are reused during index
   * update by new {@link ExecutionContext} for concurrent updates on indexes.
   * 
   * @return All {@link AbstractCompiledValue} dependencies.
   */
  public Map getDependencyGraph() {
    return dependencyGraph;
  }

  public void setDependencyGraph(Map dependencyGraph) {
    this.dependencyGraph = dependencyGraph;
  }

  public Object getBindArgument(int index) {
    if (index > this.bindArguments.length)
        throw new IllegalArgumentException(LocalizedStrings.ExecutionContext_TOO_FEW_QUERY_PARAMETERS.toLocalizedString());
    return this.bindArguments[index - 1];
  }

  //TODO:ASIF:Query
  /** bind a named iterator (to current scope) */
  public void bindIterator(RuntimeIterator itr) {
    //int currScopeID = this.scopes.size();
    QScope currentScope= this.currentScope();
    int currScopeID = currentScope.getScopeID();
    itr.setScopeID(currScopeID);
    currentScope.bindIterator(itr);
  }

  public CompiledValue resolve(String name) throws TypeMismatchException,
      AmbiguousNameException {
    CompiledValue value = resolveAsVariable(name);
    if (value != null) return value;
    // attribute name or operation name (no args) of a variable in the current
    // scope
    // when there is no ambiguity, i.e. this property name belongs to only one
    // variable in the scope
    value = resolveImplicitPath(name);
    if (value == null)
        // cannot be resolved
        throw new TypeMismatchException(LocalizedStrings.ExecutionContext_THE_ATTRIBUTE_OR_METHOD_NAME_0_COULD_NOT_BE_RESOLVED.toLocalizedString(name));
    return value;
  }

  /** Return null if cannot be resolved as a variable in current scope */
  private CompiledValue resolveAsVariable(String name) {
    CompiledValue value = null;
    for (int i = scopes.size() - 1; i >= 0; i--) {
      QScope scope = (QScope) scopes.get(i);
      value = scope.resolve(name);
      if (value != null) return value;
    }
    return null;
  }

  public void newScope(int scopeID) {  
    scopes.push(new QScope(scopeID));    
  }

  public void popScope() {
    scopes.pop();    
  }
  
  /**
   * 
   * @return int indentifying the scope ID which can be assosciated with the scope
   */
  int assosciateScopeID() {     
    //this.scopeIDMap.put(cs, Integer.valueOf(num));
    return ++this.scopeNum;
  }
  

  public QScope currentScope() {
    return (QScope) scopes.peek();
  }
 
  public List getCurrentIterators() {
    return currentScope().getIterators();
  }

  /**
   * This function returns a List of RuntimeIterators which have ultimate
   * dependency on the Single Independent Iterator which is passed as a
   * parameter to the function. For correct usage it is necessary that the
   * RuntimeIterator passed is independent. If there are no dependent Iterators
   * then the list will just contain one element which will be the
   * RuntimeIterator passed as argument . Also the self independent Runtime
   * Iterator present in the scope ( that is teh RuntimeIterator same as the
   * independent iterator passed as argument) is added at start of the list. If
   * an iterator is dependent on more than one independent iterator, it is not
   * added to the List TODO:Asif If we are storing a single Iterator instead of
   * Set , in the itrDefToIndpndtRuntimeItrMap , we need to take care of this
   * function.
   * 
   * <P>author Asif
   * @param rIter Independent RuntimeIterator on which dependent iterators of
   *          current scope need to identified
   * @return List containing the independent Runtime Iterator & its dependent
   *         iterators
   */
  public List getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(
      RuntimeIterator rIter) {
    Iterator iter = currentScope().getIterators().iterator();
    List list = new ArrayList();
    list.add(rIter);
    while (iter.hasNext()) {
      RuntimeIterator iteratorInCurrentScope = (RuntimeIterator) iter.next();
      Set itrSet = (Set) itrDefToIndpndtRuntimeItrMap
          .get(iteratorInCurrentScope.getCmpIteratorDefn());
      if (rIter != iteratorInCurrentScope
          && itrSet.size() == 1
          && ((RuntimeIterator) itrSet.iterator().next()) == rIter) {
        list.add(iteratorInCurrentScope);
      }
    }
    return list;
  }

  public List getAllIterators() {
    int numScopes = scopes.size();
    List iterators = new ArrayList();
    for (int i = 1; i <= numScopes; i++) {
      iterators.addAll(((QScope) scopes.get(numScopes - i)).getIterators());
    }
    return iterators;
  }

  void setOneIndexLookup(boolean b) {
    QScope scope = currentScope();
    Support.Assert(scope != null, "must be called within valid scope");
    scope._oneIndexLookup = b;
  }

  
  void setCurrent(RuntimeIterator iter, Object obj) {
    currentScope().setCurrent(iter, obj);
  }

  public Cache getCache() {
    return this.cache;
  }

  private CompiledValue resolveImplicitPath(String name)
      throws AmbiguousNameException {
    CompiledValue result = resolveImplicitOperationName(name, 0, false);
    return (result == null) ? null : new CompiledPath(result, name);
  }

  /**
   * returns implicit iterator receiver of operation with numArgs args, or null
   * if cannot be resolved.
   * 
   * SPECIAL CASE: If we are unable to resolve the name on any iterator, but
   * there is only one iterator that we don't have type information for it (for
   * now OBJECT_TYPE, this has to change), then return that one iterator under
   * the assumption that the operation name must belong to it.
   */
  RuntimeIterator resolveImplicitOperationName(String name, int numArgs,
      boolean mustBeMethod) throws AmbiguousNameException {
    //System.out.println("In resolveImplicitOperationName");
    // iterate through all properties of iterator variables in scope
    // to see if there is a unique resolution
    RuntimeIterator oneUnknown = null;
    List hits = new ArrayList(2);
    boolean foundOneUnknown = false;
    NEXT_SCOPE: for (int i = scopes.size() - 1; i >= 0; i--) {
      QScope scope = (QScope) scopes.get(i);
      Iterator iter = scope.getIterators().iterator();
      while (iter.hasNext()) {
        RuntimeIterator itr = (RuntimeIterator) iter.next();
        Assert.assertTrue(itr != null);
        // if scope is limited to this iterator, then don't check any more
        // iterators in this scope
        if (scope.getLimit() == itr) {
          continue NEXT_SCOPE; // don't go any farther in this scope
        }
        //Shobhit: If Element type is ObjectType then we don't need to
        // apply reflection to find out field or method. This save lot of CPU time.
        if (!TypeUtils.OBJECT_TYPE.equals(itr.getElementType()) && itr.containsProperty(name, numArgs, mustBeMethod)) {
          hits.add(itr);
        }
        else if (TypeUtils.OBJECT_TYPE.equals(itr.getElementType())) {
          if (foundOneUnknown) {
            oneUnknown = null; // more than one
          }
          else {
            foundOneUnknown = true;
            oneUnknown = itr;
          }
        }
      }
    }
    if (hits.size() == 1) return (RuntimeIterator) hits.get(0);
    if (hits.size() > 1) {
      // ambiguous
      if (mustBeMethod)
          throw new AmbiguousNameException(LocalizedStrings.ExecutionContext_METHOD_NAMED_0_WITH_1_ARGUMENTS_IS_AMBIGUOUS_BECAUSE_IT_CAN_APPLY_TO_MORE_THAN_ONE_VARIABLE_IN_SCOPE.toLocalizedString(new Object[] {name, Integer.valueOf(numArgs)}));
      throw new AmbiguousNameException(LocalizedStrings.ExecutionContext_ATTRIBUTE_NAMED_0_IS_AMBIGUOUS_BECAUSE_IT_CAN_APPLY_TO_MORE_THAN_ONE_VARIABLE_IN_SCOPE.toLocalizedString(name));
    }
    Assert.assertTrue(hits.isEmpty());
    // if there is a single unknown, then return that one under the assumption
    // that the name must belong to it
    // otherwise, returns null, unable to resolve here
    return oneUnknown;
  }

  protected CompiledValue resolveScopeVariable(String name) {
    CompiledValue value = null;
    for (int i = scopes.size() - 1; i >= 0; i--) {
      QScope scope = (QScope) scopes.get(i);
      value = scope.resolve(name);
      if (value != null) break;
    }
    return value;
  }

  /**
   * Tries to find for RuntimeIterator associated with specified expression
   */
  public RuntimeIterator findRuntimeIterator(CompiledValue expr) {
    //Check if expr is itself RuntimeIterator
    if (expr instanceof RuntimeIterator) {
      RuntimeIterator rIter = (RuntimeIterator) expr;
      return rIter;
    }
    // Try to find RuntimeIterator
    return (RuntimeIterator) findIterator(expr);
  }

  private CompiledValue findIterator(CompiledValue path) {
    try {
      if (path == null) { return null; }
      if (path instanceof RuntimeIterator) { return path; }
      if (path instanceof CompiledPath) {
        CompiledValue rec = ((CompiledPath) path).getReceiver();
        return findIterator(rec);
      }
      if (path instanceof CompiledOperation) {
        CompiledOperation operation = (CompiledOperation) path;
        CompiledValue rec = operation.getReceiver(this);
        if (rec == null) {
          RuntimeIterator rcvrItr = resolveImplicitOperationName(operation
              .getMethodName(), operation.getArguments().size(), true);
          return rcvrItr;
        }
        return findIterator(rec);
      }
      if (path instanceof CompiledIndexOperation) {
        CompiledIndexOperation cio = (CompiledIndexOperation) path;
        CompiledValue rec = cio.getReceiver();
        return findIterator(rec);
      }
      if (path instanceof CompiledID) {
        CompiledValue expr = resolve(((CompiledID) path).getId());
        return findIterator(expr);
      } //if we get these exceptions return null
    }
    catch (TypeMismatchException e) {
    }
    catch (NameResolutionException e) {
    }
    return null;
  }

  int getScopeCount() {
    return this.scopes.size();
  }

  /**
   * 
   * Calculates set of Runtime Iterators on which a given CompiledValue
   * ultimately depends. The independent iterators may belong to other scopes.
   * 
   * <P>author Asif/Ketan
   * @param cv
   * @param set
   */
  //Ketan - Asif:This function will populate the set to its independent
  // RuntimeIterators
  //However if the CompiledValue happens to be a CompiledIteratorDef & if it is
  // independent of any other RuntimeIterators then no adition will be done in
  // the Set
  //TODO: Asif : The behaviour of this function will change if we modify the
  // computeDependency
  // function of the CompiledIteratorDef as in that case the Set will be added
  // with the self RuntimeIterator ( if the CompiledIteratorDef is independent)
  // which is
  //not the case now
  //TODO:Asif : If a CompiledIteratorDef has only one dependent RuntimeIterator
  // should it still be
  // stored in a Set or should it be a single value?
  public void computeUtlimateDependencies(CompiledValue cv, Set set) {
    Set dependencySet = this.getDependencySet(cv, true /* readOnly */);
    if (dependencySet != Collections.EMPTY_SET) {
      Iterator iter = dependencySet.iterator();
      RuntimeIterator rIter;
      while (iter.hasNext()) {
        rIter = (RuntimeIterator) iter.next();
        Set indRuntimeIterators = (Set) this.itrDefToIndpndtRuntimeItrMap.get(
            rIter.getCmpIteratorDefn());
        if (indRuntimeIterators != null) {
          set.addAll(indRuntimeIterators);
        }
      }
    }
  }

  /**
   * Asif : This function populates the Map itrDefToIndpndtRuntimeItrMap. It
   * creates a Set of RuntimeIterators to which the current CompilediteratorDef
   * is dependent upon. Also it sets the index_internal_id for the
   * RuntimeIterator, which is used for calculating the canonicalized iterator
   * definitions for identifying the available index.
   * 
   * @param itrDef
   *          CompiledIteratorDef object representing iterator in the query from
   *          clause
   * @throws AmbiguousNameException
   * @throws TypeMismatchException
   */
  public void addToIndependentRuntimeItrMap(CompiledIteratorDef itrDef)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    Set set = new HashSet();
    this.computeUtlimateDependencies(itrDef, set);
    RuntimeIterator itr = null;
    String rgnPath = null;
    //If the set is empty then add the self RuntimeIterator to the Map.
    if (set.isEmpty()) {
      itr = itrDef.getRuntimeIterator(this);
      set.add(itr);
      //Asif : Since it is a an independent RuntimeIterator , check if its
      // Collection Expr
      // boils down to a Region. If it is , we need to store the QRegion in the
      // Map
      CompiledValue startVal = QueryUtils
          .obtainTheBottomMostCompiledValue(itrDef.getCollectionExpr());
      if (startVal.getType() == OQLLexerTokenTypes.RegionPath) {
        rgnPath = ((QRegion)((CompiledRegion)startVal).evaluate(this)).getFullPath();
        this.indpndtItrToRgnMap.put(itr, rgnPath);
      }
      else if (startVal.getType() == OQLLexerTokenTypes.QUERY_PARAM) {
        Object rgn;
        CompiledBindArgument cba = (CompiledBindArgument)startVal;
        if ((rgn = cba.evaluate(this)) instanceof Region) {
          this.indpndtItrToRgnMap.put(itr, rgnPath = ((Region)rgn)
              .getFullPath());
        }
      }
    }
    this.itrDefToIndpndtRuntimeItrMap.put(itrDef, set);
    IndexManager mgr = null;
    //Asif : Set the canonicalized index_internal_id if the condition is
    // satisfied
    if (set.size() == 1) {
      if (itr == null) {
        itr = (RuntimeIterator)set.iterator().next();
        //if (itr.getScopeID() == this.getScopeCount()) {
        if (itr.getScopeID() == this.currentScope().getScopeID()) {
          rgnPath = (String)this.indpndtItrToRgnMap.get(itr);
        }
      }
      if (rgnPath != null) {
        mgr = IndexUtils.getIndexManager(this.cache.getRegion(rgnPath), false);
        //put a check for null and see if we will be executing on a bucket region.
        if ( ( null == mgr ) && ( null != this.bukRgn) ) {
          // for bucket region index use
          mgr = IndexUtils.getIndexManager(this.cache.getRegion(this.bukRgn.getFullPath()), false);
        }
      }
    }
    String tempIndexID = null;
    RuntimeIterator currItr = itrDef.getRuntimeIterator(this);
    currItr
        .setIndexInternalID((mgr == null || (tempIndexID = mgr
            .getCanonicalizedIteratorName(itrDef.genFromClause(this))) == null) ? currItr
            .getInternalId()
            : tempIndexID);
    
  }
  
  public  List getAllIndependentIteratorsOfCurrentScope() {
   List independentIterators = new ArrayList(this.indpndtItrToRgnMap.size());
   Iterator itr = this.indpndtItrToRgnMap.keySet().iterator();
   int currentScopeId = this.currentScope().getScopeID();
   while(itr.hasNext()) {
     RuntimeIterator rIter = (RuntimeIterator)itr.next();
     if(rIter.getScopeID() == currentScopeId) {
       independentIterators.add(rIter);
     }
   }
   return independentIterators;
 }

  /**
   * Asif : This method returns the Region path for the independent
   * RuntimeIterator if itr exists else returns null. It is the caller's
   * responsibility to ensure that the passed Iterator is the ultimate
   * Independent Runtime Iterator or else the method may return null if the
   * RunTimeIterator is genuinely dependent on a Region iterator
   * 
   * @param riter
   * @return String containing region path
   */
  String getRegionPathForIndependentRuntimeIterator(RuntimeIterator riter) {
    return (String) this.indpndtItrToRgnMap.get(riter);
  }
  
  /**
   * Populates the independent runtime iterator map for index creation purposes.
   * This method does not create any canonicalized index ids etc.
   * <p>author Asif
   * @param itrDef
   * @throws AmbiguousNameException
   * @throws TypeMismatchException
   */
  public void addToIndependentRuntimeItrMapForIndexCreation(
      CompiledIteratorDef itrDef) throws AmbiguousNameException,
      TypeMismatchException, NameResolutionException
  {

    Set set = new HashSet();
    this.computeUtlimateDependencies(itrDef, set);
    RuntimeIterator itr = null;
    //If the set is empty then add the self RuntimeIterator to the Map.
    if (set.isEmpty()) {
      itr = itrDef.getRuntimeIterator(this);
      set.add(itr);
    }
    this.itrDefToIndpndtRuntimeItrMap.put(itrDef, set);
  }  
  
  public void setBindArguments(Object[] bindArguments) {
    this.bindArguments = bindArguments;
  }
  
  public int getScopeNum() {
    return this.scopeNum;
  }
  
  /**
   * Added to reset the state from the last execution. This is added for CQs only.
   */
  public void reset(){
    this.scopes.clear();
  }
  
  public BucketRegion getBucketRegion() {
    return this.bukRgn;
  }

  public void setBucketRegion(PartitionedRegion pr, BucketRegion bukRgn) {
    this.bukRgn = bukRgn;
    this.pr = pr;
  }
  
  public PartitionedRegion getPartitionedRegion() {
    return this.pr;
  }

  // General purpose caching methods for data that is only valid for one
  // query execution
  void cachePut(Object key, Object value) {
    //throw new UnsupportedOperationException("Method should not have been called");
  }

  public Object cacheGet(Object key) {
    return null;
  }
  
  public Object cacheGet(Object key, Object defaultValue) {
    return defaultValue;
  }

  public boolean isCqQueryContext() {
    return false;
  }

  public List getBucketList() {
    return null;
  }

  public void pushExecCache(int scopeNum) {
    throw new UnsupportedOperationException("Method should not have been called");
  }
  
  public void popExecCache() {
    throw new UnsupportedOperationException("Method should not have been called");
  }

  int nextFieldNum() {
    throw new UnsupportedOperationException("Method should not have been called");
  }
  
  public void setCqQueryContext(boolean cqQuery) {
    throw new UnsupportedOperationException("Method should not have been called");
  }

  public Query getQuery() {
    throw new UnsupportedOperationException("Method should not have been called");
  }
  
  public void setBucketList(List list) {
    throw new UnsupportedOperationException("Method should not have been called");
  }
  
  public void addToSuccessfulBuckets(int bId) {
    throw new UnsupportedOperationException("Method should not have been called");
  }
  
  public int[] getSuccessfulBuckets() {
    throw new UnsupportedOperationException("Method should not have been called");
  }
  
  public PdxString getSavedPdxString(int index){
    throw new UnsupportedOperationException("Method should not have been called");
  }

  public boolean isDistinct() {
    return distinct;
  }

  public void setDistinct(boolean distinct) {
    this.distinct = distinct;
  }
  
  public boolean isBindArgsSet() {
    return this.bindArguments != null;
  }
  
  public void setCurrentProjectionField(Object field) {
    this.currentProjectionField = field;
  }
  
  public Object getCurrentProjectionField() {
    return this.currentProjectionField ;
  }
  
  public void setIsPRQueryNode(boolean isPRQueryNode) {
    this.isPRQueryNode = isPRQueryNode;
  }
  
  public boolean getIsPRQueryNode() {
    return this.isPRQueryNode;
  }
  
}

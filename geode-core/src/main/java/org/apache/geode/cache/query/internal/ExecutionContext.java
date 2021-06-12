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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexUtils;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.pdx.internal.PdxString;

/**
 * This is used to carry the state of a query or index update operation. A state of a query is a set
 * of flags to be applied during query execution life cycle. Also, carries the dependencies of where
 * clauses or index expressions to from clause iterators.
 *
 * @see QueryExecutionContext for extended version of this ONLY for querying.
 */
public class ExecutionContext {

  Object[] bindArguments;

  private final Stack<QScope> scopes = new Stack<>();

  private final InternalCache cache;

  /**
   * a Sequentially increasing number identifying a scope & also indicating whether a given scope
   * came prior or later to another scope. It is needed to distinguish between two scopes having
   * same nesting level relative to the top scope
   */
  private int scopeNum = 0;

  /**
   * Dependency graph. Maps CompiledValues in tree to the RuntimeIterators each node is dependent
   * on. This information is computed just before the query is evaluated. The information is good
   * for only one execution, since regions can be destroyed and re-created with different type
   * constraints. Type of this map: map &lt;CompiledValue, set &lt;RuntimeIterator&gt;&gt;
   */
  private Map<CompiledValue, Set<RuntimeIterator>> dependencyGraph = new HashMap<>();

  /**
   * Map which stores the CompiledIteratorDef as the key & the value is the set of Independent
   * RuntimeIterators on which it is dependent upon. The idea is that this Map will identify the
   * final Independent RuntimeIterator or Iterators , ie. those referring to a Region or
   * BindArgument, on which the CompiledIteratorDef depends upon .
   */
  private final Map<CompiledIteratorDef, Set<RuntimeIterator>> itrDefToIndpndtRuntimeItrMap =
      new HashMap<>();

  /**
   * This Map will store its Region Path String against an Independent RuntimeIterator An entry in
   * this Map will be only for those RuntimeIterators which have an underlying Region as its
   * Collection Expression
   */
  private final Map<RuntimeIterator, String> indpndtItrToRgnMap = new HashMap<>();

  // used when querying on a PR: Substitute reference to PartitionedRegion with BucketRegion
  private BucketRegion bukRgn = null;
  private PartitionedRegion pr = null;
  private boolean distinct = false;
  private Object currentProjectionField = null;
  private boolean isPRQueryNode = false;

  private Optional<ScheduledFuture> cancellationTask;
  private volatile CacheRuntimeException canceledException;
  static final ThreadLocal<AtomicBoolean> isCanceled =
      ThreadLocal.withInitial(AtomicBoolean::new);

  /**
   * Authorizer to use during query execution.
   * It can not be changed for queries in flight, but it should be modifiable for running CQs,
   * that's the only reason why this field is not marked as {@code final}.
   *
   * @see ExecutionContext#reset()
   */
  private MethodInvocationAuthorizer methodInvocationAuthorizer;
  private final QueryConfigurationService queryConfigurationService;

  /**
   * Returns the {@link MethodInvocationAuthorizer} that will be used, if needed, during the
   * execution of the query associated with this context.
   *
   * @return the {@link MethodInvocationAuthorizer} that will be used, if needed, during the
   *         execution of the query associated with this context.
   */
  public MethodInvocationAuthorizer getMethodInvocationAuthorizer() {
    return methodInvocationAuthorizer;
  }

  /**
   * Param specialIteratorVar name of special variable to use to denote the current iteration
   * element. Used to implement the "this" var in the query shortcut methods
   *
   * @see org.apache.geode.cache.Region#query
   */
  public ExecutionContext(Object[] bindArguments, InternalCache cache) {
    this.cache = cache;
    this.bindArguments = bindArguments;
    this.cancellationTask = Optional.empty();
    this.queryConfigurationService = cache.getService(QueryConfigurationService.class);
    this.methodInvocationAuthorizer = queryConfigurationService.getMethodAuthorizer();
  }

  Optional<ScheduledFuture> getCancellationTask() {
    return cancellationTask;
  }

  void setCancellationTask(final ScheduledFuture cancellationTask) {
    this.cancellationTask = Optional.of(cancellationTask);
  }

  public CachePerfStats getCachePerfStats() {
    return cache.getCachePerfStats();
  }

  /**
   * Add RuntimeIterator as a dependency of a CompiledValue. ASSUMPTION: unsynchronized, assumed to
   * be single-threaded.
   *
   * @return the dependency set as a shortcut
   */
  Set addDependency(CompiledValue cv, RuntimeIterator itr) {
    Set<RuntimeIterator> ds = getDependencySet(cv, false);
    ds.add(itr);
    return ds;
  }

  /** @return the dependency set as a shortcut */
  public Set addDependencies(CompiledValue cv, Set<RuntimeIterator> set) {
    if (set.isEmpty()) {
      return getDependencySet(cv, true);
    }
    Set<RuntimeIterator> ds = getDependencySet(cv, false);
    ds.addAll(set);
    return ds;
  }

  /**
   * Return true if given CompiledValue is dependent on any RuntimeIterator in current scope
   */
  boolean isDependentOnCurrentScope(CompiledValue cv) {
    // return !getDependencySet(cv, true).isEmpty();
    Set<RuntimeIterator> setRItr = getDependencySet(cv, true);
    boolean isDependent = false;
    if (!setRItr.isEmpty()) {
      int currScopeID = currentScope().getScopeID();
      for (RuntimeIterator ritr : setRItr) {
        if (currScopeID == ritr.getScopeID()) {
          isDependent = true;
          break;
        }
      }
    }
    return isDependent;
  }

  /**
   * Return true if given CompiledValue is dependent on any RuntimeIterator in all of the scopes
   */
  boolean isDependentOnAnyIterator(CompiledValue cv) {
    return !getDependencySet(cv, true).isEmpty();
  }

  /**
   * Return true if given CompiledValue is dependent on specified RuntimeIterator
   */
  boolean isDependentOn(CompiledValue cv, RuntimeIterator itr) {
    return getDependencySet(cv, true).contains(itr);
  }

  Set<RuntimeIterator> getDependencySet(CompiledValue cv, boolean readOnly) {
    Set<RuntimeIterator> set = dependencyGraph.get(cv);
    if (set == null) {
      if (readOnly) {
        return Collections.emptySet();
      }
      set = new HashSet<>(1);
      dependencyGraph.put(cv, set);
    }
    return set;
  }

  /**
   * Returns all dependencies in from this context which are reused during index update by new
   * ExecutionContext for concurrent updates on indexes.
   *
   * @return All {@link AbstractCompiledValue} dependencies.
   */
  public Map getDependencyGraph() {
    return dependencyGraph;
  }

  public void setDependencyGraph(Map<CompiledValue, Set<RuntimeIterator>> dependencyGraph) {
    this.dependencyGraph = dependencyGraph;
  }

  public Object getBindArgument(int index) {
    if (index > bindArguments.length) {
      throw new IllegalArgumentException(
          "Too few query parameters");
    }
    return bindArguments[index - 1];
  }

  /** bind a named iterator (to current scope) */
  public void bindIterator(RuntimeIterator itr) {
    QScope currentScope = currentScope();
    int currScopeID = currentScope.getScopeID();
    itr.setScopeID(currScopeID);
    currentScope.bindIterator(itr);
  }

  public CompiledValue resolve(String name) throws TypeMismatchException, AmbiguousNameException {
    CompiledValue value = resolveAsVariable(name);
    if (value != null) {
      return value;
    }
    // attribute name or operation name (no args) of a variable in the current scope when there is
    // no ambiguity, i.e. this property name belongs to only one variable in the scope
    value = resolveImplicitPath(name);
    if (value == null)
    // cannot be resolved
    {
      throw new TypeMismatchException(
          String.format("The attribute or method name ' %s ' could not be resolved",
              name));
    }
    return value;
  }

  /** Return null if cannot be resolved as a variable in current scope */
  private CompiledValue resolveAsVariable(String name) {
    CompiledValue value;
    for (int i = scopes.size() - 1; i >= 0; i--) {
      QScope scope = scopes.get(i);
      value = scope.resolve(name);
      if (value != null) {
        return value;
      }
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
   * @return the scope ID which can be associated with the scope
   */
  int associateScopeID() {
    return ++scopeNum;
  }


  QScope currentScope() {
    return scopes.peek();
  }

  public List getCurrentIterators() {
    return currentScope().getIterators();
  }

  /**
   * This function returns a List of RuntimeIterators which have ultimate dependency on the Single
   * Independent Iterator which is passed as a parameter to the function. For correct usage it is
   * necessary that the RuntimeIterator passed is independent. If there are no dependent Iterators
   * then the list will just contain one element which will be the RuntimeIterator passed as
   * argument . Also the self independent Runtime Iterator present in the scope ( that is the
   * RuntimeIterator same as the independent iterator passed as argument) is added at start of the
   * list. If an iterator is dependent on more than one independent iterator, it is not added to the
   * List
   * <p>
   * TODO: If we are storing a single Iterator instead of Set , in the itrDefToIndpndtRuntimeItrMap
   * , we need to take care of this function.
   *
   * @param rIter Independent RuntimeIterator on which dependent iterators of current scope need to
   *        identified
   * @return List containing the independent Runtime Iterator & its dependent iterators
   */
  public List getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(RuntimeIterator rIter) {
    List<RuntimeIterator> list = new ArrayList<>();
    list.add(rIter);
    for (RuntimeIterator iteratorInCurrentScope : currentScope().getIterators()) {
      Set<RuntimeIterator> itrSet =
          itrDefToIndpndtRuntimeItrMap.get(iteratorInCurrentScope.getCmpIteratorDefn());
      if (rIter != iteratorInCurrentScope && itrSet.size() == 1
          && itrSet.iterator().next() == rIter) {
        list.add(iteratorInCurrentScope);
      }
    }
    return list;
  }

  void setOneIndexLookup(boolean b) {
    QScope scope = currentScope();
    Support.Assert(scope != null, "must be called within valid scope");
    scope._oneIndexLookup = b;
  }

  public InternalCache getCache() {
    return cache;
  }

  private CompiledValue resolveImplicitPath(String name) throws AmbiguousNameException {
    CompiledValue result = resolveImplicitOperationName(name, 0, false);
    return (result == null) ? null : new CompiledPath(result, name);
  }

  /**
   * returns implicit iterator receiver of operation with numArgs args, or null if cannot be
   * resolved.
   *
   * SPECIAL CASE: If we are unable to resolve the name on any iterator, but there is only one
   * iterator that we don't have type information for it (for now OBJECT_TYPE, this has to change),
   * then return that one iterator under the assumption that the operation name must belong to it.
   */
  RuntimeIterator resolveImplicitOperationName(String name, int numArgs, boolean mustBeMethod)
      throws AmbiguousNameException {
    // iterate through all properties of iterator variables in scope
    // to see if there is a unique resolution
    RuntimeIterator oneUnknown = null;
    List<RuntimeIterator> hits = new ArrayList<>(2);
    boolean foundOneUnknown = false;
    NEXT_SCOPE: for (int i = scopes.size() - 1; i >= 0; i--) {
      QScope scope = scopes.get(i);
      for (RuntimeIterator itr : scope.getIterators()) {
        Assert.assertTrue(itr != null);
        // if scope is limited to this iterator, then don't check any more
        // iterators in this scope
        if (scope.getLimit() == itr) {
          continue NEXT_SCOPE; // don't go any farther in this scope
        }
        // If Element type is ObjectType then we don't need to apply reflection to find out field or
        // method. This save lot of CPU time.
        if (!TypeUtils.OBJECT_TYPE.equals(itr.getElementType())
            && itr.containsProperty(this, name, numArgs, mustBeMethod)) {
          hits.add(itr);
        } else if (TypeUtils.OBJECT_TYPE.equals(itr.getElementType())) {
          if (foundOneUnknown) {
            oneUnknown = null; // more than one
          } else {
            foundOneUnknown = true;
            oneUnknown = itr;
          }
        }
      }
    }
    if (hits.size() == 1) {
      return hits.get(0);
    }
    if (hits.size() > 1) {
      // ambiguous
      if (mustBeMethod) {
        throw new AmbiguousNameException(
            String.format(
                "Method named ' %s ' with %s arguments is ambiguous because it can apply to more than one variable in scope.",
                name, numArgs));
      }
      throw new AmbiguousNameException(
          String.format(
              "Attribute named ' %s ' is ambiguous because it can apply to more than one variable in scope.",
              name));
    }
    // if there is a single unknown, then return that one under the assumption
    // that the name must belong to it
    // otherwise, returns null, unable to resolve here
    return oneUnknown;
  }

  /**
   * Tries to find for RuntimeIterator associated with specified expression
   */
  RuntimeIterator findRuntimeIterator(CompiledValue expr) {
    // Check if expr is itself RuntimeIterator
    if (expr instanceof RuntimeIterator) {
      return (RuntimeIterator) expr;
    }
    // Try to find RuntimeIterator
    return (RuntimeIterator) findIterator(expr);
  }

  private CompiledValue findIterator(CompiledValue path) {
    try {
      if (path == null) {
        return null;
      }
      if (path instanceof RuntimeIterator) {
        return path;
      }
      if (path instanceof CompiledPath) {
        CompiledValue rec = path.getReceiver();
        return findIterator(rec);
      }
      if (path instanceof CompiledOperation) {
        CompiledOperation operation = (CompiledOperation) path;
        CompiledValue rec = operation.getReceiver(this);
        if (rec == null) {
          return resolveImplicitOperationName(operation.getMethodName(),
              operation.getArguments().size(), true);
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
      } // if we get these exceptions return null
    } catch (TypeMismatchException | NameResolutionException ignore) {
    }
    return null;
  }

  /**
   * Calculates set of Runtime Iterators on which a given CompiledValue ultimately depends. The
   * independent iterators may belong to other scopes.
   * <p>
   * This function will populate the set to its independent RuntimeIterators. However if the
   * CompiledValue happens to be a CompiledIteratorDef & if it is independent of any other
   * RuntimeIterators then no addition will be done in the Set.
   * <p>
   * TODO: the behavior of this function will change if we modify the computeDependency function of
   * the CompiledIteratorDef as in that case the Set will be added with the self RuntimeIterator (
   * if the CompiledIteratorDef is independent) which is not the case now.
   * <p>
   * TODO: If a CompiledIteratorDef has only one dependent RuntimeIterator should it still be stored
   * in a Set or should it be a single value?
   */
  void computeUltimateDependencies(CompiledValue cv, Set<RuntimeIterator> set) {
    Set<RuntimeIterator> dependencySet = getDependencySet(cv, true);
    for (RuntimeIterator rIter : dependencySet) {
      Set<RuntimeIterator> indRuntimeIterators =
          itrDefToIndpndtRuntimeItrMap.get(rIter.getCmpIteratorDefn());
      if (indRuntimeIterators != null) {
        set.addAll(indRuntimeIterators);
      }
    }
  }

  /**
   * This function populates the Map itrDefToIndpndtRuntimeItrMap. It creates a Set of
   * RuntimeIterators to which the current CompilediteratorDef is dependent upon. Also it sets the
   * index_internal_id for the RuntimeIterator, which is used for calculating the canonicalized
   * iterator definitions for identifying the available index.
   *
   * @param itrDef CompiledIteratorDef object representing iterator in the query from clause
   */
  public void addToIndependentRuntimeItrMap(CompiledIteratorDef itrDef)
      throws TypeMismatchException, NameResolutionException {
    Set<RuntimeIterator> set = new HashSet<>();
    computeUltimateDependencies(itrDef, set);
    RuntimeIterator itr = null;
    String rgnPath = null;
    // If the set is empty then add the self RuntimeIterator to the Map.
    if (set.isEmpty()) {
      itr = itrDef.getRuntimeIterator(this);
      set.add(itr);
      // Since it is a an independent RuntimeIterator , check if its Collection Expr boils down to a
      // Region. If it is , we need to store the QRegion in the Map
      CompiledValue startVal =
          QueryUtils.obtainTheBottomMostCompiledValue(itrDef.getCollectionExpr());
      if (startVal.getType() == OQLLexerTokenTypes.RegionPath) {
        rgnPath = ((QRegion) ((CompiledRegion) startVal).evaluate(this)).getFullPath();
        indpndtItrToRgnMap.put(itr, rgnPath);
      } else if (startVal.getType() == OQLLexerTokenTypes.QUERY_PARAM) {
        Object rgn;
        CompiledBindArgument cba = (CompiledBindArgument) startVal;
        if ((rgn = cba.evaluate(this)) instanceof Region) {
          indpndtItrToRgnMap.put(itr, rgnPath = ((Region) rgn).getFullPath());
        }
      }
    }
    itrDefToIndpndtRuntimeItrMap.put(itrDef, set);
    IndexManager mgr = null;
    // Set the canonicalized index_internal_id if the condition is satisfied
    if (set.size() == 1) {
      if (itr == null) {
        itr = set.iterator().next();
        if (itr.getScopeID() == currentScope().getScopeID()) {
          rgnPath = indpndtItrToRgnMap.get(itr);
        }
      }
      if (rgnPath != null) {
        mgr = IndexUtils.getIndexManager(cache, cache.getRegion(rgnPath), false);
        // put a check for null and see if we will be executing on a bucket region.
        if ((null == mgr) && (null != bukRgn)) {
          // for bucket region index use
          mgr = IndexUtils.getIndexManager(cache, cache.getRegion(bukRgn.getFullPath()), false);
        }
      }
    }
    String tempIndexID;
    RuntimeIterator currItr = itrDef.getRuntimeIterator(this);
    currItr.setIndexInternalID((mgr == null
        || (tempIndexID = mgr.getCanonicalizedIteratorName(itrDef.genFromClause(this))) == null)
            ? currItr.getInternalId() : tempIndexID);
  }

  List getAllIndependentIteratorsOfCurrentScope() {
    List<RuntimeIterator> independentIterators = new ArrayList<>(indpndtItrToRgnMap.size());
    int currentScopeId = currentScope().getScopeID();
    for (RuntimeIterator rIter : indpndtItrToRgnMap.keySet()) {
      if (rIter.getScopeID() == currentScopeId) {
        independentIterators.add(rIter);
      }
    }
    return independentIterators;
  }

  /**
   * This method returns the Region path for the independent RuntimeIterator if itr exists else
   * returns null. It is the caller's responsibility to ensure that the passed Iterator is the
   * ultimate Independent Runtime Iterator or else the method may return null if the RunTimeIterator
   * is genuinely dependent on a Region iterator
   *
   * @return String containing region path
   */
  String getRegionPathForIndependentRuntimeIterator(RuntimeIterator riter) {
    return indpndtItrToRgnMap.get(riter);
  }

  /**
   * Populates the independent runtime iterator map for index creation purposes. This method does
   * not create any canonicalized index ids etc.
   */
  public void addToIndependentRuntimeItrMapForIndexCreation(CompiledIteratorDef itrDef)
      throws TypeMismatchException, NameResolutionException {

    Set<RuntimeIterator> set = new HashSet<>();
    computeUltimateDependencies(itrDef, set);
    // If the set is empty then add the self RuntimeIterator to the Map.
    if (set.isEmpty()) {
      RuntimeIterator itr = itrDef.getRuntimeIterator(this);
      set.add(itr);
    }
    itrDefToIndpndtRuntimeItrMap.put(itrDef, set);
  }

  public void setBindArguments(Object[] bindArguments) {
    this.bindArguments = bindArguments;
  }

  public int getScopeNum() {
    return scopeNum;
  }

  /**
   * Added to reset the state from the last execution. This is added for CQs only.
   */
  public void reset() {
    scopes.clear();
    // Make sure we use the most up to date authorizer in CQs.
    methodInvocationAuthorizer = queryConfigurationService.getMethodAuthorizer();
  }

  public BucketRegion getBucketRegion() {
    return bukRgn;
  }

  public void setBucketRegion(PartitionedRegion pr, BucketRegion bukRgn) {
    this.bukRgn = bukRgn;
    this.pr = pr;
  }

  public PartitionedRegion getPartitionedRegion() {
    return pr;
  }

  void cachePut(Object key, Object value) {}

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

  public Query getQuery() {
    throw new UnsupportedOperationException("Method should not have been called");
  }

  public void setBucketList(List list) {
    throw new UnsupportedOperationException("Method should not have been called");
  }

  public PdxString getSavedPdxString(int index) {
    throw new UnsupportedOperationException("Method should not have been called");
  }

  public boolean isDistinct() {
    return distinct;
  }

  public void setDistinct(boolean distinct) {
    this.distinct = distinct;
  }

  boolean isBindArgsSet() {
    return bindArguments != null;
  }

  void setCurrentProjectionField(Object field) {
    this.currentProjectionField = field;
  }

  Object getCurrentProjectionField() {
    return currentProjectionField;
  }

  public void setIsPRQueryNode(boolean isPRQueryNode) {
    this.isPRQueryNode = isPRQueryNode;
  }

  boolean getIsPRQueryNode() {
    return isPRQueryNode;
  }

  /**
   * Check to see if the query execution was canceled. The query gets canceled by the QueryMonitor
   * if it takes more than the max query execution time or low memory situations
   */
  public boolean isCanceled() {
    return getQueryCanceledException() != null;
  }

  public CacheRuntimeException getQueryCanceledException() {
    return canceledException;
  }

  public void setQueryCanceledException(final CacheRuntimeException queryCanceledException) {
    this.canceledException = queryCanceledException;
  }

  /**
   * This method attempts to reintrepret a {@link QueryExecutionCanceledException} using the
   * the value returned by {@link #getQueryCanceledException} (set by the {@link QueryMonitor}).
   *
   * @throws CacheRuntimeException if {@link #getQueryCanceledException} doesn't return {@code null}
   * @throws QueryExecutionCanceledException otherwise
   */
  Object reinterpretQueryExecutionCanceledException() {
    final CacheRuntimeException queryCanceledException = getQueryCanceledException();
    if (queryCanceledException != null) {
      throw queryCanceledException;
    } else {
      throw new QueryExecutionCanceledException(
          "Query was canceled. It may be due to low memory or the query was running longer than the MAX_QUERY_EXECUTION_TIME.");
    }
  }
}

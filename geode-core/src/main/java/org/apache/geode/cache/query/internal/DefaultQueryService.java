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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.internal.InternalPool;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.ServerProxy;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.CqServiceStatistics;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexCreationException;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.cq.ClientCQ;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.cache.query.internal.index.AbstractIndex;
import org.apache.geode.cache.query.internal.index.IndexCreationData;
import org.apache.geode.cache.query.internal.index.IndexData;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexUtils;
import org.apache.geode.cache.query.internal.index.PartitionedIndex;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.MemoryThresholdInfo;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.MemoryThresholds;
import org.apache.geode.internal.logging.LogService;

/**
 * @version $Revision: 1.2 $
 */
public class DefaultQueryService implements InternalQueryService {
  private static final Logger logger = LogService.getLogger();

  /**
   * System property to allow query on region with heterogeneous objects. By default its set to
   * false.
   */
  public static final boolean QUERY_HETEROGENEOUS_OBJECTS = Boolean
      .valueOf(System.getProperty(
          DistributionConfig.GEMFIRE_PREFIX + "QueryService.QueryHeterogeneousObjects", "true"))
      .booleanValue();

  public static boolean COPY_ON_READ_AT_ENTRY_LEVEL = Boolean
      .valueOf(System.getProperty(
          DistributionConfig.GEMFIRE_PREFIX + "QueryService.CopyOnReadAtEntryLevel", "false"))
      .booleanValue();

  public static boolean ALLOW_UNTRUSTED_METHOD_INVOCATION = Boolean.getBoolean(
      DistributionConfig.GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation");


  /** Test purpose only */
  public static boolean TEST_QUERY_HETEROGENEOUS_OBJECTS = false;

  private final InternalCache cache;

  private final MethodInvocationAuthorizer methodInvocationAuthorizer;

  private InternalPool pool;

  private Map<Region, HashSet<IndexCreationData>> indexDefinitions =
      Collections.synchronizedMap(new HashMap<Region, HashSet<IndexCreationData>>());


  public DefaultQueryService(InternalCache cache) {
    if (cache == null)
      throw new IllegalArgumentException(
          "cache must not be null");
    this.cache = cache;
    if (!cache.getSecurityService().isIntegratedSecurity() || ALLOW_UNTRUSTED_METHOD_INVOCATION) {
      // A no-op authorizer, allow method invocation
      this.methodInvocationAuthorizer = ((Method m, Object t) -> {
      });
    } else {
      this.methodInvocationAuthorizer =
          new RestrictedMethodInvocationAuthorizer(cache.getSecurityService());

    }
  }

  /**
   * Constructs a new <code>Query</code> object. Uses the default namespace, which is the Objects
   * Context of the current application.
   *
   * @return The new <code>Query</code> object.
   * @throws IllegalArgumentException if the query syntax is invalid.
   * @see org.apache.geode.cache.query.Query
   */
  public Query newQuery(String queryString) {
    if (QueryMonitor.isLowMemory()) {
      String reason = String.format(
          "Query execution canceled due to memory threshold crossed in system, memory used: %s bytes.",
          QueryMonitor.getMemoryUsedDuringLowMemory());
      throw new QueryExecutionLowMemoryException(reason);
    }
    if (queryString == null)
      throw new QueryInvalidException(
          "The query string must not be null");
    if (queryString.length() == 0)
      throw new QueryInvalidException(
          "The query string must not be empty");
    ServerProxy serverProxy = pool == null ? null : new ServerProxy(pool);
    DefaultQuery query = new DefaultQuery(queryString, this.cache, serverProxy != null);
    query.setServerProxy(serverProxy);
    return query;
  }

  public Query newQuery(String queryString, ProxyCache proxyCache) {
    Query query = newQuery(queryString);
    ((DefaultQuery) query).setProxyCache(proxyCache);
    return query;
  }

  public Index createHashIndex(String indexName, String indexedExpression, String fromClause)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    return createHashIndex(indexName, indexedExpression, fromClause, null);
  }

  public Index createHashIndex(String indexName, String indexedExpression, String fromClause,
      String imports)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    return createIndex(indexName, IndexType.HASH, indexedExpression, fromClause, imports);
  }

  public Index createIndex(String indexName, String indexedExpression, String fromClause)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    return createIndex(indexName, IndexType.FUNCTIONAL, indexedExpression, fromClause, null);
  }

  public Index createIndex(String indexName, String indexedExpression, String fromClause,
      String imports)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    return createIndex(indexName, IndexType.FUNCTIONAL, indexedExpression, fromClause, imports);
  }

  public Index createKeyIndex(String indexName, String indexedExpression, String fromClause)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    return createIndex(indexName, IndexType.PRIMARY_KEY, indexedExpression, fromClause, null);
  }

  public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
      String fromClause)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    return createIndex(indexName, indexType, indexedExpression, fromClause, null);
  }

  public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
      String fromClause, String imports, boolean loadEntries)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    return createIndex(indexName, indexType, indexedExpression, fromClause, imports, loadEntries,
        null);
  }

  public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
      String fromClause, String imports, boolean loadEntries, Region region)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {


    if (pool != null) {
      throw new UnsupportedOperationException(
          "Index creation on the server is not supported from the client.");
    }
    PartitionedIndex parIndex = null;
    if (region == null) {
      region = getRegionFromPath(imports, fromClause);
    }
    RegionAttributes ra = region.getAttributes();


    // Asif: If the evistion action is Overflow to disk then do not allow index creation
    // It is Ok to have index creation if it is persist only mode as data will always
    // exist in memory
    // if(ra.getEvictionAttributes().getAction().isOverflowToDisk() ) {
    // throw new
    // UnsupportedOperationException(String.format("The specified index conditions are not supported
    // for regions which overflow to disk. The region involved is %s",regionPath));
    // }
    // if its a pr the create index on all of the local buckets.
    if (!MemoryThresholds.isLowMemoryExceptionDisabled()) {
      InternalRegion internalRegion = (InternalRegion) region;
      MemoryThresholdInfo info = internalRegion.getAtomicThresholdInfo();
      if (info.isMemoryThresholdReached()) {
        throw new LowMemoryException(
            String.format(
                "Cannot create index on region %s because the target member is running low on memory",
                region.getName()),
            info.getMembersThatReachedThreshold());
      }
    }
    if (region instanceof PartitionedRegion) {
      try {
        parIndex = (PartitionedIndex) ((PartitionedRegion) region).createIndex(false, indexType,
            indexName, indexedExpression, fromClause, imports, loadEntries);
      } catch (ForceReattemptException ex) {
        region.getCache().getLogger().info(
            "Exception while creating index on pr default query processor.",
            ex);
      } catch (IndexCreationException exx) {
        region.getCache().getLogger().info(
            "Exception while creating index on pr default query processor.",
            exx);
      }
      return parIndex;

    } else {

      IndexManager indexManager = IndexUtils.getIndexManager(this.cache, region, true);
      Index index = indexManager.createIndex(indexName, indexType, indexedExpression, fromClause,
          imports, null, null, loadEntries);

      return index;
    }
  }


  public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
      String fromClause, String imports)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {

    return createIndex(indexName, indexType, indexedExpression, fromClause, imports, true);
  }

  private Region getRegionFromPath(String imports, String fromClause)
      throws RegionNotFoundException {
    QCompiler compiler = new QCompiler();
    if (imports != null) {
      compiler.compileImports(imports);
    }
    List list = compiler.compileFromClause(fromClause);
    CompiledValue cv = QueryUtils
        .obtainTheBottomMostCompiledValue(((CompiledIteratorDef) list.get(0)).getCollectionExpr());
    String regionPath = null;
    if (cv.getType() == OQLLexerTokenTypes.RegionPath) {
      regionPath = ((CompiledRegion) cv).getRegionPath();
    } else {
      throw new RegionNotFoundException(
          String.format(
              "DefaultQueryService::createIndex:First Iterator of Index >From Clause does not evaluate to a Region Path. The from clause used for Index creation is %s",
              fromClause));
    }
    Region region = cache.getRegion(regionPath);
    if (region == null) {
      throw new RegionNotFoundException(
          String.format("Region ' %s ' not found: from %s",
              new Object[] {regionPath, fromClause}));
    }
    return region;
  }

  /**
   * Asif : Gets an exact match index ( match level 0)
   *
   * @param regionPath String containing the region name
   * @param definitions An array of String objects containing canonicalized definitions of
   *        RuntimeIterators. A Canonicalized definition of a RuntimeIterator is the canonicalized
   *        expression obtainded from its underlying collection expression.
   * @param indexType IndexType object which can be either of type RangeIndex or PrimaryKey Index
   * @param indexedExpression CompiledValue containing the path expression on which index needs to
   *        be created
   * @param context ExecutionContext
   * @return IndexData object
   */
  public IndexData getIndex(String regionPath, String[] definitions, IndexType indexType,
      CompiledValue indexedExpression, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    Region region = cache.getRegion(regionPath);
    if (region == null) {
      return null;
    }
    IndexManager indexManager = IndexUtils.getIndexManager(cache, region, true);
    IndexData indexData = indexManager.getIndex(indexType, definitions, indexedExpression, context);
    return indexData;
  }

  public Index getIndex(Region region, String indexName) {

    if (pool != null) {
      throw new UnsupportedOperationException(
          "Index Operation is not supported on the Server Region.");
    }

    // A Partition Region does not have an IndexManager, but it's buckets have.
    if (region instanceof PartitionedRegion) {
      return (Index) ((PartitionedRegion) region).getIndex().get(indexName);
    } else {
      IndexManager indexManager = IndexUtils.getIndexManager(cache, region, false);
      if (indexManager == null)
        return null;
      return indexManager.getIndex(indexName);
    }
  }

  /**
   * Asif: Gets a best match index which is available. An index with match level equal to 0 is the
   * best index to use as it implies that the query from clause iterators belonging to the region
   * exactly match the index from clause iterators ( the difference in the relative positions of the
   * iterators do not matter). A match level less than 0 means that number of iteratots in the index
   * resultset is more than that present in the query from clause and hence index resultset will
   * need a cutdown. A match level greater than 0 means that there definitely is atleast one
   * iterator in the query from clause which is more than the index from clause iterators & hence
   * definitely expansion of index results will be needed. Pls note that a match level greater than
   * 0 does not imply that index from clause does not have an extra iterator in it , too. Hence a
   * match level greater than 0 will definitely mean expansion of index results but may also require
   * a cut down of results . The order of preference is match level 0 , less than 0 and lastly
   * greater than 0
   *
   * @param regionPath String containing the region name
   * @param definitions An array of String objects containing canonicalized definitions of
   *        RuntimeIterators. A Canonicalized definition of a RuntimeIterator is the canonicalized
   *        expression obtainded from its underlying collection expression.
   * @param indexType IndexType object which can be either of type RangeIndex or PrimaryKey Index
   * @param indexedExpression CompiledValue representing the path expression on which index needs to
   *        be created
   * @param context ExecutionContext object
   * @return IndexData object
   */
  public IndexData getBestMatchIndex(String regionPath, String definitions[], IndexType indexType,
      CompiledValue indexedExpression, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    Region region = cache.getRegion(regionPath);
    if (region == null) {
      return null;
    }
    // return getBestMatchIndex(region, indexType, definitions,
    // indexedExpression);
    IndexManager indexManager = IndexUtils.getIndexManager(cache, region, false);
    if (indexManager == null) {
      return null;
    }
    return indexManager.getBestMatchIndex(indexType, definitions, indexedExpression, context);
  }

  public Collection getIndexes() {
    ArrayList allIndexes = new ArrayList();
    Iterator rootRegions = cache.rootRegions().iterator();
    while (rootRegions.hasNext()) {
      Region region = (Region) rootRegions.next();
      allIndexes.addAll(getIndexes(region));

      Iterator subRegions = region.subregions(true).iterator();
      while (subRegions.hasNext()) {
        allIndexes.addAll(getIndexes((Region) subRegions.next()));
      }
    }

    return allIndexes;
  }

  public Collection getIndexes(Region region) {

    if (pool != null) {
      throw new UnsupportedOperationException(
          "Index Operation is not supported on the Server Region.");
    }

    if (region instanceof PartitionedRegion) {
      return ((PartitionedRegion) region).getIndexes();
    }

    IndexManager indexManager = IndexUtils.getIndexManager(cache, region, false);
    if (indexManager == null) {
      return Collections.emptyList();
    }

    return indexManager.getIndexes();
  }

  public Collection getIndexes(Region region, IndexType indexType) {

    if (pool != null) {
      throw new UnsupportedOperationException(
          "Index Operation is not supported on the Server Region.");
    }

    IndexManager indexManager = IndexUtils.getIndexManager(cache, region, false);
    if (indexManager == null) {
      return Collections.emptyList();
    }

    return indexManager.getIndexes(indexType);
  }

  public void removeIndex(Index index) {

    if (pool != null) {
      throw new UnsupportedOperationException(
          "Index Operation is not supported on the Server Region.");
    }

    Region region = index.getRegion();
    if (region instanceof PartitionedRegion) {
      try {
        ((PartitionedRegion) region).removeIndex(index, false);
      } catch (ForceReattemptException ex) {
        logger.info(String.format("Exception removing index : %s", ex));
      }
      return;
    }
    // get write lock for indexes in replicated region
    // for PR lock will be taken in PartitionRegion.removeIndex
    ((AbstractIndex) index).acquireIndexWriteLockForRemove();
    try {
      IndexManager indexManager = ((LocalRegion) index.getRegion()).getIndexManager();
      indexManager.removeIndex(index);
    } finally {
      ((AbstractIndex) index).releaseIndexWriteLockForRemove();
    }
  }

  public void removeIndexes() {
    if (pool != null) {
      throw new UnsupportedOperationException(
          "Index Operation is not supported on the Server Region.");
    }

    Iterator rootRegions = cache.rootRegions().iterator();
    while (rootRegions.hasNext()) {
      Region region = (Region) rootRegions.next();
      Iterator subRegions = region.subregions(true).iterator();
      while (subRegions.hasNext()) {
        removeIndexes((Region) subRegions.next());
      }
      removeIndexes(region);
    }
  }

  public void removeIndexes(Region region) {

    if (pool != null) {
      throw new UnsupportedOperationException(
          "Index Operation is not supported on the Server Region.");
    }

    // removing indexes on paritioned region will reguire sending message and
    // remvoing all the local indexes on the local bucket regions.
    if (region instanceof PartitionedRegion) {
      try {
        // not remotely orignated
        ((PartitionedRegion) region).removeIndexes(false);
      } catch (ForceReattemptException ex) {
        // will have to throw a proper exception relating to remove index.
        logger.info(String.format("Exception removing index : %s", ex));
      }
    }
    IndexManager indexManager = IndexUtils.getIndexManager(cache, region, false);
    if (indexManager == null)
      return;

    indexManager.removeIndexes();
  }


  // CqService Related API implementation.

  /**
   * Constructs a new continuous query, represented by an instance of CqQuery. The CqQuery is not
   * executed until the execute method is invoked on the CqQuery.
   *
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @return the newly created CqQuery object
   * @throws IllegalArgumentException if queryString or cqAttr is null
   * @throws IllegalStateException if this method is called from a cache server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating managing cq metadata info.
   *         E.g.: Query string should refer only one region, join not supported. The query must be
   *         a SELECT statement. DISTINCT queries are not supported. Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. Bind
   *         parameters in the query are not yet supported.
   */
  public CqQuery newCq(String queryString, CqAttributes cqAttributes)
      throws QueryInvalidException, CqException {
    ClientCQ cq = null;
    try {
      cq = (ClientCQ) getCqService().newCq(null, queryString, cqAttributes, this.pool, false);
    } catch (CqExistsException cqe) {
      // Should not throw in here.
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to createCq. Error :{}", cqe.getMessage(), cqe);
      }
    }
    return cq;
  }

  /**
   * Constructs a new continuous query, represented by an instance of CqQuery. The CqQuery is not
   * executed until the execute method is invoked on the CqQuery.
   *
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @param isDurable true if the CQ is durable
   * @return the newly created CqQuery object
   * @throws IllegalArgumentException if queryString or cqAttr is null
   * @throws IllegalStateException if this method is called from a cache server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating managing cq metadata info.
   *         E.g.: Query string should refer only one region, join not supported. The query must be
   *         a SELECT statement. DISTINCT queries are not supported. Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. Bind
   *         parameters in the query are not yet supported.
   */
  public CqQuery newCq(String queryString, CqAttributes cqAttributes, boolean isDurable)
      throws QueryInvalidException, CqException {
    ClientCQ cq = null;
    try {
      cq = (ClientCQ) getCqService().newCq(null, queryString, cqAttributes, this.pool, isDurable);
    } catch (CqExistsException cqe) {
      // Should not throw in here.
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to createCq. Error :{}", cqe.getMessage(), cqe);
      }
    }
    return cq;
  }


  /**
   * Constructs a new named continuous query, represented by an instance of CqQuery. The CqQuery is
   * not executed, however, until the execute method is invoked on the CqQuery. The name of the
   * query will be used to identify this query in statistics archival.
   *
   * @param cqName the String name for this query
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @return the newly created CqQuery object
   * @throws CqExistsException if a CQ by this name already exists on this client
   * @throws IllegalArgumentException if queryString or cqAttr is null
   * @throws IllegalStateException if this method is called from a cache server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating managing cq metadata info.
   *         E.g.: Query string should refer only one region, join not supported. The query must be
   *         a SELECT statement. DISTINCT queries are not supported. Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. Bind
   *         parameters in the query are not yet supported.
   */
  public CqQuery newCq(String cqName, String queryString, CqAttributes cqAttributes)
      throws QueryInvalidException, CqExistsException, CqException {
    if (cqName == null) {
      throw new IllegalArgumentException(
          "cqName must not be null");
    }
    ClientCQ cq =
        (ClientCQ) getCqService().newCq(cqName, queryString, cqAttributes, this.pool, false);
    return cq;
  }

  /**
   * Constructs a new named continuous query, represented by an instance of CqQuery. The CqQuery is
   * not executed, however, until the execute method is invoked on the CqQuery. The name of the
   * query will be used to identify this query in statistics archival.
   *
   * @param cqName the String name for this query
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @param isDurable true if the CQ is durable
   * @return the newly created CqQuery object
   * @throws CqExistsException if a CQ by this name already exists on this client
   * @throws IllegalArgumentException if queryString or cqAttr is null
   * @throws IllegalStateException if this method is called from a cache server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating managing cq metadata info.
   *         E.g.: Query string should refer only one region, join not supported. The query must be
   *         a SELECT statement. DISTINCT queries are not supported. Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. Bind
   *         parameters in the query are not yet supported.
   */
  public CqQuery newCq(String cqName, String queryString, CqAttributes cqAttributes,
      boolean isDurable) throws QueryInvalidException, CqExistsException, CqException {
    if (cqName == null) {
      throw new IllegalArgumentException(
          "cqName must not be null");
    }
    ClientCQ cq =
        (ClientCQ) getCqService().newCq(cqName, queryString, cqAttributes, this.pool, isDurable);
    return cq;
  }

  /**
   * Close all CQs executing in this VM, and release resources associated with executing CQs.
   * CqQuerys created by other VMs are unaffected.
   *
   */
  public void closeCqs() {
    try {
      getCqService().closeAllCqs(true);
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to closeAll Cqs. Error :{}", cqe.getMessage(), cqe);
      }
    }
  }

  /**
   * Retrieve a CqQuery by name.
   *
   * @return the CqQuery or null if not found
   */
  public CqQuery getCq(String cqName) {
    CqQuery cq = null;
    try {
      cq = (CqQuery) getCqService().getCq(cqName);
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to getCq. Error :{}", cqe.getMessage(), cqe);
      }
    }
    return cq;
  }

  /**
   * Retrieve all CqQuerys created by this VM.
   *
   * @return null if there are no cqs.
   */
  public CqQuery[] getCqs() {
    CqQuery[] cqs = null;
    try {
      return toArray(getCqService().getAllCqs());
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to getAllCqs. Error :{}", cqe.getMessage(), cqe);
      }
    }
    return cqs;
  }

  private CqQuery[] toArray(Collection<? extends InternalCqQuery> allCqs) {
    CqQuery[] cqs = new CqQuery[allCqs.size()];
    allCqs.toArray(cqs);
    return cqs;
  }

  /**
   * Returns all the cq on a given region.
   */
  public CqQuery[] getCqs(final String regionName) throws CqException {
    return toArray(getCqService().getAllCqs(regionName));
  }

  /**
   * Starts execution of all the registered continuous queries for this client. This is
   * complementary to stopCqs.
   *
   * @see QueryService#stopCqs()
   *
   * @throws CqException if failure to execute CQ.
   */
  public void executeCqs() throws CqException {
    try {
      getCqService().executeAllClientCqs();
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to execute all cqs. Error :{}", cqe.getMessage(), cqe);
      }
    }
  }


  /**
   * Stops execution of all the continuous queries for this client to become inactive. This is
   * useful when client needs to control the incoming cq messages during bulk region operations.
   *
   * @see QueryService#executeCqs()
   *
   * @throws CqException if failure to execute CQ.
   */
  public void stopCqs() throws CqException {
    try {
      getCqService().stopAllClientCqs();
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to stop all CQs. Error :{}", cqe.getMessage(), cqe);
      }
    }
  }

  /**
   * Starts execution of all the continuous queries registered on the specified region for this
   * client. This is complementary method to stopCQs().
   *
   * @see QueryService#stopCqs()
   *
   * @throws CqException if failure to stop CQs.
   */
  public void executeCqs(String regionName) throws CqException {
    try {
      getCqService().executeAllRegionCqs(regionName);
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to execute cqs on the specified region. Error :{}", cqe.getMessage(),
            cqe);
      }
    }
  }

  /**
   * Stops execution of all the continuous queries registered on the specified region for this
   * client. This is useful when client needs to control the incoming cq messages during bulk region
   * operations.
   *
   * @see QueryService#executeCqs()
   *
   * @throws CqException if failure to execute CQs.
   */
  public void stopCqs(String regionName) throws CqException {
    try {
      getCqService().stopAllRegionCqs(regionName);
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to stop cqs on the specified region. Error :{}", cqe.getMessage(),
            cqe);
      }
    }
  }

  /**
   * Get statistics information for this query.
   *
   * @return CQ statistics null if the continuous query object not found for the given cqName.
   */
  public CqServiceStatistics getCqStatistics() {
    CqServiceStatistics stats = null;
    try {
      stats = getCqService().getCqStatistics();
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable get CQ Statistics. Error :{}", cqe.getMessage(), cqe);
      }
    }
    return stats;
  }

  /**
   * Is the CQ service in a cache server environment
   *
   * @return true if cache server, false otherwise
   */
  public boolean isServer() {
    if (this.cache.getCacheServers().isEmpty()) {
      return false;
    }
    return true;
  }

  /**
   * Close the CQ Service after clean up if any.
   *
   */
  public void closeCqService() {
    cache.getCqService().close();
  }

  public CqService getCqService() throws CqException {
    CqService service = cache.getCqService();
    service.start();
    return service;
  }

  public void setPool(InternalPool pool) {
    this.pool = pool;
    if (logger.isDebugEnabled()) {
      logger.debug("Setting ServerProxy with the Query Service using the pool :{} ",
          pool.getName());
    }
  }

  public List<String> getAllDurableCqsFromServer() throws CqException {
    if (!isServer()) {
      if (pool != null) {
        return getCqService().getAllDurableCqsFromServer(pool);
      } else {
        throw new UnsupportedOperationException(
            "GetAllDurableCQsFromServer requires a pool to be configured.");
      }
    } else {
      // we are a server
      return Collections.EMPTY_LIST;
    }
  }

  public UserAttributes getUserAttributes(String cqName) {
    try {
      return getCqService().getUserAttributes(cqName);
    } catch (CqException ce) {
      return null;
    }
  }

  @Override
  public void defineKeyIndex(String indexName, String indexedExpression, String fromClause)
      throws RegionNotFoundException {
    defineIndex(indexName, IndexType.PRIMARY_KEY, indexedExpression, fromClause, null);
  }

  @Override
  public void defineHashIndex(String indexName, String indexedExpression, String fromClause)
      throws RegionNotFoundException {
    defineIndex(indexName, IndexType.HASH, indexedExpression, fromClause, null);
  }

  @Override
  public void defineHashIndex(String indexName, String indexedExpression, String fromClause,
      String imports) throws RegionNotFoundException {
    defineIndex(indexName, IndexType.HASH, indexedExpression, fromClause, imports);
  }

  @Override
  public void defineIndex(String indexName, String indexedExpression, String fromClause)
      throws RegionNotFoundException {
    defineIndex(indexName, IndexType.FUNCTIONAL, indexedExpression, fromClause, null);
  }


  @Override
  public void defineIndex(String indexName, String indexedExpression, String fromClause,
      String imports) throws RegionNotFoundException {
    defineIndex(indexName, IndexType.FUNCTIONAL, indexedExpression, fromClause, imports);
  }

  public void defineIndex(String indexName, IndexType indexType, String indexedExpression,
      String fromClause, String imports) throws RegionNotFoundException {
    IndexCreationData indexData = new IndexCreationData(indexName);
    indexData.setIndexData(indexType, fromClause, indexedExpression, imports);
    Region r = getRegionFromPath(imports, fromClause);
    synchronized (indexDefinitions) {
      HashSet<IndexCreationData> s = indexDefinitions.get(r);
      if (s == null) {
        s = new HashSet<IndexCreationData>();
      }
      s.add(indexData);
      indexDefinitions.put(r, s);
    }
  }

  @Override
  public List<Index> createDefinedIndexes() throws MultiIndexCreationException {
    HashSet<Index> indexes = new HashSet<Index>();
    boolean throwException = false;
    HashMap<String, Exception> exceptionsMap = new HashMap<String, Exception>();

    synchronized (indexDefinitions) {
      for (Entry<Region, HashSet<IndexCreationData>> e : indexDefinitions.entrySet()) {
        Region region = e.getKey();
        HashSet<IndexCreationData> icds = e.getValue();
        if (region instanceof PartitionedRegion) {
          throwException =
              createDefinedIndexesForPR(indexes, (PartitionedRegion) region, icds, exceptionsMap);
        } else {
          throwException =
              createDefinedIndexesForReplicatedRegion(indexes, region, icds, exceptionsMap);
        }
      }
    } // end sync

    if (throwException) {
      throw new MultiIndexCreationException(exceptionsMap);
    }

    return new ArrayList<Index>(indexes);
  }

  private boolean createDefinedIndexesForPR(HashSet<Index> indexes, PartitionedRegion region,
      HashSet<IndexCreationData> icds, HashMap<String, Exception> exceptionsMap) {
    try {
      indexes.addAll(((PartitionedRegion) region).createIndexes(false, icds));
    } catch (IndexCreationException e1) {
      logger.info("Exception while creating index on pr default query processor.",
          e1);
    } catch (CacheException e1) {
      logger.info(
          "Exception while creating index on pr default query processor.",
          e1);
      return true;
    } catch (ForceReattemptException e1) {
      logger.info("Exception while creating index on pr default query processor.",
          e1);
      return true;
    } catch (MultiIndexCreationException e) {
      exceptionsMap.putAll(e.getExceptionsMap());
      return true;
    }
    return false;
  }

  private boolean createDefinedIndexesForReplicatedRegion(HashSet<Index> indexes, Region region,
      Set<IndexCreationData> icds, HashMap<String, Exception> exceptionsMap) {
    boolean throwException = false;
    for (IndexCreationData icd : icds) {
      try {
        // First step is creating all the defined indexes. Do this only if
        // the region is not PR. For PR creation and population is done in
        // the PartitionedRegion#createDefinedIndexes
        indexes.add(createIndex(icd.getIndexName(), icd.getIndexType(), icd.getIndexExpression(),
            icd.getIndexFromClause(), icd.getIndexImportString(), false, region));
      } catch (Exception ex) {
        // If an index creation fails, add the exception to the map and
        // continue creating rest of the indexes.The failed indexes will
        // be removed from the IndexManager#indexes map by the createIndex
        // method so that those indexes will not be populated in the next
        // step.
        if (logger.isDebugEnabled()) {
          logger.debug("Index creation failed, {}, {}", icd.getIndexName(), ex.getMessage(), ex);
        }
        exceptionsMap.put(icd.getIndexName(), ex);
        throwException = true;
      }
    }
    if (IndexManager.testHook != null) {
      IndexManager.testHook.hook(13);
    }
    // Second step is iterating over REs and populating all the created
    // indexes
    IndexManager indexManager = IndexUtils.getIndexManager(cache, region, false);
    if (indexManager == null) {
      for (IndexCreationData icd : icds) {
        exceptionsMap.put(icd.getIndexName(),
            new IndexCreationException("Index Creation Failed due to region destroy"));
      }
      return true;
    }

    if (indexes.size() > 0) {
      try {
        indexManager.populateIndexes(indexes);
      } catch (MultiIndexCreationException ex) {
        exceptionsMap.putAll(ex.getExceptionsMap());
        throwException = true;
      }
    }
    return throwException;

  }

  public boolean clearDefinedIndexes() {
    this.indexDefinitions.clear();
    return true;
  }

  public InternalPool getPool() {
    return pool;
  }

  public MethodInvocationAuthorizer getMethodInvocationAuthorizer() {
    return methodInvocationAuthorizer;
  }
}

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
package com.gemstone.gemfire.cache.query;

import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.internal.Undefined;

/**
 * Interface for the query service, which is used for instantiating queries, 
 * creating and destroying indexes, creating CQs and operating on CQs.
 * 
 * Creating an index on an employee's age using QueryService in region "employeeRegion",
 * QueryService queryService  =  cache.getQueryService();
 * queryService.createIndex ("SampleIndex",       //IndexName
 *                           "e.age"              //indexExpression
 *                           "/employeeRegion e", //regionPath
 *                           );
 *                           
 * The CQs work on the server regions, the client can use the CQ methods supported 
 * in this class to create/operate CQs on the server. The CQ obtains the Server 
 * connection from the corresponding local region on the client.  
 * The implementation of this interface is obtained from the Cache 
 * using {@link Cache#getQueryService}.
 *
 * 
 * @author Eric Zoerner
 * @since 4.0
 */
public interface QueryService {

  /** The undefined constant */
  public static final Object UNDEFINED = new Undefined();

  /**
   * Constructs a new <code>Query</code> object.
   * 
   * @param queryString the String that is the query program
   * @return The new <code>Query</code> object.
   * @throws QueryInvalidException if the syntax of the queryString is invalid.
   * @see Query
   */
  public Query newQuery(String queryString);

  /**
   * Create a hash index that can be used when executing equal and not equal
   * queries. Hash index is not supported with asynchronous index maintenance.
   * Hash index is also not supported with a from clause with multiple iterators.
   * Queries on numeric types must match the indexed value.  For Example: 
   * For a float field the query should be specified as floatField = 1.0f
   * 
   * @param indexName the name of this index.
   * @param indexedExpression refers to the field of the region values that 
   *          are referenced by the regionPath.
   * @param regionPath that resolves to region values or nested
   *          collections of region values which will correspond to the
   *          FROM clause in a query. Check following examples.
   *          The regionPath is restricted to only one expression
   *          
   *          Example:
   *          Query1: "Select * from /portfolio p where p.mktValue = 25.00"
   *          For index on mktValue field:
   *          indexExpression: "p.mktValue"
   *          regionPath:      "/portfolio p"
   *          
   * @return the newly created Index
   * @throws QueryInvalidException if the argument query language strings have
   *           invalid syntax
   * @throws IndexInvalidException if the arguments do not correctly specify an
   *           index
   * @throws IndexNameConflictException if an index with this name already
   *           exists
   * @throws IndexExistsException if an index with these parameters already
   *           exists with a different name
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   * @throws UnsupportedOperationException If Index is being created on a region
   *           which does not support indexes.
   * 
   */
  public Index createHashIndex(String indexName, String indexedExpression,
      String regionPath) throws IndexInvalidException,
      IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException;
  
  /**
   * Defines a key index that can be used when executing queries. The key index
   * expression indicates query engine to use region key as index for query
   * evaluation. They are used to make use of the implicit hash index 
   * supported with GemFire regions.
   * 
   * @param indexName the name of this index.
   * @param indexedExpression refers to the keys of the region that 
   *          is referenced by the regionPath.
   *          For example, an index with indexedExpression "ID" might
   *          be used for a query with a WHERE clause of "ID > 10", In this case
   *          the ID value is evaluated using region keys.
   * @param regionPath that resolves to the region which will
   *          correspond to the FROM clause in a query.
   *          The regionPath must include exactly one region.
   *          
   *          Example:
   *          Query1: "Select * from /portfolio p where p.ID = 10"
   *          indexExpression: "p.ID"
   *          regionPath:      "/portfolio p"
   *          
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   */
  public void defineKeyIndex(String indexName, String indexedExpression,
      String regionPath) throws RegionNotFoundException;
 
  /**
   * Defines a hash index that can be used when executing equal and not equal
   * queries. Hash index is not supported with asynchronous index maintenance.
   * Hash index is also not supported with a from clause with multiple iterators.
   * Queries on numeric types must match the indexed value.  For Example: 
   * For a float field the query should be specified as floatField = 1.0f
   * To create all the defined indexes call {@link #createDefinedIndexes()} 
   * 
   * @param indexName the name of this index.
   * @param indexedExpression refers to the field of the region values that 
   *          are referenced by the regionPath.
   * @param regionPath that resolves to region values or nested
   *          collections of region values which will correspond to the
   *          FROM clause in a query. Check following examples.
   *          The regionPath is restricted to only one expression
   *          
   *          Example:
   *          Query1: "Select * from /portfolio p where p.mktValue = 25.00"
   *          For index on mktValue field:
   *          indexExpression: "p.mktValue"
   *          regionPath:      "/portfolio p"
   *          
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   * 
   */
  public void defineHashIndex(String indexName, String indexedExpression,
      String regionPath) throws RegionNotFoundException;
  
  
  /**
   * Defines a hash index that can be used when executing equal and not equal
   * queries.  Hash index is not supported with asynchronous index maintenance.
   * Hash index is also not supported with a from clause with multiple iterators.
   * Queries on numeric types must match the indexed value.  For Example: 
   * For a float field the query should be specified as floatField = 1.0f
   * To create all the defined indexes call {@link #createDefinedIndexes()} 
   *
   * @param indexName the name of this index.
   * @param indexedExpression refers to the field of the region values that 
   *          are referenced by the regionPath.
   * @param regionPath that resolves to region values or nested
   *          collections of region values which will correspond to the
   *          FROM clause in a query. 
   *          The regionPath must include exactly one region
   *          The regionPath is restricted to only one expression
   * @param imports string containing imports (in the query language syntax,
   *          each import statement separated by a semicolon), provides packages
   *          and classes used in variable typing in the Indexed and FROM
   *          expressions. The use is the same as for the FROM clause in
   *          querying.
   *          
   *          Example:
   *          Query1: "Select * from /portfolio p where p.mktValue = 25.00"
   *          For index on mktValue field:
   *          indexExpression: "p.mktValue"
   *          regionPath:      "/portfolio p"
   *         
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   */
  public void defineHashIndex(String indexName, String indexedExpression,
      String regionPath, String imports) throws RegionNotFoundException;
 
  /**
   * Defines an index that can be used when executing queries.
   * To create all the defined indexes call {@link #createDefinedIndexes()} 
   *
   * @param indexName the name of this index.
   * @param indexedExpression refers to the field of the region values that 
   *          are referenced by the regionPath.
   * @param regionPath that resolves to region values or nested
   *          collections of region values which will correspond to the
   *          FROM clause in a query. Check following examples.
   *          The regionPath must include exactly one region, but may include
   *          multiple expressions as required to drill down into nested
   *          region contents.
   *          
   *          Example:
   *          Query1: "Select * from /portfolio p where p.mktValue > 25.00"
   *          For index on mktValue field:
   *          indexExpression: "p.mktValue"
   *          regionPath:      "/portfolio p"
   *          
   *          Query2: "Select * from /portfolio p, p.positions.values pos where pos.secId ='VMWARE'"
   *          For index on secId field:
   *          indexExpression: "pos.secId"
   *          regionPath:      "/portfolio p, p.positions.values pos"
   * @throws  RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   * 
   */
  public void defineIndex(String indexName, String indexedExpression,
      String regionPath) throws RegionNotFoundException;

  /**
   * Defines an index that can be used when executing queries.
   * To create all the defined indexes call {@link #createDefinedIndexes()} 
   * 
   * @param indexName the name of this index.
   * @param indexedExpression refers to the field of the region values that 
   *          are referenced by the regionPath.
   * @param regionPath that resolves to region values or nested
   *          collections of region values which will correspond to the
   *          FROM clause in a query. 
   *          The regionPath must include exactly one region, but may include
   *          multiple expressions as required to drill down into nested
   *          region contents. Check following examples.
   * @param imports string containing imports (in the query language syntax,
   *          each import statement separated by a semicolon), provides packages
   *          and classes used in variable typing in the Indexed and FROM
   *          expressions. The use is the same as for the FROM clause in
   *          querying.
   *          
   *          Example:
   *          Query1: "Select * from /portfolio p where p.mktValue > 25.00"
   *          For index on mktValue field:
   *          indexExpression: "p.mktValue"
   *          regionPath:      "/portfolio p"
   *          
   *          Query2: "Select * from /portfolio p, p.positions.values pos where pos.secId ='VMWARE'"
   *          For index on secId field:
   *          indexExpression: "pos.secId"
   *          regionPath:      "/portfolio p, p.positions.values pos TYPE Position"
   *          imports:         "package.Position"
   * @throws QueryInvalidException if the argument query language strings have
   *           invalid syntax
   * @throws IndexInvalidException if the arguments do not correctly specify an
   *           index
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   * @throws UnsupportedOperationException If Index is being created on a region
   *           which overflows to disk
   */
  public void defineIndex(String indexName, String indexedExpression,
      String regionPath, String imports) throws RegionNotFoundException;

  /**
   * Create a hash index that can be used when executing equal and not equal
   * queries.  Hash index is not supported with asynchronous index maintenance.
   * Hash index is also not supported with a from clause with multiple iterators.
   * Queries on numeric types must match the indexed value.  For Example: 
   * For a float field the query should be specified as floatField = 1.0f
   *
   * @param indexName the name of this index.
   * @param indexedExpression refers to the field of the region values that 
   *          are referenced by the regionPath.
   * @param regionPath that resolves to region values or nested
   *          collections of region values which will correspond to the
   *          FROM clause in a query. 
   *          The regionPath must include exactly one region
   *          The regionPath is restricted to only one expression
   * @param imports string containing imports (in the query language syntax,
   *          each import statement separated by a semicolon), provides packages
   *          and classes used in variable typing in the Indexed and FROM
   *          expressions. The use is the same as for the FROM clause in
   *          querying.
   *          
   *          Example:
   *          Query1: "Select * from /portfolio p where p.mktValue = 25.00"
   *          For index on mktValue field:
   *          indexExpression: "p.mktValue"
   *          regionPath:      "/portfolio p"
   *         
   * @return the newly created Index
   * @throws QueryInvalidException if the argument query language strings have
   *           invalid syntax
   * @throws IndexInvalidException if the arguments do not correctly specify an
   *           index
   * @throws IndexNameConflictException if an index with this name already
   *           exists
   * @throws IndexExistsException if an index with these parameters already
   *           exists with a different name
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   * @throws UnsupportedOperationException If Index is being created on a region
   *           which overflows to disk
   */
  public Index createHashIndex(String indexName, String indexedExpression,
      String regionPath, String imports) throws IndexInvalidException,
      IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException;

  /**
   * 
   * @deprecated As of 6.6.2, use
   *             {@link #createIndex(String, String, String)} and
   *             {@link #createKeyIndex(String, String, String)} instead.
   * 
   * Create an index that can be used when executing queries.
   * 
   * @param indexName the name of this index, used for statistics collection and
   *          to identify this index for later access
   * @param indexType the type of index. The indexType must be either 
   *          IndexType.FUNCTIONAL or IndexType.PRIMARY_KEY.
   * @param indexedExpression refers to the elements of the collection (or 
   *          collection of structs) that are referenced in the fromClause. 
   *          This expression is used to optimize the comparison of the same 
   *          path found in a query's WHERE clause when used to compare against 
   *          a constant expression. 
   *          For example, an index with indexedExpression "mktValue" might 
   *          be used for a query with a WHERE clause of "mktValue > 25.00". 
   *          The exact use and specification of the indexedExpression varies 
   *          depending on the indexType. 
   *          Query parameters and region paths are not allowed in the 
   *          indexedExpression (e.g. $1). 
   * @param fromClause expression, that resolves to a collection or list of 
   *          collections which will correspond to the FROM clause or part of a 
   *          FROM clause in a SELECT statement. The FROM clause must include 
   *          exactly one region, but may include multiple FROM expressions as 
   *          required to drill down into nested region contents. 
   *          The collections that the FROM expressions evaluate to must be 
   *          dependent on one and only one entry in the referenced region 
   *          (otherwise the index could not be maintained on single entry 
   *          updates). References to query parameters are not allowed. 
   *          For primary key indexes, the fromClause must be just one collection 
   *          which must be a region path only.
   * @return the newly created Index
   * @throws QueryInvalidException if the argument query language strings have
   *           invalid syntax
   * @throws IndexInvalidException if the arguments do not correctly specify an
   *           index
   * @throws IndexNameConflictException if an index with this name already
   *           exists
   * @throws IndexExistsException if an index with these parameters already
   *           exists with a different name
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   * @throws UnsupportedOperationException If Index is being created on a region which 
   *         overflows to disk  
   * 
   */
  @Deprecated
  public Index createIndex(String indexName, IndexType indexType,
      String indexedExpression, String fromClause)
      throws IndexInvalidException, IndexNameConflictException,
      IndexExistsException, RegionNotFoundException, UnsupportedOperationException;

  /**
   * @deprecated As of 6.6.2, use
   *             {@link #createIndex(String, String, String, String)} and
   *             {@link #createKeyIndex(String, String, String)} instead.
   * 
   * Create an index that can be used when executing queries.
   * 
   * @param indexName the name of this index, used for statistics collection and
   *          to identify this index for later access
   * @param indexType the type of index. The indexType must be either 
   *          IndexType.FUNCTIONAL or IndexType.PRIMARY_KEY.
   * @param indexedExpression refers to the elements of the collection (or 
   *          collection of structs) that are referenced in the fromClause. 
   *          This expression is used to optimize the comparison of the same 
   *          path found in a query's WHERE clause when used to compare against 
   *          a constant expression. 
   *          For example, an index with indexedExpression "mktValue" might 
   *          be used for a query with a WHERE clause of "mktValue > 25.00". 
   *          The exact use and specification of the indexedExpression varies 
   *          depending on the indexType. 
   *          Query parameters and region paths are not allowed in the 
   *          indexedExpression (e.g. $1). 
   * @param fromClause expression, that resolves to a collection or list of 
   *          collections which will correspond to the FROM clause or part of a 
   *          FROM clause in a SELECT statement. The FROM clause must include 
   *          exactly one region, but may include multiple FROM expressions as 
   *          required to drill down into nested region contents. 
   *          The collections that the FROM expressions evaluate to must be 
   *          dependent on one and only one entry in the referenced region 
   *          (otherwise the index could not be maintained on single entry 
   *          updates). References to query parameters are not allowed. 
   *          For primary key indexes, the fromClause must be just one collection 
   *          which must be a region path only.
   * @param imports string containing imports (in the query language syntax,
   *          each import statement separated by a semicolon), provides packages 
   *          and classes used in variable typing in the Indexed and FROM 
   *          expressions. 
   *          The use is the same as for the FROM clause in querying. 
   * @return the newly created Index
   * @throws QueryInvalidException if the argument query language strings have
   *           invalid syntax
   * @throws IndexInvalidException if the arguments do not correctly specify an
   *           index
   * @throws IndexNameConflictException if an index with this name already
   *           exists
   * @throws IndexExistsException if an index with these parameters already
   *           exists with a different name
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   * @throws UnsupportedOperationException If Index is being created on a region which 
   *         overflows to disk 
   */
  @Deprecated
  public Index createIndex(String indexName, IndexType indexType,
      String indexedExpression, String fromClause, String imports)
      throws IndexInvalidException, IndexNameConflictException,
      IndexExistsException, RegionNotFoundException, UnsupportedOperationException;

  /**
   * Create an index that can be used when executing queries.
   * 
   * @param indexName the name of this index.
   * @param indexedExpression refers to the field of the region values that 
   *          are referenced by the regionPath.
   * @param regionPath that resolves to region values or nested
   *          collections of region values which will correspond to the
   *          FROM clause in a query. Check following examples.
   *          The regionPath must include exactly one region, but may include
   *          multiple expressions as required to drill down into nested
   *          region contents.
   *          
   *          Example:
   *          Query1: "Select * from /portfolio p where p.mktValue > 25.00"
   *          For index on mktValue field:
   *          indexExpression: "p.mktValue"
   *          regionPath:      "/portfolio p"
   *          
   *          Query2: "Select * from /portfolio p, p.positions.values pos where pos.secId ='VMWARE'"
   *          For index on secId field:
   *          indexExpression: "pos.secId"
   *          regionPath:      "/portfolio p, p.positions.values pos"
   * @return the newly created Index
   * @throws QueryInvalidException if the argument query language strings have
   *           invalid syntax
   * @throws IndexInvalidException if the arguments do not correctly specify an
   *           index
   * @throws IndexNameConflictException if an index with this name already
   *           exists
   * @throws IndexExistsException if an index with these parameters already
   *           exists with a different name
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   * @throws UnsupportedOperationException If Index is being created on a region
   *           which does not support indexes.
   * 
   */
  public Index createIndex(String indexName, String indexedExpression,
      String regionPath) throws IndexInvalidException,
      IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException;
  
  /**
   * Create an index that can be used when executing queries.
   * 
   * @param indexName the name of this index.
   * @param indexedExpression refers to the field of the region values that 
   *          are referenced by the regionPath.
   * @param regionPath that resolves to region values or nested
   *          collections of region values which will correspond to the
   *          FROM clause in a query. 
   *          The regionPath must include exactly one region, but may include
   *          multiple expressions as required to drill down into nested
   *          region contents. Check following examples.
   * @param imports string containing imports (in the query language syntax,
   *          each import statement separated by a semicolon), provides packages
   *          and classes used in variable typing in the Indexed and FROM
   *          expressions. The use is the same as for the FROM clause in
   *          querying.
   *          
   *          Example:
   *          Query1: "Select * from /portfolio p where p.mktValue > 25.00"
   *          For index on mktValue field:
   *          indexExpression: "p.mktValue"
   *          regionPath:      "/portfolio p"
   *          
   *          Query2: "Select * from /portfolio p, p.positions.values pos where pos.secId ='VMWARE'"
   *          For index on secId field:
   *          indexExpression: "pos.secId"
   *          regionPath:      "/portfolio p, p.positions.values pos TYPE Position"
   *          imports:         "package.Position"
   * @return the newly created Index
   * @throws QueryInvalidException if the argument query language strings have
   *           invalid syntax
   * @throws IndexInvalidException if the arguments do not correctly specify an
   *           index
   * @throws IndexNameConflictException if an index with this name already
   *           exists
   * @throws IndexExistsException if an index with these parameters already
   *           exists with a different name
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   * @throws UnsupportedOperationException If Index is being created on a region
   *           which overflows to disk
   */
  public Index createIndex(String indexName, String indexedExpression,
      String regionPath, String imports) throws IndexInvalidException,
      IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException;
  
  /**
   * Create a key index that can be used when executing queries. The key index
   * expression indicates query engine to use region key as index for query
   * evaluation. They are used to make use of the implicit hash index 
   * supported with GemFire regions.
   * 
   * @param indexName the name of this index.
   * @param indexedExpression refers to the keys of the region that 
   *          is referenced by the regionPath.
   *          For example, an index with indexedExpression "ID" might
   *          be used for a query with a WHERE clause of "ID > 10", In this case
   *          the ID value is evaluated using region keys.
   * @param regionPath that resolves to the region which will
   *          correspond to the FROM clause in a query.
   *          The regionPath must include exactly one region.
   *          
   *          Example:
   *          Query1: "Select * from /portfolio p where p.ID = 10"
   *          indexExpression: "p.ID"
   *          regionPath:      "/portfolio p"
   *          
   * @return the newly created Index
   * @throws QueryInvalidException if the argument query language strings have
   *           invalid syntax
   * @throws IndexInvalidException if the arguments do not correctly specify an
   *           index
   * @throws IndexNameConflictException if an index with this name already
   *           exists
   * @throws IndexExistsException if an index with these parameters already
   *           exists with a different name
   * @throws RegionNotFoundException if the region referred to in the fromClause
   *           doesn't exist
   * @throws UnsupportedOperationException If Index is being created on a region
   *           which overflows to disk
   */
  public Index createKeyIndex(String indexName, String indexedExpression,
      String regionPath) throws IndexInvalidException,
      IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException;
 
  
  /**
   * Creates all the indexes that were defined using
   * {@link #defineIndex(String, String, String)} 
   * @throws MultiIndexCreationException
   *           which consists a map of failed indexNames and the Exceptions.
   */
  public List<Index> createDefinedIndexes() throws MultiIndexCreationException;

  /**
   * Clears all the indexes that were defined using
   * {@link #defineIndex(String, String, String)} 
   */
  public boolean clearDefinedIndexes();

  /**
   * Get the Index from the specified Region with the specified name.
   * 
   * @param region the Region for the requested index
   * @return the index of the region with this name, or null if there isn't one
   */
  public Index getIndex(Region<?,?> region, String indexName);

  /**
   * Get a collection of all the indexes in the Cache.
   * 
   * @return the collection of all indexes in this Cache
   */
  public Collection<Index> getIndexes();

  /**
   * Get a collection of all the indexes on the specified Region
   * 
   * @param region the region for the requested indexes
   * @return the collection of indexes on the specified region
   */
  public Collection<Index> getIndexes(Region<?,?> region);

  /**
   * 
   * @deprecated As of 6.6.2, use {@link #getIndexes(Region)} only.
   * 
   * Get a collection of all the indexes on the specified Region of
   * the specified index type.
   * 
   * @param region the region for the requested indexes
   * @param indexType the type of indexes to get. Currently must be
   *          Indexable.FUNCTIONAL
   * @return the collection of indexes for the specified region and type
   */
  @Deprecated
  public Collection<Index> getIndexes(Region<?, ?> region, IndexType indexType);

  /**
   * Remove the specified index.
   * 
   * @param index the Index to remove
   */
  public void removeIndex(Index index);

  /**
   * Remove all the indexes from this cache.
   */
  public void removeIndexes();

  /**
   * Remove all the indexes on the specified Region
   * 
   * @param region the Region to remove all indexes from
   */
  public void removeIndexes(Region<?,?> region);
  

  // CQ Service related APIs.
  
  /**
   * Constructs a new continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed until the execute method
   * is invoked on the CqQuery.
   * @since 5.5
   * @param queryString the OQL query
   * @param cqAttr the CqAttributes
   * @return the newly created CqQuery object
   * @throws IllegalArgumentException if queryString or cqAttr is null.
   * @throws IllegalStateException if this method is called from a cache
   *         server.
   * @throws QueryInvalidException if there is a syntax error in the query.
   * @throws CqException if failed to create CQ.
   *   E.g.: Query string should refer to only one region. 
   *         Joins are not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. 
   *         Bind parameters in the query are not yet supported.
   */
  public CqQuery newCq(String queryString, CqAttributes  cqAttr)
  				throws QueryInvalidException, CqException;

  /**
   * Constructs a new continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed until the execute method
   * is invoked on the CqQuery.
   *
   * @since 5.5
   * @param queryString the OQL query
   * @param cqAttr the CqAttributes
   * @param isDurable true if the CQ is durable
   * @return the newly created CqQuery object
   * @throws IllegalArgumentException if queryString or cqAttr is null.
   * @throws IllegalStateException if this method is called from a cache
   *         server.
   * @throws QueryInvalidException if there is a syntax error in the query.
   * @throws CqException if failed to create CQ.
   *   E.g.: Query string should refer to only one region. 
   *         Joins are not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. 
   *         Bind parameters in the query are not yet supported.
   */
  public CqQuery newCq(String queryString, CqAttributes  cqAttr, boolean isDurable)
                                throws QueryInvalidException, CqException;
  /**
   * Constructs a new named continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed until the execute method
   * is invoked on the CqQuery. The name of the query will be used
   * to identify this query in statistics archival.
   *
   * @since 5.5
   * @param name the String name for this query
   * @param queryString the OQL query
   * @param cqAttr the CqAttributes
   * @return the newly created  CqQuery object
   * @throws CqExistsException if a CQ by this name already exists on this
   * client
   * @throws IllegalArgumentException if queryString or cqAttr is null.
   * @throws IllegalStateException if this method is called from a cache
   *         server.
   * @throws QueryInvalidException if there is a syntax error in the query.
   * @throws CqException if failed to create cq.
   *   E.g.: Query string should refer to only one region. 
   *         Joins are not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. 
   *         Bind parameters in the query are not yet supported.
   *
   */
  public CqQuery newCq(String name, String queryString, CqAttributes cqAttr)
  		throws QueryInvalidException, CqExistsException, CqException;

  
  /**
   * Constructs a new named continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed until the execute method
   * is invoked on the CqQuery. The name of the query will be used
   * to identify this query in statistics archival.
   *
   * @since 5.5
   * @param name the String name for this query
   * @param queryString the OQL query
   * @param cqAttr the CqAttributes
   * @param isDurable true if the CQ is durable
   * @return the newly created  CqQuery object
   * @throws CqExistsException if a CQ by this name already exists on this
   * client
   * @throws IllegalArgumentException if queryString or cqAttr is null.
   * @throws IllegalStateException if this method is called from a cache
   *         server.
   * @throws QueryInvalidException if there is a syntax error in the query.
   * @throws CqException if failed to create cq.
   *   E.g.: Query string should refer to only one region. 
   *         Joins are not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. 
   *         Bind parameters in the query are not yet supported.
   *
   */
  public CqQuery newCq(String name, String queryString, CqAttributes cqAttr, boolean isDurable)
                throws QueryInvalidException, CqExistsException, CqException;
  /** 
   * Unregister all Continuous Queries. 
   * All artifacts and resources associated with the CQs are released.
   * Any attempt to access closed CqQuery objects will result in the 
   * CqClosedException being thrown to the caller.
   * 
   * @since 5.5
   */
  public void closeCqs();

  /**
   * Retrieve all registered Continuous Queries.
   * This is a collection of CqQuery objects.
   *
   * @since 5.5
   * @return CqQuery[] list of registered CQs,
   *         null if there are no CQs.   
   */
  public CqQuery[] getCqs();
  
  /**
   * Retrieves all the registered Continuous Queries for a given region.
   * This is a collection of CqQuery objects. 
   * 
   * @since 5.5
   * @return CqQuery[] list of registered CQs on the specified region,
   *         null if there are no CQs.
   * @exception CqException
   *              if the region does not exist.
   */
  public CqQuery[] getCqs(String regionName) throws CqException;

  /**
   * Retrieves the Continuous Query specified by the name.
   * 
   * @since 5.5
   * @param  cqName - String, name of the CQ
   * @return  CqQuery object, 
   *          null if no CqQuery object is found.
   */
  public CqQuery getCq(String cqName);
    
  /**
   * Starts execution of all the registered continuous queries for this client.
   * This is complementary to stopCqs.
   * @see QueryService#stopCqs()
   * 
   * @since 5.5
   * @throws CqException if failure to execute CQ.
   */
  public void executeCqs() throws CqException;
  
  /**
   * Stops execution of all the continuous queries for this client to become inactive.
   * This is useful when client needs to control the incoming CQ messages during
   * bulk region operations.
   * @see QueryService#executeCqs()
   * 
   * @since 5.5
   * @throws CqException if failure to execute CQ.
   */
  public void stopCqs() throws CqException;

  /**
   * Starts execution of all the continuous queries registered on the specified 
   * region for this client. 
   * This is complementary method to stopCQs().  
   * @see  QueryService#stopCqs()
   * 
   * @since 5.5
   * @throws CqException if failure to stop CQs.
   */
  public void executeCqs(String regionName) throws CqException;

  /**
   * Stops execution of all the continuous queries registered on the specified 
   * region for this client. 
   * This is useful when client needs to control the incoming CQ messages during
   * bulk region operations.
   * @see QueryService#executeCqs()
   * 
   * @since 5.5
   * @throws CqException if failure to execute CQs.
   */  
  public void stopCqs(String regionName) throws CqException;
  
  /**
   * Retrieves all the durable CQs registered by the client calling this method. 
   *
   * @since 7.0
   * @return List of names of registered durable CQs,
   *         empty list if no durable cqs.   
   */
  public List<String> getAllDurableCqsFromServer() throws CqException;
    
  /**
   * Returns CqServiceStatistics object, which provides helper methods to 
   * get CQ service related statistics for this client. 
   * Specifically the following aggregate information on the client's CQs is collected:
   *    Number of CQs created (cumulative)
   *    Number of CQs active currently
   *    Number of CQs stopped or suspended currently
   *    Number of CQs closed (cumulative)
   *    Number of CQs active on a specified region currently
   *    
   * @see CqServiceStatistics
   * 
   * @since 5.5
   * @return CqServiceStatistics
   * 
   */
  public CqServiceStatistics getCqStatistics();
    
}

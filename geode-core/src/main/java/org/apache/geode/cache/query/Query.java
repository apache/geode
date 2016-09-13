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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;

/**
 * Interface for query objects. Supports execution of queries with optional
 * parameters.
 *
 * @since GemFire 4.0
 */

public interface Query {
  /**
   * Return the original query string that was specified in the constructor.
   * @return the original query string
   */
  public String getQueryString();
  
  
  /**
   * Executes this query and returns an object that represent its
   * result.  If the query resolves to a primitive type, an instance
   * of the corresponding wrapper type ({@link java.lang.Integer},
   * etc.) is returned.  If the query resolves to more than one
   * object, a {@link SelectResults} is returned.
   * 
   * Query execution can potentially take a long time depending on 
   * data size and query complexity. The system property 
   * "gemfire.Cache.MAX_QUERY_EXECUTION_TIME" can be set to define the 
   * maximum time allowed for a query to complete its execution. If query 
   * execution time exceeds "gemfire.Cache.MAX_QUERY_EXECUTION_TIME", 
   * then the query is canceled and QueryExecutionTimeoutException is 
   * thrown back to the caller, if the execution is local to the VM. 
   * If the canceled query was initiated by a GemFire client, then a 
   * QueryException is thrown on the client with its cause set to 
   * QueryExecutionTimeoutException. This timeout does not account for 
   * the time taken to construct the results after execution completes 
   * and the results returned to the caller.
   * 
   * @return The object that represents the result of the query. Note that if
   *         a query is just a select statement then it will return a result
   *         that is an instance of {@link SelectResults}. However, since a query is not
   *         necessarily just a select statement, the return type of this
   *         method is <code>Object</code>.
   *         For example, the query <code><b>(select distinct * from /rgn).size</b></code>
   *         returns an instance of <code>java.lang.Integer</code>.
   *
   * @throws FunctionDomainException
   *         A function was applied to a parameter that is improper
   *         for that function.  For example, the ELEMENT function
   *         was applied to a collection of more than one element 
   * @throws TypeMismatchException
   *         If a bound parameter is not of the expected type.
   * @throws NameResolutionException
   *         If a name in the query cannot be resolved.
   * @throws IllegalArgumentException
   *         The number of bound parameters does not match the number
   *         of placeholders
   * @throws IllegalStateException
   *         If the query is not permitted on this type of region
   * @throws QueryExecutionTimeoutException
   *         If the query gets canceled due to setting system variable
   *         "gemfire.Cache.MAX_QUERY_EXECUTION_TIME". This is a thrown when 
   *         query is executed on the local regions (embedded mode).
   * @throws QueryInvocationTargetException
   *         If the data referenced in from clause is not available for
   *         querying.
   * @throws QueryExecutionLowMemoryException
   *         If the query gets canceled due to low memory conditions and
   *         the resource manager critical heap percentage has been set
   */
  public Object execute()
    throws FunctionDomainException, TypeMismatchException, NameResolutionException,
           QueryInvocationTargetException;
  
  /**
   * Executes this query with the given parameters and returns an
   * object that represent its result.  If the query resolves to a
   * primitive type, an instance of the corresponding wrapper type
   * ({@link java.lang.Integer}, etc.) is returned.  If the query
   * resolves to more than one object, a {@link SelectResults} is
   * returned.
   *
   * Query execution can potentially take a long time depending on 
   * data size and query complexity. The system property 
   * "gemfire.Cache.MAX_QUERY_EXECUTION_TIME" can be set to define the 
   * maximum time allowed for a query to complete its execution. If query 
   * execution time exceeds "gemfire.Cache.MAX_QUERY_EXECUTION_TIME", 
   * then the query is canceled and QueryExecutionTimeoutException is 
   * thrown back to the caller, if the execution is local to the VM. 
   * If the canceled query was initiated by a GemFire client, then a 
   * QueryException is thrown on the client with its cause set to 
   * QueryExecutionTimeoutException. This timeout does not account for 
   * the time taken to construct the results after execution completes 
   * and the results returned to the caller.

   * @param params
   *        Values that are bound to parameters (such as
   *        <code>$1</code>) in this query. 
   *
   * @return The object that represents the result of the query. Note that if
   *         a query is just a select statement then it will return a result
   *         that is an instance of {@link SelectResults}. However, since a query is not
   *         necessarily just a select statement, the return type of this
   *         method is <code>Object</code>.
   *         For example, the query <code><b>(select distinct * from /rgn).size</b></code>
   *         returns an instance of <code>java.lang.Integer</code>.
   *
   * @throws FunctionDomainException
   *         A function was applied to a parameter that is improper
   *         for that function.  For example, the ELEMENT function
   *         was applied to a collection of more than one element 
   * @throws TypeMismatchException
   *         If a bound parameter is not of the expected type.
   * @throws NameResolutionException
   *         If a name in the query cannot be resolved.
   * @throws IllegalArgumentException
   *         The number of bound parameters does not match the number
   *         of placeholders
   * @throws IllegalStateException
   *         If the query is not permitted on this type of region
   * @throws QueryExecutionTimeoutException
   *         If the query gets canceled due to setting system variable
   *         "gemfire.Cache.MAX_QUERY_EXECUTION_TIME". This is a thrown when 
   *         query is executed on the local regions (embedded mode).
   * @throws QueryInvocationTargetException
   *         If the data referenced in from clause is not available for
   *         querying.
   * @throws QueryExecutionLowMemoryException
   *         If the query gets canceled due to low memory conditions and
   *         the resource manager critical heap percentage has been set
   *         
   */
  public Object execute(Object[] params)
    throws FunctionDomainException, TypeMismatchException, NameResolutionException,
           QueryInvocationTargetException;

  /**
   * Executes this query on the partitioned data-store associated with the given
   * RegionFunctionContext and returns an object that represents its result. An
   * Exception is thrown when data-set associated with the {@link RegionFunctionContext}
   * gets moved or destroyed from the local node.
   *
   * The RegionFunctionContext defines the execution context of a data dependent
   * {@link Function}. And is obtained when the function is executed using
   * {@link FunctionService#onRegion(Region)}, the
   * {@link Function#execute(FunctionContext) FunctionContext}
   *
   *      E.g.: // Function service execute() method:
   *
   *      public void execute(FunctionContext context) {
   *        RegionFunctionContext regionContext = (RegionFunctionContext)context;
   *        Query query = cache.getQueryService().newQuery("Select * from /MyRegion");
   *        SelectResults results = (SelectResults)query.execute(regionContext);
   *      }
   *
   * Using this method a join query query can be executed between two co-located
   * PartitionRegion data-set referenced through {@link RegionFunctionContext}.
   * 
   * This method should NOT be used for multiple partitioned regions based join 
   * queries. We support equi-join queries only on co-located PartitionedRegions 
   * if the the co-located columns ACTUALLY EXIST IN WHERE CLAUSE  and  in case of
   * multi-column partitioning , should have "AND" clause.
   *
   *    E.g.: // Equi-join query:
   *            
   *    Select * from /partitionRegion1 p1, /PartitionRegion2 p2 where
   *    p1.field = p2.field [AND .....]
   *    
   * @since GemFire 6.2.2
   * @param context RegionFunctionContext which will target the query on subset
   *          of data if executed on PartitionedRegion.
   *
   * @return The object that represents the result of the query. Note that if a
   *         query is just a select statement then it will return a result that
   *         is an instance of {@link SelectResults}. However, since a query is
   *         not necessarily just a select statement, the return type of this
   *         method is <code>Object</code>. For example, the query
   *         <code><b>(select distinct * from /rgn).size</b></code> returns an
   *         instance of <code>java.lang.Integer</code>.
   *
   * @throws FunctionDomainException A function was applied to a parameter that
   *           is improper for that function. For example, the ELEMENT function
   *           was applied to a collection of more than one element
   * @throws TypeMismatchException If a bound parameter is not of the expected
   *           type.
   * @throws NameResolutionException If a name in the query cannot be resolved.
   * @throws IllegalArgumentException The number of bound parameters does not
   *           match the number of place holders
   * @throws IllegalStateException If the query is not permitted on this type of
   *           region
   * @throws QueryExecutionTimeoutException If the query gets canceled due to
   *           setting system variable "gemfire.Cache.MAX_QUERY_EXECUTION_TIME".
   *           This is a thrown when query is executed on the local regions
   *           (embedded mode).
   * @throws QueryInvocationTargetException
   *         If the data referenced in from clause is not available for
   *         querying.
   * @throws QueryExecutionLowMemoryException
   *         If the query gets canceled due to low memory conditions and
   *         the resource manager critical heap percentage has been set
   */
  public Object execute(RegionFunctionContext context)
    throws FunctionDomainException, TypeMismatchException, NameResolutionException,
           QueryInvocationTargetException;

  /**
   * Executes this query on the partitioned data-store associated with the given
   * RegionFunctionContext and parameters and returns an object that represents
   * its result. An Exception is thrown if data moving across nodes (Like,
   * partitioned data-store rebalancing) coincides with data to be queried.
   *
   * The RegionFunctionContext defines the execution context of a data dependent
   * {@link Function}. And is obtained when the function is executed using
   * {@link FunctionService#onRegion(Region)}, the
   * {@link Function#execute(FunctionContext) FunctionContext}
   * 
   *      E.g.: // Function service execute() method:
   *
   *      public void execute(FunctionContext context) {
   *        RegionFunctionContext regionContext = (RegionFunctionContext)context;
   *        Query query = cache.getQueryService().newQuery("Select * from /MyRegion");
   *        SelectResults results = (SelectResults)query.execute(regionContext);
   *      }
   *
   * Using this method a join query query can be executed between two co-located
   * PartitionRegion data-set referenced through {@link RegionFunctionContext}.
   * 
   * This method should NOT be used for multiple partitioned regions based join 
   * queries. We support equi-join queries only on co-located PartitionedRegions 
   * if the the co-located columns ACTUALLY EXIST IN WHERE CLAUSE  and  in case of
   * multi-column partitioning , should have "AND" clause.
   *
   *    E.g.: // Equi-join query:
   *            
   *    Select * from /partitionRegion1 p1, /PartitionRegion2 p2 where
   *    p1.field = p2.field [AND .....]
   *    
   * @since GemFire 6.2.2
   * @param context RegionFunctionContext which will target the query on subset
   *          of data if executed on PartitionedRegion.
   * @param params Values that are bound to parameters (such as <code>$1</code>)
   *          in this query.
   *
   * @return The object that represents the result of the query. Note that if a
   *         query is just a select statement then it will return a result that
   *         is an instance of {@link SelectResults}. However, since a query is
   *         not necessarily just a select statement, the return type of this
   *         method is <code>Object</code>. For example, the query
   *         <code><b>(select distinct * from /rgn).size</b></code> returns an
   *         instance of <code>java.lang.Integer</code>.
   *
   * @throws FunctionDomainException A function was applied to a parameter that
   *           is improper for that function. For example, the ELEMENT function
   *           was applied to a collection of more than one element
   * @throws TypeMismatchException If a bound parameter is not of the expected
   *           type.
   * @throws NameResolutionException If a name in the query cannot be resolved.
   * @throws IllegalArgumentException The number of bound parameters does not
   *           match the number of place holders
   * @throws IllegalStateException If the query is not permitted on this type of
   *           region
   * @throws QueryExecutionTimeoutException If the query gets canceled due to
   *           setting system variable "gemfire.Cache.MAX_QUERY_EXECUTION_TIME".
   *           This is a thrown when query is executed on the local regions
   *           (embedded mode).
   * @throws QueryInvocationTargetException
   *         If the data referenced in from clause is not available for
   *         querying.
   * @throws QueryExecutionLowMemoryException
   *         If the query gets canceled due to low memory conditions and
   *         the resource manager critical heap percentage has been set
   */
  public Object execute(RegionFunctionContext context, Object[] params)
    throws FunctionDomainException, TypeMismatchException, NameResolutionException,
           QueryInvocationTargetException;

  /**
   * Compiles this <code>Query</code> to achieve higher performance
   * execution.
   *
   * @throws TypeMismatchException 
   *         If the compile-time type of a name, parameter, or
   *         expression is not the expected type 
   * @throws QueryInvalidException 
   *         The syntax of the query string is not correct
   * @deprecated as of 6.5
   */
  @Deprecated
  public void compile() 
    throws TypeMismatchException, NameResolutionException;
  
  /**
   * Return whether this query has been compiled into VM bytecodes.
   * @return <code>true</code> if this query has been compiled into bytecodes
   * @deprecated as of 6.5
   */
  @Deprecated
  public boolean isCompiled();

  
  /**
   * Get statistics information for this query.
   */
  public QueryStatistics getStatistics();
}

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
package org.apache.geode.rest.internal.web.controllers;

import java.util.concurrent.ConcurrentHashMap;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.rest.internal.web.exception.GemfireRestException;
import org.apache.geode.rest.internal.web.exception.ResourceNotFoundException;
import org.apache.geode.rest.internal.web.util.JSONUtils;
import org.apache.geode.rest.internal.web.util.ValidationUtils;

/**
 * The QueryingController class serves all HTTP REST requests related to the gemfire querying
 *
 * @see org.springframework.stereotype.Controller
 * @since GemFire 8.0
 */
@Controller("queryController")
@Api(value = "queries", produces = MediaType.APPLICATION_JSON_VALUE, tags = "queries")
@RequestMapping(QueryAccessController.REST_API_VERSION + "/queries")
@SuppressWarnings("unused")
public class QueryAccessController extends AbstractBaseController {

  private static final Logger logger = LogService.getLogger();

  static final String PARAMETERIZED_QUERIES_REGION = "__ParameterizedQueries__";

  private final ConcurrentHashMap<String, DefaultQuery> compiledQueries = new ConcurrentHashMap<>();

  // Constant String value indicating the version of the REST API.
  static final String REST_API_VERSION = "/v1";

  /**
   * Gets the version of the REST API implemented by this @Controller.
   *
   * @return a String indicating the REST API version.
   */
  @Override
  protected String getRestApiVersion() {
    return REST_API_VERSION;
  }

  /**
   * list all parametrized Queries created in a Gemfire data node
   *
   * @return result as a JSON document.
   */
  @RequestMapping(method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
  @ApiOperation(value = "list all parametrized queries",
      notes = "List all parametrized queries by id/name")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 500, message = "if GemFire throws an error or exception")})
  @ResponseBody
  @ResponseStatus(HttpStatus.OK)
  @PreAuthorize("@securityService.authorize('DATA', 'READ')")
  public ResponseEntity<?> list() {
    logger.debug("Listing all parametrized Queries in GemFire...");

    final Region<String, String> parametrizedQueryRegion =
        getQueryStore(PARAMETERIZED_QUERIES_REGION);

    String queryListAsJson = JSONUtils.formulateJsonForListQueriesCall(parametrizedQueryRegion);
    final HttpHeaders headers = new HttpHeaders();
    headers.setLocation(toUri("queries"));
    return new ResponseEntity<>(queryListAsJson, headers, HttpStatus.OK);
  }

  /**
   * Create a named, parametrized Query
   *
   * @param queryId uniquely identify the query
   * @param oqlInUrl OQL query string specified in a request URL
   * @param oqlInBody OQL query string specified in a request body
   * @return result as a JSON document.
   */
  @RequestMapping(method = RequestMethod.POST)
  @ApiOperation(value = "create a parametrized Query",
      notes = "Prepare the specified parametrized query and assign the corresponding ID for lookup")
  @ApiResponses({@ApiResponse(code = 201, message = "Successfully created."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 409, message = "QueryId already assigned to other query."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('DATA', 'READ')")
  public ResponseEntity<?> create(@RequestParam("id") final String queryId,
      @RequestParam(value = "q", required = false) String oqlInUrl,
      @RequestBody(required = false) final String oqlInBody) {
    final String oqlStatement = validateQuery(oqlInUrl, oqlInBody);
    logger.debug("Creating a named, parametrized Query ({}) with ID ({})...", oqlStatement,
        queryId);


    // store the compiled OQL statement with 'queryId' as the Key into the hidden,
    // Parametrized Queries Region...
    final String existingOql =
        createNamedQuery(PARAMETERIZED_QUERIES_REGION, queryId, oqlStatement);

    final HttpHeaders headers = new HttpHeaders();
    headers.setLocation(toUri("queries", queryId));

    if (existingOql != null) {
      headers.setContentType(MediaType.APPLICATION_JSON);
      return new ResponseEntity<>(JSONUtils.formulateJsonForExistingQuery(queryId, existingOql),
          headers, HttpStatus.CONFLICT);
    } else {
      return new ResponseEntity<String>(headers, HttpStatus.CREATED);
    }
  }

  /**
   * Run an adhoc Query specified in a query string
   *
   * @param oql OQL query string to be executed
   * @return query result as a JSON document
   */
  @RequestMapping(method = RequestMethod.GET, value = "/adhoc",
      produces = {MediaType.APPLICATION_JSON_VALUE})
  @ApiOperation(value = "run an adhoc query",
      notes = "Run an unnamed (unidentified), ad-hoc query passed as a URL parameter")
  @ApiResponses({@ApiResponse(code = 200, message = "OK."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception")})
  @ResponseBody
  @ResponseStatus(HttpStatus.OK)
  @PreAuthorize("@securityService.authorize('DATA', 'READ')")
  public ResponseEntity<String> runAdhocQuery(@RequestParam("q") String oql) {
    logger.debug("Running an adhoc Query ({})...", oql);

    oql = decode(oql);
    final Query query = getQueryService().newQuery(oql);

    // NOTE Query.execute throws many checked Exceptions; let the BaseControllerAdvice Exception
    // handlers catch
    // and handle the Exceptions appropriately (500 Server Error)!
    try {
      Object queryResult = query.execute();
      return processQueryResponse(query, null, queryResult);
    } catch (FunctionDomainException fde) {
      throw new GemfireRestException(
          "A function was applied to a parameter that is improper for that function!", fde);
    } catch (TypeMismatchException tme) {
      throw new GemfireRestException("Bind parameter is not of the expected type!", tme);
    } catch (NameResolutionException nre) {
      throw new GemfireRestException("Name in the query cannot be resolved!", nre);
    } catch (IllegalArgumentException iae) {
      throw new GemfireRestException(
          " The number of bound parameters does not match the number of placeholders!", iae);
    } catch (IllegalStateException ise) {
      throw new GemfireRestException("Query is not permitted on this type of region!", ise);
    } catch (QueryExecutionTimeoutException qete) {
      throw new GemfireRestException(
          "Query execution time is exceeded max query execution time (gemfire.Cache.MAX_QUERY_EXECUTION_TIME) configured! ",
          qete);
    } catch (QueryInvocationTargetException qite) {
      throw new GemfireRestException(
          "Data referenced in from clause is not available for querying!", qite);
    } catch (QueryExecutionLowMemoryException qelme) {
      throw new GemfireRestException(
          "Query execution gets canceled due to low memory conditions and the resource manager critical heap percentage has been set!",
          qelme);
    } catch (Exception ex) {
      throw new GemfireRestException(ex.getMessage(), ex);
    }
  }

  /**
   * Run named parametrized Query with ID
   *
   * @param queryId id of the OQL string
   * @param arguments query bind params required while executing query
   * @return query result as a JSON document
   */
  @RequestMapping(method = RequestMethod.POST, value = "/{query}",
      produces = {MediaType.APPLICATION_JSON_VALUE})
  @ApiOperation(value = "run parametrized query",
      notes = "run the specified named query passing in scalar values for query parameters in the GemFire cluster")
  @ApiResponses({@ApiResponse(code = 200, message = "Query successfully executed."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 400,
          message = "Query bind params specified as JSON document in the request body is invalid"),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception")})
  @ResponseBody
  @ResponseStatus(HttpStatus.OK)
  @PreAuthorize("@securityService.authorize('DATA', 'READ')")
  public ResponseEntity<String> runNamedQuery(@PathVariable("query") String queryId,
      @RequestBody String arguments) {
    logger.debug("Running named Query with ID ({})...", queryId);

    queryId = decode(queryId);

    if (arguments != null) {
      // Its a compiled query.

      // Convert arguments into Object[]
      Object[] args = jsonToObjectArray(arguments);

      Query compiledQuery = compiledQueries.get(queryId);
      if (compiledQuery == null) {
        // This is first time the query is seen by this server.
        final String oql = getQueryStore(PARAMETERIZED_QUERIES_REGION).get(queryId);

        ValidationUtils.returnValueThrowOnNull(oql, new ResourceNotFoundException(
            String.format("No Query with ID (%1$s) was found!", queryId)));
        try {
          compiledQuery = getQueryService().newQuery(oql);
        } catch (QueryInvalidException qie) {
          throw new GemfireRestException("Syntax of the OQL queryString is invalid!", qie);
        }
        compiledQueries.putIfAbsent(queryId, (DefaultQuery) compiledQuery);
      }
      // NOTE Query.execute throws many checked Exceptions; let the BaseControllerAdvice Exception
      // handlers catch
      // and handle the Exceptions appropriately (500 Server Error)!
      try {
        Object queryResult = compiledQuery.execute(args);
        return processQueryResponse(compiledQuery, args, queryResult);
      } catch (FunctionDomainException fde) {
        throw new GemfireRestException(
            "A function was applied to a parameter that is improper for that function!", fde);
      } catch (TypeMismatchException tme) {
        throw new GemfireRestException("Bind parameter is not of the expected type!", tme);
      } catch (NameResolutionException nre) {
        throw new GemfireRestException("Name in the query cannot be resolved!", nre);
      } catch (IllegalArgumentException iae) {
        throw new GemfireRestException(
            " The number of bound parameters does not match the number of placeholders!", iae);
      } catch (IllegalStateException ise) {
        throw new GemfireRestException("Query is not permitted on this type of region!", ise);
      } catch (QueryExecutionTimeoutException qete) {
        throw new GemfireRestException(
            "Query execution time is exceeded  max query execution time (gemfire.Cache.MAX_QUERY_EXECUTION_TIME) configured!",
            qete);
      } catch (QueryInvocationTargetException qite) {
        throw new GemfireRestException(
            "Data referenced in from clause is not available for querying!", qite);
      } catch (QueryExecutionLowMemoryException qelme) {
        throw new GemfireRestException(
            "Query gets canceled due to low memory conditions and the resource manager critical heap percentage has been set!",
            qelme);
      } catch (Exception e) {
        throw new GemfireRestException("Error encountered while executing named query!", e);
      }
    } else {
      throw new GemfireRestException(
          " Bind params either not specified or not processed properly by the server!");
    }
  }

  /**
   * Update named, parametrized Query
   *
   * @param queryId uniquely identify the query
   * @param oqlInUrl OQL query string specified in a request URL
   * @param oqlInBody OQL query string specified in a request body
   */
  @RequestMapping(method = RequestMethod.PUT, value = "/{query}")
  @ApiOperation(value = "update parametrized query",
      notes = "Update named, parametrized query by ID")
  @ApiResponses({@ApiResponse(code = 200, message = "Updated successfully."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "queryId does not exist."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception.")})
  @PreAuthorize("@securityService.authorize('DATA', 'READ')")
  public ResponseEntity<?> update(@PathVariable("query") final String queryId,
      @RequestParam(value = "q", required = false) String oqlInUrl,
      @RequestBody(required = false) final String oqlInBody) {
    final String oqlStatement = validateQuery(oqlInUrl, oqlInBody);

    logger.debug("Updating a named, parametrized Query ({}) with ID ({})...", oqlStatement,
        queryId);

    // update the OQL statement with 'queryId' as the Key into the hidden, Parametrized Queries
    // Region...
    checkForQueryIdExist(PARAMETERIZED_QUERIES_REGION, queryId);
    updateNamedQuery(PARAMETERIZED_QUERIES_REGION, queryId, oqlStatement);
    compiledQueries.remove(queryId);

    return new ResponseEntity<>(HttpStatus.OK);
  }

  /**
   * Delete named, parametrized Query
   *
   * @param queryId uniquely identify the query to be deleted
   */
  @RequestMapping(method = RequestMethod.DELETE, value = "/{query}")
  @ApiOperation(value = "delete parametrized query",
      notes = "delete named, parametrized query by ID")
  @ApiResponses({@ApiResponse(code = 200, message = "Deleted successfully."),
      @ApiResponse(code = 401, message = "Invalid Username or Password."),
      @ApiResponse(code = 403, message = "Insufficient privileges for operation."),
      @ApiResponse(code = 404, message = "queryId does not exist."),
      @ApiResponse(code = 500, message = "GemFire throws an error or exception")})
  @PreAuthorize("@securityService.authorize('DATA', 'WRITE')")
  public ResponseEntity<?> delete(@PathVariable("query") final String queryId) {
    logger.debug("Deleting a named, parametrized Query with ID ({}).", queryId);

    // delete the OQL statement with 'queryId' as the Key into the hidden,
    // ParameterizedQueries Region...
    deleteNamedQuery(PARAMETERIZED_QUERIES_REGION, queryId);
    compiledQueries.remove(queryId);
    return new ResponseEntity<>(HttpStatus.OK);
  }

}

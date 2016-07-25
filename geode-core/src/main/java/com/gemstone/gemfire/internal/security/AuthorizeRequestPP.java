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

package com.gemstone.gemfire.internal.security;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.operations.*;
import com.gemstone.gemfire.cache.operations.internal.GetOperationContextImpl;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.NotAuthorizedException;

/**
 * This class implements post-processing authorization calls for various
 * operations. It provides methods to invoke authorization callback ({@link AccessControl#authorizeOperation})
 * to invoke the post-processing callback that may modify the results. The data
 * being passed for the operation is encapsulated in a {@link OperationContext}
 * object that can be modified by the post-processing authorization callbacks.
 * 
 * @since GemFire 5.5
 */
public class AuthorizeRequestPP {

  private AccessControl postAuthzCallback;

  private final ClientProxyMembershipID id;

  private final LogWriterI18n logger;
  
  private final Principal principal;
  
  private final boolean isPrincipalSerializable;

  public AuthorizeRequestPP(String postAuthzFactoryName,
      ClientProxyMembershipID id, Principal principal, Cache cache)
      throws ClassNotFoundException, NoSuchMethodException,
      IllegalAccessException, InvocationTargetException {

    this.id = id;
    this.principal = principal;
    if (this.principal instanceof Serializable) {
      this.isPrincipalSerializable = true;
    } else {
      this.isPrincipalSerializable = false;
    }
    this.logger = cache.getSecurityLoggerI18n();
    Method postAuthzMethod = ClassLoadUtil.methodFromName(postAuthzFactoryName);
    this.postAuthzCallback = (AccessControl)postAuthzMethod.invoke(null,
        (Object[])null);
    this.postAuthzCallback.init(principal, id.getDistributedMember(), cache);
    if (this.logger.infoEnabled()) {
      this.logger.info(
        LocalizedStrings.AuthorizeRequestPP_AUTHORIZEREQUESTPP_SETTING_POST_PROCESS_AUTHORIZATION_CALLBACK_TO_1_FOR_CLIENT_0, 
        new Object[] {id, postAuthzFactoryName});
    }
  }

  public AccessControl getPostAuthzCallback() {

    return this.postAuthzCallback;
  }

  public GetOperationContext getAuthorize(String regionName, Object key,
      Object result, boolean isObject, GetOperationContext getContext)
      throws NotAuthorizedException {

    if (getContext == null) {
      getContext = new GetOperationContextImpl(key, true);
    }
    else {
      getContext.setPostOperation();
    }
    getContext.setObject(result, isObject);
    if (!this.postAuthzCallback.authorizeOperation(regionName, getContext)) {
      String errStr = LocalizedStrings.AuthorizeRequestPP_IN_POSTPROCESS_NOT_AUTHORIZED_TO_PERFORM_GET_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
        LocalizedStrings.TWO_ARG_COLON,
        new Object[] {this.id, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger
           .finest(this.id
                + ": In post-process: authorized to perform GET operation on region ["
                + regionName + ']');
      }
    }
    return getContext;
  }

  public QueryOperationContext queryAuthorize(String queryString,
      Set regionNames, Object queryResult, QueryOperationContext queryContext, Object[] queryParams)
      throws NotAuthorizedException {

    if (queryContext == null) {
      queryContext = new QueryOperationContext(queryString, regionNames, true, queryParams);
    }
    else {
      queryContext.setPostOperation();
    }
    queryContext.setQueryResult(queryResult);
    if (!this.postAuthzCallback.authorizeOperation(null, queryContext)) {
      String errStr = LocalizedStrings.AuthorizeRequestPP_IN_POSTPROCESS_NOT_AUTHORIZED_TO_PERFORM_QUERY_OPERATION_0_ON_THE_CACHE.toLocalizedString(queryString);
      this.logger.warning(
        LocalizedStrings.TWO_ARG_COLON,
        new Object[] {this.id, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(this.id
            + ": In post-process: authorized to perform QUERY operation ["
            + queryString + "] on cache");
      }
    }
    return queryContext;
  }

  public QueryOperationContext executeCQAuthorize(String cqName,
      String queryString, Set regionNames, Object queryResult,
      QueryOperationContext executeCQContext) throws NotAuthorizedException {

    if (executeCQContext == null) {
      executeCQContext = new ExecuteCQOperationContext(cqName, queryString,
          regionNames, true);
    }
    else {
      executeCQContext.setPostOperation();
    }
    executeCQContext.setQueryResult(queryResult);
    if (!this.postAuthzCallback.authorizeOperation(null, executeCQContext)) {
      String errStr =  LocalizedStrings.AuthorizeRequestPP_IN_POSTPROCESS_NOT_AUTHORIZED_TO_PERFORM_EXECUTE_CQ_OPERATION_0_ON_THE_CACHE.toLocalizedString(queryString);
      this.logger.warning(
        LocalizedStrings.TWO_ARG_COLON,
        new Object[] {this.id, errStr});    
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(this.id
            + ": In post-process: authorized to perform EXECUTE_CQ operation ["
            + queryString + "] on cache");
      }
    }
    return executeCQContext;
  }

  public KeySetOperationContext keySetAuthorize(String regionName, Set keySet,
      KeySetOperationContext keySetContext) throws NotAuthorizedException {

    if (keySetContext == null) {
      keySetContext = new KeySetOperationContext(true);
    }
    else {
      keySetContext.setPostOperation();
    }
    keySetContext.setKeySet(keySet);
    if (!this.postAuthzCallback.authorizeOperation(regionName, keySetContext)) {
      String errStr = LocalizedStrings.AuthorizeRequestPP_IN_POSTPROCESS_NOT_AUTHORIZED_TO_PERFORM_KEY_SET_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
        LocalizedStrings.TWO_ARG_COLON,
        new Object[] {this.id, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger
            .finest(this.id
                + ": In post-process: authorized to perform KEY_SET operation on region ["
                + regionName + ']');
      }
    }
    return keySetContext;
  }
  
  public ExecuteFunctionOperationContext executeFunctionAuthorize(
      Object oneResult, ExecuteFunctionOperationContext executeContext)
      throws NotAuthorizedException {

    executeContext.setResult(oneResult);
    final String regionName = executeContext.getRegionName();
    if (!this.postAuthzCallback.authorizeOperation(regionName, executeContext)) {
    	String errStr = LocalizedStrings.AuthorizeRequestPP_0_NOT_AUTHORIZED_TO_PERFORM_EXECUTE_REGION_FUNCTION_1.toLocalizedString(new Object[] {toString(), regionName});
      if (this.logger.warningEnabled()) {
        this.logger.warning(LocalizedStrings.ONE_ARG, errStr);
      }
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger
            .finest(this.id
                + ": In post-process: authorized to perform EXECUTE_REGION_FUNCTION operation on region ["
                + regionName + ']');
      }
    }
    return executeContext;
  }

  public void close() {

    this.postAuthzCallback.close();
  }

}

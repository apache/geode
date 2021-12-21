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

package org.apache.geode.internal.security;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.Set;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.ExecuteCQOperationContext;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.cache.operations.KeySetOperationContext;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.cache.operations.QueryOperationContext;
import org.apache.geode.cache.operations.internal.GetOperationContextImpl;
import org.apache.geode.internal.ClassLoadUtils;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.security.AccessControl;
import org.apache.geode.security.NotAuthorizedException;

/**
 * This class implements post-processing authorization calls for various operations. It provides
 * methods to invoke authorization callback ({@link AccessControl#authorizeOperation}) to invoke the
 * post-processing callback that may modify the results. The data being passed for the operation is
 * encapsulated in a {@link OperationContext} object that can be modified by the post-processing
 * authorization callbacks.
 *
 * @since GemFire 5.5
 */
public class AuthorizeRequestPP {

  private final AccessControl postAuthzCallback;

  private final ClientProxyMembershipID id;

  private final LogWriter logger;

  private final Principal principal;

  private final boolean isPrincipalSerializable;

  public AuthorizeRequestPP(String postAuthzFactoryName, ClientProxyMembershipID id,
      Principal principal, Cache cache) throws ClassNotFoundException, NoSuchMethodException,
      IllegalAccessException, InvocationTargetException {

    this.id = id;
    this.principal = principal;
    isPrincipalSerializable = this.principal instanceof Serializable;
    logger = cache.getSecurityLogger();
    Method postAuthzMethod = ClassLoadUtils.methodFromName(postAuthzFactoryName);
    postAuthzCallback = (AccessControl) postAuthzMethod.invoke(null, (Object[]) null);
    postAuthzCallback.init(principal, id.getDistributedMember(), cache);
    if (logger.infoEnabled()) {
      logger.info(
          String.format(
              "AuthorizeRequestPP: Setting post process authorization callback to %s for client[%s].",
              id, postAuthzFactoryName));
    }
  }

  public AccessControl getPostAuthzCallback() {

    return postAuthzCallback;
  }

  public GetOperationContext getAuthorize(String regionName, Object key, Object result,
      boolean isObject, GetOperationContext getContext) throws NotAuthorizedException {

    if (getContext == null) {
      getContext = new GetOperationContextImpl(key, true);
    } else {
      getContext.setPostOperation();
    }
    getContext.setObject(result, isObject);
    if (!postAuthzCallback.authorizeOperation(regionName, getContext)) {
      String errStr =
          String.format("In post-process: not authorized to perform GET operation on region %s",
              regionName);
      logger.warning(String.format("%s : %s", id, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger
            .finest(id + ": In post-process: authorized to perform GET operation on region ["
                + regionName + ']');
      }
    }
    return getContext;
  }

  public QueryOperationContext queryAuthorize(String queryString, Set regionNames,
      Object queryResult, QueryOperationContext queryContext, Object[] queryParams)
      throws NotAuthorizedException {

    if (queryContext == null) {
      queryContext = new QueryOperationContext(queryString, regionNames, true, queryParams);
    } else {
      queryContext.setPostOperation();
    }
    queryContext.setQueryResult(queryResult);
    if (!postAuthzCallback.authorizeOperation(null, queryContext)) {
      String errStr =
          String.format(
              "In post-process: not authorized to perform QUERY operation %s on the cache",
              queryString);
      logger.warning(String.format("%s : %s", id, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(id + ": In post-process: authorized to perform QUERY operation ["
            + queryString + "] on cache");
      }
    }
    return queryContext;
  }

  public QueryOperationContext executeCQAuthorize(String cqName, String queryString,
      Set regionNames, Object queryResult, QueryOperationContext executeCQContext)
      throws NotAuthorizedException {

    if (executeCQContext == null) {
      executeCQContext = new ExecuteCQOperationContext(cqName, queryString, regionNames, true);
    } else {
      executeCQContext.setPostOperation();
    }
    executeCQContext.setQueryResult(queryResult);
    if (!postAuthzCallback.authorizeOperation(null, executeCQContext)) {
      String errStr =
          String.format(
              "In post-process: not authorized to perform EXECUTE_CQ operation %s on the cache",
              queryString);
      logger.warning(String.format("%s : %s", id, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger
            .finest(id + ": In post-process: authorized to perform EXECUTE_CQ operation ["
                + queryString + "] on cache");
      }
    }
    return executeCQContext;
  }

  public KeySetOperationContext keySetAuthorize(String regionName, Set keySet,
      KeySetOperationContext keySetContext) throws NotAuthorizedException {

    if (keySetContext == null) {
      keySetContext = new KeySetOperationContext(true);
    } else {
      keySetContext.setPostOperation();
    }
    keySetContext.setKeySet(keySet);
    if (!postAuthzCallback.authorizeOperation(regionName, keySetContext)) {
      String errStr =
          String.format("In post-process: not authorized to perform KEY_SET operation on region %s",
              regionName);
      logger.warning(String.format("%s : %s", id, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(
            id + ": In post-process: authorized to perform KEY_SET operation on region ["
                + regionName + ']');
      }
    }
    return keySetContext;
  }

  public ExecuteFunctionOperationContext executeFunctionAuthorize(Object oneResult,
      ExecuteFunctionOperationContext executeContext) throws NotAuthorizedException {

    executeContext.setResult(oneResult);
    final String regionName = executeContext.getRegionName();
    if (!postAuthzCallback.authorizeOperation(regionName, executeContext)) {
      String errStr =
          String.format(
              "%s: In post-process: Not authorized to perform EXECUTE_REGION_FUNCTION operation on region [%s]",
              this, regionName);
      if (logger.warningEnabled()) {
        logger.warning(String.format("%s", errStr));
      }
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(id
            + ": In post-process: authorized to perform EXECUTE_REGION_FUNCTION operation on region ["
            + regionName + ']');
      }
    }
    return executeContext;
  }

  public void close() {

    postAuthzCallback.close();
  }

}

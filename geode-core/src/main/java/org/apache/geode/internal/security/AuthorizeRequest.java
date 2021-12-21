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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.operations.CloseCQOperationContext;
import org.apache.geode.cache.operations.DestroyOperationContext;
import org.apache.geode.cache.operations.ExecuteCQOperationContext;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.cache.operations.GetDurableCQsOperationContext;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.cache.operations.InterestType;
import org.apache.geode.cache.operations.InvalidateOperationContext;
import org.apache.geode.cache.operations.KeySetOperationContext;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.cache.operations.PutAllOperationContext;
import org.apache.geode.cache.operations.PutOperationContext;
import org.apache.geode.cache.operations.QueryOperationContext;
import org.apache.geode.cache.operations.RegionClearOperationContext;
import org.apache.geode.cache.operations.RegionCreateOperationContext;
import org.apache.geode.cache.operations.RegionDestroyOperationContext;
import org.apache.geode.cache.operations.RegisterInterestOperationContext;
import org.apache.geode.cache.operations.RemoveAllOperationContext;
import org.apache.geode.cache.operations.StopCQOperationContext;
import org.apache.geode.cache.operations.UnregisterInterestOperationContext;
import org.apache.geode.cache.operations.internal.GetOperationContextImpl;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassLoadUtils;
import org.apache.geode.internal.cache.operations.ContainsKeyOperationContext;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.security.AccessControl;
import org.apache.geode.security.NotAuthorizedException;

/**
 * This class implements authorization calls for various operations. It provides methods to invoke
 * authorization callback ({@link AccessControl#authorizeOperation}) before the actual operation to
 * check for authorization (pre-processing) that may modify the arguments to the operations. The
 * data being passed for the operation is encapsulated in a {@link OperationContext} object that can
 * be modified by the pre-processing authorization callbacks.
 *
 * @since GemFire 5.5
 */
public class AuthorizeRequest {

  private final AccessControl authzCallback;

  private final Principal principal;

  private final boolean isPrincipalSerializable;

  private ClientProxyMembershipID id;

  private final LogWriter logger;

  public AuthorizeRequest(String authzFactoryName, DistributedMember dm, Principal principal,
      Cache cache) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
      InvocationTargetException, NotAuthorizedException {

    this.principal = principal;
    isPrincipalSerializable = this.principal instanceof Serializable;

    logger = cache.getSecurityLogger();
    Method authzMethod = ClassLoadUtils.methodFromName(authzFactoryName);
    authzCallback = (AccessControl) authzMethod.invoke(null, (Object[]) null);
    authzCallback.init(principal, dm, cache);
    id = null;
  }

  public AuthorizeRequest(String authzFactoryName, ClientProxyMembershipID id, Principal principal,
      Cache cache) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
      InvocationTargetException, NotAuthorizedException {
    this(authzFactoryName, id.getDistributedMember(), principal, cache);
    this.id = id;
    if (logger.infoEnabled()) {
      logger.info(
          String.format("AuthorizeRequest: Client[%s] is setting authorization callback to %s.",
              id, authzFactoryName));
    }
  }

  public GetOperationContext getAuthorize(String regionName, Object key, Object callbackArg)
      throws NotAuthorizedException {

    GetOperationContext getContext = new GetOperationContextImpl(key, false);
    getContext.setCallbackArg(callbackArg);
    if (!authzCallback.authorizeOperation(regionName, getContext)) {
      String errStr =
          String.format("Not authorized to perform GET operation on region [%s]",
              regionName);
      if (logger.fineEnabled()) {
        logger.warning(String.format("%s : %s", this, errStr));
      }
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(
            this + ": Authorized to perform GET operation on region [" + regionName + ']');
      }
    }
    return getContext;
  }

  public PutOperationContext putAuthorize(String regionName, Object key, Object value,
      boolean isObject, Object callbackArg) throws NotAuthorizedException {

    return putAuthorize(regionName, key, value, isObject, callbackArg, PutOperationContext.UNKNOWN);
  }

  public PutOperationContext putAuthorize(String regionName, Object key, Object value,
      boolean isObject, Object callbackArg, byte opType) throws NotAuthorizedException {

    PutOperationContext putContext = new PutOperationContext(key, value, isObject, opType, false);
    putContext.setCallbackArg(callbackArg);
    if (!authzCallback.authorizeOperation(regionName, putContext)) {
      String errStr =
          String.format("Not authorized to perform PUT operation on region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(
            this + ": Authorized to perform PUT operation on region [" + regionName + ']');
      }
    }
    return putContext;
  }

  public PutAllOperationContext putAllAuthorize(String regionName, Map map, Object callbackArg)
      throws NotAuthorizedException {
    PutAllOperationContext putAllContext = new PutAllOperationContext(map);
    putAllContext.setCallbackArg(callbackArg);
    if (!authzCallback.authorizeOperation(regionName, putAllContext)) {
      final String errStr =
          String.format("Not authorized to perform PUTALL operation on region [%s]",
              regionName);
      if (logger.warningEnabled()) {
        logger.warning(String.format("%s : %s", this, errStr));
      }
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(
            this + ": Authorized to perform PUTALL operation on region [" + regionName + ']');
      }

      // now since we've authorized to run PUTALL, we also need to verify all the
      // <key,value> are authorized to run PUT
      /*
       * According to Jags and Suds, we will not auth PUT for PUTALL for now We will only do auth
       * once for each operation, i.e. PUTALL only Collection entries = map.entrySet(); Iterator
       * iterator = entries.iterator(); Map.Entry mapEntry = null; while (iterator.hasNext()) {
       * mapEntry = (Map.Entry)iterator.next(); String currkey = (String)mapEntry.getKey(); Object
       * value = mapEntry.getValue(); boolean isObject = true; if (value instanceof byte[]) {
       * isObject = false; } byte[] serializedValue =
       * ((CachedDeserializable)value).getSerializedValue();
       *
       * PutOperationContext putContext = new PutOperationContext(currkey, serializedValue,
       * isObject, PutOperationContext.UNKNOWN, false); putContext.setCallbackArg(null); if
       * (!this.authzCallback.authorizeOperation(regionName, putContext)) { String errStr =
       * "Not authorized to perform PUT operation on region [" + regionName + ']' +
       * " for key "+currkey +". PUTALL is not authorized either."; if
       * (this.logger.warningEnabled()) { this.logger.warning(toString() + ": " + errStr); } if
       * (this.isPrincipalSerializable) { throw new NotAuthorizedException(errStr, this.principal);
       * } else { throw new NotAuthorizedException(errStr); } } else { if
       * (this.logger.finestEnabled()) { this.logger.finest(toString() +
       * ": PUT is authorized in PUTALL for "+currkey+" isObject("+isObject+") on region [" +
       * regionName + ']'); } } } // while iterating map
       */
    }
    return putAllContext;
  }

  public RemoveAllOperationContext removeAllAuthorize(String regionName, Collection<?> keys,
      Object callbackArg) throws NotAuthorizedException {
    RemoveAllOperationContext removeAllContext = new RemoveAllOperationContext(keys);
    removeAllContext.setCallbackArg(callbackArg);
    if (!authzCallback.authorizeOperation(regionName, removeAllContext)) {
      final String errStr =
          String.format("Not authorized to perform removeAll operation on region [%s]",
              regionName);
      if (logger.warningEnabled()) {
        logger.warning(String.format("%s : %s", this, errStr));
      }
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this + ": Authorized to perform removeAll operation on region ["
            + regionName + ']');
      }
    }
    return removeAllContext;
  }

  public DestroyOperationContext destroyAuthorize(String regionName, Object key, Object callbackArg)
      throws NotAuthorizedException {

    DestroyOperationContext destroyEntryContext = new DestroyOperationContext(key);
    destroyEntryContext.setCallbackArg(callbackArg);
    if (!authzCallback.authorizeOperation(regionName, destroyEntryContext)) {
      String errStr =
          String.format("Not authorized to perform DESTROY operation on region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this + ": Authorized to perform DESTROY operation on region ["
            + regionName + ']');
      }
    }
    return destroyEntryContext;
  }

  public QueryOperationContext queryAuthorize(String queryString, Set regionNames)
      throws NotAuthorizedException {
    return queryAuthorize(queryString, regionNames, null);
  }

  public QueryOperationContext queryAuthorize(String queryString, Set regionNames,
      Object[] queryParams) throws NotAuthorizedException {

    if (regionNames == null) {
      regionNames = new HashSet();
    }
    QueryOperationContext queryContext =
        new QueryOperationContext(queryString, regionNames, false, queryParams);
    if (!authzCallback.authorizeOperation(null, queryContext)) {
      String errStr =
          String.format("Not authorized to perfom QUERY operation [%s] on the cache",
              queryString);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(
            this + ": Authorized to perform QUERY operation [" + queryString + "] on cache");
      }
    }
    return queryContext;
  }

  public ExecuteCQOperationContext executeCQAuthorize(String cqName, String queryString,
      Set regionNames) throws NotAuthorizedException {

    if (regionNames == null) {
      regionNames = new HashSet();
    }
    ExecuteCQOperationContext executeCQContext =
        new ExecuteCQOperationContext(cqName, queryString, regionNames, false);
    if (!authzCallback.authorizeOperation(null, executeCQContext)) {
      String errStr =
          String.format("Not authorized to perfom EXECUTE_CQ operation [%s] on the cache",
              queryString);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this + ": Authorized to perform EXECUTE_CQ operation ["
            + queryString + "] on cache");
      }
    }
    return executeCQContext;
  }

  public void stopCQAuthorize(String cqName, String queryString, Set regionNames)
      throws NotAuthorizedException {

    StopCQOperationContext stopCQContext =
        new StopCQOperationContext(cqName, queryString, regionNames);
    if (!authzCallback.authorizeOperation(null, stopCQContext)) {
      String errStr =
          String.format("Not authorized to perfom STOP_CQ operation [%s] on the cache",
              cqName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this + ": Authorized to perform STOP_CQ operation [" + cqName + ','
            + queryString + "] on cache");
      }
    }
  }

  public void closeCQAuthorize(String cqName, String queryString, Set regionNames)
      throws NotAuthorizedException {

    CloseCQOperationContext closeCQContext =
        new CloseCQOperationContext(cqName, queryString, regionNames);
    if (!authzCallback.authorizeOperation(null, closeCQContext)) {
      String errStr =
          String.format("Not authorized to perfom CLOSE_CQ operation [%s] on the cache",
              cqName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this + ": Authorized to perform CLOSE_CQ operation [" + cqName
            + ',' + queryString + "] on cache");
      }
    }
  }

  public void getDurableCQsAuthorize() throws NotAuthorizedException {

    GetDurableCQsOperationContext getDurableCQsContext = new GetDurableCQsOperationContext();
    if (!authzCallback.authorizeOperation(null, getDurableCQsContext)) {
      String errStr =
          "Not authorized to perform GET_DURABLE_CQS operation on cache";
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger
            .finest(this + ": Authorized to perform GET_DURABLE_CQS operation on cache");
      }
    }
  }

  public RegionClearOperationContext clearAuthorize(String regionName, Object callbackArg)
      throws NotAuthorizedException {

    RegionClearOperationContext regionClearContext = new RegionClearOperationContext(false);
    regionClearContext.setCallbackArg(callbackArg);
    if (!authzCallback.authorizeOperation(regionName, regionClearContext)) {
      String errStr =
          String.format("Not authorized to perform REGION_CLEAR operation on region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this + ": Authorized to perform REGION_CLEAR operation on region ["
            + regionName + ']');
      }
    }
    return regionClearContext;
  }

  public RegisterInterestOperationContext registerInterestAuthorize(String regionName, Object key,
      final @NotNull org.apache.geode.internal.cache.tier.InterestType interestType,
      InterestResultPolicy policy) throws NotAuthorizedException {

    RegisterInterestOperationContext registerInterestContext = new RegisterInterestOperationContext(
        key, InterestType.fromOrdinal((byte) interestType.ordinal()), policy);
    if (!authzCallback.authorizeOperation(regionName, registerInterestContext)) {
      String errStr =
          String.format("Not authorized to perform REGISTER_INTEREST operation for region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger
            .finest(this + ": Authorized to perform REGISTER_INTEREST operation for region ["
                + regionName + ']');
      }
    }
    return registerInterestContext;
  }

  public RegisterInterestOperationContext registerInterestListAuthorize(String regionName,
      List keys, InterestResultPolicy policy) throws NotAuthorizedException {

    RegisterInterestOperationContext registerInterestListContext;
    registerInterestListContext =
        new RegisterInterestOperationContext(keys, InterestType.LIST, policy);
    if (!authzCallback.authorizeOperation(regionName, registerInterestListContext)) {
      String errStr =
          String.format("Not authorized to perform REGISTER_INTEREST_LIST operation for region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(
            this + ": Authorized to perform REGISTER_INTEREST_LIST operation for region ["
                + regionName + ']');
      }
    }
    return registerInterestListContext;
  }

  public UnregisterInterestOperationContext unregisterInterestAuthorize(String regionName,
      Object key, final @NotNull org.apache.geode.internal.cache.tier.InterestType interestType)
      throws NotAuthorizedException {

    UnregisterInterestOperationContext unregisterInterestContext;
    unregisterInterestContext =
        new UnregisterInterestOperationContext(key,
            InterestType.fromOrdinal((byte) interestType.ordinal()));
    if (!authzCallback.authorizeOperation(regionName, unregisterInterestContext)) {
      String errStr =
          String.format("Not authorized to perform UNREGISTER_INTEREST operation for region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this + ": Authorized to perform DESTROY operation on region ["
            + regionName + ']');
      }
    }
    return unregisterInterestContext;
  }

  public UnregisterInterestOperationContext unregisterInterestListAuthorize(String regionName,
      List keys) throws NotAuthorizedException {

    UnregisterInterestOperationContext unregisterInterestListContext;
    unregisterInterestListContext = new UnregisterInterestOperationContext(keys, InterestType.LIST);
    if (!authzCallback.authorizeOperation(regionName, unregisterInterestListContext)) {
      String errStr =
          String.format(
              "Not authorized to perform UNREGISTER_INTEREST_LIST operation for region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(
            this + ": Authorized to perform UNREGISTER_INTEREST_LIST operation for region ["
                + regionName + ']');
      }
    }
    return unregisterInterestListContext;
  }

  public KeySetOperationContext keySetAuthorize(String regionName) throws NotAuthorizedException {

    KeySetOperationContext keySetContext = new KeySetOperationContext(false);
    if (!authzCallback.authorizeOperation(regionName, keySetContext)) {
      String errStr =
          String.format("Not authorized to perform KEY_SET operation on region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this + ": Authorized to perform KEY_SET operation on region ["
            + regionName + ']');
      }
    }
    return keySetContext;
  }

  public void containsKeyAuthorize(String regionName, Object key) throws NotAuthorizedException {

    ContainsKeyOperationContext containsKeyContext = new ContainsKeyOperationContext(key);
    if (!authzCallback.authorizeOperation(regionName, containsKeyContext)) {
      String errStr =
          String.format("Not authorized to perform CONTAINS_KEY operation on region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this + ": Authorized to perform CONTAINS_KEY operation on region ["
            + regionName + ']');
      }
    }
  }

  public void createRegionAuthorize(String regionName) throws NotAuthorizedException {

    RegionCreateOperationContext regionCreateContext = new RegionCreateOperationContext(false);
    if (!authzCallback.authorizeOperation(regionName, regionCreateContext)) {
      String errStr =
          String.format("Not authorized to perform CREATE_REGION operation for the region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this
            + ": Authorized to perform REGION_CREATE operation of region [" + regionName + ']');
      }
    }
  }

  public RegionDestroyOperationContext destroyRegionAuthorize(String regionName, Object callbackArg)
      throws NotAuthorizedException {

    RegionDestroyOperationContext regionDestroyContext = new RegionDestroyOperationContext(false);
    regionDestroyContext.setCallbackArg(callbackArg);
    if (!authzCallback.authorizeOperation(regionName, regionDestroyContext)) {
      String errStr =
          String.format("Not authorized to perform REGION_DESTROY operation for the region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this
            + ": Authorized to perform REGION_DESTROY operation for region [" + regionName + ']');
      }
    }
    return regionDestroyContext;
  }

  public ExecuteFunctionOperationContext executeFunctionAuthorize(String functionName,
      String region, Set keySet, Object arguments, boolean optimizeForWrite)
      throws NotAuthorizedException {
    ExecuteFunctionOperationContext executeContext = new ExecuteFunctionOperationContext(
        functionName, region, keySet, arguments, optimizeForWrite, false);
    if (!authzCallback.authorizeOperation(region, executeContext)) {
      final String errStr =
          "Not authorized to perform EXECUTE_REGION_FUNCTION operation";
      if (logger.warningEnabled()) {
        logger.warning(String.format("%s : %s", this, errStr));
      }
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger
            .finest(this + ": Authorized to perform EXECUTE_REGION_FUNCTION operation ");
      }
    }
    return executeContext;
  }

  public InvalidateOperationContext invalidateAuthorize(String regionName, Object key,
      Object callbackArg) throws NotAuthorizedException {

    InvalidateOperationContext invalidateEntryContext = new InvalidateOperationContext(key);
    invalidateEntryContext.setCallbackArg(callbackArg);
    if (!authzCallback.authorizeOperation(regionName, invalidateEntryContext)) {
      String errStr =
          String.format("Not authorized to perform INVALIDATE operation on region %s",
              regionName);
      logger.warning(String.format("%s : %s", this, errStr));
      if (isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    } else {
      if (logger.finestEnabled()) {
        logger.finest(this + ": Authorized to perform INVALIDATE operation on region ["
            + regionName + ']');
      }
    }
    return invalidateEntryContext;
  }

  public void close() {

    authzCallback.close();
  }

  @Override
  public String toString() {
    return (id == null ? "ClientProxyMembershipID not available" : id.toString())
        + ",Principal:" + (principal == null ? "" : principal.getName());
  }

}

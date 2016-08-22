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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.operations.*;
import com.gemstone.gemfire.cache.operations.internal.GetOperationContextImpl;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.cache.operations.ContainsKeyOperationContext;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.NotAuthorizedException;

/**
 * This class implements authorization calls for various operations. It provides
 * methods to invoke authorization callback ({@link AccessControl#authorizeOperation})
 * before the actual operation to check for authorization (pre-processing) that
 * may modify the arguments to the operations. The data being passed for the
 * operation is encapsulated in a {@link OperationContext} object that can be
 * modified by the pre-processing authorization callbacks.
 * 
 * @since GemFire 5.5
 */
public class AuthorizeRequest {

  private AccessControl authzCallback;

  private final Principal principal;

  private boolean isPrincipalSerializable;

  private ClientProxyMembershipID id; 

  private final LogWriterI18n logger;

  public AuthorizeRequest(String authzFactoryName, DistributedMember dm,
      Principal principal, Cache cache) throws ClassNotFoundException,
      NoSuchMethodException, IllegalAccessException, InvocationTargetException,
      NotAuthorizedException {

    this.principal = principal;
    if (this.principal instanceof Serializable) {
      this.isPrincipalSerializable = true;
    }
    else {
      this.isPrincipalSerializable = false;
    }

    this.logger = cache.getSecurityLoggerI18n();
    Method authzMethod = ClassLoadUtil.methodFromName(authzFactoryName);
    this.authzCallback = (AccessControl)authzMethod
        .invoke(null, (Object[])null);
    this.authzCallback.init(principal, dm, cache);  
    this.id = null;
  }
  
  public AuthorizeRequest(String authzFactoryName, ClientProxyMembershipID id,
      Principal principal, Cache cache) throws ClassNotFoundException,
      NoSuchMethodException, IllegalAccessException, InvocationTargetException,
      NotAuthorizedException {
    this(authzFactoryName, id.getDistributedMember(), principal, cache);
    this.id = id; 
    if (this.logger.infoEnabled()) {
      this.logger.info(
        LocalizedStrings.AuthorizeRequest_AUTHORIZEREQUEST_CLIENT_0_IS_SETTING_AUTHORIZATION_CALLBACK_TO_1,
        new Object[] {id, authzFactoryName});
    }
  }

  public GetOperationContext getAuthorize(String regionName, Object key,
      Object callbackArg) throws NotAuthorizedException {

    GetOperationContext getContext = new GetOperationContextImpl(key, false);
    getContext.setCallbackArg(callbackArg);
    if (!this.authzCallback.authorizeOperation(regionName, getContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_GET_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      if (this.logger.fineEnabled()) {
        this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr}); 
      }
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform GET operation on region [" + regionName
            + ']');
      }
    }
    return getContext;
  }

  public PutOperationContext putAuthorize(String regionName, Object key,
      Object value, boolean isObject, Object callbackArg)
      throws NotAuthorizedException {

    return putAuthorize(regionName, key, value, isObject,
        callbackArg, PutOperationContext.UNKNOWN);
  }

  public PutOperationContext putAuthorize(String regionName, Object key,
      Object value, boolean isObject, Object callbackArg, byte opType)
      throws NotAuthorizedException {
    
    PutOperationContext putContext = new PutOperationContext(key,
        value, isObject, opType, false);    
    putContext.setCallbackArg(callbackArg);
    if (!this.authzCallback.authorizeOperation(regionName, putContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_PUT_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform PUT operation on region [" + regionName
            + ']');
      }
    }
    return putContext;
  }

  public PutAllOperationContext putAllAuthorize(String regionName, Map map, Object callbackArg)
  throws NotAuthorizedException {
    PutAllOperationContext putAllContext = new PutAllOperationContext(map);
    putAllContext.setCallbackArg(callbackArg);
    if (!this.authzCallback.authorizeOperation(regionName, putAllContext)) {
      final String errStr = 
        LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_PUTALL_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      if (this.logger.warningEnabled()) {
        this.logger.warning(LocalizedStrings.TWO_ARG_COLON, new Object[] {this, errStr});
      }
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform PUTALL operation on region [" + regionName
            + ']');
      }
      
      // now since we've authorized to run PUTALL, we also need to verify all the 
      // <key,value> are authorized to run PUT
      /* According to Jags and Suds, we will not auth PUT for PUTALL for now
       * We will only do auth once for each operation, i.e. PUTALL only
      Collection entries = map.entrySet();
      Iterator iterator = entries.iterator();
      Map.Entry mapEntry = null;
      while (iterator.hasNext()) {
        mapEntry = (Map.Entry)iterator.next();
        String currkey = (String)mapEntry.getKey();
        Object value = mapEntry.getValue();
        boolean isObject = true;
        if (value instanceof byte[]) {
          isObject = false;
        }
        byte[] serializedValue = ((CachedDeserializable)value).getSerializedValue();

        PutOperationContext putContext = new PutOperationContext(currkey,
            serializedValue, isObject, PutOperationContext.UNKNOWN, false);
        putContext.setCallbackArg(null);
        if (!this.authzCallback.authorizeOperation(regionName, putContext)) {
          String errStr = "Not authorized to perform PUT operation on region ["
              + regionName + ']' + " for key "+currkey +". PUTALL is not authorized either.";
          if (this.logger.warningEnabled()) {
            this.logger.warning(toString() + ": " + errStr);
          }
          if (this.isPrincipalSerializable) {
            throw new NotAuthorizedException(errStr, this.principal);
          }
          else {
            throw new NotAuthorizedException(errStr);
          }
        } else {
          if (this.logger.finestEnabled()) {
            this.logger.finest(toString()
                + ": PUT is authorized in PUTALL for "+currkey+" isObject("+isObject+") on region ["
                + regionName + ']');
          }
        }
      } // while iterating map
      */
    }
    return putAllContext;
  }
  
  public RemoveAllOperationContext removeAllAuthorize(String regionName, Collection<?> keys, Object callbackArg)
  throws NotAuthorizedException {
    RemoveAllOperationContext removeAllContext = new RemoveAllOperationContext(keys);
    removeAllContext.setCallbackArg(callbackArg);
    if (!this.authzCallback.authorizeOperation(regionName, removeAllContext)) {
      final String errStr = 
        LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_REMOVEALL_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      if (this.logger.warningEnabled()) {
        this.logger.warning(LocalizedStrings.TWO_ARG_COLON, new Object[] {this, errStr});
      }
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      } else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform removeAll operation on region [" + regionName
            + ']');
      }
    }
    return removeAllContext;
  }
  public DestroyOperationContext destroyAuthorize(String regionName,
      Object key, Object callbackArg) throws NotAuthorizedException {

    DestroyOperationContext destroyEntryContext = new DestroyOperationContext(
        key);
    destroyEntryContext.setCallbackArg(callbackArg);
    if (!this.authzCallback.authorizeOperation(regionName, destroyEntryContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_DESTROY_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform DESTROY operation on region ["
            + regionName + ']');
      }
    }
    return destroyEntryContext;
  }

  public QueryOperationContext queryAuthorize(String queryString,
      Set regionNames) throws NotAuthorizedException {
    return queryAuthorize(queryString, regionNames, null);
  }
  
  public QueryOperationContext queryAuthorize(String queryString,
      Set regionNames, Object[] queryParams) throws NotAuthorizedException {

    if (regionNames == null) {
      regionNames = new HashSet();
    }
    QueryOperationContext queryContext = new QueryOperationContext(queryString,
        regionNames, false, queryParams);
    if (!this.authzCallback.authorizeOperation(null, queryContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFOM_QUERY_OPERATION_0_ON_THE_CACHE.toLocalizedString(queryString);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform QUERY operation [" + queryString
            + "] on cache");
      }
    }
    return queryContext;
  }

  public ExecuteCQOperationContext executeCQAuthorize(String cqName,
      String queryString, Set regionNames) throws NotAuthorizedException {

    if (regionNames == null) {
      regionNames = new HashSet();
    }
    ExecuteCQOperationContext executeCQContext = new ExecuteCQOperationContext(
        cqName, queryString, regionNames, false);
    if (!this.authzCallback.authorizeOperation(null, executeCQContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFOM_EXECUTE_CQ_OPERATION_0_ON_THE_CACHE.toLocalizedString(queryString);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform EXECUTE_CQ operation [" + queryString
            + "] on cache");
      }
    }
    return executeCQContext;
  }

  public void stopCQAuthorize(String cqName, String queryString, Set regionNames)
      throws NotAuthorizedException {

    StopCQOperationContext stopCQContext = new StopCQOperationContext(cqName,
        queryString, regionNames);
    if (!this.authzCallback.authorizeOperation(null, stopCQContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFOM_STOP_CQ_OPERATION_0_ON_THE_CACHE.toLocalizedString(cqName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform STOP_CQ operation [" + cqName + ','
            + queryString + "] on cache");
      }
    }
  }

  public void closeCQAuthorize(String cqName, String queryString,
      Set regionNames) throws NotAuthorizedException {

    CloseCQOperationContext closeCQContext = new CloseCQOperationContext(
        cqName, queryString, regionNames);
    if (!this.authzCallback.authorizeOperation(null, closeCQContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFOM_CLOSE_CQ_OPERATION_0_ON_THE_CACHE.toLocalizedString(cqName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform CLOSE_CQ operation [" + cqName + ','
            + queryString + "] on cache");
      }
    }
  }

  public void getDurableCQsAuthorize()
      throws NotAuthorizedException {

    GetDurableCQsOperationContext getDurableCQsContext = new GetDurableCQsOperationContext();
    if (!this.authzCallback.authorizeOperation(null, getDurableCQsContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_GET_DURABLE_CQS_OPERATION_0_ON_THE_CACHE.toLocalizedString();
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform GET_DURABLE_CQS operation on cache");
      }
    }
  }
  
  public RegionClearOperationContext clearAuthorize(String regionName,
      Object callbackArg) throws NotAuthorizedException {

    RegionClearOperationContext regionClearContext = new RegionClearOperationContext(
        false);
    regionClearContext.setCallbackArg(callbackArg);
    if (!this.authzCallback.authorizeOperation(regionName, regionClearContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_REGION_CLEAR_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform REGION_CLEAR operation on region ["
            + regionName + ']');
      }
    }
    return regionClearContext;
  }

  public RegisterInterestOperationContext registerInterestAuthorize(
      String regionName, Object key, int interestType,
      InterestResultPolicy policy) throws NotAuthorizedException {

    RegisterInterestOperationContext registerInterestContext = new RegisterInterestOperationContext(
        key, InterestType.fromOrdinal((byte)interestType), policy);
    if (!this.authzCallback.authorizeOperation(regionName,
        registerInterestContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_REGISTER_INTEREST_OPERATION_FOR_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger
            .finest(toString()
                + ": Authorized to perform REGISTER_INTEREST operation for region ["
                + regionName + ']');
      }
    }
    return registerInterestContext;
  }

  public RegisterInterestOperationContext registerInterestListAuthorize(
      String regionName, List keys, InterestResultPolicy policy)
      throws NotAuthorizedException {

    RegisterInterestOperationContext registerInterestListContext;
    registerInterestListContext = new RegisterInterestOperationContext(keys,
        InterestType.LIST, policy);
    if (!this.authzCallback.authorizeOperation(regionName,
        registerInterestListContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_REGISTER_INTEREST_LIST_OPERATION_FOR_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger
            .finest(toString()
                + ": Authorized to perform REGISTER_INTEREST_LIST operation for region ["
                + regionName + ']');
      }
    }
    return registerInterestListContext;
  }

  public UnregisterInterestOperationContext unregisterInterestAuthorize(
      String regionName, Object key, int interestType)
      throws NotAuthorizedException {

    UnregisterInterestOperationContext unregisterInterestContext;
    unregisterInterestContext = new UnregisterInterestOperationContext(key,
        InterestType.fromOrdinal((byte)interestType));
    if (!this.authzCallback.authorizeOperation(regionName,
        unregisterInterestContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_UNREGISTER_INTEREST_OPERATION_FOR_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform DESTROY operation on region ["
            + regionName + ']');
      }
    }
    return unregisterInterestContext;
  }

  public UnregisterInterestOperationContext unregisterInterestListAuthorize(
      String regionName, List keys) throws NotAuthorizedException {

    UnregisterInterestOperationContext unregisterInterestListContext;
    unregisterInterestListContext = new UnregisterInterestOperationContext(
        keys, InterestType.LIST);
    if (!this.authzCallback.authorizeOperation(regionName,
        unregisterInterestListContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_UNREGISTER_INTEREST_LIST_OPERATION_FOR_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger
            .finest(toString()
                + ": Authorized to perform UNREGISTER_INTEREST_LIST operation for region ["
                + regionName + ']');
      }
    }
    return unregisterInterestListContext;
  }

  public KeySetOperationContext keySetAuthorize(String regionName)
      throws NotAuthorizedException {

    KeySetOperationContext keySetContext = new KeySetOperationContext(false);
    if (!this.authzCallback.authorizeOperation(regionName, keySetContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_KEY_SET_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform KEY_SET operation on region ["
            + regionName + ']');
      }
    }
    return keySetContext;
  }

  public void containsKeyAuthorize(String regionName, Object key)
      throws NotAuthorizedException {

    ContainsKeyOperationContext containsKeyContext = new ContainsKeyOperationContext(
        key);
    if (!this.authzCallback.authorizeOperation(regionName, containsKeyContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_CONTAINS_KEY_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform CONTAINS_KEY operation on region ["
            + regionName + ']');
      }
    }
  }

  public void createRegionAuthorize(String regionName)
      throws NotAuthorizedException {

    RegionCreateOperationContext regionCreateContext = new RegionCreateOperationContext(
        false);
    if (!this.authzCallback.authorizeOperation(regionName, regionCreateContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_CREATE_REGION_OPERATION_FOR_THE_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform REGION_CREATE operation of region ["
            + regionName + ']');
      }
    }
  }

  public RegionDestroyOperationContext destroyRegionAuthorize(
      String regionName, Object callbackArg) throws NotAuthorizedException {

    RegionDestroyOperationContext regionDestroyContext = new RegionDestroyOperationContext(
        false);
    regionDestroyContext.setCallbackArg(callbackArg);
    if (!this.authzCallback
        .authorizeOperation(regionName, regionDestroyContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_REGION_DESTROY_OPERATION_FOR_THE_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform REGION_DESTROY operation for region ["
            + regionName + ']');
      }
    }
    return regionDestroyContext;
  }
  
  public ExecuteFunctionOperationContext executeFunctionAuthorize(
      String functionName, String region, Set keySet, Object arguments, boolean optimizeForWrite)
      throws NotAuthorizedException {
    ExecuteFunctionOperationContext executeContext = new ExecuteFunctionOperationContext(
        functionName, region, keySet, arguments, optimizeForWrite, false);
    if (!this.authzCallback.authorizeOperation(region, executeContext)) {
      final String errStr = LocalizedStrings.
        AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_EXECUTE_REGION_FUNCTION_OPERATION
          .toLocalizedString();
      if (this.logger.warningEnabled()) {
        this.logger.warning(LocalizedStrings.TWO_ARG_COLON,
            new Object[] {this, errStr});
      }
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform EXECUTE_REGION_FUNCTION operation ");
      }
    }
    return executeContext;
  }

  public InvalidateOperationContext invalidateAuthorize(String regionName,
      Object key, Object callbackArg) throws NotAuthorizedException {

    InvalidateOperationContext invalidateEntryContext = new InvalidateOperationContext(
        key);
    invalidateEntryContext.setCallbackArg(callbackArg);
    if (!this.authzCallback.authorizeOperation(regionName, invalidateEntryContext)) {
      String errStr = LocalizedStrings.AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_INVALIDATE_OPERATION_ON_REGION_0.toLocalizedString(regionName);
      this.logger.warning(
          LocalizedStrings.TWO_ARG_COLON,
          new Object[] {this, errStr});
      if (this.isPrincipalSerializable) {
        throw new NotAuthorizedException(errStr, this.principal);
      }
      else {
        throw new NotAuthorizedException(errStr);
      }
    }
    else {
      if (this.logger.finestEnabled()) {
        this.logger.finest(toString()
            + ": Authorized to perform INVALIDATE operation on region ["
            + regionName + ']');
      }
    }
    return invalidateEntryContext;
  }

  public void close() {

    this.authzCallback.close();
  }

  @Override
  public String toString() {
    return (this.id == null ? "ClientProxyMembershipID not available" : id.toString()) + ",Principal:"
    + (this.principal == null ? "" : this.principal.getName());
  }

}

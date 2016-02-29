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
package com.gemstone.gemfire.management.internal;

import static com.gemstone.gemfire.management.internal.security.ResourceConstants.ACCESS_DENIED_MESSAGE;

import java.lang.reflect.Method;
import java.security.Principal;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.AuthenticationRequiredException;
import com.gemstone.gemfire.security.Authenticator;
import com.gemstone.gemfire.internal.lang.StringUtils;


/**
 * This class acts as a single gateway to authorize and authenticate REST ADMIN
 * APIS. This stores credentials against CommandAuthZRequest object which is
 * used to identify a particular client. As REST requests are state less we need
 * to store this map to avoid re-authenticating same client on subsequent
 * requests. However this map needs to be purged and cleaned up on some expiry
 * policy.
 *
 *
 * @author rishim
 *
 */
public class AuthManager {

  private Map<Properties, CommandAuthZRequest> authMap = new ConcurrentHashMap<Properties, CommandAuthZRequest>();

  private Cache cache;

  private final LogWriterI18n logger;

  private long DEFAULT_EXPIRY_TIME = 30; // in minutes

  private long EXPIRY_TIME ;

  String authzFactoryName;

  String postAuthzFactoryName;

  public static String EXPIRY_TIME_FOR_REST_ADMIN_AUTH = "gemfire.expriyTimeForRESTAdminAuth";

  public AuthManager(Cache cache) {
    this.cache = cache;
    this.logger = cache.getSecurityLoggerI18n();
    this.EXPIRY_TIME = Long.getLong(EXPIRY_TIME_FOR_REST_ADMIN_AUTH, DEFAULT_EXPIRY_TIME);
    DistributedSystem system = cache.getDistributedSystem();
    Properties sysProps = system.getProperties();
    this.authzFactoryName = sysProps.getProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME);
    this.postAuthzFactoryName = sysProps.getProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_PP_NAME);
  }

  private Authenticator getAuthenticator(String authenticatorMethod, Properties securityProperties,
      InternalLogWriter logWriter, InternalLogWriter securityLogWriter) throws AuthenticationFailedException {
    Authenticator auth;
    try {

      Method instanceGetter = ClassLoadUtil.methodFromName(authenticatorMethod);
      auth = (Authenticator) instanceGetter.invoke(null, (Object[]) null);
    } catch (Exception ex) {
      throw new AuthenticationFailedException(
          LocalizedStrings.HandShake_FAILED_TO_ACQUIRE_AUTHENTICATOR_OBJECT.toLocalizedString(), ex);
    }
    if (auth == null) {
      throw new AuthenticationFailedException(
          LocalizedStrings.HandShake_AUTHENTICATOR_INSTANCE_COULD_NOT_BE_OBTAINED.toLocalizedString());
    }
    auth.init(securityProperties, logWriter, securityLogWriter);
    return auth;

  }

  public void verifyCredentials(Properties credentials) {

    DistributedSystem system = this.cache.getDistributedSystem();
    Properties sysProps = system.getProperties();
    String authenticator = sysProps.getProperty(DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME);

    if (authenticator != null && authenticator.length() > 0) {

      CommandAuthZRequest authZRequest = authMap.get(credentials);

      if (authZRequest != null && !authZRequest.hasExpired()) {
        return; //Already existing credentials . Return from here

      } else {
        Principal principal = verifyCredentials(authenticator, credentials, system.getSecurityProperties(),
            (InternalLogWriter) this.cache.getLogger(), (InternalLogWriter) this.cache.getSecurityLogger(), cache
                .getDistributedSystem().getDistributedMember());

        if(authZRequest != null){ //i.e its an expired credential
          CommandAuthZRequest expiredAuth = authMap.remove(credentials);
          try{
            expiredAuth.close();
          }catch(Exception e){
            logger.error(e);//Don't throw an exception , just logs it
          }
        }

        authZRequest = new CommandAuthZRequest(principal).init();
        authMap.put(credentials, authZRequest);
      }
    }

  }

  public void expireAllAuthZ() {
    for (CommandAuthZRequest auth : authMap.values()) {
      try {
        auth.close();

      } catch (Exception e) {
        logger.error(e);// Don't throw an exception , just log it, as it depends on the user code.
      }finally{
        authMap.clear();
      }
    }
  }

  public void authorize(Properties credentials, OperationContext context) {

    if (!StringUtils.isBlank(authzFactoryName)) {
      CommandAuthZRequest authZRequest = authMap.get(credentials);
      boolean authorized = authZRequest.authorize(context);
      if (!authorized)
        throw new SecurityException(ACCESS_DENIED_MESSAGE);
    }
  }

  public void postAuthorize(Properties credentials, OperationContext context) {
    if (!StringUtils.isBlank(postAuthzFactoryName)) {
      CommandAuthZRequest authZRequest = authMap.get(credentials);
      boolean authorized = authZRequest.postAuthorize(context);
      if (!authorized)
        throw new SecurityException(ACCESS_DENIED_MESSAGE);
    }

  }

  private Principal verifyCredentials(String authenticatorMethod, Properties credentials,
      Properties securityProperties, InternalLogWriter logWriter, InternalLogWriter securityLogWriter,
      DistributedMember member) throws AuthenticationRequiredException, AuthenticationFailedException {

    Authenticator authenticator = getAuthenticator(authenticatorMethod, securityProperties, logWriter,
        securityLogWriter);
    Principal principal;

    try {
      principal = authenticator.authenticate(credentials, member);
    } finally {
      authenticator.close();
    }

    return principal;

  }

  public class CommandAuthZRequest {

    private Principal principal;

    private AccessControl authzCallback;

    private AccessControl postAuthzCallback;

    private long initTime = System.currentTimeMillis();

    public CommandAuthZRequest(Principal principal) {
      this.principal = principal;
    }

    public boolean authorize(OperationContext context) {
      if (authzCallback != null) {
        return authzCallback.authorizeOperation(null, context);
      }
      return true; // If no AccessControl is set then always return true
    }

    public boolean postAuthorize(OperationContext context) {
      if (postAuthzCallback != null) {
        return postAuthzCallback.authorizeOperation(null, context);
      }
      return true; // If no AccessControl is set then always return true
    }

    public boolean hasExpired(){
      if(System.currentTimeMillis() - initTime >= EXPIRY_TIME * 60 * 1000){
        return true;
      }
      return false;
    }

    public void close() {
      if (authzCallback != null) {
        authzCallback.close();
      }
      if (postAuthzCallback != null) {
        postAuthzCallback.close();
      }
    }

    private CommandAuthZRequest init() {
      try {
        if (!StringUtils.isBlank(authzFactoryName)) {
          Method authzMethod = ClassLoadUtil.methodFromName(authzFactoryName);
          this.authzCallback = (AccessControl) authzMethod.invoke(null, (Object[]) null);
          this.authzCallback.init(principal, null, cache);
        }
        if (!StringUtils.isBlank(postAuthzFactoryName)) {
          Method postAuthzMethod = ClassLoadUtil.methodFromName(postAuthzFactoryName);
          this.postAuthzCallback = (AccessControl) postAuthzMethod.invoke(null, (Object[]) null);
          this.postAuthzCallback.init(principal, null, cache);
        }
      } catch (IllegalAccessException e) {
        logger.error(e);
        throw new GemFireConfigException("Error while configuring accesscontrol for rest resource", e);
      } catch (Exception e) {
        logger.error(e);
        throw new GemFireConfigException("Error while configuring accesscontrol for rest resource", e);
      }
      return this;
    }

    public AccessControl getAuthzCallback() {
      return authzCallback;
    }

    public AccessControl getPostAuthzCallback() {
      return postAuthzCallback;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + (int) (initTime ^ (initTime >>> 32));
      result = prime * result + ((principal == null) ? 0 : principal.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      CommandAuthZRequest other = (CommandAuthZRequest) obj;
      if (!getOuterType().equals(other.getOuterType()))
        return false;
      if (initTime != other.initTime)
        return false;
      if (principal == null) {
        if (other.principal != null)
          return false;
      } else if (!principal.equals(other.principal))
        return false;
      return true;
    }

    private AuthManager getOuterType() {
      return AuthManager.this;
    }

  }

  public Map<Properties, CommandAuthZRequest> getAuthMap() {
    return this.authMap;
  }

}


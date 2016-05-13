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

import java.security.AccessController;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.cache.operations.OperationContext.Resource;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.security.shiro.CustomAuthRealm;
import com.gemstone.gemfire.internal.security.shiro.ShiroPrincipal;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import com.gemstone.gemfire.management.internal.security.ResourceOperationContext;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.GemFireSecurityException;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.ShiroException;
import org.apache.shiro.UnavailableSecurityManagerException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.config.Ini.Section;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

public class GeodeSecurityUtil {

  private static Logger logger = LogService.getLogger();

  /**
   *
   * @param username
   * @param password
   * @return null if security is not enabled, otherwise return a shiro subject
   */
  public static Subject login(String username, String password){
    if(!isSecured())
      return null;

    Subject currentUser = SecurityUtils.getSubject();

    UsernamePasswordToken token =
        new UsernamePasswordToken(username, password);
    try {
      logger.info("Logging in "+username+"/"+password);
      currentUser.login(token);
    } catch (ShiroException e) {
      logger.info(e.getMessage(), e);
      throw new AuthenticationFailedException(e.getMessage(), e);
    }

    return currentUser;
  }

  /**
   * It first looks the shiro subject in AccessControlContext since JMX will use multiple threads to process operations from the same client.
   * then it looks into Shiro's thead context.
   *
   * @return the shiro subject, null if security is not enabled
   */
  public static Subject getSubject(){
    if(!isSecured())
      return null;

    Subject currentUser = null;

    // First try get the principal out of AccessControlContext instead of Shiro's Thread context
    // since threads can be shared between JMX clients.
    javax.security.auth.Subject jmxSubject =
      javax.security.auth.Subject.getSubject(AccessController.getContext());

    if(jmxSubject!=null){
      Set<ShiroPrincipal> principals = jmxSubject.getPrincipals(ShiroPrincipal.class);
      if(principals.size()>0){
        ShiroPrincipal principal = principals.iterator().next();
        currentUser = principal.getSubject();
        ThreadContext.bind(currentUser);
        return currentUser;
      }
    }

    // in other cases like admin rest call or pulse authorization
    currentUser = SecurityUtils.getSubject();

    if(currentUser==null || currentUser.getPrincipal()==null){
      throw new GemFireSecurityException("Error: Anonymous User");
    }

    return currentUser;
  }

  public static void logout(){
    Subject currentUser = getSubject();
    if(currentUser==null)
      return;

    try {
      logger.info("Logging out "+currentUser.getPrincipal());
      currentUser.logout();
    }
    catch(ShiroException e){
      logger.info(e.getMessage(), e);
      throw new AuthenticationFailedException(e.getMessage(), e);
    }
    // clean out Shiro's thread local content
    ThreadContext.remove();
  }

  public static Callable associateWith(Callable callable){
    Subject currentUser = getSubject();
    if(currentUser==null)
      return callable;

    return currentUser.associateWith(callable);
  }

  public static void authorize(ResourceOperation resourceOperation) {
    if(resourceOperation==null)
      return;

    authorize(resourceOperation.resource().name(),
      resourceOperation.operation().name(),
      null);
  }

  public static void authorizeClusterManage(){
    authorize("CLUSTER", "MANAGE");
  }

  public static void authorizeClusterWrite(){
    authorize("CLUSTER", "WRITE");
  }

  public static void authorizeClusterRead(){
    authorize("CLUSTER", "READ");
  }

  public static void authorizeDataManage(){
    authorize("DATA", "MANAGE");
  }

  public static void authorizeDataWrite(){
    authorize("DATA", "WRITE");
  }

  public static void authorizeDataRead(){
    authorize("DATA", "READ");
  }

  public static void authorizeRegionWrite(String regionName){
    authorize("DATA", "WRITE", regionName);
  }

  public static void authorizeRegionRead(String regionName){
    authorize("DATA", "READ", regionName);
  }

  public static void authorize(String resource, String operation){
    authorize(resource, operation, null);
  }

  private static void authorize(String resource, String operation, String regionName){
    regionName = StringUtils.stripStart(regionName, "/");
    authorize(new ResourceOperationContext(resource, operation, regionName));
  }

  public static void authorize(OperationContext context) {
    if(context==null)
      return;

    if(context.getResource()== Resource.NULL && context.getOperationCode()== OperationCode.NULL)
      return;

    Subject currentUser = getSubject();
    if(currentUser==null)
      return;

    try {
      currentUser.checkPermission(context);
    }
    catch(ShiroException e){
      logger.info(currentUser.getPrincipal() + " not authorized for " + context);
      throw new GemFireSecurityException(e.getMessage(), e);
    }
  }

  private static boolean isSecured(){
    try{
      SecurityUtils.getSecurityManager();
    }
    catch(UnavailableSecurityManagerException e){
      return false;
    }
    return true;
  }

  /**
   * initialize Shiro's Security Manager and Security Utilities
   * @param securityProps
   */
  public static void initSecurity(Properties securityProps){
    if(securityProps==null)
      return;

    String shiroConfig = securityProps.getProperty(DistributionConfig.SECURITY_SHIRO_INIT_NAME);
    String customAuthenticator =securityProps.getProperty(DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME);
    if (!com.gemstone.gemfire.internal.lang.StringUtils.isBlank(shiroConfig)) {
      IniSecurityManagerFactory factory = new IniSecurityManagerFactory("classpath:"+shiroConfig);

      // we will need to make sure that shiro uses a case sensitive permission resolver
      Section main = factory.getIni().addSection("main");
      main.put("geodePermissionResolver", "com.gemstone.gemfire.internal.security.shiro.GeodePermissionResolver");
      if(!main.containsKey("iniRealm.permissionResolver")) {
        main.put("iniRealm.permissionResolver", "$geodePermissionResolver");
      }

      SecurityManager securityManager = factory.getInstance();
      SecurityUtils.setSecurityManager(securityManager);
    }
    else if (!com.gemstone.gemfire.internal.lang.StringUtils.isBlank(customAuthenticator)) {
      Realm realm = new CustomAuthRealm(securityProps);
      SecurityManager securityManager = new DefaultSecurityManager(realm);
      SecurityUtils.setSecurityManager(securityManager);
    }
    else{
      SecurityUtils.setSecurityManager(null);
    }
  }

}

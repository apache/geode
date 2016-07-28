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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.Principal;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.ShiroException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.config.Ini.Section;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.apache.shiro.util.ThreadContext;
import org.apache.shiro.util.ThreadState;

import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.security.shiro.CustomAuthRealm;
import com.gemstone.gemfire.internal.security.shiro.ShiroPrincipal;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.GemFireSecurityException;
import com.gemstone.gemfire.security.NotAuthorizedException;

public class GeodeSecurityUtil {

  private static Logger logger = LogService.getLogger();

  private static PostProcessor postProcessor;
  private static SecurityManager securityManager;
  private static boolean isIntegratedSecurity;
  private static boolean isClientAuthenticator;
  private static boolean isPeerAuthenticator;

  /**
   * It first looks the shiro subject in AccessControlContext since JMX will
   * use multiple threads to process operations from the same client, then it
   * looks into Shiro's thead context.
   *
   * @return the shiro subject, null if security is not enabled
   */
  public static Subject getSubject() {
    if (!isIntegratedSecurity) {
      return null;
    }

    Subject currentUser = null;

    // First try get the principal out of AccessControlContext instead of Shiro's Thread context
    // since threads can be shared between JMX clients.
    javax.security.auth.Subject jmxSubject =
      javax.security.auth.Subject.getSubject(AccessController.getContext());

    if (jmxSubject != null) {
      Set<ShiroPrincipal> principals = jmxSubject.getPrincipals(ShiroPrincipal.class);
      if (principals.size() > 0) {
        ShiroPrincipal principal = principals.iterator().next();
        currentUser = principal.getSubject();
        ThreadContext.bind(currentUser);
        return currentUser;
      }
    }

    // in other cases like admin rest call or pulse authorization
    currentUser = SecurityUtils.getSubject();

    if (currentUser == null || currentUser.getPrincipal() == null) {
      throw new GemFireSecurityException("Error: Anonymous User");
    }

    return currentUser;
  }

  /**
   * @return null if security is not enabled, otherwise return a shiro subject
   */
  public static Subject login(String username, String password) {
    if (!isIntegratedSecurity) {
      return null;
    }

    // this makes sure it starts with a clean user object
    ThreadContext.remove();

    Subject currentUser = SecurityUtils.getSubject();

    UsernamePasswordToken token =
      new UsernamePasswordToken(username, password);
    try {
      logger.info("Logging in " + username);
      currentUser.login(token);
    }
    catch (ShiroException e) {
      logger.info(e.getMessage(), e);
      throw new AuthenticationFailedException("Authentication error. Please check your username/password.", e);
    }

    return currentUser;
  }

  public static void logout() {
    Subject currentUser = getSubject();
    if (currentUser == null) {
      return;
    }

    try {
      logger.info("Logging out " + currentUser.getPrincipal());
      currentUser.logout();
    }
    catch (ShiroException e) {
      logger.info(e.getMessage(), e);
      throw new GemFireSecurityException(e.getMessage(), e);
    }
    // clean out Shiro's thread local content
    ThreadContext.remove();
  }

  public static Callable associateWith(Callable callable) {
    Subject currentUser = getSubject();
    if (currentUser == null) {
      return callable;
    }

    return currentUser.associateWith(callable);
  }

  /**
   * this binds the passed-in subject to the executing thread, normally, you
   * would do this:
   *
   * ThreadState state = null;
   * try{
   *   state = GeodeSecurityUtil.bindSubject(subject);
   *   //do the rest of the work as this subject
   * }
   * finally{
   *   if(state!=null)
   *      state.clear();
   * }
   */
  public static ThreadState bindSubject(Subject subject){
    if (subject == null) {
      return null;
    }

    ThreadState threadState = new SubjectThreadState(subject);
    threadState.bind();
    return threadState;
  }

  public static void authorize(ResourceOperation resourceOperation) {
    if (resourceOperation == null) {
      return;
    }

    authorize(resourceOperation.resource().name(),
      resourceOperation.operation().name(),
      null);
  }

  public static void authorizeClusterManage() {
    authorize("CLUSTER", "MANAGE");
  }

  public static void authorizeClusterWrite() {
    authorize("CLUSTER", "WRITE");
  }

  public static void authorizeClusterRead() {
    authorize("CLUSTER", "READ");
  }

  public static void authorizeDataManage() {
    authorize("DATA", "MANAGE");
  }

  public static void authorizeDataWrite() {
    authorize("DATA", "WRITE");
  }

  public static void authorizeDataRead() {
    authorize("DATA", "READ");
  }

  public static void authorizeRegionManage(String regionName) {
    authorize("DATA", "MANAGE", regionName);
  }

  public static void authorizeRegionManage(String regionName, String key) {
    authorize("DATA", "MANAGE", regionName, key);
  }

  public static void authorizeRegionWrite(String regionName) {
    authorize("DATA", "WRITE", regionName);
  }

  public static void authorizeRegionWrite(String regionName, String key) {
    authorize("DATA", "WRITE", regionName, key);
  }

  public static void authorizeRegionRead(String regionName) {
    authorize("DATA", "READ", regionName);
  }

  public static void authorizeRegionRead(String regionName, String key) {
    authorize("DATA", "READ", regionName, key);
  }

  public static void authorize(String resource, String operation) {
    authorize(resource, operation, null);
  }

  private static void authorize(String resource, String operation, String regionName){
    authorize(resource, operation, regionName, null);
  }

  private static void authorize(String resource, String operation, String regionName, String key) {
    regionName = StringUtils.stripStart(regionName, "/");
    authorize(new ResourcePermission(resource, operation, regionName, key));
  }

  public static void authorize(ResourcePermission context) {
    Subject currentUser = getSubject();
    if (currentUser == null) {
      return;
    }

    if (context == null) {
      return;
    }

    if (context.getResource() == Resource.NULL && context.getOperation() == Operation.NULL) {
      return;
    }

    try {
      currentUser.checkPermission(context);
    }
    catch (ShiroException e) {
      String msg = currentUser.getPrincipal() + " not authorized for " + context;
      logger.info(msg);
      throw new NotAuthorizedException(msg, e);
    }
  }

  /**
   * initialize Shiro's Security Manager and Security Utilities
   */
  public static void initSecurity(Properties securityProps) {
    if (securityProps == null) {
      return;
    }

    String shiroConfig = securityProps.getProperty(SECURITY_SHIRO_INIT);
    String securityConfig = securityProps.getProperty(SECURITY_MANAGER);
    String clientAuthenticatorConfig = securityProps.getProperty(SECURITY_CLIENT_AUTHENTICATOR);
    String peerAuthenticatorConfig = securityProps.getProperty(SECURITY_PEER_AUTHENTICATOR);

    if (!StringUtils.isBlank(shiroConfig)) {
      IniSecurityManagerFactory factory = new IniSecurityManagerFactory("classpath:" + shiroConfig);

      // we will need to make sure that shiro uses a case sensitive permission resolver
      Section main = factory.getIni().addSection("main");
      main.put("geodePermissionResolver", "com.gemstone.gemfire.internal.security.shiro.GeodePermissionResolver");
      if (!main.containsKey("iniRealm.permissionResolver")) {
        main.put("iniRealm.permissionResolver", "$geodePermissionResolver");
      }

      org.apache.shiro.mgt.SecurityManager securityManager = factory.getInstance();
      SecurityUtils.setSecurityManager(securityManager);
      isIntegratedSecurity = true;
    }
    // only set up shiro realm if user has implemented SecurityManager
    else if (!StringUtils.isBlank(securityConfig)) {
      securityManager = getObjectOfTypeFromClassName(securityConfig, SecurityManager.class);
      securityManager.init(securityProps);
      Realm realm = new CustomAuthRealm(securityManager);
      org.apache.shiro.mgt.SecurityManager shiroManager = new DefaultSecurityManager(realm);
      SecurityUtils.setSecurityManager(shiroManager);
      isIntegratedSecurity = true;
    }
    else if( !StringUtils.isBlank(clientAuthenticatorConfig)) {
      isClientAuthenticator = true;
    }
    else if (!StringUtils.isBlank(peerAuthenticatorConfig)) {
      isPeerAuthenticator = true;
    }
    else {
      isIntegratedSecurity = false;
      isClientAuthenticator = false;
      isPeerAuthenticator = false;
    }

    // this initializes the post processor
    String customPostProcessor = securityProps.getProperty(SECURITY_POST_PROCESSOR);
    if( !StringUtils.isBlank(customPostProcessor)) {
      postProcessor = getObjectOfTypeFromClassName(customPostProcessor, PostProcessor.class);
      postProcessor.init(securityProps);
    }
    else{
      postProcessor = null;
    }
  }

  public static void close() {
    if (securityManager != null) {
      securityManager.close();
      securityManager = null;
    }

    if (postProcessor != null) {
      postProcessor.close();
      postProcessor = null;
    }
    ThreadContext.remove();
    isIntegratedSecurity = false;
    isClientAuthenticator = false;
    isPeerAuthenticator = false;
  }

  /**
   * postProcess call already has this logic built in, you don't need to call
   * this everytime you call postProcess. But if your postProcess is pretty
   * involved with preparations and you need to bypass it entirely, call this
   * first.
   */
  public static boolean needPostProcess(){
    return (isIntegratedSecurity && postProcessor != null);
  }

  public static Object postProcess(String regionPath, Object key, Object result){
    if(postProcessor == null)
      return result;

    Subject subject = getSubject();

    if(subject == null)
      return result;

    String regionName = StringUtils.stripStart(regionPath, "/");
    return postProcessor.processRegionValue((Principal)subject.getPrincipal(), regionName, key,  result);
  }

  /**
   * this method would never return null, it either throws an exception or
   * returns an object
   */
  public static <T> T getObjectOfTypeFromClassName(String className, Class<T> expectedClazz) {
    Class actualClass = null;
    try {
      actualClass = ClassLoadUtil.classFromName(className);
    }
    catch (Exception ex) {
      throw new GemFireSecurityException("Instance could not be obtained, "+ex.toString(), ex);
    }

    if(!expectedClazz.isAssignableFrom(actualClass)){
      throw new GemFireSecurityException("Instance could not be obtained. Expecting a "+expectedClazz.getName()+" class.");
    }

    T actualObject = null;
    try {
      actualObject =  (T)actualClass.newInstance();
    } catch (Exception e) {
      throw new GemFireSecurityException("Instance could not be obtained. Error instantiating "+actualClass.getName(), e);
    }
    return actualObject;
  }

  /**
   * this method would never return null, it either throws an exception or
   * returns an object
   */
  public static <T> T getObjectOfTypeFromFactoryMethod(String factoryMethodName, Class<T> expectedClazz){
    T actualObject = null;
    try {
      Method factoryMethod = ClassLoadUtil.methodFromName(factoryMethodName);
      actualObject = (T)factoryMethod.invoke(null, (Object[])null);
    } catch (Exception e) {
      throw new GemFireSecurityException("Instance could not be obtained from "+factoryMethodName, e);
    }

    if(actualObject == null){
      throw new GemFireSecurityException("Instance could not be obtained from "+factoryMethodName);
    }

    return actualObject;
  }

  /**
   * this method would never return null, it either throws an exception or
   * returns an object
   *
   * @return an object of type expectedClazz. This method would never return
   * null. It either returns an non-null object or throws exception.
   */
  public static <T> T getObjectOfType(String classOrMethod, Class<T> expectedClazz) {
    T object = null;
    try{
      object = getObjectOfTypeFromClassName(classOrMethod, expectedClazz);
    }
    catch (Exception e){
      object = getObjectOfTypeFromFactoryMethod(classOrMethod, expectedClazz);
    }
    return object;
  }

  public static SecurityManager getSecurityManager(){
    return securityManager;
  }

  public static boolean isClientSecurityRequired() {
    return isClientAuthenticator || isIntegratedSecurity;
  }

  public static boolean isPeerSecurityRequired() {
    return isPeerAuthenticator || isIntegratedSecurity;
  }

  public static boolean isIntegratedSecurity(){
    return isIntegratedSecurity;
  }

}

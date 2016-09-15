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
package org.apache.geode.internal.security;

import static org.apache.geode.distributed.ConfigurationProperties.*;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.StringUtils;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.SecurableComponents;
import org.apache.geode.security.SecurityManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.ShiroException;
import org.apache.shiro.config.Ini.Section;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.apache.shiro.util.ThreadContext;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.GemFireIOException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.ClassLoadUtil;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.shiro.CustomAuthRealm;
import org.apache.geode.internal.security.shiro.GeodeAuthenticationToken;
import org.apache.geode.internal.security.shiro.ShiroPrincipal;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.NotAuthorizedException;

public class IntegratedSecurityService implements SecurityService{

  private static Logger logger = LogService.getLogger(LogService.SECURITY_LOGGER_NAME);

  private static SecurityService defaultInstance = new IntegratedSecurityService();

  public static SecurityService getSecurityService() {
    return defaultInstance;
  }

  private IntegratedSecurityService() {
  }

  private PostProcessor postProcessor;
  private SecurityManager securityManager;

  private boolean isIntegratedSecurity;

  private boolean isClientAuthenticator; // is there a SECURITY_CLIENT_AUTHENTICATOR
  private boolean isPeerAuthenticator; // is there a SECURITY_PEER_AUTHENTICATOR

  private boolean isJmxSecurityRequired;
  private boolean isHttpSecurityRequired;
  private boolean isGatewaySecurityRequired;
  private boolean isClusterSecurityRequired;
  private boolean isServerSecurityRequired;

  /**
   * It first looks the shiro subject in AccessControlContext since JMX will
   * use multiple threads to process operations from the same client, then it
   * looks into Shiro's thead context.
   *
   * @return the shiro subject, null if security is not enabled
   */
  public Subject getSubject() {
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
   * convenient method for testing
   */
  public Subject login(String username, String password){
    if(StringUtils.isBlank(username) || StringUtils.isBlank(password))
      return null;

    Properties credentials = new Properties();
    credentials.setProperty(ResourceConstants.USER_NAME, username);
    credentials.setProperty(ResourceConstants.PASSWORD, password);
    return login(credentials);
  }

  /**
   * @return null if security is not enabled, otherwise return a shiro subject
   */
  public Subject login(Properties credentials) {
    if (!isIntegratedSecurity) {
      return null;
    }

    if(credentials == null)
      return null;

    // this makes sure it starts with a clean user object
    ThreadContext.remove();

    Subject currentUser = SecurityUtils.getSubject();
    GeodeAuthenticationToken token = new GeodeAuthenticationToken(credentials);
    try {
      logger.info("Logging in " + token.getPrincipal());
      currentUser.login(token);
    }
    catch (ShiroException e) {
      logger.info(e.getMessage(), e);
      throw new AuthenticationFailedException("Authentication error. Please check your credentials.", e);
    }

    return currentUser;
  }

  public void logout() {
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

  public Callable associateWith(Callable callable) {
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
   *   state = IntegratedSecurityService.bindSubject(subject);
   *   //do the rest of the work as this subject
   * }
   * finally{
   *   if(state!=null)
   *      state.clear();
   * }
   */
  public ThreadState bindSubject(Subject subject){
    if (subject == null) {
      return null;
    }

    ThreadState threadState = new SubjectThreadState(subject);
    threadState.bind();
    return threadState;
  }

  public void authorize(ResourceOperation resourceOperation) {
    if (resourceOperation == null) {
      return;
    }

    authorize(resourceOperation.resource().name(),
      resourceOperation.operation().name(),
      null);
  }

  public void authorizeClusterManage() {
    authorize("CLUSTER", "MANAGE");
  }

  public void authorizeClusterWrite() {
    authorize("CLUSTER", "WRITE");
  }

  public void authorizeClusterRead() {
    authorize("CLUSTER", "READ");
  }

  public void authorizeDataManage() {
    authorize("DATA", "MANAGE");
  }

  public void authorizeDataWrite() {
    authorize("DATA", "WRITE");
  }

  public void authorizeDataRead() {
    authorize("DATA", "READ");
  }

  public void authorizeRegionManage(String regionName) {
    authorize("DATA", "MANAGE", regionName);
  }

  public void authorizeRegionManage(String regionName, String key) {
    authorize("DATA", "MANAGE", regionName, key);
  }

  public void authorizeRegionWrite(String regionName) {
    authorize("DATA", "WRITE", regionName);
  }

  public void authorizeRegionWrite(String regionName, String key) {
    authorize("DATA", "WRITE", regionName, key);
  }

  public void authorizeRegionRead(String regionName) {
    authorize("DATA", "READ", regionName);
  }

  public void authorizeRegionRead(String regionName, String key) {
    authorize("DATA", "READ", regionName, key);
  }

  public void authorize(String resource, String operation) {
    authorize(resource, operation, null);
  }

  private void authorize(String resource, String operation, String regionName){
    authorize(resource, operation, regionName, null);
  }

  private void authorize(String resource, String operation, String regionName, String key) {
    regionName = StringUtils.stripStart(regionName, "/");
    authorize(new ResourcePermission(resource, operation, regionName, key));
  }

  public void authorize(ResourcePermission context) {
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
  public void initSecurity(Properties securityProps) {
    if (securityProps == null) {
      return;
    }

    String enabledComponentsString = securityProps.getProperty(SECURITY_ENABLED_COMPONENTS);
    if (enabledComponentsString == null) {
      enabledComponentsString = DistributionConfig.DEFAULT_SECURITY_ENABLED_COMPONENTS;
    }

    boolean isClusterSecured = enabledComponentsString.contains(SecurableComponents.ALL) || enabledComponentsString.contains(SecurableComponents.CLUSTER);
    boolean isGatewaySecured = enabledComponentsString.contains(SecurableComponents.ALL) || enabledComponentsString.contains(SecurableComponents.GATEWAY);
    boolean isHttpSecured = enabledComponentsString.contains(SecurableComponents.ALL) || enabledComponentsString.contains(SecurableComponents.HTTP_SERVICE);
    boolean isJmxSecured = enabledComponentsString.contains(SecurableComponents.ALL) || enabledComponentsString.contains(SecurableComponents.JMX);
    boolean isServerSecured = enabledComponentsString.contains(SecurableComponents.ALL) || enabledComponentsString.contains(SecurableComponents.SERVER);

    String shiroConfig = securityProps.getProperty(SECURITY_SHIRO_INIT);
    String securityConfig = securityProps.getProperty(SECURITY_MANAGER);
    String clientAuthenticatorConfig = securityProps.getProperty(SECURITY_CLIENT_AUTHENTICATOR);
    String peerAuthenticatorConfig = securityProps.getProperty(SECURITY_PEER_AUTHENTICATOR);

    if (!StringUtils.isBlank(shiroConfig)) {
      IniSecurityManagerFactory factory = new IniSecurityManagerFactory("classpath:" + shiroConfig);

      // we will need to make sure that shiro uses a case sensitive permission resolver
      Section main = factory.getIni().addSection("main");
      main.put("geodePermissionResolver", "org.apache.geode.internal.security.shiro.GeodePermissionResolver");
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

    isServerSecurityRequired = isClientAuthenticator || (isIntegratedSecurity && isServerSecured);
    isClusterSecurityRequired = isPeerAuthenticator || (isIntegratedSecurity && isClusterSecured);

    isGatewaySecurityRequired = isClientAuthenticator || (isIntegratedSecurity && isGatewaySecured);
    isHttpSecurityRequired = isIntegratedSecurity && isHttpSecured;
    isJmxSecurityRequired = isIntegratedSecurity && isJmxSecured;

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

  public void close() {
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
  public boolean needPostProcess(){
    return (isIntegratedSecurity && postProcessor != null);
  }

  public Object postProcess(String regionPath, Object key, Object value, boolean valueIsSerialized){
    return postProcess(null, regionPath, key, value, valueIsSerialized);
  }

  public Object postProcess(Object principal, String regionPath, Object key, Object value, boolean valueIsSerialized) {
    if (!needPostProcess())
      return value;

    if (principal == null) {
      Subject subject = getSubject();
      if (subject == null)
        return value;
      principal = (Serializable) subject.getPrincipal();
    }

    String regionName = StringUtils.stripStart(regionPath, "/");
    Object newValue = null;

    // if the data is a byte array, but the data itself is supposed to be an object, we need to desearized it before we pass
    // it to the callback.
    if (valueIsSerialized && value instanceof byte[]) {
      try {
        Object oldObj = EntryEventImpl.deserialize((byte[]) value);
        Object newObj = postProcessor.processRegionValue(principal, regionName, key,  oldObj);
        newValue = BlobHelper.serializeToBlob(newObj);
      } catch (IOException|SerializationException e) {
        throw new GemFireIOException("Exception de/serializing entry value", e);
      }
    }
    else {
      newValue = postProcessor.processRegionValue(principal, regionName, key, value);
    }

    return newValue;
  }

  private static void checkSameClass(Object obj1, Object obj2){

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

  public SecurityManager getSecurityManager(){
    return securityManager;
  }

  public PostProcessor getPostProcessor() {
    return postProcessor;
  }

  public boolean isIntegratedSecurity(){
    return isIntegratedSecurity;
  }

  public boolean isClientSecurityRequired() { // TODO: rename as isServerSecurityRequired
    return isServerSecurityRequired;
  }

  public boolean isPeerSecurityRequired() { // TODO: rename as isClusterSecurityRequired
    return isClusterSecurityRequired;
  }

  public boolean isJmxSecurityRequired() {
    return isJmxSecurityRequired;
  }

  public boolean isGatewaySecurityRequired() {
    return isGatewaySecurityRequired;
  }

  public boolean isHttpSecurityRequired() {
    return isHttpSecurityRequired;
  }
}

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

import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

public interface SecurityService {

  ThreadState bindSubject(Subject subject);
  Subject getSubject();
  Subject login(Properties credentials);
  Subject login(String username, String password);
  void logout();
  Callable associateWith(Callable callable);
  void authorize(ResourceOperation resourceOperation);
  void authorizeClusterManage();
  void authorizeClusterWrite();
  void authorizeClusterRead();
  void authorizeDataManage();
  void authorizeDataWrite();
  void authorizeDataRead();
  void authorizeRegionManage(String regionName);
  void authorizeRegionManage(String regionName, String key);
  void authorizeRegionWrite(String regionName);
  void authorizeRegionWrite(String regionName, String key);
  void authorizeRegionRead(String regionName);
  void authorizeRegionRead(String regionName, String key);
  void authorize(String resource, String operation);
  void authorize(ResourcePermission context);
  void initSecurity(Properties securityProps);
  void close();
  boolean needPostProcess();
  Object postProcess(String regionPath, Object key, Object value, boolean valueIsSerialized);
  Object postProcess(Object principal, String regionPath, Object key, Object value, boolean valueIsSerialized);
  boolean isClientSecurityRequired();
  boolean isJmxSecurityRequired();
  boolean isGatewaySecurityRequired();
  boolean isHttpSecurityRequired();
  boolean isPeerSecurityRequired();
  boolean isIntegratedSecurity();
  SecurityManager getSecurityManager();
  PostProcessor getPostProcessor();

  static <T> T getObjectOfType(String factoryName, Class<T> clazz) {
    return IntegratedSecurityService.getObjectOfType(factoryName, clazz);
  }

  static <T> T getObjectOfTypeFromFactoryMethod(String factoryMethodName, Class<T> expectedClazz) {
    return IntegratedSecurityService.getObjectOfTypeFromFactoryMethod(factoryMethodName, expectedClazz);
  }

  static <T> T getObjectOfTypeFromClassName(String className, Class<T> expectedClazz) {
    return IntegratedSecurityService.getObjectOfTypeFromClassName(className, expectedClazz);
  }

  public static Properties getCredentials(Properties securityProps){
    Properties credentials = null;
    if(securityProps.containsKey(ResourceConstants.USER_NAME) && securityProps.containsKey(ResourceConstants.PASSWORD)){
      credentials = new Properties();
      credentials.setProperty(ResourceConstants.USER_NAME, securityProps.getProperty(ResourceConstants.USER_NAME));
      credentials.setProperty(ResourceConstants.PASSWORD, securityProps.getProperty(ResourceConstants.PASSWORD));
    }
    return credentials;
  }

  static SecurityService getSecurityService(){
    return IntegratedSecurityService.getSecurityService();
  }
}

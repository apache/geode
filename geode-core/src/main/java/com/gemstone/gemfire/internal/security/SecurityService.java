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

import java.util.Properties;
import java.util.concurrent.Callable;

import com.gemstone.gemfire.management.internal.security.ResourceOperation;

import org.apache.geode.security.GeodePermission;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

public interface SecurityService {

  ThreadState bindSubject(Subject subject);
  Subject getSubject();
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
  void authorize(GeodePermission context);
  void initSecurity(Properties securityProps);
  void close();
  boolean needPostProcess();
  Object postProcess(String regionPath, Object key, Object result);
  <T> T getObject(String factoryName, Class<T> clazz);
  Object getObject(String factoryName);
  boolean isClientSecurityRequired(Properties securityProps);
  boolean isPeerSecurityRequired(Properties securityProps);
  boolean isIntegratedSecurity(Properties securityProps);

  public static SecurityService getSecurityService() {
    return new SecurityService() {
      @Override public ThreadState bindSubject(final Subject subject) {
        return GeodeSecurityUtil.bindSubject(subject);
      }

      @Override public Subject getSubject() {
        return GeodeSecurityUtil.getSubject();
      }

      @Override public Subject login(final String username, final String password) {
        return GeodeSecurityUtil.login(username, password);
      }

      @Override public void logout() {
        GeodeSecurityUtil.logout();
      }

      @Override public Callable associateWith(final Callable callable) {
        return GeodeSecurityUtil.associateWith(callable);
      }

      @Override public void authorize(final ResourceOperation resourceOperation) {
        GeodeSecurityUtil.authorize(resourceOperation);
      }

      @Override public void authorizeClusterManage() {
        GeodeSecurityUtil.authorizeClusterManage();
      }

      @Override public void authorizeClusterWrite() {
        GeodeSecurityUtil.authorizeClusterWrite();
      }

      @Override public void authorizeClusterRead() {
        GeodeSecurityUtil.authorizeClusterRead();
      }

      @Override public void authorizeDataManage() {
        GeodeSecurityUtil.authorizeDataManage();
      }

      @Override public void authorizeDataWrite() {
        GeodeSecurityUtil.authorizeDataWrite();
      }

      @Override public void authorizeDataRead() {
        GeodeSecurityUtil.authorizeDataRead();
      }

      @Override public void authorizeRegionManage(final String regionName) {
        GeodeSecurityUtil.authorizeRegionManage(regionName);
      }

      @Override public void authorizeRegionManage(final String regionName, final String key) {
        GeodeSecurityUtil.authorizeRegionManage(regionName, key);
      }

      @Override public void authorizeRegionWrite(final String regionName) {
        GeodeSecurityUtil.authorizeRegionWrite(regionName);
      }

      @Override public void authorizeRegionWrite(final String regionName, final String key) {
        GeodeSecurityUtil.authorizeRegionWrite(regionName, key);
      }

      @Override public void authorizeRegionRead(final String regionName) {
        GeodeSecurityUtil.authorizeRegionRead(regionName);
      }

      @Override public void authorizeRegionRead(final String regionName, final String key) {
        GeodeSecurityUtil.authorizeRegionRead(regionName, key);
      }

      @Override public void authorize(final String resource, final String operation) {
        GeodeSecurityUtil.authorize(resource, operation);
      }

      @Override public void authorize(final GeodePermission context) {
        GeodeSecurityUtil.authorize(context);
      }

      @Override public void initSecurity(final Properties securityProps) {
        GeodeSecurityUtil.initSecurity(securityProps);
      }

      @Override public void close() {
        GeodeSecurityUtil.close();
      }

      @Override public boolean needPostProcess() {
        return GeodeSecurityUtil.needPostProcess();
      }

      @Override public Object postProcess(final String regionPath, final Object key, final Object result) {
        return GeodeSecurityUtil.postProcess(regionPath, key, result);
      }

      @Override public <T> T getObject(final String factoryName, final Class<T> clazz) {
        return GeodeSecurityUtil.getObject(factoryName, clazz);
      }

      @Override public Object getObject(final String factoryName) {
        return GeodeSecurityUtil.getObject(factoryName);
      }

      @Override public boolean isClientSecurityRequired(final Properties securityProps) {
        return GeodeSecurityUtil.isClientSecurityRequired(securityProps);
      }

      @Override public boolean isPeerSecurityRequired(final Properties securityProps) {
        return GeodeSecurityUtil.isPeerSecurityRequired(securityProps);
      }

      @Override public boolean isIntegratedSecurity(final Properties securityProps) {
        return GeodeSecurityUtil.isIntegratedSecurity(securityProps);
      }
    };
  }

}

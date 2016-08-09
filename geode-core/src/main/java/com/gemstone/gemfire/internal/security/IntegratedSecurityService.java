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

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;

/**
 * Default implementation of {@code SecurityService} for Integrated Security.
 *
 * <p>This class is serializable but always deserializes the singleton {@code defaultInstance}.
 */
public class IntegratedSecurityService implements SecurityService, Serializable {

  private static Logger logger = LogService.getLogger(LogService.SECURITY_LOGGER_NAME);

  private static SecurityService defaultInstance = new IntegratedSecurityService();

  public static SecurityService getSecurityService() {
    return defaultInstance;
  }

  private IntegratedSecurityService() {
  }

  @Override
  public ThreadState bindSubject(final Subject subject) {
    return GeodeSecurityUtil.bindSubject(subject);
  }

  @Override
  public Subject getSubject() {
    return GeodeSecurityUtil.getSubject();
  }

  @Override
  public Subject login(final String username, final String password) {
    return GeodeSecurityUtil.login(username, password);
  }

  @Override
  public void logout() {
    GeodeSecurityUtil.logout();
  }

  @Override
  public Callable associateWith(final Callable callable) {
    return GeodeSecurityUtil.associateWith(callable);
  }

  @Override
  public void authorize(final ResourceOperation resourceOperation) {
    GeodeSecurityUtil.authorize(resourceOperation);
  }

  @Override
  public void authorizeClusterManage() {
    GeodeSecurityUtil.authorizeClusterManage();
  }

  @Override
  public void authorizeClusterWrite() {
    GeodeSecurityUtil.authorizeClusterWrite();
  }

  @Override
  public void authorizeClusterRead() {
    GeodeSecurityUtil.authorizeClusterRead();
  }

  @Override
  public void authorizeDataManage() {
    GeodeSecurityUtil.authorizeDataManage();
  }

  @Override
  public void authorizeDataWrite() {
    GeodeSecurityUtil.authorizeDataWrite();
  }

  @Override
  public void authorizeDataRead() {
    GeodeSecurityUtil.authorizeDataRead();
  }

  @Override
  public void authorizeRegionManage(final String regionName) {
    GeodeSecurityUtil.authorizeRegionManage(regionName);
  }

  @Override
  public void authorizeRegionManage(final String regionName, final String key) {
    GeodeSecurityUtil.authorizeRegionManage(regionName, key);
  }

  @Override
  public void authorizeRegionWrite(final String regionName) {
    GeodeSecurityUtil.authorizeRegionWrite(regionName);
  }

  @Override
  public void authorizeRegionWrite(final String regionName, final String key) {
    GeodeSecurityUtil.authorizeRegionWrite(regionName, key);
  }

  @Override
  public void authorizeRegionRead(final String regionName) {
    GeodeSecurityUtil.authorizeRegionRead(regionName);
  }

  @Override
  public void authorizeRegionRead(final String regionName, final String key) {
    GeodeSecurityUtil.authorizeRegionRead(regionName, key);
  }

  @Override
  public void authorize(final String resource, final String operation) {
    GeodeSecurityUtil.authorize(resource, operation);
  }

  @Override
  public void authorize(final ResourcePermission context) {
    GeodeSecurityUtil.authorize(context);
  }

  @Override
  public void initSecurity(final Properties securityProps) {
    GeodeSecurityUtil.initSecurity(securityProps);
  }

  @Override
  public void close() {
    GeodeSecurityUtil.close();
  }

  @Override
  public boolean needPostProcess() {
    return GeodeSecurityUtil.needPostProcess();
  }

  @Override
  public Object postProcess(final String regionPath, final Object key, final Object value, final boolean valueIsSerialized) {
    return GeodeSecurityUtil.postProcess(regionPath, key, value, valueIsSerialized);
  }

  @Override
  public Object postProcess(final Serializable principal, final String regionPath, final Object key, final Object value, final boolean valueIsSerialized) {
    return GeodeSecurityUtil.postProcess(principal, regionPath, key, value, valueIsSerialized);
  }

  @Override
  public boolean isClientSecurityRequired() {
    return GeodeSecurityUtil.isClientSecurityRequired();
  }

  @Override
  public boolean isJmxSecurityRequired() {
    return GeodeSecurityUtil.isJmxSecurityRequired();
  }

  @Override
  public boolean isGatewaySecurityRequired() {
    return GeodeSecurityUtil.isGatewaySecurityRequired();
  }

  @Override
  public boolean isHttpSecurityRequired() {
    return GeodeSecurityUtil.isHttpServiceSecurityRequired();
  }

  @Override
  public boolean isPeerSecurityRequired() {
    return GeodeSecurityUtil.isPeerSecurityRequired();
  }

  @Override
  public boolean isIntegratedSecurity() {
    return GeodeSecurityUtil.isIntegratedSecurity();
  }

  @Override
  public SecurityManager getSecurityManager() {
    return GeodeSecurityUtil.getSecurityManager();
  }


  private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("SerializationProxy required");
  }

  private Object writeReplace() {
    return new SerializationProxy();
  }

  /**
   * Serialization proxy for {@code IntegratedSecurityService}.
   */
  private static class SerializationProxy implements Serializable {

    SerializationProxy() {
    }

    private Object readResolve() {
      return getSecurityService();
    }
  }
}

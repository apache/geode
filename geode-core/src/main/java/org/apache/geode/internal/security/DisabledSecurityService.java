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

import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;

/**
 * No-op security service that does nothing.
 */
public class DisabledSecurityService implements SecurityService {

  public DisabledSecurityService() {
    // nothing
  }

  @Override
  public void initSecurity(final Properties securityProps) {
    // nothing
  }

  @Override
  public ThreadState bindSubject(final Subject subject) {
    if (subject == null) {
      return null;
    }

    ThreadState threadState = new SubjectThreadState(subject);
    threadState.bind();
    return threadState;
  }

  @Override
  public Subject getSubject() {
    return null;
  }

  @Override
  public Subject login(final Properties credentials) {
    return null;
  }

  @Override
  public void logout() {
    // nothing
  }

  @Override
  public Callable associateWith(final Callable callable) {
    return callable;
  }

  @Override
  public void authorize(final ResourceOperation resourceOperation) {
    // nothing
  }

  @Override
  public void authorizeClusterManage() {
    // nothing
  }

  @Override
  public void authorizeClusterWrite() {
    // nothing
  }

  @Override
  public void authorizeClusterRead() {
    // nothing
  }

  @Override
  public void authorizeDataManage() {
    // nothing
  }

  @Override
  public void authorizeDataWrite() {
    // nothing
  }

  @Override
  public void authorizeDataRead() {
    // nothing
  }

  @Override
  public void authorizeRegionManage(final String regionName) {
    // nothing
  }

  @Override
  public void authorizeRegionManage(final String regionName, final String key) {
    // nothing
  }

  @Override
  public void authorizeRegionWrite(final String regionName) {
    // nothing
  }

  @Override
  public void authorizeRegionWrite(final String regionName, final String key) {
    // nothing
  }

  @Override
  public void authorizeRegionRead(final String regionName) {
    // nothing
  }

  @Override
  public void authorizeRegionRead(final String regionName, final String key) {
    // nothing
  }

  @Override
  public void authorize(final String resource, final String operation) {
    // nothing
  }

  @Override
  public void authorize(final String resource, final String operation, final String regionName) {
    // nothing
  }

  @Override
  public void authorize(final String resource, final String operation, final String regionName,
      final String key) {
    // nothing
  }

  @Override
  public void authorize(final ResourcePermission context) {
    // nothing
  }

  @Override
  public void close() {
    // nothing
  }

  @Override
  public boolean needPostProcess() {
    return false;
  }

  @Override
  public Object postProcess(final String regionPath, final Object key, final Object value,
      final boolean valueIsSerialized) {
    return value;
  }

  @Override
  public Object postProcess(final Object principal, final String regionPath, final Object key,
      final Object value, final boolean valueIsSerialized) {
    return value;
  }

  @Override
  public boolean isClientSecurityRequired() {
    return false;
  }

  @Override
  public boolean isIntegratedSecurity() {
    return false;
  }

  @Override
  public boolean isPeerSecurityRequired() {
    return false;
  }

  @Override
  public SecurityManager getSecurityManager() {
    return null;
  }

  @Override
  public PostProcessor getPostProcessor() {
    return null;
  }
}

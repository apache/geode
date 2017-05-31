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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.security.PostProcessor;

public class FakePostProcessor implements PostProcessor {

  private final AtomicInteger initInvocations = new AtomicInteger(0);
  private final AtomicInteger processRegionValueInvocations = new AtomicInteger(0);
  private final AtomicInteger closeInvocations = new AtomicInteger(0);

  private final AtomicReference<Properties> securityPropsRef = new AtomicReference<>();
  private final AtomicReference<ProcessRegionValueArguments> processRegionValueArgumentsRef =
      new AtomicReference<>();

  @Override
  public void init(Properties securityProps) {
    this.initInvocations.incrementAndGet();
    this.securityPropsRef.set(securityProps);
  }

  @Override
  public Object processRegionValue(final Object principal, final String regionName,
      final Object key, final Object value) {
    this.processRegionValueInvocations.incrementAndGet();
    this.processRegionValueArgumentsRef
        .set(new ProcessRegionValueArguments(principal, regionName, key, value));
    return this.processRegionValueArgumentsRef.get();
  }

  @Override
  public void close() {
    this.closeInvocations.incrementAndGet();
  }

  public int getInitInvocations() {
    return this.initInvocations.get();
  }

  public int getProcessRegionValueInvocations() {
    return this.processRegionValueInvocations.get();
  }

  public int getCloseInvocations() {
    return this.closeInvocations.get();
  }

  public Properties getSecurityProps() {
    return this.securityPropsRef.get();
  }

  public ProcessRegionValueArguments getProcessRegionValueArguments() {
    return this.processRegionValueArgumentsRef.get();
  }

  public static class ProcessRegionValueArguments {
    private final Object principal;
    private final String regionName;
    private final Object key;
    private final Object value;

    public ProcessRegionValueArguments(final Object principal, final String regionName,
        final Object key, final Object value) {
      this.principal = principal;
      this.regionName = regionName;
      this.key = key;
      this.value = value;
    }

    public Object getPrincipal() {
      return this.principal;
    }

    public String getRegionName() {
      return this.regionName;
    }

    public Object getKey() {
      return this.key;
    }

    public Object getValue() {
      return this.value;
    }
  }
}

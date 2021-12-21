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
package org.apache.geode.security;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.examples.SimpleSecurityManager;

public class SpySecurityManager extends SimpleSecurityManager {

  private final AtomicInteger initInvoked = new AtomicInteger(0);
  private final AtomicInteger closeInvoked = new AtomicInteger(0);

  @Override
  public void init(final Properties securityProps) {
    initInvoked.incrementAndGet();
  }

  @Override
  public boolean authorize(final Object principal, final ResourcePermission permission) {
    return true;
  }

  @Override
  public void close() {
    closeInvoked.incrementAndGet();
  }

  public int getInitInvocationCount() {
    return initInvoked.get();
  }

  public int getCloseInvocationCount() {
    return closeInvoked.get();
  }
}

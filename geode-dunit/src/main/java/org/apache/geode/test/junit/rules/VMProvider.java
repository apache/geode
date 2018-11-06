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

package org.apache.geode.test.junit.rules;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

public abstract class VMProvider {
  public static void invokeInEveryMember(SerializableRunnableIF runnableIF, VMProvider... members) {
    Arrays.stream(members).forEach(member -> member.invoke(runnableIF));
  }

  public abstract VM getVM();

  public void stop() {
    stop(true);
  }

  public void stop(boolean cleanWorkingDir) {
    getVM().invoke(() -> {
      // this did not clean up the files
      ClusterStartupRule.stopElementInsideVM();
      MemberStarterRule.disconnectDSIfAny();
    });

    // clean up all the files under the working dir if asked to do so
    if (cleanWorkingDir) {
      Arrays.stream(getWorkingDir().listFiles()).forEach(FileUtils::deleteQuietly);
    }
  }

  public boolean isClient() {
    return getVM().invoke(() -> {
      return ClusterStartupRule.clientCacheRule != null;
    });
  }

  public boolean isLocator() {
    return getVM().invoke(() -> ClusterStartupRule.getLocator() != null);
  }

  // a server can be started without a cache server, so as long as this member has no locator,
  // it's deemed as a server
  public boolean isServer() {
    return getVM().invoke(() -> ClusterStartupRule.getLocator() == null);
  }

  public void invoke(final SerializableRunnableIF runnable) {
    getVM().invoke(runnable);
  }

  public <T> T invoke(final SerializableCallableIF<T> callable) {
    return getVM().invoke(callable);
  }

  public void invoke(String name, final SerializableRunnableIF runnable) {
    getVM().invoke(name, runnable);
  }

  public <T> T invoke(String name, final SerializableCallableIF<T> callable) {
    return getVM().invoke(name, callable);
  }

  public <T> AsyncInvocation<T> invokeAsync(final SerializableCallableIF<T> callable) {
    return getVM().invokeAsync(callable);
  }


  public <T> AsyncInvocation<T> invokeAsync(String name, final SerializableCallableIF<T> callable) {
    return getVM().invokeAsync(name, callable);
  }

  public AsyncInvocation invokeAsync(final SerializableRunnableIF runnable) {
    return getVM().invokeAsync(runnable);
  }

  public AsyncInvocation invokeAsync(String name, final SerializableRunnableIF runnable) {
    return getVM().invokeAsync(name, runnable);
  }

  public File getWorkingDir() {
    return getVM().getWorkingDirectory();
  }

}

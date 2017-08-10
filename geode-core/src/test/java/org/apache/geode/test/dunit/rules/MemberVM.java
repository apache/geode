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

package org.apache.geode.test.dunit.rules;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;

public class MemberVM<T extends Member> implements Member {
  private T member;
  private VM vm;
  private boolean tempWorkingDir;

  public MemberVM(T member, VM vm) {
    this(member, vm, false);
  }

  public MemberVM(T member, VM vm, boolean tempWorkingDir) {
    this.member = member;
    this.vm = vm;
    this.tempWorkingDir = tempWorkingDir;
  }

  public boolean isLocator() {
    return (member instanceof Locator);
  }

  public VM getVM() {
    return vm;
  }

  public void invoke(final SerializableRunnableIF runnable) {
    vm.invoke(runnable);
  }

  public AsyncInvocation invokeAsync(final SerializableRunnableIF runnable) {
    return vm.invokeAsync(runnable);
  }

  public T getMember() {
    return (T) member;
  }

  @Override
  public File getWorkingDir() {
    if (tempWorkingDir)
      return member.getWorkingDir();
    return vm.getWorkingDirectory();
  }

  @Override
  public int getPort() {
    return member.getPort();
  }

  @Override
  public int getJmxPort() {
    return member.getJmxPort();
  }

  @Override
  public int getHttpPort() {
    return member.getHttpPort();
  }

  @Override
  public String getName() {
    return member.getName();
  }

  public void stopMember() {
    this.invoke(LocatorServerStartupRule::stopMemberInThisVM);
    if (tempWorkingDir) {
      /*
       * this temporary workingDir will dynamically change the "user.dir". system property to point
       * to a temporary folder. The Path API caches the first value of "user.dir" that it sees, and
       * this can result in a stale cached value of "user.dir" which points to a directory that no
       * longer exists.
       */
      vm.bounce();
    } else
      // if using the dunit/vm dir as the preset working dir, need to cleanup dir except
      // the locator0view* file, so that regions/indexes won't get persisted across tests
      Arrays.stream(getWorkingDir().listFiles((dir, name) -> {
        return !name.startsWith("locator0view");
      })).forEach(FileUtils::deleteQuietly);
  }
}

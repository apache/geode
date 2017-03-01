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
 *
 */

package org.apache.geode.test.dunit.rules;

import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;

import java.io.File;
import java.io.Serializable;

/**
 * A server or locator inside a DUnit {@link VM}.
 */
public abstract class Member implements Serializable {
  private VM vm;
  private int port;
  private File workingDir;
  private String name;

  public Member(int port, File workingDir, String name) {
    this.vm = vm;
    this.port = port;
    this.workingDir = workingDir;
    this.name = name;
  }

  /**
   * The VM object is an RMI stub which lets us execute code in the JVM of this member.
   * 
   * @return the {@link VM}
   */
  public VM getVM() {
    return vm;
  }

  public void setVM(VM vm) {
    this.vm = vm;
  }

  public int getPort() {
    return port;
  }

  public File getWorkingDir() {
    return workingDir;
  }

  public String getName() {
    return name;
  }

  /**
   * Invokes {@code runnable.run()} in the {@code VM} of this member.
   */
  public void invoke(final SerializableRunnableIF runnable) {
    this.vm.invoke(runnable);
  }
}

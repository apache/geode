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
package org.apache.geode.test.dunit.rules;

import static org.apache.geode.test.dunit.Invoke.*;

import java.io.Serializable;

import org.apache.geode.test.dunit.SerializableRunnable;

/**
 * Provides remote invocation support to a {@code TestRule}. These methods
 * will invoke a SerializableRunnable in all remote DUnit JVMs including the
 * Locator JVM.
 */
class RemoteInvoker implements Serializable {

  private static final long serialVersionUID = -1759722991299584649L;

  public void invokeEverywhere(final SerializableRunnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    invokeInEveryVM(runnable);
    invokeInLocator(runnable);
  }

  public void remoteInvokeInEveryVMAndLocator(final SerializableRunnable runnable) {
    invokeInEveryVM(runnable);
    invokeInLocator(runnable);
  }
}

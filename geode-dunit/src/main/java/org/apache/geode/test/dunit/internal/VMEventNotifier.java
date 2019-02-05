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
package org.apache.geode.test.dunit.internal;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.VMEventListener;
import org.apache.geode.test.dunit.VMEventListenerRegistry;

public class VMEventNotifier implements VMEventListenerRegistry {

  private final List<VMEventListener> listeners;

  VMEventNotifier() {
    this(new ArrayList<>());
  }

  VMEventNotifier(List<VMEventListener> listeners) {
    this.listeners = listeners;
  }

  @Override
  public void addVMEventListener(VMEventListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeVMEventListener(VMEventListener listener) {
    listeners.remove(listener);
  }

  public void notifyAfterCreateVM(VM vm) {
    List<VMEventListener> listenersToNotify = new ArrayList<>(listeners);
    for (VMEventListener listener : listenersToNotify) {
      listener.afterCreateVM(vm);
    }
  }

  public void notifyBeforeBounceVM(VM vm) {
    List<VMEventListener> listenersToNotify = new ArrayList<>(listeners);
    for (VMEventListener listener : listenersToNotify) {
      listener.beforeBounceVM(vm);
    }
  }

  public void notifyAfterBounceVM(VM vm) {
    List<VMEventListener> listenersToNotify = new ArrayList<>(listeners);
    for (VMEventListener listener : listenersToNotify) {
      listener.afterBounceVM(vm);
    }
  }
}

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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.VMEventListener;
import org.apache.geode.test.dunit.VMEventListenerRegistry;

/**
 * Implements {@code VMEventListenerRegistry} and provides thread-safe notifications to registered
 * listeners.
 */
public class VMEventNotifier implements VMEventListenerRegistry {

  private final List<VMEventListener> listeners;

  VMEventNotifier() {
    listeners = new CopyOnWriteArrayList<>();
  }

  @Override
  public void addVMEventListener(VMEventListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeVMEventListener(VMEventListener listener) {
    listeners.remove(listener);
  }

  /**
   * Notifies currently registered listeners of {@link VMEventListener#afterCreateVM(VM)}.
   * Concurrent changes to listener registration are ignored while notifying.
   */
  public void notifyAfterCreateVM(VM vm) {
    for (VMEventListener listener : listeners) {
      listener.afterCreateVM(vm);
    }
  }

  /**
   * Notifies currently registered listeners of {@link VMEventListener#beforeBounceVM(VM)}.
   * Concurrent changes to listener registration are ignored while notifying.
   */
  public void notifyBeforeBounceVM(VM vm) {
    for (VMEventListener listener : listeners) {
      listener.beforeBounceVM(vm);
    }
  }

  /**
   * Notifies currently registered listeners of {@link VMEventListener#afterBounceVM(VM)}.
   * Concurrent changes to listener registration are ignored while notifying.
   */
  public void notifyAfterBounceVM(VM vm) {
    for (VMEventListener listener : listeners) {
      listener.afterBounceVM(vm);
    }
  }
}

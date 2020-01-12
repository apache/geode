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
package org.apache.geode.test.dunit;

/**
 * Provides callback notifications for creation of and bouncing of dunit VMs.
 */
public interface VMEventListener {

  /**
   * Invoked after creating a new dunit VM.
   *
   * @see VM#getVM(int)
   */
  default void afterCreateVM(VM vm) {
    // nothing
  }

  /**
   * Invoked before bouncing a dunit VM.
   *
   * @see VM#bounce()
   * @see VM#bounceForcibly()
   */
  default void beforeBounceVM(VM vm) {
    // nothing
  }

  /**
   * Invoked after bouncing a dunit VM.
   *
   * @see VM#bounce()
   * @see VM#bounceForcibly()
   */
  default void afterBounceVM(VM vm) {
    // nothing
  }
}

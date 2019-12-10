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
package org.apache.geode.distributed.internal;

import org.apache.geode.annotations.internal.MutableForTesting;

/**
 * This class is a test hook to intercept messages in the VM receiving the message.
 */
public abstract class DistributionMessageObserver {

  @MutableForTesting
  private static DistributionMessageObserver instance;

  /**
   * Set the instance of the observer. Setting to null will clear the observer.
   *
   * @return the old observer, or null if there was no old observer.
   */
  public static DistributionMessageObserver setInstance(DistributionMessageObserver instance) {
    DistributionMessageObserver oldInstance = DistributionMessageObserver.instance;
    DistributionMessageObserver.instance = instance;
    return oldInstance;
  }

  public static DistributionMessageObserver getInstance() {
    return instance;
  }

  /**
   * Called before a the process method of the DistributionMessage is called
   *
   * @param dm the distribution manager that received the message
   * @param message The message itself
   */
  public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
    // override as needed
  }

  /**
   * Called after the process method of the DistributionMessage is called
   *
   * @param dm the distribution manager that received the message
   * @param message The message itself
   */
  public void afterProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
    // override as needed
  }

  /**
   * Called just before a message is distributed.
   *
   * @param dm the distribution manager that's sending the message
   * @param message the message itself
   */
  public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
    // override as needed
  }
}

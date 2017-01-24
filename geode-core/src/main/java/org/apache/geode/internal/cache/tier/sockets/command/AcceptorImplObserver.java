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

package org.apache.geode.internal.cache.tier.sockets.command;

import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;

/**
 * AcceptorImplObserver is an observer/visitor for AcceptorImpl that is used for testing.
 */
public abstract class AcceptorImplObserver {
  private static AcceptorImplObserver instance;

  /**
   * Set the instance of the observer. Setting to null will clear the observer.
   *
   * @param instance
   * @return the old observer, or null if there was no old observer.
   */
  public static final AcceptorImplObserver setInstance(AcceptorImplObserver instance) {
    AcceptorImplObserver oldInstance = AcceptorImplObserver.instance;
    AcceptorImplObserver.instance = instance;
    return oldInstance;
  }

  public static final AcceptorImplObserver getInstance() {
    return instance;
  }

  public void beforeClose(AcceptorImpl acceptorImpl) {}

  public void normalCloseTermination(AcceptorImpl acceptorImpl) {}

  public void afterClose(AcceptorImpl acceptorImpl) {}
}

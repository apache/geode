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

package org.apache.geode.modules.session.filter;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Listener to destroy gemfire sessions when native sessions are destroyed.
 *
 * @deprecated No longer does anything, native sessions are no longer kept around
 */
public class SessionListener implements HttpSessionListener {

  private static final Logger LOG = LoggerFactory.getLogger(SessionListener.class.getName());

  public void sessionCreated(HttpSessionEvent httpSessionEvent) {}

  /**
   * This will receive events from the container using the native sessions.
   */
  public void sessionDestroyed(HttpSessionEvent event) {
    // No op
  }
}

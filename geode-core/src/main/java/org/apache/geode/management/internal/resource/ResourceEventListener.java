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
package org.apache.geode.management.internal.resource;

/**
 * Internal interface to be implemented to catch various resource events
 */
public interface ResourceEventListener {

  /**
   * Handles various Geode resource life-cycle methods.
   *
   * @param event Resource events for which invocation has happened
   * @param resource the Geode resource type
   */
  void handleEvent(ResourceEvent event, Object resource);
}

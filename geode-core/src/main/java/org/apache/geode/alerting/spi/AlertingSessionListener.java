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
package org.apache.geode.alerting.spi;

import org.apache.geode.alerting.AlertingService;

/**
 * Defines the lifecycle callbacks of an alerting session.
 */
public interface AlertingSessionListener {

  /**
   * Notifies creation of new alerting session.
   */
  void createSession(AlertingService alertingService);

  /**
   * Notifies start of alerting session.
   */
  void startSession();

  /**
   * Notifies stop of alerting session.
   */
  void stopSession();
}

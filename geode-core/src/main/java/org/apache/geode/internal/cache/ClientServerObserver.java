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
package org.apache.geode.internal.cache;

import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.sockets.Message;

/**
 * This interface is used by testing/debugging code to be notified of different client/server
 * events. See the documentation for class ClientServerObserverHolder for details.
 *
 * @since GemFire 5.1
 *
 */
public interface ClientServerObserver {
  /**
   * This callback is called when now primary Ep is identified.
   */
  void afterPrimaryIdentificationFromBackup(ServerLocation location);

  /**
   * This callback is called just before interest registartion
   */
  void beforeInterestRegistration();

  /**
   * This callback is called just after interest registartion
   */
  void afterInterestRegistration();

  /**
   * This callback is called just before primary identification
   */
  void beforePrimaryIdentificationFromBackup();

  /**
   * This callback is called just before Interest Recovery by DSM thread happens
   */
  void beforeInterestRecovery();

  /**
   * Invoked by CacheClientUpdater just before invoking endpointDied for fail over
   *
   * @param location ServerLocation which has failed
   */
  void beforeFailoverByCacheClientUpdater(ServerLocation location);

  /**
   * Invoked before sending an instantiator message to server
   *
   */
  void beforeSendingToServer(EventID eventId);

  /**
   * Invoked after sending an instantiator message to server
   *
   */
  void afterReceivingFromServer(EventID eventId);

  /**
   * This callback is called just before sending client ack to the primary servrer.
   */
  void beforeSendingClientAck();

  /**
   * Invoked after Message is created
   *
   */
  void afterMessageCreation(Message msg);

  /**
   * Invoked after Queue Destroy Message has been sent
   */
  void afterQueueDestroyMessage();

  /**
   * Invoked after a primary is recovered from a backup or new connection.
   */
  void afterPrimaryRecovered(ServerLocation location);

}

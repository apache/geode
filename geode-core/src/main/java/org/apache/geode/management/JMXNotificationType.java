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
package org.apache.geode.management;

import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Enumerated type for JMX notification types emitted by GemFire management system. This types can
 * be matched with javax.management.Notification.getType() for filtering required notifications.
 *
 * For detail usage see GemFire quick start.
 *
 * @since GemFire 8.0
 */
public interface JMXNotificationType {

  /**
   * Notification type which indicates that a region has been created in the cache <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.cache.region.created</CODE>.
   */
  String REGION_CREATED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.region.created";

  /**
   * Notification type which indicates that a region has been closed/destroyed in the cache <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.cache.region.closed</CODE>.
   */
  String REGION_CLOSED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.region.closed";

  /**
   * Notification type which indicates that a disk store has been created <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.cache.disk.created</CODE>.
   */
  String DISK_STORE_CREATED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.disk.created";

  /**
   * Notification type which indicates that a disk store has been closed. <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.cache.disk.closed</CODE>.
   */
  String DISK_STORE_CLOSED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.disk.closed";

  /**
   * Notification type which indicates that a lock service has been created. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.lockservice.created</CODE>.
   */
  String LOCK_SERVICE_CREATED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.lockservice.created";

  /**
   * Notification type which indicates that a lock service has been closed. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.lockservice.closed</CODE>.
   */
  String LOCK_SERVICE_CLOSED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.lockservice.closed";

  /**
   * Notification type which indicates that a member has been added to the system. <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.cache.created</CODE>.
   */
  String CACHE_MEMBER_JOINED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.member.joined";

  /**
   * Notification type which indicates that a member has departed from the system. <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.cache.created</CODE>.
   */
  String CACHE_MEMBER_DEPARTED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.member.departed";

  /**
   * Notification type which indicates that a member is suspected. <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.cache.created</CODE>.
   */
  String CACHE_MEMBER_SUSPECT =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.member.suspect";

  /**
   * Notification type which indicates that a client has joined <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cacheserver.client.joined</CODE>.
   */
  String CLIENT_JOINED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cacheserver.client.joined";

  /**
   * Notification type which indicates that a client has left <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cacheserver.client.left</CODE>.
   */
  String CLIENT_LEFT =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cacheserver.client.left";

  /**
   * Notification type which indicates that a client has crashed <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cacheserver.client.crashed</CODE>.
   */
  String CLIENT_CRASHED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cacheserver.client.crashed";

  /**
   * Notification type which indicates that a gateway receiver is created <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.receiver.created</CODE>.
   */
  String GATEWAY_RECEIVER_CREATED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.gateway.receiver.created";

  /**
   * Notification type which indicates that a gateway sender is created <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.gateway.sender.created</CODE>.
   */
  String GATEWAY_SENDER_CREATED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.created";

  /**
   * Notification type which indicates that a gateway sender is started <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.gateway.sender.started</CODE>.
   */
  String GATEWAY_SENDER_STARTED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.started";

  /**
   * Notification type which indicates that a gateway sender is stopped <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.gateway.sender.stopped</CODE>.
   */
  String GATEWAY_SENDER_STOPPED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.stopped";

  /**
   * Notification type which indicates that a gateway sender is paused <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.gateway.sender.paused</CODE>.
   */
  String GATEWAY_SENDER_PAUSED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.paused";

  /**
   * Notification type which indicates that a gateway sender is resumed <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.gateway.sender.resumed</CODE>.
   */
  String GATEWAY_SENDER_RESUMED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.resumed";

  /**
   * Notification type which indicates that a gateway sender is removed <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.gateway.sender.removed</CODE>.
   */
  String GATEWAY_SENDER_REMOVED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.removed";

  /**
   * Notification type which indicates that an async queue is created <BR>
   * The value of this type string is
   * <CODE>ggemfire.distributedsystem.asycn.event.queue.created</CODE>.
   */
  String ASYNC_EVENT_QUEUE_CREATED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.asycn.event.queue.created";

  /**
   * Notification type which indicates that an async queue has been closed. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.async.event.queue.closed</CODE>.
   */
  String ASYNC_EVENT_QUEUE_CLOSED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.async.event.queue.closed";

  /**
   * Notification type which indicates a GemFire system generated alert <BR>
   * The value of this type string is <CODE>system.alert</CODE>.
   */
  String SYSTEM_ALERT = "system.alert";

  /**
   * Notification type which indicates that cache server is started <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.cache.server.started</CODE>.
   */
  String CACHE_SERVER_STARTED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.server.started";

  /**
   * Notification type which indicates that cache server is stopped <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.cache.server.stopped</CODE>.
   */
  String CACHE_SERVER_STOPPED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.server.stopped";

  /**
   * Notification type which indicates that a gateway receiver is started <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.receiver.started</CODE>.
   */
  String GATEWAY_RECEIVER_STARTED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.gateway.receiver.started";

  /**
   * Notification type which indicates that a gateway receiver is stopped <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.receiver.stopped</CODE>.
   */
  String GATEWAY_RECEIVER_STOPPED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.gateway.receiver.stopped";

  /**
   * Notification type which indicates that a gateway receiver is destroyed <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.receiver.destroyed</CODE>.
   */
  String GATEWAY_RECEIVER_DESTROYED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.gateway.receiver.destroyed";


  /**
   * Notification type which indicates that locator is started <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.locator.started</CODE>.
   */
  String LOCATOR_STARTED = GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.locator.started";

  /**
   * Notification type which indicates that a cache service is created <BR>
   * The value of this type string is <CODE>gemfire.distributedsystem.cache.service.created</CODE>.
   */
  String CACHE_SERVICE_CREATED =
      GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.cache.service.created";

  String MANAGER_STARTED = GeodeGlossary.GEMFIRE_PREFIX + "distributedsystem.manager.started";
}

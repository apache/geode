/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * Enumerated type for JMX notification types emitted by GemFire management
 * system. This types can be matched with
 * javax.management.Notification.getType() for filtering required notifications.
 * 
 * For detail usage see GemFire quick start.
 * 
 * @since GemFire 8.0
 */
public interface JMXNotificationType {

  /**
   * Notification type which indicates that a region has been created in the
   * cache <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.region.created</CODE>.
   */
  public static final String REGION_CREATED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.region.created";

  /**
   * Notification type which indicates that a region has been closed/destroyed
   * in the cache <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.region.closed</CODE>.
   */
  public static final String REGION_CLOSED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.region.closed";

  /**
   * Notification type which indicates that a disk store has been created <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.disk.created</CODE>.
   */
  public static final String DISK_STORE_CREATED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.disk.created";

  /**
   * Notification type which indicates that a disk store has been closed. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.disk.closed</CODE>.
   */
  public static final String DISK_STORE_CLOSED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.disk.closed";

  /**
   * Notification type which indicates that a lock service has been created. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.lockservice.created</CODE>.
   */
  public static final String LOCK_SERVICE_CREATED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.lockservice.created";

  /**
   * Notification type which indicates that a lock service has been closed. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.lockservice.closed</CODE>.
   */
  public static final String LOCK_SERVICE_CLOSED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.lockservice.closed";

  /**
   * Notification type which indicates that a member has been added to the
   * system. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.created</CODE>.
   */
  public static final String CACHE_MEMBER_JOINED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.member.joined";

  /**
   * Notification type which indicates that a member has departed from the
   * system. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.created</CODE>.
   */
  public static final String CACHE_MEMBER_DEPARTED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.member.departed";

  /**
   * Notification type which indicates that a member is suspected. <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.created</CODE>.
   */
  public static final String CACHE_MEMBER_SUSPECT = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.member.suspect";

  /**
   * Notification type which indicates that a client has joined <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cacheserver.client.joined</CODE>.
   */
  public static final String CLIENT_JOINED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cacheserver.client.joined";

  /**
   * Notification type which indicates that a client has left <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cacheserver.client.left</CODE>.
   */
  public static final String CLIENT_LEFT = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cacheserver.client.left";

  /**
   * Notification type which indicates that a client has crashed <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cacheserver.client.crashed</CODE>.
   */
  public static final String CLIENT_CRASHED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cacheserver.client.crashed";

  /**
   * Notification type which indicates that a gateway receiver is created <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.receiver.created</CODE>.
   */
  public static final String GATEWAY_RECEIVER_CREATED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.gateway.receiver.created";

  /**
   * Notification type which indicates that a gateway sender is created <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.sender.created</CODE>.
   */
  public static final String GATEWAY_SENDER_CREATED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.created";

  /**
   * Notification type which indicates that a gateway sender is started <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.sender.started</CODE>.
   */
  public static final String GATEWAY_SENDER_STARTED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.started";

  /**
   * Notification type which indicates that a gateway sender is stopped <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.sender.stopped</CODE>.
   */
  public static final String GATEWAY_SENDER_STOPPED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.stopped";

  /**
   * Notification type which indicates that a gateway sender is paused <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.sender.paused</CODE>.
   */
  public static final String GATEWAY_SENDER_PAUSED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.paused";

  /**
   * Notification type which indicates that a gateway sender is resumed <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.sender.resumed</CODE>.
   */
  public static final String GATEWAY_SENDER_RESUMED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.gateway.sender.resumed";

  /**
   * Notification type which indicates that an async queue is created <BR>
   * The value of this type string is
   * <CODE>ggemfire.distributedsystem.asycn.event.queue.created</CODE>.
   */
  public static final String ASYNC_EVENT_QUEUE_CREATED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.asycn.event.queue.created";

  /**
   * Notification type which indicates a GemFire system generated alert <BR>
   * The value of this type string is <CODE>system.alert</CODE>.
   */
  public static final String SYSTEM_ALERT = "system.alert";

  /**
   * Notification type which indicates that cache server is started <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.server.started</CODE>.
   */
  public static final String CACHE_SERVER_STARTED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.server.started";

  /**
   * Notification type which indicates that cache server is stopped <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.server.stopped</CODE>.
   */
  public static final String CACHE_SERVER_STOPPED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.server.stopped";

  /**
   * Notification type which indicates that a gateway receiver is started <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.receiver.started</CODE>.
   */
  public static final String GATEWAY_RECEIVER_STARTED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.gateway.receiver.started";

  /**
   * Notification type which indicates that a gateway receiver is stopped <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.gateway.receiver.stopped</CODE>.
   */
  public static final String GATEWAY_RECEIVER_STOPPED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.gateway.receiver.stopped";

  /**
   * Notification type which indicates that locator is started <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.locator.started</CODE>.
   */
  public static final String LOCATOR_STARTED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.locator.started";

  /**
   * Notification type which indicates that a cache service is created <BR>
   * The value of this type string is
   * <CODE>gemfire.distributedsystem.cache.service.created</CODE>.
   */
  public static final String CACHE_SERVICE_CREATED = DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.cache.service.created";
}

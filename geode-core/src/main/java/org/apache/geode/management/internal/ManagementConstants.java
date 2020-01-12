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
package org.apache.geode.management.internal;

import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.management.internal.beans.QueryDataFunction;


/**
 * Management Related Constants are defined here
 *
 */
public interface ManagementConstants {

  /* *********** Constant Strings used in Federation BEGIN ****************** */
  String MGMT_FUNCTION_ID = ManagementFunction.class.getName();

  String QUERY_DATA_FUNCTION = QueryDataFunction.class.getName();

  int DEFAULT_QUERY_LIMIT = 1000;

  long GII_WAIT_TIME_IN_MILLIS = 500;

  int NUM_THREADS_FOR_GII = 10;

  int REFRESH_TIME = DistributionConfig.DEFAULT_JMX_MANAGER_UPDATE_RATE;

  String MONITORING_REGION = "_monitoringRegion";

  String NOTIFICATION_REGION = "_notificationRegion";

  String cascadingSeparator = "/";

  /* *********** Constant Strings used in Federation END ******************** */


  /* ************ Constants for JMX/MBean Interface BEGIN ******************* */
  int ZERO = 0;
  int NOT_AVAILABLE_INT = -1;
  long NOT_AVAILABLE_LONG = -1l;
  float NOT_AVAILABLE_FLOAT = -1.0f;
  double NOT_AVAILABLE_DOUBLE = -1.0;

  @Immutable
  String[] NO_DATA_STRING = new String[0];

  @Immutable
  ObjectName[] NO_DATA_OBJECTNAME = new ObjectName[0];

  String UNDEFINED = "UNDEFINED";

  int RESULT_INDEX = 0;

  @Immutable
  TimeUnit nanoSeconds = TimeUnit.NANOSECONDS;

  /** Equivalent to SEVERE level **/
  String DEFAULT_ALERT_LEVEL = "severe";
  /* ************ Constants for JMX/MBean Interface END ********************* */



  /* ************ ObjectName Strings for MBeans **************************** */
  // 1. Basic elements
  String OBJECTNAME__DEFAULTDOMAIN = "GemFire";

  /**
   * Key value separator for ObjectName
   */
  String KEYVAL_SEPARATOR = ",";

  /**
   * Key value separator for ObjectName
   */
  String DOMAIN_SEPARATOR = ":";

  /**
   * Prefix used for all the ObjectName Strings
   */
  String OBJECTNAME__PREFIX = OBJECTNAME__DEFAULTDOMAIN + DOMAIN_SEPARATOR;

  // 2. Actual ObjectNames and/or ObjectName structures
  String OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN =
      OBJECTNAME__PREFIX + "service=System,type=Distributed";

  String OBJECTNAME__MEMBER_MXBEAN = OBJECTNAME__PREFIX + "type=Member,member={0}";

  String OBJECTNAME__MANAGER_MXBEAN = OBJECTNAME__PREFIX + "service=Manager,type=Member,member={0}";

  String OBJECTNAME__DISTRIBUTEDREGION_MXBEAN =
      OBJECTNAME__PREFIX + "service=Region,name={0},type=Distributed";

  String OBJECTNAME__REGION_MXBEAN =
      OBJECTNAME__PREFIX + "service=Region,name={0},type=Member,member={1}";

  String OBJECTNAME__DISTRIBUTEDLOCKSERVICE_MXBEAN =
      OBJECTNAME__PREFIX + "service=LockService,name={0},type=Distributed";

  String OBJECTNAME__LOCKSERVICE_MXBEAN =
      OBJECTNAME__PREFIX + "service=LockService,name={0},type=Member,member={1}";

  String OBJECTNAME__ASYNCEVENTQUEUE_MXBEAN =
      OBJECTNAME__PREFIX + "service=AsyncEventQueue,queue={0},type=Member,member={1}";

  String OBJECTNAME__GATEWAYSENDER_MXBEAN =
      OBJECTNAME__PREFIX + "service=GatewaySender,gatewaySender={0},type=Member,member={1}";

  String OBJECTNAME__GATEWAYRECEIVER_MXBEAN =
      OBJECTNAME__PREFIX + "service=GatewayReceiver,type=Member,member={0}";

  String OBJECTNAME__CLIENTSERVICE_MXBEAN =
      OBJECTNAME__PREFIX + "service=CacheServer,port={0},type=Member,member={1}";

  String OBJECTNAME__DISKSTORE_MXBEAN =
      OBJECTNAME__PREFIX + "service=DiskStore,name={0},type=Member,member={1}";

  String OBJECTNAME__LOCATOR_MXBEAN = OBJECTNAME__PREFIX + "service=Locator,type=Member,member={0}";

  String OBJECTNAME__CACHESERVICE_MXBEAN =
      OBJECTNAME__PREFIX + "service=CacheService,name={0},type=Member,member={1}";

  String OBJECTNAME__FILEUPLOADER_MBEAN =
      OBJECTNAME__PREFIX + "service=FileUploader,type=Distributed";

  String AGGREGATE_MBEAN_PATTERN = OBJECTNAME__PREFIX + "*,type=Distributed";
  // Object Name keys

  String OBJECTNAME_MEMBER_APPENDER = "member";

  int MAX_SHOW_LOG_LINES = 100;
  int DEFAULT_SHOW_LOG_LINES = 30;

  String GATEWAY_SENDER_PATTERN = OBJECTNAME__PREFIX + "service=GatewaySender,*";

  String NOTIFICATION_HUB_LISTENER = "GemFire:service=NotificationHubListener";

  String LINUX_SYSTEM = "Linux";

  /**
   * Factor converting bytes to MB
   */
  long MBFactor = 1024 * 1024;

  String PULSE_URL = "http://{0}:{1}/pulse";

  String DEFAULT_HOST_NAME = "localhost";

  int NOTIF_REGION_MAX_ENTRIES = 10;



  String REGION_CREATED_PREFIX = "Region Created With Name ";
  String REGION_CLOSED_PREFIX = "Region Destroyed/Closed With Name ";

  String DISK_STORE_CREATED_PREFIX = "DiskStore Created With Name ";
  String DISK_STORE_CLOSED_PREFIX = "DiskStore Destroyed/Closed With Name ";

  String LOCK_SERVICE_CREATED_PREFIX = "LockService Created With Name ";
  String LOCK_SERVICE_CLOSED_PREFIX = "Lockservice closed With Name ";

  String CACHE_MEMBER_DEPARTED_PREFIX = "Member Departed ";
  String CACHE_MEMBER_JOINED_PREFIX = "Member Joined ";
  String CACHE_MEMBER_SUSPECT_PREFIX = "Member Suspected ";

  String GATEWAY_SENDER_CREATED_PREFIX = "GatewaySender Created in the VM ";
  String GATEWAY_SENDER_STARTED_PREFIX = "GatewaySender Started in the VM ";
  String GATEWAY_SENDER_STOPPED_PREFIX = "GatewaySender Stopped in the VM ";
  String GATEWAY_SENDER_PAUSED_PREFIX = "GatewaySender Paused in the VM ";
  String GATEWAY_SENDER_RESUMED_PREFIX = "GatewaySender Resumed in the VM ";
  String GATEWAY_SENDER_REMOVED_PREFIX = "GatewaySender Removed in the VM ";

  String GATEWAY_RECEIVER_CREATED_PREFIX = "GatewayReceiver Created in the VM ";
  String GATEWAY_RECEIVER_DESTROYED_PREFIX = "GatewayReceiver Destroyed in the VM ";
  String GATEWAY_RECEIVER_STARTED_PREFIX = "GatewayReceiver Started in the VM ";
  String GATEWAY_RECEIVER_STOPPED_PREFIX = "GatewayReceiver Stopped in the VM ";

  String ASYNC_EVENT_QUEUE_CREATED_PREFIX = "Async Event Queue is Created  in the VM ";
  String ASYNC_EVENT_QUEUE_CLOSED_PREFIX = "Async Event Queue is Closed in the VM ";

  String CACHE_SERVICE_CREATED_PREFIX = "Cache Service Created With Name ";

  String CACHE_SERVER_STARTED_PREFIX = "Cache Server is Started in the VM ";
  String CACHE_SERVER_STOPPED_PREFIX = "Cache Server is stopped in the VM ";

  String CLIENT_JOINED_PREFIX = "Client joined with Id ";
  String CLIENT_CRASHED_PREFIX = "Client crashed with Id ";
  String CLIENT_LEFT_PREFIX = "Client left with Id ";

  String LOCATOR_STARTED_PREFIX = "Locator is Started in the VM ";


}

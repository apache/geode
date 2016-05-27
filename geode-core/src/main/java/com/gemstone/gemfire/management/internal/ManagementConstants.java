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
package com.gemstone.gemfire.management.internal;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.management.internal.beans.QueryDataFunction;

import javax.management.ObjectName;
import java.util.concurrent.TimeUnit;


/**
 * Management Related Constants are defined here
 *
 */
public interface ManagementConstants {

  /* *********** Constant Strings used in Federation BEGIN ****************** */
  public static final String MGMT_FUNCTION_ID = ManagementFunction.class.getName();
  
  public static final String QUERY_DATA_FUNCTION = QueryDataFunction.class.getName();
  
  public static final int    DEFAULT_QUERY_LIMIT = 1000;
	
  public static final long GII_WAIT_TIME_IN_MILLIS = 500;
	
  public static final int NUM_THREADS_FOR_GII = 10;
	
  public static final int REFRESH_TIME = DistributionConfig.DEFAULT_JMX_MANAGER_UPDATE_RATE;
	
  public static final String MONITORING_REGION = "_monitoringRegion";

  public static final String NOTIFICATION_REGION = "_notificationRegion";
	
	public static final String cascadingSeparator = "/";
	
	/* *********** Constant Strings used in Federation END ******************** */


  /* ************ Constants for JMX/MBean Interface BEGIN ******************* */
  int    ZERO                 = 0;
  int    NOT_AVAILABLE_INT    = -1;
  long   NOT_AVAILABLE_LONG   = -1l;
  float  NOT_AVAILABLE_FLOAT  = -1.0f;
  double NOT_AVAILABLE_DOUBLE = -1.0;

  String[]     NO_DATA_STRING     = new String[0];

  ObjectName[] NO_DATA_OBJECTNAME = new ObjectName[0];

  String UNDEFINED = "UNDEFINED";

  int    RESULT_INDEX = 0;

  TimeUnit nanoSeconds = TimeUnit.NANOSECONDS;

  /** Equivalent to SEVERE level **/
  String DEFAULT_ALERT_LEVEL = "severe";
  /* ************ Constants for JMX/MBean Interface END ********************* */



  /* ************ ObjectName Strings for MBeans **************************** */
  // 1. Basic elements
  public static final String OBJECTNAME__DEFAULTDOMAIN = "GemFire";

  /**
   * Key value separator for ObjectName
   */
  public static final String KEYVAL_SEPARATOR = ",";

  /**
   * Key value separator for ObjectName
   */
  public static final String DOMAIN_SEPARATOR = ":";
  
  /**
   * Prefix used for all the ObjectName Strings
   */
  public static final String OBJECTNAME__PREFIX = OBJECTNAME__DEFAULTDOMAIN + DOMAIN_SEPARATOR;

  // 2. Actual ObjectNames and/or ObjectName structures
  public static final String OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN      = OBJECTNAME__PREFIX + "service=System,type=Distributed";

  public static final String OBJECTNAME__MEMBER_MXBEAN                 = OBJECTNAME__PREFIX + "type=Member,member={0}";

  public static final String OBJECTNAME__MANAGER_MXBEAN                = OBJECTNAME__PREFIX + "service=Manager,type=Member,member={0}";

  public static final String OBJECTNAME__DISTRIBUTEDREGION_MXBEAN      = OBJECTNAME__PREFIX + "service=Region,name={0},type=Distributed";

  public static final String OBJECTNAME__REGION_MXBEAN                 = OBJECTNAME__PREFIX + "service=Region,name={0},type=Member,member={1}";

  public static final String OBJECTNAME__DISTRIBUTEDLOCKSERVICE_MXBEAN = OBJECTNAME__PREFIX + "service=LockService,name={0},type=Distributed";

  public static final String OBJECTNAME__LOCKSERVICE_MXBEAN            = OBJECTNAME__PREFIX + "service=LockService,name={0},type=Member,member={1}";

  public static final String OBJECTNAME__ASYNCEVENTQUEUE_MXBEAN        = OBJECTNAME__PREFIX + "service=AsyncEventQueue,queue={0},type=Member,member={1}";

  public static final String OBJECTNAME__GATEWAYSENDER_MXBEAN          = OBJECTNAME__PREFIX + "service=GatewaySender,gatewaySender={0},type=Member,member={1}";

  public static final String OBJECTNAME__GATEWAYRECEIVER_MXBEAN        = OBJECTNAME__PREFIX + "service=GatewayReceiver,type=Member,member={0}";

  public static final String OBJECTNAME__CLIENTSERVICE_MXBEAN          = OBJECTNAME__PREFIX + "service=CacheServer,port={0},type=Member,member={1}";

  public static final String OBJECTNAME__DISKSTORE_MXBEAN              = OBJECTNAME__PREFIX + "service=DiskStore,name={0},type=Member,member={1}";
  
  public static final String OBJECTNAME__LOCATOR_MXBEAN                = OBJECTNAME__PREFIX + "service=Locator,type=Member,member={0}";
  
  public static final String AGGREGATE_MBEAN_PATTERN                   = OBJECTNAME__PREFIX + "*,type=Distributed";
  // Object Name keys 
  
  public static final String OBJECTNAME_MEMBER_APPENDER ="member"; 
  
  public static final int MAX_SHOW_LOG_LINES = 100;
  public static final int DEFAULT_SHOW_LOG_LINES = 30;
  
  public static final String GATEWAY_SENDER_PATTERN = OBJECTNAME__PREFIX + "service=GatewaySender,*";
  
  public static final String NOTIFICATION_HUB_LISTENER = "GemFire:service=NotificationHubListener";
  
  public static final String LINUX_SYSTEM = "Linux";
  
  /**
   * Factor converting bytes to MB
   */
  public static final long MBFactor = 1024 * 1024;
  
  public static final String PULSE_URL = "http://{0}:{1}/pulse";
  
  public static final String DEFAULT_HOST_NAME = "localhost";
  
  int NOTIF_REGION_MAX_ENTRIES = 10; 
  

  
  public static final String REGION_CREATED_PREFIX= "Region Created With Name ";
  public static final String REGION_CLOSED_PREFIX= "Region Destroyed/Closed With Name ";
  
  public static final String DISK_STORE_CREATED_PREFIX = "DiskStore Created With Name ";
  public static final String DISK_STORE_CLOSED_PREFIX= "DiskStore Destroyed/Closed With Name ";
  
  public static final String LOCK_SERVICE_CREATED_PREFIX= "LockService Created With Name ";
  public static final String LOCK_SERVICE_CLOSED_PREFIX= "Lockservice closed With Name ";
  
  public static final String CACHE_MEMBER_DEPARTED_PREFIX= "Member Departed ";
  public static final String CACHE_MEMBER_JOINED_PREFIX= "Member Joined ";
  public static final String CACHE_MEMBER_SUSPECT_PREFIX= "Member Suspected ";
  
  public static final String GATEWAY_SENDER_CREATED_PREFIX= "GatewaySender Created in the VM ";
  public static final String GATEWAY_SENDER_STARTED_PREFIX= "GatewaySender Started in the VM ";
  public static final String GATEWAY_SENDER_STOPPED_PREFIX= "GatewaySender Stopped in the VM ";
  public static final String GATEWAY_SENDER_PAUSED_PREFIX= "GatewaySender Paused in the VM ";
  public static final String GATEWAY_SENDER_RESUMED_PREFIX= "GatewaySender Resumed in the VM ";
  
  public static final String GATEWAY_RECEIVER_CREATED_PREFIX= "GatewayReceiver Created in the VM ";
  public static final String GATEWAY_RECEIVER_STARTED_PREFIX= "GatewayReceiver Started in the VM ";
  public static final String GATEWAY_RECEIVER_STOPPED_PREFIX= "GatewayReceiver Stopped in the VM ";
  
  public static final String ASYNC_EVENT_QUEUE_CREATED_PREFIX= "Async Event Queue is Created  in the VM ";
  
  public static final String CACHE_SERVER_STARTED_PREFIX= "Cache Server is Started in the VM ";
  public static final String CACHE_SERVER_STOPPED_PREFIX= "Cache Server is stopped in the VM ";
  
  public static final String CLIENT_JOINED_PREFIX= "Client joined with Id ";
  public static final String CLIENT_CRASHED_PREFIX= "Client crashed with Id ";
  public static final String CLIENT_LEFT_PREFIX= "Client left with Id ";
  
  public static final String LOCATOR_STARTED_PREFIX= "Locator is Started in the VM ";


}

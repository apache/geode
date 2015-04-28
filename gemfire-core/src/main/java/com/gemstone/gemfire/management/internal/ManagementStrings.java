/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================+
 */
package com.gemstone.gemfire.management.internal;

import com.gemstone.gemfire.i18n.StringIdImpl;
import com.gemstone.org.jgroups.util.StringId;

/**
 * All Management Localization strings are to be
 * defined here
 *
 * The range of String IDs reserved for Management Strings are
 *
 * 100001 - 110000
 *
 * @author rishim
 *
 */
public class ManagementStrings {

  public static final StringId ManagementFunction = new StringIdImpl(100001, "");

  public static final StringId Management_Service_MANAGEMENT_SERVICE_NOT_STARTED_YET = new StringIdImpl(100002, "Management Service Not Started Yet");

  public static final StringId Management_Service_MANAGEMENT_SERVICE_IS_CLOSED = new StringIdImpl(100003, "Management Service Is Closed");

  public static final StringId Management_Service_OPERATION_NOT_ALLOWED_FOR_CLIENT_CACHE = new StringIdImpl(100004, "Operation Not Allowed For Client Cache");

  public static final StringId Management_Service_NOT_A_GEMFIRE_DOMAIN_MBEAN = new StringIdImpl(100005, "Not A GemFire Domain MBean, can not Federate");

  public static final StringId Management_Service_CLOSED_CACHE = new StringIdImpl(100006, "Cache Is Closed, can not Obtain a ManagementService");

  public static final StringId Management_Service_NOT_A_MANAGING_NODE_YET = new StringIdImpl(100007, "Not a Managing Node, Either transition has not happened or in Progress");

  public static final StringId Management_Service_NOT_CONNECTED_TO_DISTRIBUTED_SYSTEM = new StringIdImpl(100008, "Not Connected To Distributed System");

  public static final StringId Management_Service_PROXY_NOT_AVAILABLE = new StringIdImpl(100009, "Proxy Not Available");

  public static final StringId Management_Service_MBEAN_NOT_REGISTERED_IN_GEMFIRE_DOMAIN = new StringIdImpl(100010, "MBean Not Registered In GemFire Domain");

  public static final StringId Management_Service_MBEAN_NOT_PRESENT_IN_THE_SYSTEM = new StringIdImpl(100011, "MBean Not Present In The System");

  public static final StringId Management_Service_MBEAN_DOES_NOT_HAVE_NOTIFICATION_SUPPORT = new StringIdImpl(100012, "MBean Does Not Have Notification Support");

  public static final StringId Management_Service_MANAGER_ALREADY_RUNNING = new StringIdImpl(100013, "Manager is already running");

  public static final StringId Monitoring_Region_CREATED_WITH_NAME__0 = new StringIdImpl(100014, "Monitoring Region created with name  {0}");

  public static final StringId Monitoring_Region_DESTROYED_WITH_NAME__0 = new StringIdImpl(100015, "Monitoring Region destroyed with name  {0}");

  public static final StringId MANAGEMENT_SERVICE_STARTED = new StringIdImpl(100016, "Management Service Started");

  public static final StringId MANAGEMENT_SERVICE_STOPEED = new StringIdImpl(100017, "Management Service Stopped");

  public static final StringId MANAGEMENT_FUNCTION_COULD_NOT_EXECUTE = new StringIdImpl(100018, "Management Function Could Not Be Executed");

  public static final StringId MEMBER_MBEAN_NOT_FOUND_IN_DS = new StringIdImpl(100019, "Member MBean Not Found In Distributed System");

  public static final StringId DISTRIBUTED_REGION_MBEAN_NOT_FOUND_IN_DS = new StringIdImpl(100020, "DistributedRegionMBean Not Found In Distributed System");

  public static final StringId REGION_MBEAN_NOT_FOUND_IN_DS = new StringIdImpl(100021, "RegionMBean Not Found In Distributed System");

  public static final StringId INVALID_MEMBER_NAME_OR_ID = new StringIdImpl(100022, "{0} is an invalid member name or Id");

  public static final StringId CACHE_SERVER_MBEAN_NOT_FOUND_IN_DS = new StringIdImpl(100023, "Cache Server MBean not Found in DS");

  public static final StringId DISK_STORE_MBEAN_NOT_FOUND_IN_DS = new StringIdImpl(100024, "Disk Store MBean not Found in DS");

  public static final StringId DISTRIBUTED_LOCK_SERVICE_MBEAN_NOT_FOUND_IN_SYSTEM = new StringIdImpl(100025, "Distributed Lock Service MBean not Found in DS");

  public static final StringId GATEWAY_RECEIVER_MBEAN_NOT_FOUND_IN_SYSTEM = new StringIdImpl(100026, "Gateway Receiver MBean not Found in DS");

  public static final StringId GATEWAY_SENDER_MBEAN_NOT_FOUND_IN_SYSTEM = new StringIdImpl(100027, "Gateway Sender MBean not Found in DS");

  public static final StringId LOCK_SERVICE_MBEAN_NOT_FOUND_IN_SYSTEM = new StringIdImpl(100028, "Lock Service MBean not Found in DS");

  public static final StringId MANAGEMENT_TASK = new StringIdImpl(100029, "Management Task");

  public static final StringId MEMBER_IS_SHUTTING_DOWN = new StringIdImpl(100030, "Member Is Shutting down");

  public static final StringId TailLogResponse_NO_LOG_FILE_WAS_SPECIFIED_IN_THE_CONFIGURATION_MESSAGES_IS_BEING_DIRECTED_TO_STDOUT = new StringIdImpl(
      100031,
      "No log file was specified in the configuration, messages is being directed to stdout.");

  public static final StringId TailLogResponse_ERROR_OCCURRED_WHILE_READING_LOGFILE_LOG__0 = new StringIdImpl(100032, "Error occurred while reading log file:  {0}");

  public static final StringId MEMBER_CACHE_MONITOR = new StringIdImpl(100033, "MemberMXBeanMonitor");

  public static final StringId GATEWAY_SENDER_MONITOR = new StringIdImpl(100034, "GatewaySenderMXBeanMonitor");

  public static final StringId DISKSTORE_MONITOR = new StringIdImpl(100035, "DiskStoreMXBeanMonitor");

  public static final StringId REGION_MONITOR = new StringIdImpl(100036, "RegionMXBeanMonitor");

  public static final StringId SERVER_MONITOR = new StringIdImpl(100037, "ServerMXBeanMonitor");

  public static final StringId INSTANCE_NOT_FOUND = new StringIdImpl(100038, "{0} Instance Not Found in Platform MBean Server");

  public static final StringId LISTENER_NOT_FOUND_FOR_0 = new StringIdImpl(100039, "Listener Not Found For MBean : {0}");

  public static final StringId GC_STATS_MONITOR = new StringIdImpl(100040, "GCStatsMonitor");

  public static final StringId VM_STATS_MONITOR = new StringIdImpl(100041, "VMStatsMonitor");

  public static final StringId SYSTEM_STATS_MONITOR = new StringIdImpl(100042, "SystemStatsManager");

  public static final StringId ASYNC_EVENT_QUEUE_MONITOR = new StringIdImpl(100043, "AsyncEventQueueMXBeanMonitor");

  //Query Error messages
  public static final StringId QUERY__MSG__INVALID_MEMBER = new StringIdImpl(100045, "Query is invalid due to invalid member : {0}");

  public static final StringId QUERY__MSG__JOIN_OP_EX = new StringIdImpl(100046, "Join operation can only be executed on targeted members, please give member input");

  public static final StringId QUERY__MSG__QUERY_EXEC = new StringIdImpl(100047, "Query could not be executed due to : {0}");

  public static final StringId QUERY__MSG__INVALID_QUERY = new StringIdImpl(100048, "Query is invalid due to error : {0}");

  public static final StringId QUERY__MSG__REGIONS_NOT_FOUND = new StringIdImpl(100049, "Cannot find regions {0} in any of the members");

  public static final StringId QUERY__MSG__QUERY_EMPTY = new StringIdImpl(100050, "Query is either empty or Null");
  
  public static final StringId QUERY__MSG__REGIONS_NOT_FOUND_ON_MEMBERS = new StringIdImpl(100051, "Cannot find regions {0} in specified members");
  
  public static final StringId QUERY__MSG__REGIONS_NOT_FOUND_ON_MEMBER = new StringIdImpl(100052, "Cannot find region {0} in member {1}");
  
  // Management Task related
  
  public static final StringId MANAGEMENT_TASK_THREAD_GROUP = new StringIdImpl(100053, "Management Task Thread Group");
  
  public static final StringId MANAGEMENT_TASK_CANCELLED = new StringIdImpl(100054, "Management Task Cacelled");
  
  public static final StringId TARGET_DIR_CANT_BE_NULL_OR_EMPTY = new StringIdImpl(100055, "Target directory path can not be null or empty");
  
  //HTTP & Jetty Related
  
  
  public static final StringId SSL_PROTOCOAL_COULD_NOT_BE_DETERMINED = new StringIdImpl(
      100056,
      "SSL Protocol could not be determined. SSL settings might not work correctly. SSL protocols checked are \"SL\", \"SSLv2\", \"SSLv3\", \"TLS\", \"TLSv1\", \"TLSv1.1\", \"TLSv1.2\" ");
}

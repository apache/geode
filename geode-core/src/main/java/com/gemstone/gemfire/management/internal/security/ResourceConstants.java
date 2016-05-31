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
package com.gemstone.gemfire.management.internal.security;

public class ResourceConstants {

	public static final String DEFAULT_LABEL="";
	
	public static final String RESOURCE_SEC_DESCRIPTOR = "resource.secDescriptor";
	public static final String RESORUCE_AUTH_ACCESSOR = "resource-auth-accessor";
	public static final String RESORUCE_AUTHENTICATOR = "resource-authenticator";
  public static final String RESORUCE_DEFAULT_SEC_DESCRIPTOR = "resourceSecDesc.json";
  public static final String CREATE_MBEAN = "createMBean";
  public static final String UNREGISTER_MBEAN = "unregisterMBean";
  public static final String SET_PREFIX = "set";
  public static final String SET_ATTRIBUTE = "setAttribute";
  public static final String SET_ATTRIBUTES= "setAttributes";
  public static final String GET_ATTRIBUTES= "getAttributes";
  public static final String GET_ATTRIBUTE = "getAttribute";
  public static final String GET_PREFIX = "get";
  public static final String GET_IS_PREFIX = "is";
  public static final String REGISTER_MBEAN = "registerMBean";
  public static final String PROCESS_COMMAND ="processCommand";
  public static final String ACCESS_DENIED_MESSAGE = "Access Denied";
  public static final String MISSING_CREDENTIALS_MESSAGE = "Missing Credentials. Please supply username/password.";
  
  public static final String ALTER_REGION = "ALTER_REGION";
  public static final String ALTER_RUNTIME = "ALTER_RUNTIME";
  public static final String BACKUP_DISKSTORE = "BACKUP_DISKSTORE";
  public static final String CHANGE_ALERT_LEVEL = "CHANGE_ALERT_LEVEL";
  public static final String CLOSE_DURABLE_CLIENT = "CLOSE_DURABLE_CLIENT";
  public static final String CLOSE_DURABLE_CQ = "CLOSE_DURABLE_CQ";
  public static final String COMPACT_DISKSTORE = "COMPACT_DISKSTORE";
  public static final String CONFIGURE_PDX = "CONFIGURE_PDX";
  public static final String CREATE_AEQ = "CREATE_AEQ";
  public static final String CREATE_DISKSTORE = "CREATE_DISKSTORE";
  public static final String CREATE_GW_RECEIVER = "CREATE_GW_RECEIVER";
  public static final String CREATE_GW_SENDER = "CREATE_GW_SENDER";
  public static final String CREATE_INDEX = "CREATE_INDEX";
  public static final String CREATE_REGION = "CREATE_REGION";
  public static final String DEPLOY = "DEPLOY";
  public static final String DESTROY_DISKSTORE = "DESTROY_DISKSTORE";
  public static final String DESTROY_FUNCTION = "DESTROY_FUNCTION";
  public static final String DESTROY_INDEX = "DESTROY_INDEX";
  public static final String DESTROY_REGION = "DESTROY_REGION";
  public static final String EXECUTE_FUNCTION = "EXECUTE_FUNCTION";
  public static final String EXPORT_CONFIG = "EXPORT_CONFIG";
  public static final String EXPORT_DATA = "EXPORT_DATA";
  public static final String EXPORT_LOGS = "EXPORT_LOGS";
  public static final String EXPORT_OFFLINE_DISKSTORE = "EXPORT_OFFLINE_DISKSTORE";
  public static final String EXPORT_STACKTRACE = "EXPORT_STACKTRACE";
  public static final String GC = "GC";
  public static final String GET = "GET";
  public static final String IMPORT_CONFIG = "IMPORT_CONFIG";
  public static final String IMPORT_DATA = "IMPORT_DATA";
  public static final String LIST_DS = "LIST_DS";
  public static final String LOAD_BALANCE_GW_SENDER = "LOAD_BALANCE_GW_SENDER";
  public static final String LOCATE_ENTRY = "LOCATE_ENTRY";
  public static final String NETSTAT = "NETSTAT";
  public static final String PAUSE_GW_SENDER = "PAUSE_GW_SENDER";
  public static final String PUT = "PUT";
  public static final String QUERY = "QUERY";
  public static final String REBALANCE = "REBALANCE";
  public static final String REMOVE = "REMOVE";
  public static final String RENAME_PDX = "RENAME_PDX";
  public static final String RESUME_GW_SENDER = "RESUME_GW_SENDER";
  public static final String REVOKE_MISSING_DISKSTORE = "REVOKE_MISSING";
  public static final String SHOW_DEADLOCKS = "SHOW_DEADLOCKS";
  public static final String SHOW_LOG = "SHOW_LOG";
  public static final String SHOW_METRICS = "SHOW_METRICS";
  public static final String SHOW_MISSING_DISKSTORES = "SHOW_MISSING_DISKSTORES";
  public static final String SHOW_SUBSCRIPTION_QUEUE_SIZE = "SHOW_SUBSCRIPTION_QUEUE_SIZE";
  public static final String SHUTDOWN = "SHUTDOWN";
  public static final String STOP_GW_RECEIVER = "STOP_GW_RECEIVER";
  public static final String STOP_GW_SENDER = "STOP_GW_SENDER";
  public static final String UNDEPLOY = "UNDEPLOY";
  public static final String BACKUP_MEMBERS = "BACKUP_MEMBERS";
  public static final String ROLL_DISKSTORE = "ROLL_DISKSTORE";
  public static final String FORCE_COMPACTION = "FORCE_COMPACTION";
  public static final String FORCE_ROLL = "FORCE_ROLL";
  public static final String FLUSH_DISKSTORE = "FLUSH_DISKSTORE";
  public static final String START_GW_RECEIVER = "START_GW_RECEIVER";
  public static final String START_GW_SENDER = "START_GW_SENDER";
  public static final String BECOME_LOCK_GRANTOR = "BECOME_LOCK_GRANTOR";
  public static final String START_MANAGER = "START_MANAGER";
  public static final String STOP_MANAGER = "STOP_MANAGER";
  public static final String CREATE_MANAGER = "CREATE_MANAGER";
  public static final String STOP_CONTINUOUS_QUERY = "STOP_CONTINUOUS_QUERY";
  public static final String SET_DISK_USAGE = "SET_DISK_USAGE";

  
  public static final String CREATE_HDFS_STORE = "CREATE_HDFS_STORE";
  public static final String ALTER_HDFS_STORE = "ALTER_HDFS_STORE";
  public static final String DESTROY_HDFS_STORE = "DESTROY_HDFS_STORE";

  public static final String PULSE_DASHBOARD = "PULSE_DASHBOARD";
  public static final String PULSE_DATABROWSER = "PULSE_DATABROWSER";

  public static final String DATA_READ = "DATA_READ";
  public static final String DATA_WRITE = "DATA_WRITE";
  public static final String MONITOR = "MONITOR";
  public static final String ADMIN = "ADMIN";

  public static final String OBJECT_NAME_ACCESSCONTROL = "GemFire:service=AccessControl,type=Distributed";
  public static final String USER_NAME = "security-username";
  public static final String PASSWORD = "security-password";

  public static final String MBEAN_TYPE_DISTRIBUTED = "Distributed";
  public static final String MBEAN_TYPE_MEMBER = "Member";

  public static final String MBEAN_SERVICE_MANAGER = "Manager";
  public static final String MBEAN_SERVICE_CACHESERVER="CacheServer";
  public static final String MBEAN_SERVICE_REGION = "Region";
  public static final String MBEAN_SERVICE_LOCKSERVICE = "LockService";
  public static final String MBEAN_SERVICE_DISKSTORE = "DiskStore";
  public static final String MBEAN_SERVICE_GATEWAY_RECEIVER = "GatewayReceiver";
  public static final String MBEAN_SERVICE_GATEWAY_SENDER = "GatewaySender";
  public static final String MBEAN_SERVICE_ASYNCEVENTQUEUE = "AsyncEventQueue";
  public static final String MBEAN_SERVICE_LOCATOR = "Locator";
  public static final String MBEAN_SERVICE_SYSTEM = "System";

  public static final String MBEAN_KEY_SERVICE ="service";
  public static final String MBEAN_KEY_TYPE ="type";

  public static final String GETTER_IS= "is";
  public static final String GETTER_GET = "get";
  public static final String GETTER_FETCH = "fetch";
  public static final String GETTER_SHOW = "show";
  public static final String GETTER_HAS = "has";
  public static final String GETTER_VIEW = "view";
  public static final String GETTER_LIST = "list";
  public static final String GETTER_DESCRIBE = "describe";
  public static final String GETTER_STATUS = "status";

  public static final String MANAGEMENT_PACKAGE = "com.gemstone.gemfire.management";


}

/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal.data;

import java.util.logging.Level;

public class PulseConstants {

  public static final String APP_NAME = "PULSE";
  public static final String PULSE_LOG_FILE = APP_NAME + ".log";
  // public static final String PULSE_LOG_FILE_LOCATION =
  // System.getProperty("user.home");
  public static final String PULSE_PROPERTIES_FILE = "pulse.properties";
  public static final String PULSE_SECURITY_PROPERTIES_FILE = "pulsesecurity.properties";
  public static final String PULSE_NOTIFICATION_ALERT_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm'Z'";
  public static final String LOG_MESSAGE_DATE_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS z";

  public static final String LOG_MESSAGES_FILE = "LogMessages";
  public static final String APPLICATION_LANGUAGE = "en";
  public static final String APPLICATION_COUNTRY = "US";

  // Pulse version details properties from properties file
  public static final String PULSE_VERSION_PROPERTIES_FILE = "GemFireVersion.properties";
  public static final String PROPERTY_PULSE_VERSION = "Product-Version";
  public static final String PROPERTY_BUILD_ID = "Build-Id";
  public static final String PROPERTY_BUILD_DATE = "Build-Date";
  public static final String PROPERTY_SOURCE_DATE = "Source-Date";
  public static final String PROPERTY_SOURCE_REVISION = "Source-Revision";
  public static final String PROPERTY_SOURCE_REPOSITORY = "Source-Repository";

  // DEFAULT CONFIGUARTION VALUES FOR PULSE LOGGER
  // Log File
  public static final String PULSE_QUERY_HISTORY_FILE_NAME = APP_NAME
      + "_QueryHistory.json";
  // Log File location
  public static final String PULSE_QUERY_HISTORY_FILE_LOCATION = System
      .getProperty("user.home");
  // Date pattern to be used in log messages
  public static final String PULSE_QUERY_HISTORY_DATE_PATTERN = "EEE, MMM dd yyyy, HH:mm:ss z";

  // Decimal format pattern "###.##" and "0.0000"
  public static final String DECIMAL_FORMAT_PATTERN = "###.##";
  public static final String DECIMAL_FORMAT_PATTERN_2 = "0.0000";

  // DEFAULT VALUES
  public static final String GEMFIRE_DEFAULT_HOST = "localhost";
  public static final String GEMFIRE_DEFAULT_PORT = "1099";

  // DEFAULT CONFIGUARTION VALUES FOR PULSE LOGGER
  // Log File
  public static final String PULSE_LOG_FILE_NAME = APP_NAME;
  // Log File location
  public static final String PULSE_LOG_FILE_LOCATION = System
      .getProperty("user.home");
  // Date pattern to be used in log messages
  public static final String PULSE_LOG_MESSAGE_DATE_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS z";
  // Log file size - 1MB.
  public static final int PULSE_LOG_FILE_SIZE = 1024 * 1024;
  // The log file count - 4 files.
  public static final int PULSE_LOG_FILE_COUNT = 4;
  // Append logs - true.
  public static final boolean PULSE_LOG_APPEND = true;
  // Log level - FINE
  public static final Level PULSE_LOG_LEVEL = Level.FINE;

  // SYSTEM PROPERTIES
  public static final String SYSTEM_PROPERTY_PULSE_EMBEDDED = "pulse.embedded";
  public static final String SYSTEM_PROPERTY_PULSE_EMBEDDED_SQLF = "pulse.embedded.sqlf";
  public static final String SYSTEM_PROPERTY_PULSE_USELOCATOR = "pulse.useLocator";
  public static final String SYSTEM_PROPERTY_PULSE_HOST = "pulse.host";
  public static final String SYSTEM_PROPERTY_PULSE_PORT = "pulse.port";

  // APPLICATION PROPERTIES
  public static final String APPLICATION_PROPERTY_PULSE_USELOCATOR = "pulse.useLocator";
  public static final String APPLICATION_PROPERTY_PULSE_HOST = "pulse.host";
  public static final String APPLICATION_PROPERTY_PULSE_PORT = "pulse.port";
  public static final String APPLICATION_PROPERTY_PULSE_JMXUSERNAME = "pulse.jmxUserName";
  public static final String APPLICATION_PROPERTY_PULSE_JMXPASSWORD = "pulse.jmxUserPassword";
  public static final String APPLICATION_PROPERTY_PULSE_LOGFILENAME = "pulse.Log-File-Name";
  public static final String APPLICATION_PROPERTY_PULSE_LOGFILELOCATION = "pulse.Log-File-Location";
  public static final String APPLICATION_PROPERTY_PULSE_LOGFILESIZE = "pulse.Log-File-Size";
  public static final String APPLICATION_PROPERTY_PULSE_LOGFILECOUNT = "pulse.Log-File-Count";
  public static final String APPLICATION_PROPERTY_PULSE_LOGDATEPATTERN = "pulse.Log-Date-Pattern";
  public static final String APPLICATION_PROPERTY_PULSE_LOGLEVEL = "pulse.Log-Level";
  public static final String APPLICATION_PROPERTY_PULSE_LOGAPPEND = "pulse.Log-Append";
  public static final String APPLICATION_PROPERTY_PULSE_PRODUCTSUPPORT = "pulse.product";
  public static final String APPLICATION_PROPERTY_PULSE_SEC_PROFILE_GEMFIRE = "pulse.authentication.gemfire";
  public static final String APPLICATION_PROPERTY_PULSE_SEC_PROFILE_DEFAULT = "pulse.authentication.default";
  public static final String APPLICATION_PROPERTY_PULSE_SPRING_PROFILE_KEY = "spring.profiles.default";

  // STRING FLAGS
  public static final String STRING_FLAG_TRUE = "true";
  public static final String STRING_FLAG_FALSE = "false";
  public static final String DEFAULT_SERVER_GROUP = "Default";
  public static final String DEFAULT_REDUNDANCY_ZONE = "Default";
  public static final String JVM_PAUSES_TYPE_CLUSTER = "cluster";
  public static final String JVM_PAUSES_TYPE_MEMBER = "member";

  // CONSTANTS FOR MBEAN DATA
  public static final String OBJECT_DOMAIN_NAME_GEMFIRE = "GemFire";
  public static final String OBJECT_DOMAIN_NAME_SQLFIRE = "GemFireXD";
  public static final String OBJECT_NAME_MEMBER = OBJECT_DOMAIN_NAME_GEMFIRE + ":type=Member,*";
  public static final String OBJECT_NAME_MEMBER_MANAGER = OBJECT_DOMAIN_NAME_GEMFIRE + ":service=Manager,type=Member,*";
  public static final String OBJECT_NAME_SYSTEM_DISTRIBUTED = OBJECT_DOMAIN_NAME_GEMFIRE + ":service=System,type=Distributed";
  public static final String OBJECT_NAME_REGION_DISTRIBUTED = OBJECT_DOMAIN_NAME_GEMFIRE + ":service=Region,type=Distributed,*";
  public static final String OBJECT_NAME_STATEMENT_DISTRIBUTED = OBJECT_DOMAIN_NAME_SQLFIRE + ":service=Statement,type=Aggregate,*";
  public static final String OBJECT_NAME_SF_CLUSTER = OBJECT_DOMAIN_NAME_SQLFIRE + ":service=Cluster";
  public static final String OBJECT_NAME_SF_MEMBER_PATTERN = OBJECT_DOMAIN_NAME_SQLFIRE + ":group=*,type=Member,member=";
  public static final String OBJECT_NAME_TABLE_AGGREGATE = OBJECT_DOMAIN_NAME_SQLFIRE + ":service=Table,type=Aggregate,table=*";
  public static final String OBJECT_NAME_TABLE_AGGREGATE_PATTERN = OBJECT_DOMAIN_NAME_SQLFIRE + ":service=Table,type=Aggregate,table=";
  public static final String OBJECT_NAME_REGION_ON_MEMBER_REGION = OBJECT_DOMAIN_NAME_GEMFIRE + ":service=Region,name=";
  public static final String OBJECT_NAME_REGION_ON_MEMBER_MEMBER = ",type=Member,member=";
  public static final String OBJECT_NAME_ACCESSCONTROL_MBEAN = "GemFire:service=AccessControl,type=Distributed";

  public static final String MBEAN_KEY_PROPERTY_SERVICE = "service";
  public static final String MBEAN_KEY_PROPERTY_SERVICE_VALUE_REGION = "Region";
  public static final String MBEAN_KEY_PROPERTY_SERVICE_VALUE_CACHESERVER = "CacheServer";
  public static final String MBEAN_KEY_PROPERTY_SERVICE_VALUE_GATEWAYRECEIVER = "GatewayReceiver";
  public static final String MBEAN_KEY_PROPERTY_SERVICE_VALUE_GATEWAYSENDER = "GatewaySender";
  public static final String MBEAN_KEY_PROPERTY_SERVICE_VALUE_ASYNCEVENTQUEUE = "AsyncEventQueue";
  public static final String MBEAN_KEY_PROPERTY_SERVICE_VALUE_LOCATOR = "Locator";
  public static final String MBEAN_KEY_PROPERTY_REGION_NAME = "name";

  public static final String MBEAN_KEY_PROPERTY_MEMBER = "member";

  public static final String MBEAN_ATTRIBUTE_MEMBER = "Member";
  public static final String MBEAN_ATTRIBUTE_MEMBERS = "Members";
  public static final String MBEAN_ATTRIBUTE_MANAGER = "Manager";
  public static final String MBEAN_ATTRIBUTE_LOCATOR = "Locator";
  public static final String MBEAN_ATTRIBUTE_SERVER = "Server";
  public static final String MBEAN_ATTRIBUTE_SERVERGROUPS = "Groups";
  public static final String MBEAN_ATTRIBUTE_REDUNDANCYZONES = "RedundancyZone";
  public static final String MBEAN_ATTRIBUTE_DATASTORE = "DataStore";
  public static final String MBEAN_ATTRIBUTE_ID = "Id";

  public static final String MBEAN_ATTRIBUTE_GEMFIREVERSION = "Version";
  public static final String MBEAN_ATTRIBUTE_MEMBERCOUNT = "MemberCount";
  public static final String MBEAN_ATTRIBUTE_NUMCLIENTS = "NumClients";
  public static final String MBEAN_ATTRIBUTE_NETWORKSERVERCLIENTCONNECTIONSTATS = "NetworkServerClientConnectionStats";
  public static final String MBEAN_ATTRIBUTE_DISTRIBUTEDSYSTEMID = "DistributedSystemId";
  public static final String MBEAN_ATTRIBUTE_LOCATORCOUNT = "LocatorCount";
  public static final String MBEAN_ATTRIBUTE_TOTALREGIONCOUNT = "TotalRegionCount";
  public static final String MBEAN_ATTRIBUTE_NUMRUNNIGFUNCTION = "NumRunningFunctions";
  public static final String MBEAN_ATTRIBUTE_PROCEDURECALLSCOMPLETED = "ProcedureCallsCompleted";
  public static final String MBEAN_ATTRIBUTE_PROCEDURECALLSINPROGRESS = "ProcedureCallsInProgress";
  public static final String MBEAN_ATTRIBUTE_REGISTEREDCQCOUNT = "RegisteredCQCount";
  public static final String MBEAN_ATTRIBUTE_NUMSUBSCRIPTIONS = "NumSubscriptions";
  public static final String MBEAN_ATTRIBUTE_NUMTXNCOMMITTED = "TransactionCommitted";
  public static final String MBEAN_ATTRIBUTE_NUMTXNROLLBACK = "TransactionRolledBack";
  public static final String MBEAN_ATTRIBUTE_TOTALHEAPSIZE = "TotalHeapSize";
  public static final String MBEAN_ATTRIBUTE_USEDHEAPSIZE = "UsedHeapSize";
  public static final String MBEAN_ATTRIBUTE_OFFHEAPFREESIZE = "OffHeapFreeSize";
  public static final String MBEAN_ATTRIBUTE_OFFHEAPUSEDSIZE = "OffHeapUsedSize";
  public static final String MBEAN_ATTRIBUTE_TOTALREGIONENTRYCOUNT = "TotalRegionEntryCount";
  public static final String MBEAN_ATTRIBUTE_CURRENTENTRYCOUNT = "CurrentQueryCount";
  public static final String MBEAN_ATTRIBUTE_TOTALDISKUSAGE = "TotalDiskUsage";
  public static final String MBEAN_ATTRIBUTE_DISKWRITESRATE = "DiskWritesRate";
  public static final String MBEAN_ATTRIBUTE_AVERAGEWRITES = "AverageWrites";
  public static final String MBEAN_ATTRIBUTE_DISKREADSRATE = "DiskReadsRate";
  public static final String MBEAN_ATTRIBUTE_AVERAGEREADS = "AverageReads";
  public static final String MBEAN_ATTRIBUTE_QUERYREQUESTRATE = "QueryRequestRate";
  public static final String MBEAN_ATTRIBUTE_JVMPAUSES = "JVMPauses";
  public static final String MBEAN_ATTRIBUTE_HOST = "Host";
  public static final String MBEAN_ATTRIBUTE_HOSTNAMEFORCLIENTS = "HostnameForClients";
  public static final String MBEAN_ATTRIBUTE_HOSTNAMEFORCLIENTS_ALT = "HostNameForClients";
  public static final String MBEAN_ATTRIBUTE_BINDADDRESS = "BindAddress";
  public static final String MBEAN_ATTRIBUTE_PORT = "Port";
  public static final String MBEAN_ATTRIBUTE_EVENTRECEIVEDDATE = "EventsReceivedRate";
  public static final String MBEAN_ATTRIBUTE_AVEARGEBATCHPROCESSINGTIME = "AverageBatchProcessingTime";
  public static final String MBEAN_ATTRIBUTE_RUNNING = "Running";
  public static final String MBEAN_ATTRIBUTE_BATCHSIZE = "BatchSize";
  public static final String MBEAN_ATTRIBUTE_SENDERID = "SenderId";
  public static final String MBEAN_ATTRIBUTE_EVENTQUEUESIZE = "EventQueueSize";
  public static final String MBEAN_ATTRIBUTE_PRIMARY = "Primary";
  public static final String MBEAN_ATTRIBUTE_PERSISTENCEENABLED = "PersistenceEnabled";
  public static final String MBEAN_ATTRIBUTE_PARALLEL = "Parallel";
  public static final String MBEAN_ATTRIBUTE_REMOTE_DS_ID = "RemoteDSId";
  public static final String MBEAN_ATTRIBUTE_EVENTS_EXCEEDING_ALERT_THRESHOLD = "EventsExceedingAlertThreshold";
  public static final String MBEAN_ATTRIBUTE_FULLPATH = "FullPath";
  public static final String MBEAN_ATTRIBUTE_EMPTYNODES = "EmptyNodes";
  public static final String MBEAN_ATTRIBUTE_GETSRATE = "GetsRate";
  public static final String MBEAN_ATTRIBUTE_PUTSRATE = "PutsRate";
  public static final String MBEAN_ATTRIBUTE_LRUEVICTIONRATE = "LruEvictionRate";
  public static final String MBEAN_ATTRIBUTE_REGIONTYPE = "RegionType";
  public static final String MBEAN_ATTRIBUTE_ENTRYSIZE = "EntrySize";
  public static final String MBEAN_ATTRIBUTE_SYSTEMREGIONENTRYCOUNT = "SystemRegionEntryCount";
  public static final String MBEAN_ATTRIBUTE_PERSISTENTENABLED = "PersistentEnabled";
  public static final String MBEAN_ATTRIBUTE_NAME = "Name";
  public static final String MBEAN_ATTRIBUTE_GATEWAYENABLED = "GatewayEnabled";
  public static final String MBEAN_ATTRIBUTE_DISKUSAGE = "DiskUsage";
  public static final String MBEAN_ATTRIBUTE_TOTALFILEDESCRIPTOROPEN = "TotalFileDescriptorOpen";
  public static final String MBEAN_ATTRIBUTE_LOADAVERAGE = "LoadAverage";
  public static final String MBEAN_ATTRIBUTE_CURRENTHEAPSIZE = "CurrentHeapSize"; // deprecated in Gemfire8.1
  public static final String MBEAN_ATTRIBUTE_USEDMEMORY = "UsedMemory";
  public static final String MBEAN_ATTRIBUTE_MAXIMUMHEAPSIZE = "MaximumHeapSize"; // deprecated in Gemfire8.1
  public static final String MBEAN_ATTRIBUTE_MAXMEMORY = "MaxMemory";
  public static final String MBEAN_ATTRIBUTE_NUMTHREADS = "NumThreads";
  public static final String MBEAN_ATTRIBUTE_MEMBERUPTIME = "MemberUpTime";
  public static final String MBEAN_ATTRIBUTE_TOTALBYTESONDISK = "TotalBytesOnDisk";
  public static final String MBEAN_ATTRIBUTE_CPUUSAGE = "CpuUsage";
  public static final String MBEAN_ATTRIBUTE_HOSTCPUUSAGE = "HostCpuUsage";
  public static final String MBEAN_ATTRIBUTE_ENTRYCOUNT = "EntryCount";
  public static final String MBEAN_ATTRIBUTE_NUMBEROFROWS = "NumberOfRows";
  public static final String MBEAN_ATTRIBUTE_LOCALMAXMEMORY = "LocalMaxMemory";

  public static final String MBEAN_ATTRIBUTE_NUMTIMESCOMPILED = "NumTimesCompiled";
  public static final String MBEAN_ATTRIBUTE_NUMEXECUTION = "NumExecution";
  public static final String MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS = "NumExecutionsInProgress";
  public static final String MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP = "NumTimesGlobalIndexLookup";
  public static final String MBEAN_ATTRIBUTE_NUMROWSMODIFIED = "NumRowsModified";
  public static final String MBEAN_ATTRIBUTE_PARSETIME = "ParseTime";
  public static final String MBEAN_ATTRIBUTE_BINDTIME = "BindTime";
  public static final String MBEAN_ATTRIBUTE_OPTIMIZETIME = "OptimizeTime";
  public static final String MBEAN_ATTRIBUTE_ROUTINGINFOTIME = "RoutingInfoTime";
  public static final String MBEAN_ATTRIBUTE_GENERATETIME = "GenerateTime";
  public static final String MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME = "TotalCompilationTime";
  public static final String MBEAN_ATTRIBUTE_EXECUTIONTIME = "ExecutionTime";
  public static final String MBEAN_ATTRIBUTE_PROJECTIONTIME = "ProjectionTime";
  public static final String MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME = "TotalExecutionTime";
  public static final String MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME = "RowsModificationTime";
  public static final String MBEAN_ATTRIBUTE_QNNUMROWSSEEN = "QNNumRowsSeen";
  public static final String MBEAN_ATTRIBUTE_QNMSGSENDTIME = "QNMsgSendTime";
  public static final String MBEAN_ATTRIBUTE_QNMSGSERTIME = "QNMsgSerTime";
  public static final String MBEAN_ATTRIBUTE_QNRESPDESERTIME = "QNRespDeSerTime";
  public static final String MBEAN_ATTRIBUTE_QUERYDEFINITION = "Query";

  public static final String MBEAN_ATTRIBUTE_AEQ_ASYNCEVENTID = "Id";
  public static final String MBEAN_ATTRIBUTE_AEQ_PRIMARY = "Primary";
  public static final String MBEAN_ATTRIBUTE_AEQ_PARALLEL = "Parallel";
  public static final String MBEAN_ATTRIBUTE_AEQ_BATCH_SIZE = "BatchSize";
  public static final String MBEAN_ATTRIBUTE_AEQ_BATCH_TIME_INTERVAL = "BatchTimeInterval";
  public static final String MBEAN_ATTRIBUTE_AEQ_BATCH_CONFLATION_ENABLED = "BatchConflationEnabled";
  public static final String MBEAN_ATTRIBUTE_AEQ_ASYNC_EVENT_LISTENER = "AsyncEventListener";
  public static final String MBEAN_ATTRIBUTE_AEQ_EVENT_QUEUE_SIZE = "EventQueueSize";

  // column names
  public static final String MBEAN_COLNAME_NUMTIMESCOMPILED = "NumTimesCompiled";
  public static final String MBEAN_COLNAME_NUMEXECUTION = "NumExecution";
  public static final String MBEAN_COLNAME_NUMEXECUTIONSINPROGRESS = "NumExecutionsInProgress";
  public static final String MBEAN_COLNAME_NUMTIMESGLOBALINDEXLOOKUP = "NumTimesGlobalIndexLookup";
  public static final String MBEAN_COLNAME_NUMROWSMODIFIED = "NumRowsModified";
  public static final String MBEAN_COLNAME_PARSETIME = "ParseTime(ms)";
  public static final String MBEAN_COLNAME_BINDTIME = "BindTime(ms)";
  public static final String MBEAN_COLNAME_OPTIMIZETIME = "OptimizeTime(ms)";
  public static final String MBEAN_COLNAME_ROUTINGINFOTIME = "RoutingInfoTime(ms)";
  public static final String MBEAN_COLNAME_GENERATETIME = "GenerateTime(ms)";
  public static final String MBEAN_COLNAME_TOTALCOMPILATIONTIME = "TotalCompilationTime(ms)";
  public static final String MBEAN_COLNAME_EXECUTIONTIME = "ExecutionTime(ns)";
  public static final String MBEAN_COLNAME_PROJECTIONTIME = "ProjectionTime(ns)";
  public static final String MBEAN_COLNAME_TOTALEXECUTIONTIME = "TotalExecutionTime(ns)";
  public static final String MBEAN_COLNAME_ROWSMODIFICATIONTIME = "RowsModificationTime(ns)";
  public static final String MBEAN_COLNAME_QNNUMROWSSEEN = "QNNumRowsSeen";
  public static final String MBEAN_COLNAME_QNMSGSENDTIME = "QNMsgSendTime(ns)";
  public static final String MBEAN_COLNAME_QNMSGSERTIME = "QNMsgSerTime(ns)";
  public static final String MBEAN_COLNAME_QNRESPDESERTIME = "QNRespDeSerTime(ns)";
  public static final String MBEAN_COLNAME_QUERYDEFINITION = "Query";

  // TODO : add attributes for aggregate statistics
  // public static final String MBEAN_ATTRIBUTE_ENTRYCOUNT = "EntryCount";

  public static final String MBEAN_MANAGER_ATTRIBUTE_PULSEURL = "PulseURL";

  public static final String MBEAN_OPERATION_LISTCACHESERVER = "listCacheServers";
  public static final String MBEAN_OPERATION_LISTSERVERS = "listServers";
  public static final String MBEAN_OPERATION_VIEWREMOTECLUSTERSTATUS = "viewRemoteClusterStatus";
  public static final String MBEAN_OPERATION_SHOWALLCLIENTS = "showAllClientStats";
  public static final String MBEAN_OPERATION_LISTREGIONATTRIBUTES = "listRegionAttributes";
  public static final String MBEAN_OPERATION_QUERYDATABROWSER = "queryData";

  // COMPOSITE DATA KEYS
  public static final String COMPOSITE_DATA_KEY_CLIENTID = "clientId";
  public static final String COMPOSITE_DATA_KEY_NAME = "name";
  public static final String COMPOSITE_DATA_KEY_HOSTNAME = "hostName";
  public static final String COMPOSITE_DATA_KEY_QUEUESIZE = "queueSize";
  public static final String COMPOSITE_DATA_KEY_PROCESSCPUTIME = "processCpuTime";
  public static final String COMPOSITE_DATA_KEY_UPTIME = "upTime";
  public static final String COMPOSITE_DATA_KEY_NUMOFTHREADS = "numOfThreads";
  public static final String COMPOSITE_DATA_KEY_NUMOFGETS = "numOfGets";
  public static final String COMPOSITE_DATA_KEY_NUMOFPUTS = "numOfPuts";
  public static final String COMPOSITE_DATA_KEY_CPUS = "cpus";
  public static final String COMPOSITE_DATA_KEY_CLIENTCQCOUNT = "clientCQCount"; 
  public static final String COMPOSITE_DATA_KEY_SUBSCRIPTIONENABLED = "subscriptionEnabled"; 
  public static final String COMPOSITE_DATA_KEY_SCOPE = "scope";
  public static final String COMPOSITE_DATA_KEY_DISKSTORENAME = "diskStoreName";
  public static final String COMPOSITE_DATA_KEY_DISKSYNCHRONOUS = "diskSynchronous";
  public static final String COMPOSITE_DATA_KEY_COMPRESSIONCODEC = "compressionCodec";
  public static final String COMPOSITE_DATA_KEY_ENABLEOFFHEAPMEMORY = "enableOffHeapMemory";
  public static final String COMPOSITE_DATA_KEY_CONNECTIONSACTIVE = "connectionsActive";
  public static final String COMPOSITE_DATA_KEY_CONNECTED = "connected";

  public static final String ALERT_DESC_SEVERE = "Severe Alert! The cluster is on fire !";
  public static final String ALERT_DESC_ERROR = "Error Alert! There is a problem with your cluster ! Better fix it !";
  public static final String ALERT_DESC_WARNING = "Warning Alert! Look at this cluster after you finish your coffee !";
  public static final String ALERT_DESC_INFO = "Info Alert! For your kind information !";

  public static final String NOTIFICATION_TYPE_SYSTEM_ALERT = "system.alert";
  public static final String NOTIFICATION_TYPE_CACHE_MEMBER_DEPARTED = "gemfire.distributedsystem.cache.member.departed";
  public static final String NOTIFICATION_TYPE_REGION_DESTROYED = "gemfire.distributedsystem.cache.region.closed";

  public static final String PRODUCT_NAME_GEMFIRE = "gemfire"; // For GemFire
  public static final String PRODUCT_NAME_SQLFIRE = "gemfirexd"; // For SQLFire

  //Following attributes are not present in 9.0
  //"Members"
  //"EmptyNodes"
  //"SystemRegionEntryCount"
  //"MemberCount"
  public static final String[] REGION_MBEAN_ATTRIBUTES = {
      MBEAN_ATTRIBUTE_MEMBERS, MBEAN_ATTRIBUTE_FULLPATH,
      MBEAN_ATTRIBUTE_DISKREADSRATE, MBEAN_ATTRIBUTE_DISKWRITESRATE,
      MBEAN_ATTRIBUTE_EMPTYNODES, MBEAN_ATTRIBUTE_GETSRATE,
      MBEAN_ATTRIBUTE_LRUEVICTIONRATE, MBEAN_ATTRIBUTE_PUTSRATE,
      MBEAN_ATTRIBUTE_REGIONTYPE, MBEAN_ATTRIBUTE_ENTRYSIZE,
      MBEAN_ATTRIBUTE_ENTRYCOUNT, MBEAN_ATTRIBUTE_SYSTEMREGIONENTRYCOUNT,
      MBEAN_ATTRIBUTE_MEMBERCOUNT, MBEAN_ATTRIBUTE_PERSISTENTENABLED,
      MBEAN_ATTRIBUTE_NAME, MBEAN_ATTRIBUTE_GATEWAYENABLED,
      MBEAN_ATTRIBUTE_DISKUSAGE, MBEAN_ATTRIBUTE_LOCALMAXMEMORY };

  public static final String[] CLUSTER_MBEAN_ATTRIBUTES = {
      MBEAN_ATTRIBUTE_MEMBERCOUNT, MBEAN_ATTRIBUTE_NUMCLIENTS,
      MBEAN_ATTRIBUTE_DISTRIBUTEDSYSTEMID, MBEAN_ATTRIBUTE_LOCATORCOUNT,
      MBEAN_ATTRIBUTE_TOTALREGIONCOUNT, MBEAN_ATTRIBUTE_NUMRUNNIGFUNCTION,
      MBEAN_ATTRIBUTE_REGISTEREDCQCOUNT, MBEAN_ATTRIBUTE_NUMSUBSCRIPTIONS,
      MBEAN_ATTRIBUTE_NUMTXNCOMMITTED, MBEAN_ATTRIBUTE_NUMTXNROLLBACK,
      MBEAN_ATTRIBUTE_TOTALHEAPSIZE, MBEAN_ATTRIBUTE_USEDHEAPSIZE,
      MBEAN_ATTRIBUTE_TOTALREGIONENTRYCOUNT, MBEAN_ATTRIBUTE_CURRENTENTRYCOUNT,
      MBEAN_ATTRIBUTE_TOTALDISKUSAGE, MBEAN_ATTRIBUTE_DISKWRITESRATE,
      MBEAN_ATTRIBUTE_AVERAGEWRITES, MBEAN_ATTRIBUTE_AVERAGEREADS,
      MBEAN_ATTRIBUTE_QUERYREQUESTRATE, MBEAN_ATTRIBUTE_DISKREADSRATE,
      MBEAN_ATTRIBUTE_JVMPAUSES };

  public static final String[] GATEWAY_MBEAN_ATTRIBUTES = {
      MBEAN_ATTRIBUTE_PORT, MBEAN_ATTRIBUTE_EVENTRECEIVEDDATE,
      MBEAN_ATTRIBUTE_AVEARGEBATCHPROCESSINGTIME, MBEAN_ATTRIBUTE_RUNNING };

  public static final String[] GATEWAYSENDER_MBEAN_ATTRIBUTES = {
      MBEAN_ATTRIBUTE_EVENTRECEIVEDDATE, MBEAN_ATTRIBUTE_BATCHSIZE,
      MBEAN_ATTRIBUTE_SENDERID, MBEAN_ATTRIBUTE_EVENTQUEUESIZE,
      MBEAN_ATTRIBUTE_RUNNING, MBEAN_ATTRIBUTE_PRIMARY,
      MBEAN_ATTRIBUTE_PERSISTENCEENABLED, MBEAN_ATTRIBUTE_PARALLEL,
      MBEAN_ATTRIBUTE_REMOTE_DS_ID, MBEAN_ATTRIBUTE_EVENTS_EXCEEDING_ALERT_THRESHOLD};

  public static final String[] ASYNC_EVENT_QUEUE_MBEAN_ATTRIBUTES = {
    MBEAN_ATTRIBUTE_AEQ_ASYNCEVENTID, MBEAN_ATTRIBUTE_AEQ_PRIMARY,
    MBEAN_ATTRIBUTE_AEQ_PARALLEL, MBEAN_ATTRIBUTE_AEQ_BATCH_SIZE,
    MBEAN_ATTRIBUTE_AEQ_BATCH_TIME_INTERVAL, MBEAN_ATTRIBUTE_AEQ_BATCH_CONFLATION_ENABLED,
    MBEAN_ATTRIBUTE_AEQ_ASYNC_EVENT_LISTENER, MBEAN_ATTRIBUTE_AEQ_EVENT_QUEUE_SIZE};

  public static final String[] MEMBER_MBEAN_ATTRIBUTES = {
      MBEAN_ATTRIBUTE_MANAGER, MBEAN_ATTRIBUTE_TOTALREGIONCOUNT,
      MBEAN_ATTRIBUTE_LOCATOR, MBEAN_ATTRIBUTE_TOTALDISKUSAGE,
      MBEAN_ATTRIBUTE_SERVER, MBEAN_ATTRIBUTE_TOTALFILEDESCRIPTOROPEN,
      MBEAN_ATTRIBUTE_LOADAVERAGE, MBEAN_ATTRIBUTE_DISKWRITESRATE,
      MBEAN_ATTRIBUTE_DISKREADSRATE, MBEAN_ATTRIBUTE_JVMPAUSES,
      MBEAN_ATTRIBUTE_USEDMEMORY, MBEAN_ATTRIBUTE_MAXMEMORY,
      MBEAN_ATTRIBUTE_NUMTHREADS, MBEAN_ATTRIBUTE_MEMBERUPTIME,
      MBEAN_ATTRIBUTE_HOST, MBEAN_ATTRIBUTE_HOSTNAMEFORCLIENTS,
      MBEAN_ATTRIBUTE_BINDADDRESS, MBEAN_ATTRIBUTE_TOTALBYTESONDISK,
      MBEAN_ATTRIBUTE_CPUUSAGE, MBEAN_ATTRIBUTE_HOSTCPUUSAGE,
      MBEAN_ATTRIBUTE_MEMBER, MBEAN_ATTRIBUTE_ID, MBEAN_ATTRIBUTE_AVERAGEREADS,
      MBEAN_ATTRIBUTE_AVERAGEWRITES, MBEAN_ATTRIBUTE_OFFHEAPFREESIZE,
      MBEAN_ATTRIBUTE_OFFHEAPUSEDSIZE, MBEAN_ATTRIBUTE_SERVERGROUPS,
      MBEAN_ATTRIBUTE_REDUNDANCYZONES, MBEAN_ATTRIBUTE_GEMFIREVERSION };

  public static final String[] STATEMENT_MBEAN_ATTRIBUTES = {
      MBEAN_ATTRIBUTE_NAME, MBEAN_ATTRIBUTE_NUMTIMESCOMPILED,
      MBEAN_ATTRIBUTE_NUMEXECUTION, MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS,
      MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP,
      MBEAN_ATTRIBUTE_NUMROWSMODIFIED, MBEAN_ATTRIBUTE_PARSETIME,
      MBEAN_ATTRIBUTE_BINDTIME, MBEAN_ATTRIBUTE_OPTIMIZETIME,
      MBEAN_ATTRIBUTE_ROUTINGINFOTIME, MBEAN_ATTRIBUTE_GENERATETIME,
      MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME, MBEAN_ATTRIBUTE_EXECUTIONTIME,
      MBEAN_ATTRIBUTE_PROJECTIONTIME, MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME,
      MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME, MBEAN_ATTRIBUTE_QNNUMROWSSEEN,
      MBEAN_ATTRIBUTE_QNMSGSENDTIME, MBEAN_ATTRIBUTE_QNMSGSERTIME };

  public static final String[] REGION_ON_MEMBER_MBEAN_ATTRIBUTES = {
    MBEAN_ATTRIBUTE_ENTRYSIZE,
    MBEAN_ATTRIBUTE_ENTRYCOUNT,
    MBEAN_ATTRIBUTE_PUTSRATE,
    MBEAN_ATTRIBUTE_GETSRATE,
    MBEAN_ATTRIBUTE_DISKREADSRATE,
    MBEAN_ATTRIBUTE_DISKWRITESRATE,
    MBEAN_ATTRIBUTE_LOCALMAXMEMORY
    };

  public static final String[] SF_CLUSTER_MBEAN_ATTRIBUTES = {
      MBEAN_ATTRIBUTE_PROCEDURECALLSINPROGRESS,
      MBEAN_ATTRIBUTE_NETWORKSERVERCLIENTCONNECTIONSTATS };

  public static final String[] SF_MEMBER_MBEAN_ATTRIBUTES = {
    MBEAN_ATTRIBUTE_DATASTORE,
    MBEAN_ATTRIBUTE_NETWORKSERVERCLIENTCONNECTIONSTATS };

  public static final String[] SF_TABLE_MBEAN_ATTRIBUTES = {
      MBEAN_ATTRIBUTE_ENTRYSIZE, MBEAN_ATTRIBUTE_NUMBEROFROWS };
  
  public static final String PULSE_ROLES[] = {
    "CLUSTER:READ",
    "DATA:READ"
  };

  // SSL Related attributes

  public static final String SSL_KEYSTORE = "javax.net.ssl.keyStore";
  public static final String SSL_KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
  public static final String SSL_TRUSTSTORE = "javax.net.ssl.trustStore";
  public static final String SSL_TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  public static final String SSL_ENABLED_CIPHERS = "javax.rmi.ssl.client.enabledCipherSuites";
  public static final String SSL_ENABLED_PROTOCOLS = "javax.rmi.ssl.client.enabledProtocols";

  public static final String SYSTEM_PROPERTY_PULSE_USESSL_LOCATOR = "pulse.useSSL.locator";
  public static final String SYSTEM_PROPERTY_PULSE_USESSL_MANAGER = "pulse.useSSL.manager";

  public static final String REQUEST_PARAM_REGION_FULL_PATH = "regionFullPath";

}

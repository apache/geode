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
package org.apache.geode.management.internal.cli.i18n;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.cli.shell.Gfsh;

import java.text.MessageFormat;

import static org.apache.geode.distributed.ConfigurationProperties.*;

/**-
 *  * Contains 'String' constants used as key to the Localized strings to be used
 * in classes under <code>org.apache.geode.management.internal.cli</code>
 * for Command Line Interface (CLI).
 * NOTES:
 * 1. CONVENTIONS: Defining constants for Command Name, option, argument, help:
 * 1.1 Command Name:
 *     Command name in BOLD. Multiple words separated by single underscore ('_')
 *     E.g. COMPACT_DISK_STORE
 * 1.2 Command Help Text:
 *     Command name in BOLD followed by double underscore ('__') followed by HELP
 *     in BOLD.
 *     E.g.COMPACT_DISK_STORE__HELP
 * 1.3 Command Option:
 *     Command name in BOLD followed by double underscore ('__') followed by option
 *     name in BOLD - multiple words concatenated by removing space.
 *     E.g. for option "--disk-dirs" - COMPACT_DISK_STORE__DISKDIRS
 * 1.4 Command Option Help:
 *     As mentioned in 1.3, followed by double underscore ('__') followed by HELP
 *     in BOLD.
 *     E.g. COMPACT_DISK_STORE__DISKDIRS__HELP
 * 1.5 Info/Error Message used in a command:
 *     Command name in BOLD followed by double underscore ('__') followed by MSG
 *     in BOLD followed by brief name for the message (Similar to used in LocalizedStrings).
 *     E.g. COMPACT_DISK_STORE__MSG__ERROR_WHILE_COMPACTING = "Error occurred while compacting disk store."
 * 1.6 Parameterized messages are supposed to be handled by users. It's
 *     recommended to use the same conventions as used in LocalizedStrings.
 *     E.g. COMPACT_DISK_STORE__MSG__ERROR_WHILE_COMPACTING__0 = "Error occurred while compacting disk store {0}."
 *
 * 2. Defining Topic constants:
 * 2.1 The constants' names should begin with "TOPIC_"
 *     E.g. TOPIC_GEODE_REGION
 * 2.2 Topic brief description should be defined with suffix "__DESC".
 *     E.g. TOPIC_GEODE_REGION__DESC
 *
 * 3. Order for adding constants: It should be alphabetically sorted at least
 *    on the first name within the current group
 *
 * @since GemFire 7.0
 */
public class CliStrings {

  /*-*************************************************************************
   *************                  T O P I C S                  ***************
   ***************************************************************************/
  public static final String DEFAULT_TOPIC_GEODE = "Geode";
  public static final String DEFAULT_TOPIC_GEODE__DESC = "Apache Geode is a distributed data management platform providing dynamic scalability, high performance and database-like persistence.";
  public static final String TOPIC_GEODE_REGION = "Region";
  public static final String TOPIC_GEODE_REGION__DESC = "A region is the core building block of the Apache Geode distributed system. Cached data is organized into regions and all data puts, gets, and querying activities are done against them.";
  public static final String TOPIC_GEODE_WAN = "WAN";
  public static final String TOPIC_GEODE_WAN__DESC = "For multiple data centers in remote locations, Geode provides a WAN gateway to facilitate data sharing. The WAN gateway connects two or more remote sites and then sends asynchronous, batched updates as data is changed.";
  public static final String TOPIC_GEODE_JMX = "JMX";
  public static final String TOPIC_GEODE_JMX__DESC = "JMX technology provides the tools for building distributed, Web-based, modular and dynamic solutions for managing and monitoring devices, applications, and service-driven networks.";
  public static final String TOPIC_GEODE_DISKSTORE = "Disk Store";
  public static final String TOPIC_GEODE_DISKSTORE__DESC = "Disk stores are used to persist data to disk as a backup to your in-memory copy or as overflow storage when memory use is too high.";
  public static final String TOPIC_GEODE_LOCATOR = "Locator";
  public static final String TOPIC_GEODE_LOCATOR__DESC = "JVMs running Geode discover each other through a TCP service named the locator.";
  public static final String TOPIC_GEODE_SERVER = "Server";
  public static final String TOPIC_GEODE_SERVER__DESC = "A server is Geode cluster member which holds a Geode cache. Depending on the topology used it can refer to either a system that responds to client requests or a system that is only a peer to other members.";
  public static final String TOPIC_GEODE_MANAGER = "Manager";
  public static final String TOPIC_GEODE_MANAGER__DESC = "The Manager is a member which has the additional role of a managing & monitoring the Geode distributed system.";
  public static final String TOPIC_GEODE_STATISTICS = "Statistics";
  public static final String TOPIC_GEODE_STATISTICS__DESC = "iEvery application and server in a Apache Geode distributed system can be configured to perform statistical data collection for analysis.";
  public static final String TOPIC_GEODE_LIFECYCLE = "Lifecycle";
  public static final String TOPIC_GEODE_LIFECYCLE__DESC = "Launching, execution and termination of Geode cluster members such as servers and locators.";
  public static final String TOPIC_GEODE_M_AND_M = "Management-Monitoring";
  public static final String TOPIC_GEODE_M_AND_M__DESC = "The management of and monitoring of Geode systems using Geode tools, such as Apache Geode Pulse or Data Browser, and JConsole, which is provided with the JDK(TM)";
  public static final String TOPIC_GEODE_DATA = "Data";
  public static final String TOPIC_GEODE_DATA__DESC = "User data as stored in regions of the Geode distributed system.";
  public static final String TOPIC_GEODE_CONFIG = "Configuration";
  public static final String TOPIC_GEODE_CONFIG__DESC = "Configuration of Apache Geode Cache & Servers/Locators hosting the Cache.";
  public static final String TOPIC_GEODE_FUNCTION = "Function Execution";
  public static final String TOPIC_GEODE_FUNCTION__DESC = "The function execution service provides solutions for these application use cases: \n\tAn application that executes a server-side transaction or carries out data updates using the Geode distributed locking service. \n\tAn application that needs to initialize some of its components once on each server, which might be used later by executed functions. Initialization and startup of a third-party service, such as a messaging service. \n\tAny arbitrary aggregation operation that requires iteration over local data sets that can be done more efficiently through a single call to the cache server. \n\tAny kind of external resource provisioning that can be done by executing a function on a server.";
  public static final String TOPIC_GEODE_HELP = "Help";
  public static final String TOPIC_GEODE_HELP__DESC = "Provides usage information for gfsh & its commands.";
  public static final String TOPIC_GEODE_DEBUG_UTIL = "Debug-Utility";
  public static final String TOPIC_GEODE_DEBUG_UTIL__DESC = "Debugging aids & utilities to use with Apache Geode.";
  public static final String TOPIC_GFSH = "GFSH";
  public static final String TOPIC_GFSH__DESC = "The Geode Shell";
  public static final String TOPIC_SHARED_CONFIGURATION = "Cluster Configuration";
  public static final String TOPIC_SHARED_CONFIGURATION_HELP = "Configuration for cluster and various groups. It consists of cache.xml, geode properties and deployed jars.\nChanges due to gfshs command are persisted to the locator hosting the cluster configuration service.";
  public static final String TOPIC_CHANGELOGLEVEL = "User can change the log-level for a  member run time and generate log contents as per the need";

  /*-*************************************************************************
   * ********* String Constants other than command name, options & help ******
   * *************************************************************************/

  public static final String DESKSTOP_APP_RUN_ERROR_MESSAGE = "Running desktop applications is not supported on %1$s.";
  public static final String GEODE_HOME_NOT_FOUND_ERROR_MESSAGE = "The GEODE environment variable was not defined.  Please set the GEODE environment variable to the directory where GEODE is installed.";
  public static final String JAVA_HOME_NOT_FOUND_ERROR_MESSAGE = "Unable to locate the Java executables and dependencies.  Please set the JAVA_HOME environment variable.";
  public static final String CACHE_XML_NOT_FOUND_MESSAGE = "Warning: The Geode cache XML file {0} could not be found.";
  public static final String GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE = "Warning: The Geode {0}properties file {1} could not be found.";
  public static final String MEMBER_NOT_FOUND_ERROR_MESSAGE = "Member {0} could not be found.  Please verify the member name or ID and try again.";
  public static final String NO_MEMBERS_IN_GROUP_ERROR_MESSAGE = "No caching members for group {0} could be found.  Please verify the group and try again.";
  public static final String NO_MEMBERS_FOUND_MESSAGE = "No Members Found";
  public static final String NO_CACHING_MEMBERS_FOUND_MESSAGE = "No caching members found.";
  public static final String COMMAND_FAILURE_MESSAGE = "Error occured while executing : {0}";
  public static final String EXCEPTION_CLASS_AND_MESSAGE = "Exception : {0} , Message : {1}";
  public static final String GROUP_EMPTY_ERROR_MESSAGE = "No members found in group : {0}";
  public static final String REGION_NOT_FOUND = "Region : {0} not found";
  public static final String INVALID_REGION_NAME = "Invalid region name";
  public static final String INVALID_FILE_EXTENTION = "Invalid file type, the file extension must be \"{0}\"";
  public static final String GEODE_DATA_FILE_EXTENSION = ".gfd";
  public static final String LOCATOR_HEADER = "Locator";
  public static final String ERROR__MSG__HEADER = "Error";
  public static final String ZIP_FILE_EXTENSION=".zip";
  // This should be thrown for FunctionInvocationTargetException
  public static final String COULD_NOT_EXECUTE_COMMAND_TRY_AGAIN = "Could not execute \" {0} \", please try again ";
  public static final String UNEXPECTED_RETURN_TYPE_EXECUTING_COMMAND_ERROR_MESSAGE = "Received an unexpected return type ({0}) while executing command {1}.";
  public static final String PROVIDE_ATLEAST_ONE_OPTION = "\"{0}\" requires that one or more parameters be provided.";
  public static final String STATUS_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE = "Gfsh must be connected in order to get the status of a {0} by member name or ID.";
  public static final String STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE = "Gfsh must be connected in order to stop a {0} by member name or ID.";
  public static final String PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE = "Please provide either \"member\" or \"group\" option.";
  public static final String GFSH_MUST_BE_CONNECTED_FOR_LAUNCHING_0 = "Gfsh must be connected for launching {0}";
  public static final String GFSH_MUST_BE_CONNECTED_VIA_JMX_FOR_LAUNCHING_0 = "Gfsh must be connected via JMX for launching {0}";
  public static final String ATTACH_API_IN_0_NOT_FOUND_ERROR_MESSAGE = "The JDK {0} is required by 'start', 'status' and 'stop' commands for Locators and Servers. Please set JAVA_HOME to the JDK Directory or add the JDK {0} to the classpath, restart Gfsh and try again.";
  public static final String ASYNC_PROCESS_LAUNCH_MESSAGE = "Broken out of wait... the %1$s process will continue to startup in the background.%n";
  public static final String NO_NON_LOCATOR_MEMBERS_FOUND = "No non-locator members found.";
  public static final String NO_CLIENT_FOUND = "No client found on this server";
  public static final String NO_CLIENT_FOUND_WITH_CLIENT_ID = "No client found with client-id : {0}";
  public static final String ACTION_SUCCCEEDED_ON_MEMBER = "{0} on following members.";
  public static final String OCCURRED_ON_MEMBERS = "Occurred on members";
  public static final String SHARED_CONFIGURATION_NOT_STARTED = "Cluster configuration service is enabled but has not started yet.";
  public static final String SHARED_CONFIGURATION_NO_LOCATORS_WITH_SHARED_CONFIGURATION = "No locators with cluster configuration enabled.";
  public static final String SHARED_CONFIGURATION_FAILED_TO_PERSIST_COMMAND_CHANGES = "Failed to persist the configuration changes due to this command, Revert the command to maintain consistency.\nPlease use \"status cluster-config-service\" to determine whether Cluster configuration service is RUNNING.";
  /* Other Constants */
  public static final String GFSH__MSG__NO_LONGER_CONNECTED_TO_0 = "No longer connected to {0}.";
  public static final String GFSHPARSER__MSG__REQUIRED_ARGUMENT_0 = "Required Argument: \"{0}\"";
  public static final String GFSHPARSER__MSG__VALUE_REQUIRED_FOR_OPTION_0 = "Value is required for parameter \"{0}\"";
  public static final String GFSHPARSER__MSG__AMBIGIOUS_COMMAND_0_FOR_ASSISTANCE_USE_1_OR_HINT_HELP = "Ambigious command \"{0}\" (for assistance press \"{1}\" or type \"hint\" or \"help <command name>\" & then hit ENTER)";
  public static final String GFSHPARSER__MSG__COMMAND_ARGUMENT_0_IS_REQUIRED_USE_HELP = "Command parameter \"{0}\" is required. Use \"help <command name>\" for assistance.";
  public static final String GFSHPARSER__MSG__COMMAND_OPTION_0_IS_REQUIRED_USE_HELP = "Parameter \"{0}\" is required. Use \"help <command name>\" for assistance.";
  public static final String GFSHPARSER__MSG__VALUE_0_IS_NOT_APPLICABLE_FOR_1 = "Value \"{0}\" is not applicable for \"{1}\".";
  public static final String GFSHPARSER__MSG__INVALID_COMMAND_STRING_0 = "Invalid command String: {0}";
  public static final String GFSHPARSER__MSG__COMMAND_0_IS_NOT_VALID = "Command \"{0}\" is not valid.";
  public static final String GFSHPARSER__MSG__NO_MATCHING_COMMAND = "No matching command";
  public static final String GFSHPARSER__MSG__USE_0_HELP_COMMAND_TODISPLAY_DETAILS = "Use {0}help <command name> to display detailed usage information for a specific command.";
  public static final String GFSHPARSER__MSG__HELP_CAN_ALSO_BE_OBTAINED_BY_0_KEY = "Help with command and parameter completion can also be obtained by entering all or a portion of either followed by the \"{0}\" key.";
  public static final String GFSHPARSER__MSG__0_IS_NOT_AVAILABLE_REASON_1 = "\"{0}\" is not available. Reason: {1}";
  public static final String GFSHPARSER__MSG__OTHER_COMMANDS_STARTING_WITH_0_ARE = "Other commands starting with \"{0}\" are: ";

  public static final String ABSTRACTRESULTDATA__MSG__FILE_WITH_NAME_0_EXISTS_IN_1 = "File with name \"{0}\" already exists in \"{1}\".";
  public static final String ABSTRACTRESULTDATA__MSG__PARENT_DIRECTORY_OF_0_DOES_NOT_EXIST = "Parent directory of \"{0}\" does not exist.";
  public static final String ABSTRACTRESULTDATA__MSG__PARENT_DIRECTORY_OF_0_IS_NOT_WRITABLE = "Parent directory of \"{0}\" is not writable.";
  public static final String ABSTRACTRESULTDATA__MSG__PARENT_OF_0_IS_NOT_DIRECTORY = "Parent of \"{0}\" is not a directory.";

  public static final String AVAILABILITYTARGET_MSG_DEFAULT_UNAVAILABILITY_DESCRIPTION = "Requires connection.";

  public static final String LAUNCHERLIFECYCLECOMMANDS__MSG__FAILED_TO_START_0_REASON_1 = "Failed to start {0}. Reason: {1}";

  public static final String GFSH__PLEASE_CHECK_LOGS_AT_0 = "Please check logs {0}";

  /*-**************************************************************************
   * *********** COMMAND NAME, OPTION, ARGUMENT, HELP, MESSAGES ****************
   * **************************************************************************/
  /* 'alter disk-store' command */
  public static final String ALTER_DISK_STORE = "alter disk-store";
  public static final String ALTER_DISK_STORE__HELP = "Alter some options for a region or remove a region in an offline disk store.";
  public static final String ALTER_DISK_STORE__DISKSTORENAME = "name";
  public static final String ALTER_DISK_STORE__DISKSTORENAME__HELP = "Name of the disk store whose contents will be altered.";
  public static final String ALTER_DISK_STORE__REGIONNAME = "region";
  public static final String ALTER_DISK_STORE__REGIONNAME__HELP = "Name/Path of the region in the disk store to alter.";
  public static final String ALTER_DISK_STORE__DISKDIRS = "disk-dirs";
  public static final String ALTER_DISK_STORE__DISKDIRS__HELP = "Directories where data for the disk store was previously written.";
  public static final String ALTER_DISK_STORE__LRU__EVICTION__ALGORITHM = "lru-algorithm";
  public static final String ALTER_DISK_STORE__LRU__EVICTION__ALGORITHM__HELP = "Least recently used eviction algorithm.  Valid values are: none, lru-entry-count, lru-heap-percentage and lru-memory-size.";
  public static final String ALTER_DISK_STORE__LRU__EVICTION__ACTION = "lru-action";
  public static final String ALTER_DISK_STORE__LRU__EVICTION__ACTION__HELP = "Action to take when evicting entries from the region. Valid values are: none, overflow-to-disk and local-destroy.";
  public static final String ALTER_DISK_STORE__LRU__EVICTION__LIMIT = "lru-limit";
  public static final String ALTER_DISK_STORE__LRU__EVICTION__LIMIT__HELP = "Number of entries allowed in the region before eviction will occur.";
  public static final String ALTER_DISK_STORE__CONCURRENCY__LEVEL = "concurrency-level";
  public static final String ALTER_DISK_STORE__CONCURRENCY__LEVEL__HELP = "An estimate of the maximum number of application threads that will concurrently modify a region at one time. This attribute does not apply to partitioned regions.";
  public static final String ALTER_DISK_STORE__INITIAL__CAPACITY = "initial-capacity";
  public static final String ALTER_DISK_STORE__INITIAL__CAPACITY__HELP = "Together with --load-factor, sets the parameters on the underlying java.util.ConcurrentHashMap used for storing region entries.";
  public static final String ALTER_DISK_STORE__LOAD__FACTOR = "load-factor";
  public static final String ALTER_DISK_STORE__LOAD__FACTOR__HELP = "Together with --initial-capacity, sets the parameters on the underlying java.util.ConcurrentHashMap used for storing region entries. This must be a floating point number between 0 and 1, inclusive.";
  public static final String ALTER_DISK_STORE__COMPRESSOR = "compressor";
  public static final String ALTER_DISK_STORE__COMPRESSOR__HELP = "The fully-qualifed class name of the Compressor to use when compressing region entry values. A value of 'none' will remove the Compressor.";
  public static final String ALTER_DISK_STORE__STATISTICS__ENABLED = "enable-statistics";
  public static final String ALTER_DISK_STORE__STATISTICS__ENABLED__HELP = "Whether to enable statistics. Valid values are: true and false.";
  public static final String ALTER_DISK_STORE__OFF_HEAP = "off-heap";
  public static final String ALTER_DISK_STORE__OFF_HEAP__HELP = "Whether to use off-heap memory for the region. Valid values are: true and false.";
  public static final String ALTER_DISK_STORE__REMOVE = "remove";
  public static final String ALTER_DISK_STORE__REMOVE__HELP = "Whether to remove the region from the disk store.";

  /* 'alter region' command */
  public static final String ALTER_REGION = "alter region";
  public static final String ALTER_REGION__HELP = "Alter a region with the given path and configuration.";
  public static final String ALTER_REGION__REGION = "name";
  public static final String ALTER_REGION__REGION__HELP = "Name/Path of the region to be altered.";
  public static final String ALTER_REGION__GROUP = "group";
  public static final String ALTER_REGION__GROUP__HELP = "Group(s) of members on which the region will be altered.";
  public static final String ALTER_REGION__ENTRYEXPIRATIONIDLETIME = "entry-idle-time-expiration";
  public static final String ALTER_REGION__ENTRYEXPIRATIONIDLETIME__HELP = "How long the region's entries can remain in the cache without being accessed. The default is no expiration of this type.";
  public static final String ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION = "entry-idle-time-expiration-action";
  public static final String ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION__HELP = "Action to be taken on an entry that has exceeded the idle expiration.";
  public static final String ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE = "entry-time-to-live-expiration";
  public static final String ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE__HELP = "How long the region's entries can remain in the cache without being accessed or updated. The default is no expiration of this type.";
  public static final String ALTER_REGION__ENTRYEXPIRATIONTTLACTION = "entry-time-to-live-expiration-action";
  public static final String ALTER_REGION__ENTRYEXPIRATIONTTLACTION__HELP = "Action to be taken on an entry that has exceeded the TTL expiration.";
  public static final String ALTER_REGION__REGIONEXPIRATIONIDLETIME = "region-idle-time-expiration";
  public static final String ALTER_REGION__REGIONEXPIRATIONIDLETIME__HELP = "How long the region can remain in the cache without being accessed. The default is no expiration of this type.";
  public static final String ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION = "region-idle-time-expiration-action";
  public static final String ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION__HELP = "Action to be taken on a region that has exceeded the idle expiration.";
  public static final String ALTER_REGION__REGIONEXPIRATIONTTL = "region-time-to-live-expiration";
  public static final String ALTER_REGION__REGIONEXPIRATIONTTL__HELP = "How long the region can remain in the cache without being accessed or updated. The default is no expiration of this type.";
  public static final String ALTER_REGION__REGIONEXPIRATIONTTLACTION = "region-time-to-live-expiration-action";
  public static final String ALTER_REGION__REGIONEXPIRATIONTTLACTION__HELP = "Action to be taken on a region that has exceeded the TTL expiration.";
  public static final String ALTER_REGION__CACHELISTENER = "cache-listener";
  public static final String ALTER_REGION__CACHELISTENER__HELP = "Fully qualified class name of a plug-in to be instantiated for receiving after-event notification of changes to the region and its entries. Any number of cache listeners can be configured.";
  public static final String ALTER_REGION__CACHELOADER = "cache-loader";
  public static final String ALTER_REGION__CACHELOADER__HELP = "Fully qualified class name of a plug-in to be instantiated for receiving notification of cache misses in the region. At most, one cache loader can be defined in each member for the region. For distributed regions, a cache loader may be invoked remotely from other members that have the region defined.";
  public static final String ALTER_REGION__CACHEWRITER = "cache-writer";
  public static final String ALTER_REGION__CACHEWRITER__HELP = "Fully qualified class name of a plug-in to be instantiated for receiving before-event notification of changes to the region and its entries. The plug-in may cancel the event. At most, one cache writer can be defined in each member for the region.";
  public static final String ALTER_REGION__ASYNCEVENTQUEUEID = "async-event-queue-id";
  public static final String ALTER_REGION__ASYNCEVENTQUEUEID__HELP = "IDs of the Async Event Queues that will be used for write-behind operations."; // TODO - Abhishek Is this correct?
  public static final String ALTER_REGION__GATEWAYSENDERID = "gateway-sender-id";
  public static final String ALTER_REGION__GATEWAYSENDERID__HELP = "IDs of the Gateway Senders to which data will be routed.";
  public static final String ALTER_REGION__CLONINGENABLED = "enable-cloning";
  public static final String ALTER_REGION__CLONINGENABLED__HELP = "Determines how fromDelta applies deltas to the local cache for delta propagation. When true, the updates are applied to a clone of the value and then the clone is saved to the cache. When false, the value is modified in place in the cache.";
  public static final String ALTER_REGION__EVICTIONMAX = "eviction-max";
  public static final String ALTER_REGION__EVICTIONMAX__HELP = "Maximum value for the Eviction Attributes which the Eviction Algorithm uses to determine when to perform its Eviction Action. The unit of the maximum value is determined by the Eviction Algorithm.";
  public static final String ALTER_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELISTENER_0_IS_INVALID = "Specify a valid class name for "
      + CliStrings.CREATE_REGION__CACHELISTENER + ". \"{0}\" is not valid.";
  public static final String ALTER_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHEWRITER_0_IS_INVALID = "Specify a valid class name for "
      + CliStrings.CREATE_REGION__CACHEWRITER + ". \"{0}\" is not valid.";
  public static final String ALTER_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELOADER_0_IS_INVALID = "Specify a valid class name for "
      + CliStrings.CREATE_REGION__CACHELOADER + ". \"{0}\" is not valid.";
  public static final String ALTER_REGION__MSG__REGION_0_ALTERED_ON_1 = "Region \"{0}\" altered on \"{1}\"";
  public static final String ALTER_REGION__MSG__COULDNOT_FIND_CLASS_0_SPECIFIED_FOR_1 = "Could not find class \"{0}\" specified for \"{1}\".";
  public static final String ALTER_REGION__MSG__COULDNOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1 = "Could not instantiate class \"{0}\" specified for \"{1}\".";
  public static final String ALTER_REGION__MSG__CLASS_SPECIFIED_FOR_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE = "Class \"{0}\" specified for \"{1}\" is not of an expected type.";
  public static final String ALTER_REGION__MSG__COULDNOT_ACCESS_CLASS_0_SPECIFIED_FOR_1 = "Could not access class \"{0}\" specified for \"{1}\".";
  public static final String ALTER_REGION__MSG__SPECIFY_POSITIVE_INT_FOR_EVICTIONMAX_0_IS_NOT_VALID = "Specify 0 or a positive integer value for "
      + CliStrings.ALTER_REGION__EVICTIONMAX + ".  \"{0}\" is not valid.";
  public static final String ALTER_REGION__MSG__REGION_DOESNT_EXIST_0 = "Region doesn't exist: {0}";

  public static final String ALTER_RUNTIME_CONFIG = "alter runtime";
  public static final String ALTER_RUNTIME_CONFIG__HELP = "Alter a subset of member or members configuration properties while running.";
  public static final String ALTER_RUNTIME_CONFIG__MEMBER = "member";
  public static final String ALTER_RUNTIME_CONFIG__MEMBER__HELP = "Name/Id of the member in whose configuration will be altered.";
  public static final String ALTER_RUNTIME_CONFIG__GROUP = "group";
  public static final String ALTER_RUNTIME_CONFIG__GROUP__HELP = "Group of members whose configuration will be altered.";
  public static final String ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT = ARCHIVE_FILE_SIZE_LIMIT;
  public static final String ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT__HELP = "Archive file size limit. Valid values are (in megabytes): "
      + DistributionConfig.MIN_ARCHIVE_FILE_SIZE_LIMIT + " - " + DistributionConfig.MAX_ARCHIVE_FILE_SIZE_LIMIT + ".";
  public static final String ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT = ARCHIVE_DISK_SPACE_LIMIT;
  public static final String ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT__HELP = "Archive disk space limit. Valid values are (in megabytes): "
      + DistributionConfig.MIN_ARCHIVE_DISK_SPACE_LIMIT + " - " + DistributionConfig.MAX_ARCHIVE_DISK_SPACE_LIMIT + ".";
  public static final String ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT = LOG_FILE_SIZE_LIMIT;
  public static final String ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT__HELP = "Log file size limit. Valid values are (in megabytes): "
      + DistributionConfig.MIN_LOG_FILE_SIZE_LIMIT + " - " + DistributionConfig.MAX_LOG_FILE_SIZE_LIMIT + ".";
  public static final String ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT = LOG_DISK_SPACE_LIMIT;
  public static final String ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT__HELP = "Log disk space limit. Valid values are (in megabytes): "
      + DistributionConfig.MIN_LOG_DISK_SPACE_LIMIT + " - " + DistributionConfig.MAX_LOG_DISK_SPACE_LIMIT + ".";
  public static final String ALTER_RUNTIME_CONFIG__LOG__LEVEL = LOG_LEVEL;
  public static final String ALTER_RUNTIME_CONFIG__LOG__LEVEL__HELP = "Log level. Valid values are: none, error, info, config , warning, severe, fine, finer and finest.";
  public static final String ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED = "enable-statistics";
  public static final String ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED__HELP = "Whether statistic sampling should be enabled. Valid values are: true and false.";
  public static final String ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE = STATISTIC_SAMPLE_RATE;
  public static final String ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE__HELP = "Statistic sampling rate. Valid values are (in milliseconds): "
      + DistributionConfig.MIN_STATISTIC_SAMPLE_RATE + " - " + DistributionConfig.MAX_STATISTIC_SAMPLE_RATE + ".";
  public static final String ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE = STATISTIC_ARCHIVE_FILE;
  public static final String ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE__HELP = "File to which the statistics will be written.";
  
  public static final String ALTER_RUNTIME_CONFIG__COPY__ON__READ = CacheXml.COPY_ON_READ;
  public static final String ALTER_RUNTIME_CONFIG__COPY__ON__READ__HELP = "Sets the \"copy on read\" feature for cache read operations";
  public static final String ALTER_RUNTIME_CONFIG__LOCK__LEASE = CacheXml.LOCK_LEASE;
  public static final String ALTER_RUNTIME_CONFIG__LOCK__LEASE__HELP = "Sets the length, in seconds, of distributed lock leases obtained by this cache.";
  public static final String ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT = CacheXml.LOCK_TIMEOUT;
  public static final String ALTER_RUNTIME_CONFIG__LOCK__TIMEOUT__HELP  = "Sets the number of seconds a cache operation may wait to obtain a distributed lock lease before timing out.";  
  public static final String ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL = CacheXml.MESSAGE_SYNC_INTERVAL;
  public static final String ALTER_RUNTIME_CONFIG__MESSAGE__SYNC__INTERVAL__HELP = "Sets the frequency (in seconds) at which a message will be sent by the primary cache-server node to all the secondary cache-server nodes to remove the events which have already been dispatched from the queue";
  public static final String ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT = CacheXml.SEARCH_TIMEOUT;
  public static final String ALTER_RUNTIME_CONFIG__SEARCH__TIMEOUT__HELP = "Sets the number of seconds a cache get operation can spend searching for a value.";
  
  
  public static final String ALTER_RUNTIME_CONFIG__SUCCESS__MESSAGE = "Runtime configuration altered successfully for the following member(s)";
  public static final String ALTER_RUNTIME_CONFIG__MEMBER__NOT__FOUND = "Member : {0} not found";
  public static final String ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE = "Please provide a relevant parameter(s)";

  /* 'backup disk-store' command */
  public static final String BACKUP_DISK_STORE = "backup disk-store";
  public static final String BACKUP_DISK_STORE__HELP = "Perform a backup on all members with persistent data. The target directory must exist on all members, but can be either local or shared. This command can safely be executed on active members and is strongly recommended over copying files via operating system commands.";
  public static final String BACKUP_DISK_STORE__DISKDIRS = "dir";
  public static final String BACKUP_DISK_STORE__BASELINEDIR = "baseline-dir";
  public static final String BACKUP_DISK_STORE__BASELINEDIR__HELP = "Directory which contains the baseline backup used for comparison during an incremental backup.";
  public static final String BACKUP_DISK_STORE__DISKDIRS__HELP = "Directory to which backup files will be written.";
  public static final String BACKUP_DISK_STORE_MSG_BACKED_UP_DISK_STORES = "The following disk stores were backed up successfully";
  public static final String BACKUP_DISK_STORE_MSG_OFFLINE_DISK_STORES = "The backup may be incomplete. The following disk stores are not online";
  public static final String BACKUP_DISK_STORE_MSG_HOST = "Host";
  public static final String BACKUP_DISK_STORE_MSG_DIRECTORY = "Directory";
  public static final String BACKUP_DISK_STORE_MSG_UUID = "UUID";
  public static final String BACKUP_DISK_STORE_MSG_MEMBER = "Member";
  public static final String BACKUP_DISK_STORE_MSG_NO_DISKSTORES_BACKED_UP = "No disk store(s) were backed up.";

  /* 'compact disk-store' command */
  public static final String COMPACT_DISK_STORE = "compact disk-store";
  public static final String COMPACT_DISK_STORE__HELP = "Compact a disk store on all members with that disk store. This command uses the compaction threshold that each member has configured for its disk stores. The disk store must have \"allow-force-compaction\" set to true.";
  public static final String COMPACT_DISK_STORE__NAME = "name";
  public static final String COMPACT_DISK_STORE__NAME__HELP = "Name of the disk store to be compacted.";
  public static final String COMPACT_DISK_STORE__GROUP = "group";
  public static final String COMPACT_DISK_STORE__GROUP__HELP = "Group(s) of members that will perform disk compaction. If no group is specified the disk store will be compacted by all members.";
  public static final String COMPACT_DISK_STORE__DISKSTORE_0_DOESNOT_EXIST = "Disk store \"{0}\" does not exist.";
  public static final String COMPACT_DISK_STORE__MSG__FOR_GROUP = " for group(s) \"{0}\"";
  public static final String COMPACT_DISK_STORE__NO_MEMBERS_FOUND_IN_SPECIFED_GROUP = "No members found in the specified group(s) \"{0}\".";
  public static final String COMPACT_DISK_STORE__COMPACTION_ATTEMPTED_BUT_NOTHING_TO_COMPACT = "Attempted to compact disk store, but there was nothing to do.";
  public static final String COMPACT_DISK_STORE__ERROR_WHILE_COMPACTING_REASON_0 = "An error occurred while doing compaction: \"{0}\"";

  /* 'compact offline-disk-store' command */
  public static final String COMPACT_OFFLINE_DISK_STORE = "compact offline-disk-store";
  public static final String COMPACT_OFFLINE_DISK_STORE__HELP = "Compact an offline disk store. If the disk store is large, additional memory may need to be allocated to the process using the --J=-Xmx??? parameter.";
  public static final String COMPACT_OFFLINE_DISK_STORE__NAME = "name";
  public static final String COMPACT_OFFLINE_DISK_STORE__NAME__HELP = "Name of the offline disk store to be compacted.";
  public static final String COMPACT_OFFLINE_DISK_STORE__DISKDIRS = "disk-dirs";
  public static final String COMPACT_OFFLINE_DISK_STORE__DISKDIRS__HELP = "Directories where data for the disk store was previously written.";
  public static final String COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE = "max-oplog-size";
  public static final String COMPACT_OFFLINE_DISK_STORE__MAXOPLOGSIZE__HELP = "Maximum size (in megabytes) of the oplogs created by compaction.";
  public static final String COMPACT_OFFLINE_DISK_STORE__J = "J";
  public static final String COMPACT_OFFLINE_DISK_STORE__J__HELP = "Arguments passed to the Java Virtual Machine performing the compact operation on the disk store.";
  public static final String COMPACT_OFFLINE_DISK_STORE__MSG__DISKSTORE_0_DOESNOT_EXIST = "Disk store \"{0}\" does not exist.";
  public static final String COMPACT_OFFLINE_DISK_STORE__MSG__COMPACTION_ATTEMPTED_BUT_NOTHING_TO_COMPACT = "Attempted to compact disk store, but there was nothing to do.";
  public static final String COMPACT_OFFLINE_DISK_STORE__MSG__ERROR_WHILE_COMPACTING_REASON_0 = "An error occurred while doing compaction: \"{0}\"";
  public static final String COMPACT_OFFLINE_DISK_STORE__MSG__VERIFY_WHETHER_DISKSTORE_EXISTS_IN_0 = "Verify whether the disk store exists in \"{0}\".";
  public static final String COMPACT_OFFLINE_DISK_STORE__MSG__DISKSTORE_IN_USE_COMPACT_DISKSTORE_CAN_BE_USED = "This disk store is in use by other process. \""
      + CliStrings.COMPACT_DISK_STORE + "\" can be used to compact disk store that is current in use.";
  public static final String COMPACT_OFFLINE_DISK_STORE__MSG__CANNOT_ACCESS_DISKSTORE_0_FROM_1_CHECK_GFSH_LOGS = "Can not access disk store \"{0}\" from  \"{1}\". Check "
      + org.apache.geode.management.internal.cli.shell.Gfsh.GFSH_APP_NAME + " logs for error.";
  public static final String COMPACT_OFFLINE_DISK_STORE__MSG__ERROR_WHILE_COMPACTING_DISKSTORE_0_WITH_1_REASON_2 = "While compacting disk store={0} {1}. Reason: {2}";

  /* connect command */
  public static final String CONNECT = "connect";
  public static final String CONNECT__HELP = "Connect to a jmx-manager either directly or via a Locator. If connecting via a Locator, and a jmx-manager doesn't already exist, the Locator will start one.";
  public static final String CONNECT__JMX_MANAGER = JMX_MANAGER;
  public static final String CONNECT__JMX_MANAGER__HELP = "Network address of the jmx-manager in the form: host[port].";
  public static final String CONNECT__LOCATOR = "locator";
  public static final String CONNECT__LOCATOR__HELP = "Network address of the Locator in the form: host[port].";
  public static final String CONNECT__URL = "url";
  public static final String CONNECT__DEFAULT_BASE_URL = "http://localhost:" + DistributionConfig.DEFAULT_HTTP_SERVICE_PORT + "/gemfire/v1";
  public static final String CONNECT__DEFAULT_SSL_BASE_URL = "https://localhost:" + DistributionConfig.DEFAULT_HTTP_SERVICE_PORT + "/gemfire/v1";
  public static final String CONNECT__URL__HELP = "Indicates the base URL to the Manager's HTTP service.  For example: 'http://<host>:<port>/gemfire/v1' Default is '" + CONNECT__DEFAULT_BASE_URL + "'";
  public static final String CONNECT__USE_HTTP = "use-http";
  public static final String CONNECT__USE_HTTP__HELP = "Connects to Manager by sending HTTP requests to HTTP service hosting the Management REST API.  You must first 'disconnect' in order to reconnect to the Manager via locator or jmx-manager using JMX.";
  public static final String CONNECT__USERNAME = "user";
  public static final String CONNECT__USERNAME__HELP = "User name to securely connect to the jmx-manager. If the --password parameter is not specified then it will be prompted for.";
  public static final String CONNECT__PASSWORD = "password";
  public static final String CONNECT__PASSWORD__HELP = "Password to securely connect to the jmx-manager.";
  public static final String CONNECT__KEY_STORE = "key-store";
  public static final String CONNECT__KEY_STORE__HELP = "Java keystore file containing this application's certificate and private key. If the --key-store-password parameter is not specified then it will be prompted for.";
  public static final String CONNECT__KEY_STORE_PASSWORD = "key-store-password";
  public static final String CONNECT__KEY_STORE_PASSWORD__HELP = "Password to access the private key from the keystore file specified by --key-store.";
  public static final String CONNECT__TRUST_STORE = "trust-store";
  public static final String CONNECT__TRUST_STORE__HELP = "Java keystore file containing the collection of CA certificates trusted by this application. If the --trust-store-password parameter is not specified then it will be prompted for.";
  public static final String CONNECT__TRUST_STORE_PASSWORD = "trust-store-password";
  public static final String CONNECT__TRUST_STORE_PASSWORD__HELP = "Password to unlock the keystore file specified by --trust-store";
  public static final String CONNECT__SSL_CIPHERS = "ciphers";
  public static final String CONNECT__SSL_CIPHERS__HELP = "SSL/TLS ciphers used when encrypting the connection. The default is \"any\".";
  public static final String CONNECT__SSL_PROTOCOLS = "protocols";
  public static final String CONNECT__SSL_PROTOCOLS__HELP = "SSL/TLS protocol versions to enable when encrypting the connection. The default is \"any\".";
  public static final String CONNECT__MSG__CONNECTING_TO_LOCATOR_AT_0 = "Connecting to Locator at {0} ..";
  public static final String CONNECT__SECURITY_PROPERTIES = "security-properties-file";
  public static final String CONNECT__SECURITY_PROPERTIES__HELP = "The gfsecurity.properties file for configuring gfsh to connect to the Locator/Manager. The file's path can be absolute or relative to gfsh directory.";
  public static final String CONNECT__USE_SSL = "use-ssl";
  public static final String CONNECT__USE_SSL__HELP = "Whether to use SSL for communication with Locator and/or JMX Manager. If set to \"true\", will also read \"gfsecurity.properties\". SSL Options take precedence over proeprties file. If none are specified, defaults will be used. The default value for this options is \"false\".";
  public static final String CONNECT__MSG__CONNECTING_TO_MANAGER_AT_0 = "Connecting to Manager at {0} ..";
  public static final String CONNECT__MSG__CONNECTING_TO_MANAGER_HTTP_SERVICE_AT_0 = "Connecting to Manager's HTTP service at {0} ..";
  public static final String CONNECT__MSG__LOCATOR_COULD_NOT_FIND_MANAGER = "Locator could not find a JMX Manager";
  public static final String CONNECT__MSG__JMX_PASSWORD_MUST_BE_SPECIFIED = "password must be specified.";
  public static final String CONNECT__MSG__ALREADY_CONNECTED = "Already connected to: {0}";
  public static final String CONNECT__MSG__SUCCESS = "Successfully connected to: {0}";
  public static final String CONNECT__MSG__ERROR = "Could not connect to : {0}. {1}";
  public static final String CONNECT__MSG__SERVICE_UNAVAILABLE_ERROR = "Could not find a Geode jmx-manager service at {0}.";
  public static final String CONNECT__MSG__COULD_NOT_CONNECT_TO_LOCATOR_0 = "Could not connect to Geode Locator service at {0}.";
  public static final String CONNECT__MSG__COULD_NOT_READ_CONFIG_FROM_0 = "Could not read config from {0}.";
  public static final String CONNECT__MSG__COULD_NOT_CONNECT_TO_LOCATOR_0_POSSIBLY_SSL_CONFIG_ERROR = "Could not connect to Locator at {0}."+Gfsh.LINE_SEPARATOR+"Possible reason: Wrong or no SSL configuration provided.";
  public static final String CONNECT__MSG__COULD_NOT_CONNECT_TO_MANAGER_0_POSSIBLY_SSL_CONFIG_ERROR = "Could not connect to Manager at {0}."+Gfsh.LINE_SEPARATOR+"Possible reason: Wrong or no SSL configuration provided.";

  /* 'create async-event-queue' command */
  public static final String CREATE_ASYNC_EVENT_QUEUE = "create async-event-queue";
  public static final String CREATE_ASYNC_EVENT_QUEUE__HELP = "Create Async Event Queue.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__ID = "id";
  public static final String CREATE_ASYNC_EVENT_QUEUE__ID__HELP = "ID of the queue to be created.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__PARALLEL = "parallel";
  public static final String CREATE_ASYNC_EVENT_QUEUE__PARALLEL__HELP = "Whether this queue is parallel.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE = "batch-size";
  public static final String CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE__HELP = "Maximum number of events that a batch can contain.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL = "batch-time-interval";
  public static final String CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL__HELP = "Maximum amount of time, in ms, that can elapse before a batch is delivered.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION = "enable-batch-conflation";
  public static final String CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION__HELP = "Whether to enable batch conflation.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__PERSISTENT = "persistent";
  public static final String CREATE_ASYNC_EVENT_QUEUE__PERSISTENT__HELP = "Whether events should be persisted to a disk store.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__DISK_STORE = "disk-store";
  public static final String CREATE_ASYNC_EVENT_QUEUE__DISK_STORE__HELP = "Disk store to be used by this queue.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS = "disk-synchronous";
  public static final String CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS__HELP = "Whether disk writes are synchronous.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY = "forward-expiration-destroy";
  public static final String CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY__HELP = "Whether to forward expiration destroy events.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY = "max-queue-memory";
  public static final String CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY__HELP = "Maximum amount of memory, in megabytes, that the queue can consume before overflowing to disk.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER = "gateway-event-filter";
  public static final String CREATE_ASYNC_EVENT_QUEUE__GATEWAYEVENTFILTER__HELP = "List of fully qualified class names of GatewayEventFilters for this queue.  These classes filter events before dispatching to remote servers.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY = "order-policy";
  public static final String CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY__HELP = "Policy for dispatching events when --dispatcher-threads is > 1. Possible values are 'THREAD', 'KEY', 'PARTITION'.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS = "dispatcher-threads";
  public static final String CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS__HELP = "Number of threads to use for sending events.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER = "gateway-event-substitution-filter";
  public static final String CREATE_ASYNC_EVENT_QUEUE__SUBSTITUTION_FILTER__HELP = "Fully qualified class name of the GatewayEventSubstitutionFilter for this queue.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__LISTENER = "listener";
  public static final String CREATE_ASYNC_EVENT_QUEUE__LISTENER__HELP = "Fully qualified class name of the AsyncEventListener for this queue.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE = "listener-param";
  public static final String CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE__HELP = "Parameter name for the AsyncEventListener.  Optionally, parameter names may be followed by # and a value for the parameter.  Example: --listener-param=loadAll --listener-param=maxRead#1024";
  public static final String CREATE_ASYNC_EVENT_QUEUE__GROUP = "group";
  public static final String CREATE_ASYNC_EVENT_QUEUE__GROUP__HELP = "Group(s) of members on which queue will be created. If no group is specified the queue will be created on all members.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__ERROR_WHILE_CREATING_REASON_0 = "An error occurred while creating the queue: {0}";
  public static final String CREATE_ASYNC_EVENT_QUEUE__MSG__COULDNOT_ACCESS_CLASS_0_SPECIFIED_FOR_1 = "Could not access class \"{0}\" specified for \"{1}\".";
  public static final String CREATE_ASYNC_EVENT_QUEUE__MSG__COULDNOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1 = "Could not instantiate class \"{0}\" specified for \"{1}\".";
  public static final String CREATE_ASYNC_EVENT_QUEUE__MSG__CLASS_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE = "Class \"{0}\" specified for \"{1}\" is not of an expected type.";
  public static final String CREATE_ASYNC_EVENT_QUEUE__MSG__COULDNOT_FIND_CLASS_0_SPECIFIED_FOR_1 = "Could not find class \"{0}\" specified for \"{1}\".";

  /* 'create disk-store' command */
  public static final String CREATE_DISK_STORE = "create disk-store";
  public static final String CREATE_DISK_STORE__HELP = "Create a disk store.";
  public static final String CREATE_DISK_STORE__NAME = "name";
  public static final String CREATE_DISK_STORE__NAME__HELP = "Name of the disk store to be created.";
  public static final String CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION = "allow-force-compaction";
  public static final String CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION__HELP = "Whether to allow manual compaction through the API or command-line tools.";
  public static final String CREATE_DISK_STORE__AUTO_COMPACT = "auto-compact";
  public static final String CREATE_DISK_STORE__AUTO_COMPACT__HELP = "Whether to automatically compact a file when it reaches the compaction-threshold.";
  public static final String CREATE_DISK_STORE__COMPACTION_THRESHOLD = "compaction-threshold";
  public static final String CREATE_DISK_STORE__COMPACTION_THRESHOLD__HELP = "Percentage of garbage allowed in the file before it is eligible for compaction.";
  public static final String CREATE_DISK_STORE__MAX_OPLOG_SIZE = "max-oplog-size";
  public static final String CREATE_DISK_STORE__MAX_OPLOG_SIZE__HELP = "The largest size, in megabytes, to allow an operation log to become before automatically rolling to a new file.";
  public static final String CREATE_DISK_STORE__QUEUE_SIZE = "queue-size";
  public static final String CREATE_DISK_STORE__QUEUE_SIZE__HELP = "For asynchronous queueing. The maximum number of operations to allow into the write queue before automatically flushing the queue. The default of 0 indicates no limit.";
  public static final String CREATE_DISK_STORE__TIME_INTERVAL = "time-interval";
  public static final String CREATE_DISK_STORE__TIME_INTERVAL__HELP = "For asynchronous queueing. The number of milliseconds that can elapse before data is flushed to disk. Reaching this limit or the queue-size limit causes the queue to flush.";
  public static final String CREATE_DISK_STORE__WRITE_BUFFER_SIZE = "write-buffer-size";
  public static final String CREATE_DISK_STORE__WRITE_BUFFER_SIZE__HELP = "Size of the buffer used to write to disk.";
  public static final String CREATE_DISK_STORE__DIRECTORY_AND_SIZE = "dir";
  public static final String CREATE_DISK_STORE__DIRECTORY_AND_SIZE__HELP = "Directories where the disk store files will be written, the directories will be created if they don't exist.  Optionally, directory names may be followed by # and the maximum number of megabytes that the disk store can use in the directory.  Example: --dir=/data/ds1 --dir=/data/ds2#5000";
  public static final String CREATE_DISK_STORE__GROUP = "group";
  public static final String CREATE_DISK_STORE__GROUP__HELP = "Group(s) of members on which the disk store will be created. If no group is specified the disk store will be created on all members.";
  public static final String CREATE_DISK_STORE__DISK_USAGE_WARNING_PCT = "disk-usage-warning-percentage";
  public static final String CREATE_DISK_STORE__DISK_USAGE_WARNING_PCT__HELP = "Warning percentage for disk volume usage.";
  public static final String CREATE_DISK_STORE__DISK_USAGE_CRITICAL_PCT = "disk-usage-critical-percentage";
  public static final String CREATE_DISK_STORE__DISK_USAGE_CRITICAL_PCT__HELP = "Critical percentage for disk volume usage.";
  public static final String CREATE_DISK_STORE__ERROR_WHILE_CREATING_REASON_0 = "An error occurred while creating the disk store: \"{0}\"";

  /* create index */
  public static final String CREATE_INDEX = "create index";
  public static final String CREATE_INDEX__HELP = "Create an index that can be used when executing queries.";
  public static final String CREATE_INDEX__NAME = "name";
  public static final String CREATE_INDEX__NAME__HELP = "Name of the index to create.";
  public static final String CREATE_INDEX__EXPRESSION = "expression";
  public static final String CREATE_INDEX__EXPRESSION__HELP = "Field of the region values that are referenced by the index.";
  public static final String CREATE_INDEX__REGION = "region";
  public static final String CREATE_INDEX__REGION__HELP = "Name/Path of the region which corresponds to the \"from\" clause in a query.";
  public static final String CREATE_INDEX__MEMBER = "member";
  public static final String CREATE_INDEX__MEMBER__HELP = "Name/Id of the member in which the index will be created.";
  public static final String CREATE_INDEX__TYPE = "type";
  public static final String CREATE_INDEX__TYPE__HELP = "Type of the index. Valid values are: range, key and hash.";
  public static final String CREATE_INDEX__GROUP = "group";
  public static final String CREATE_INDEX__GROUP__HELP = "Group of members in which the index will be created.";
  public static final String CREATE_INDEX__INVALID__INDEX__TYPE__MESSAGE = "Invalid index type,value must be one of the following: range, key or hash.";
  public static final String CREATE_INDEX__SUCCESS__MSG = "Index successfully created with following details";
  public static final String CREATE_INDEX__FAILURE__MSG = "Failed to create index \"{0}\" due to following reasons";
  public static final String CREATE_INDEX__ERROR__MSG = "Exception \"{0}\" occured on following members";
  public static final String CREATE_INDEX__NAME__CONFLICT = "Index \"{0}\" already exists.  "
      + "Create failed due to duplicate name.";
  public static final String CREATE_INDEX__INDEX__EXISTS = "Index \"{0}\" already exists.  "
      + "Create failed due to duplicate definition.";
  public static final String CREATE_INDEX__INVALID__EXPRESSION = "Invalid indexed expression : \"{0}\"";
  public static final String CREATE_INDEX__INVALID__REGIONPATH = "Region not found : \"{0}\"";
  public static final String CREATE_INDEX__NAME__MSG = "Name       : {0}";
  public static final String CREATE_INDEX__EXPRESSION__MSG = "Expression : {0}";
  public static final String CREATE_INDEX__REGIONPATH__MSG = "RegionPath : {0}";
  public static final String CREATE_INDEX__TYPE__MSG = "Type       : {0}";
  public static final String CREATE_INDEX__MEMBER__MSG = "Members which contain the index";
  public static final String CREATE_INDEX__NUMBER__AND__MEMBER = "{0}. {1}";
  public static final String CREATE_INDEX__EXCEPTION__OCCURRED__ON = "Occurred on following members";
  public static final String CREATE_INDEX__INVALID__INDEX__NAME = "Invalid index name";

  public static final String DEFINE_INDEX = "define index";
  public static final String DEFINE_INDEX__HELP = "Define an index that can be used when executing queries.";
  public static final String DEFINE_INDEX__SUCCESS__MSG = "Index successfully defined with following details";
  public static final String DEFINE_INDEX__FAILURE__MSG = "No indexes defined";
  public static final String DEFINE_INDEX_NAME = CREATE_INDEX__NAME;
  public static final String DEFINE_INDEX__EXPRESSION = CREATE_INDEX__EXPRESSION;
  public static final String DEFINE_INDEX__EXPRESSION__HELP = CREATE_INDEX__EXPRESSION__HELP;
  public static final String DEFINE_INDEX__REGION = CREATE_INDEX__REGION;
  public static final String DEFINE_INDEX__REGION__HELP = CREATE_INDEX__REGION__HELP;
  public static final String DEFINE_INDEX__TYPE = CREATE_INDEX__TYPE;
  public static final String DEFINE_INDEX__TYPE__HELP = CREATE_INDEX__TYPE__HELP;
  public static final String DEFINE_INDEX__INVALID__INDEX__TYPE__MESSAGE = CREATE_INDEX__INVALID__INDEX__TYPE__MESSAGE;
  public static final String DEFINE_INDEX__INVALID__INDEX__NAME = CREATE_INDEX__INVALID__INDEX__NAME;
  public static final String DEFINE_INDEX__INVALID__EXPRESSION = CREATE_INDEX__INVALID__EXPRESSION;
  public static final String DEFINE_INDEX__INVALID__REGIONPATH = CREATE_INDEX__INVALID__REGIONPATH;
  public static final String DEFINE_INDEX__NAME__MSG = CREATE_INDEX__NAME__MSG;
  public static final String DEFINE_INDEX__EXPRESSION__MSG = CREATE_INDEX__EXPRESSION__MSG;
  public static final String DEFINE_INDEX__REGIONPATH__MSG = CREATE_INDEX__REGIONPATH__MSG;
  
  public static final String CREATE_DEFINED_INDEXES = "create defined indexes";
  public static final String CREATE_DEFINED__HELP = "Creates all the defined indexes.";
  public static final String CREATE_DEFINED_INDEXES__SUCCESS__MSG = "Indexes successfully created. Use list indexes to get details.";
  public static final String CREATE_DEFINED_INDEXES__FAILURE__MSG = "Failed to create some or all indexes \"{0}\" due to following reasons";
  public static final String CREATE_DEFINED_INDEXES__MEMBER = CREATE_INDEX__MEMBER;
  public static final String CREATE_DEFINED_INDEXES__MEMBER__HELP = CREATE_INDEX__MEMBER__HELP;
  public static final String CREATE_DEFINED_INDEXES__GROUP = CREATE_INDEX__GROUP;
  public static final String CREATE_DEFINED_INDEXES__GROUP__HELP = CREATE_INDEX__GROUP__HELP;
  public static final String CREATE_DEFINED_INDEXES__MEMBER__MSG = CREATE_INDEX__MEMBER__MSG;
  public static final String CREATE_DEFINED_INDEXES__NUMBER__AND__MEMBER = CREATE_INDEX__NUMBER__AND__MEMBER;
  public static final String CREATE_DEFINED_INDEXES__EXCEPTION__OCCURRED__ON = CREATE_INDEX__EXCEPTION__OCCURRED__ON;

  
  public static final String CLEAR_DEFINED_INDEXES = "clear defined indexes";
  public static final String CLEAR_DEFINED__HELP = "Clears all the defined indexes.";
  public static final String CLEAR_DEFINED_INDEX__SUCCESS__MSG = "Index definitions successfully cleared";
  
  /* create region */
  public static final String CREATE_REGION = "create region";
  public static final String CREATE_REGION__HELP = "Create a region with the given path and configuration. Specifying a --key-constraint and --value-constraint makes object type information available during querying and indexing.";
  public static final String CREATE_REGION__REGION = "name";
  public static final String CREATE_REGION__REGION__HELP = "Name/Path of the region to be created.";
  public static final String CREATE_REGION__REGIONSHORTCUT = "type";
  public static final String CREATE_REGION__REGIONSHORTCUT__HELP = "Type of region to create. The following types are pre-defined by the product (see RegionShortcut javadocs for more information): PARTITION, PARTITION_REDUNDANT, PARTITION_PERSISTENT, PARTITION_REDUNDANT_PERSISTENT, PARTITION_OVERFLOW, PARTITION_REDUNDANT_OVERFLOW, PARTITION_PERSISTENT_OVERFLOW, PARTITION_REDUNDANT_PERSISTENT_OVERFLOW, PARTITION_HEAP_LRU, PARTITION_REDUNDANT_HEAP_LRU, REPLICATE, REPLICATE_PERSISTENT, REPLICATE_OVERFLOW, REPLICATE_PERSISTENT_OVERFLOW, REPLICATE_HEAP_LRU, LOCAL, LOCAL_PERSISTENT, LOCAL_HEAP_LRU, LOCAL_OVERFLOW, LOCAL_PERSISTENT_OVERFLOW, PARTITION_PROXY, PARTITION_PROXY_REDUNDANT, and REPLICATE_PROXY.";
  public static final String CREATE_REGION__GROUP = "group";
  public static final String CREATE_REGION__GROUP__HELP = "Group(s) of members on which the region will be created.";
  public static final String CREATE_REGION__USEATTRIBUTESFROM = "template-region";
  public static final String CREATE_REGION__USEATTRIBUTESFROM__HELP = "Name/Path of the region whose attributes should be duplicated when creating this region.";
  public static final String CREATE_REGION__SKIPIFEXISTS = "skip-if-exists";
  public static final String CREATE_REGION__SKIPIFEXISTS__HELP = "Skip region creation if the region already exists.";
  public static final String CREATE_REGION__KEYCONSTRAINT = "key-constraint";
  public static final String CREATE_REGION__KEYCONSTRAINT__HELP = "Fully qualified class name of the objects allowed as region keys. Ensures that keys for region entries are all of the same class.";
  public static final String CREATE_REGION__VALUECONSTRAINT = "value-constraint";
  public static final String CREATE_REGION__VALUECONSTRAINT__HELP = "Fully qualified class name of the objects allowed as region values. If not specified then region values can be of any class.";
  public static final String CREATE_REGION__STATISTICSENABLED = "enable-statistics";
  public static final String CREATE_REGION__STATISTICSENABLED__HELP = "Whether to gather statistics for the region. Must be true to use expiration on the region.";
  public static final String CREATE_REGION__ENTRYEXPIRATIONIDLETIME = "entry-idle-time-expiration";
  public static final String CREATE_REGION__ENTRYEXPIRATIONIDLETIME__HELP = "How long the region's entries can remain in the cache without being accessed. The default is no expiration of this type.";
  public static final String CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION = "entry-idle-time-expiration-action";
  public static final String CREATE_REGION__ENTRYEXPIRATIONIDLETIMEACTION__HELP = "Action to be taken on an entry that has exceeded the idle expiration.";
  public static final String CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE = "entry-time-to-live-expiration";
  public static final String CREATE_REGION__ENTRYEXPIRATIONTIMETOLIVE__HELP = "How long the region's entries can remain in the cache without being accessed or updated. The default is no expiration of this type.";
  public static final String CREATE_REGION__ENTRYEXPIRATIONTTLACTION = "entry-time-to-live-expiration-action";
  public static final String CREATE_REGION__ENTRYEXPIRATIONTTLACTION__HELP = "Action to be taken on an entry that has exceeded the TTL expiration.";
  public static final String CREATE_REGION__REGIONEXPIRATIONIDLETIME = "region-idle-time-expiration";
  public static final String CREATE_REGION__REGIONEXPIRATIONIDLETIME__HELP = "How long the region can remain in the cache without being accessed. The default is no expiration of this type.";
  public static final String CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION = "region-idle-time-expiration-action";
  public static final String CREATE_REGION__REGIONEXPIRATIONIDLETIMEACTION__HELP = "Action to be taken on a region that has exceeded the idle expiration.";
  public static final String CREATE_REGION__REGIONEXPIRATIONTTL = "region-time-to-live-expiration";
  public static final String CREATE_REGION__REGIONEXPIRATIONTTL__HELP = "How long the region can remain in the cache without being accessed or updated. The default is no expiration of this type.";
  public static final String CREATE_REGION__REGIONEXPIRATIONTTLACTION = "region-time-to-live-expiration-action";
  public static final String CREATE_REGION__REGIONEXPIRATIONTTLACTION__HELP = "Action to be taken on a region that has exceeded the TTL expiration.";
  public static final String CREATE_REGION__DISKSTORE = "disk-store";
  public static final String CREATE_REGION__DISKSTORE__HELP = "Disk Store to be used by this region. \"list disk-store\" can be used to display existing disk stores.";
  public static final String CREATE_REGION__DISKSYNCHRONOUS = "enable-synchronous-disk";
  public static final String CREATE_REGION__DISKSYNCHRONOUS__HELP = "Whether writes are done synchronously for regions that persist data to disk.";
  public static final String CREATE_REGION__ENABLEASYNCCONFLATION = "enable-async-conflation";
  public static final String CREATE_REGION__ENABLEASYNCCONFLATION__HELP = "Whether to allow aggregation of asynchronous TCP/IP messages sent by the producer member of the region. A false value causes all asynchronous messages to be sent individually.";
  public static final String CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION = "enable-subscription-conflation";
  public static final String CREATE_REGION__ENABLESUBSCRIPTIONCONFLATION__HELP = "Whether the server should conflate its messages to the client. A false value causes all server-client messages to be sent individually.";
  public static final String CREATE_REGION__CACHELISTENER = "cache-listener";
  public static final String CREATE_REGION__CACHELISTENER__HELP = "Fully qualified class name of a plug-in to be instantiated for receiving after-event notification of changes to the region and its entries. Any number of cache listeners can be configured.";
  public static final String CREATE_REGION__CACHELOADER = "cache-loader";
  public static final String CREATE_REGION__CACHELOADER__HELP = "Fully qualified class name of a plug-in to be instantiated for receiving notification of cache misses in the region. At most, one cache loader can be defined in each member for the region. For distributed regions, a cache loader may be invoked remotely from other members that have the region defined.";
  public static final String CREATE_REGION__CACHEWRITER = "cache-writer";
  public static final String CREATE_REGION__CACHEWRITER__HELP = "Fully qualified class name of a plug-in to be instantiated for receiving before-event notification of changes to the region and its entries. The plug-in may cancel the event. At most, one cache writer can be defined in each member for the region.";
  public static final String CREATE_REGION__ASYNCEVENTQUEUEID = "async-event-queue-id";
  public static final String CREATE_REGION__ASYNCEVENTQUEUEID__HELP = "IDs of the Async Event Queues that will be used for write-behind operations."; // TODO - Abhishek Is this correct?
  public static final String CREATE_REGION__GATEWAYSENDERID = "gateway-sender-id";
  public static final String CREATE_REGION__GATEWAYSENDERID__HELP = "IDs of the Gateway Senders to which data will be routed.";
  public static final String CREATE_REGION__CONCURRENCYCHECKSENABLED = "enable-concurrency-checks";
  public static final String CREATE_REGION__CONCURRENCYCHECKSENABLED__HELP = "Enables a versioning system that detects concurrent modifications and ensures that region contents are consistent across the distributed system.";
  public static final String CREATE_REGION__CLONINGENABLED = "enable-cloning";
  public static final String CREATE_REGION__CLONINGENABLED__HELP = "Determines how fromDelta applies deltas to the local cache for delta propagation. When true, the updates are applied to a clone of the value and then the clone is saved to the cache. When false, the value is modified in place in the cache.";
  public static final String CREATE_REGION__CONCURRENCYLEVEL = "concurrency-level";
  public static final String CREATE_REGION__CONCURRENCYLEVEL__HELP = "An estimate of the maximum number of application threads that will concurrently access a region entry at one time. This attribute does not apply to partitioned regions.";
  public static final String CREATE_REGION__COLOCATEDWITH = "colocated-with";
  public static final String CREATE_REGION__COLOCATEDWITH__HELP = "Central Region with which this region should be colocated.";
  public static final String CREATE_REGION__LOCALMAXMEMORY = "local-max-memory";
  public static final String CREATE_REGION__LOCALMAXMEMORY__HELP = "Sets the maximum amount of memory, in megabytes, to be used by the region in this process. (Default: 90% of available heap)";
  public static final String CREATE_REGION__MULTICASTENABLED = "enable-multicast";
  public static final String CREATE_REGION__MULTICASTENABLED__HELP = "Enables multicast messaging on the region.  Multicast must also be enabled in the cache distributed system properties.  This is primarily useful for replicated regions that are in all servers.";
  public static final String CREATE_REGION__RECOVERYDELAY = "recovery-delay";
  public static final String CREATE_REGION__RECOVERYDELAY__HELP = "Sets the delay in milliseconds that existing members will wait before satisfying redundancy after another member crashes. -1 (the default) indicates that redundancy will not be recovered after a failure.";
  public static final String CREATE_REGION__REDUNDANTCOPIES = "redundant-copies";
  public static final String CREATE_REGION__REDUNDANTCOPIES__HELP = "Sets the number of extra copies of buckets desired. Extra copies allow for both high availability in the face of VM departure (intended or unintended) and and load balancing read operations. (Allowed values: 0, 1, 2 and 3)";
  public static final String CREATE_REGION__STARTUPRECOVERYDDELAY = "startup-recovery-delay";
  public static final String CREATE_REGION__STARTUPRECOVERYDDELAY__HELP = "Sets the delay in milliseconds that new members will wait before satisfying redundancy. -1 indicates that adding new members will not trigger redundancy recovery. The default is to recover redundancy immediately when a new member is added.";
  public static final String CREATE_REGION__TOTALMAXMEMORY = "total-max-memory";
  public static final String CREATE_REGION__TOTALMAXMEMORY__HELP = "Sets the maximum amount of memory, in megabytes, to be used by the region in all processes.";
  public static final String CREATE_REGION__TOTALNUMBUCKETS = "total-num-buckets";
  public static final String CREATE_REGION__TOTALNUMBUCKETS__HELP = "Sets the total number of hash buckets to be used by the region in all processes. (Default: "		  
      + PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT + ").";
  public static final String CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH = "Specify a valid " + CliStrings.CREATE_REGION__REGION;
  public static final String CREATE_REGION__MSG__PARENT_REGION_FOR_0_DOESNOT_EXIST = "Parent region for \"{0}\" doesn't exist. ";
  public static final String CREATE_REGION__MSG__GROUPS_0_ARE_INVALID = "Group(s) \"{0}\" are invalid.";
  public static final String CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_USE_ATTR_FROM = "Specify a valid region path for "
      + CliStrings.CREATE_REGION__USEATTRIBUTESFROM;
  public static final String CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_KEYCONSTRAINT_0_IS_INVALID = "Specify a valid class name for "
      + CliStrings.CREATE_REGION__KEYCONSTRAINT + ". \"{0}\" is not valid.";
  public static final String CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_VALUECONSTRAINT_0_IS_INVALID = "Specify a valid class name for "
      + CliStrings.CREATE_REGION__VALUECONSTRAINT + ". \"{0}\" is not valid.";
  public static final String CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELISTENER_0_IS_INVALID = "Specify a valid class name for "
      + CliStrings.CREATE_REGION__CACHELISTENER + ". \"{0}\" is not valid.";
  public static final String CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHEWRITER_0_IS_INVALID = "Specify a valid class name for "
      + CliStrings.CREATE_REGION__CACHEWRITER + ". \"{0}\" is not valid.";
  public static final String CREATE_REGION__MSG__SPECIFY_VALID_CLASSNAME_FOR_CACHELOADER_0_IS_INVALID = "Specify a valid class name for "
      + CliStrings.CREATE_REGION__CACHELOADER + ". \"{0}\" is not valid.";
  public static final String CREATE_REGION__MSG__NO_GATEWAYSENDERS_IN_THE_SYSTEM = "There are no GatewaySenders defined currently in the system.";
  public static final String CREATE_REGION__MSG__SPECIFY_VALID_GATEWAYSENDER_ID_UNKNOWN_0 = "Specify valid "
      + CliStrings.CREATE_REGION__GATEWAYSENDERID + ". Unknown Gateway Sender(s): \"{0}\".";
  public static final String CREATE_REGION__MSG__SPECIFY_POSITIVE_INT_FOR_CONCURRENCYLEVEL_0_IS_NOT_VALID = "Specify positive integer value for "
      + CliStrings.CREATE_REGION__CONCURRENCYLEVEL + ".  \"{0}\" is not valid.";
  public static final String CREATE_REGION__MSG__SPECIFY_VALID_DISKSTORE_UNKNOWN_DISKSTORE_0 = "Specify valid "
      + CliStrings.CREATE_REGION__DISKSTORE + ". Unknown Disk Store : \"{0}\".";
  public static final String CREATE_REGION__MSG__USE_ONE_OF_THESE_SHORTCUTS_0 = "Use one of these shortcuts: {0}";
  public static final String CREATE_REGION__MSG__SKIPPING_0_REGION_PATH_1_ALREADY_EXISTS = "Skipping \"{0}\". Region \"{1}\" already exists.";
  public static final String CREATE_REGION__MSG__REGION_PATH_0_ALREADY_EXISTS_ON_1 = "Region with path \"{0}\" already exists on \"{1}\"";
  public static final String CREATE_REGION__MSG__REGION_0_CREATED_ON_1 = "Region \"{0}\" created on \"{1}\"";
  public static final String CREATE_REGION__MSG__ONLY_ONE_OF_REGIONSHORTCUT_AND_USEATTRIBUESFROM_CAN_BE_SPECIFIED = "Only one of "
      + CREATE_REGION__REGIONSHORTCUT + " & " + CREATE_REGION__USEATTRIBUTESFROM + " can be specified.";
  public static final String CREATE_REGION__MSG__ONE_OF_REGIONSHORTCUT_AND_USEATTRIBUESFROM_IS_REQUIRED = "One of \""
      + CREATE_REGION__REGIONSHORTCUT + "\" or \"" + CREATE_REGION__USEATTRIBUTESFROM + "\" is required.";
  public static final String CREATE_REGION__MSG__SPECIFY_VALID_REGION_PATH_FOR_0_REGIONPATH_1_NOT_FOUND = "Specify a valid region path for {0}. Region {1} not found.";
  public static final String CREATE_REGION__MSG__COULDNOT_FIND_CLASS_0_SPECIFIED_FOR_1 = "Could not find class \"{0}\" specified for \"{1}\".";
  public static final String CREATE_REGION__MSG__CLASS_SPECIFIED_FOR_0_SPECIFIED_FOR_1_IS_NOT_OF_EXPECTED_TYPE = "Class \"{0}\" specified for \"{1}\" is not of an expected type.";
  public static final String CREATE_REGION__MSG__COULDNOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1 = "Could not instantiate class \"{0}\" specified for \"{1}\".";
  public static final String CREATE_REGION__MSG__COULDNOT_ACCESS_CLASS_0_SPECIFIED_FOR_1 = "Could not access class \"{0}\" specified for \"{1}\".";
  public static final String CREATE_REGION__MSG__EXPIRATION_ACTION_0_IS_NOT_VALID = "Expiration action \"{0}\" is not valid.";
  public static final String CREATE_REGION__MSG__ERROR_ON_MEMBER_0 = "Error on member: {0}. "; // leave space in the end
                                                                                               // for further message
  public static final String CREATE_REGION__MSG__COULD_NOT_RETRIEVE_REGION_ATTRS_FOR_PATH_0_REASON_1 = "Could not retrieve region attributes for given path \"{0}\". Reason: {1}";
  public static final String CREATE_REGION__MSG__COULD_NOT_RETRIEVE_REGION_ATTRS_FOR_PATH_0_VERIFY_REGION_EXISTS = "Could not retrieve region attributes for given path \"{0}\". Use \""
      + CliStrings.LIST_REGION + "\" to verify region exists.";
  public static final String CREATE_REGION__MSG__USE_ATTRIBUTES_FORM_REGIONS_EXISTS_BUT_DIFFERENT_SCOPE_OR_DATAPOLICY_USE_DESCRIBE_REGION_FOR_0 = "The region mentioned for \""
      + CliStrings.CREATE_REGION__USEATTRIBUTESFROM
      + "\" exists in this Geode Cluster but with different Scopes or Data Policies on different members. For details, use command \""
      + CliStrings.DESCRIBE_REGION + "\" for \"{0}\".";
  public static final String CREATE_REGION__MSG__USE_ATTRIBUTES_FROM_REGION_0_IS_NOT_WITH_PERSISTENCE = CREATE_REGION__USEATTRIBUTESFROM
      + " region \"{0}\" is not persistent.";
  public static final String CREATE_REGION__MSG__OPTION_0_CAN_BE_USED_ONLY_FOR_PARTITIONEDREGION = "Parameter(s) \"{0}\" can be used only for creating a Partitioned Region.";
  public static final String CREATE_REGION__MSG__0_IS_NOT_A_PARITIONEDREGION = "\"{0}\" is not a Partitioned Region.";
  public static final String CREATE_REGION__MSG__COLOCATEDWITH_REGION_0_IS_NOT_PARTITIONEDREGION = CREATE_REGION__COLOCATEDWITH
      + " \"{0}\" is not a Partitioned Region.";
  public static final String CREATE_REGION__MSG__COLOCATEDWITH_REGION_0_DOESNOT_EXIST = CREATE_REGION__COLOCATEDWITH
      + " \"{0}\" does not exists.";
  public static final String CREATE_REGION__MSG__REDUNDANT_COPIES_SHOULD_BE_ONE_OF_0123 = CREATE_REGION__REDUNDANTCOPIES
      + " \"{0}\" is not valid. It should be one of 0, 1, 2, 3.";
  public static final String CREATE_REGION__MSG__COULDNOT_LOAD_REGION_ATTRIBUTES_FOR_SHORTCUT_0 = "Could not load Region Attributes for a valid "
      + CREATE_REGION__REGIONSHORTCUT + "={0}. Please check logs for any errors.";
  public static final String CREATE_REGION__MSG__0_IS_A_PR_CANNOT_HAVE_SUBREGIONS = "\"{0}\" is a Partitioned Region and cannot have Subregions.";

  public static final String CREATE_REGION__COMPRESSOR = "compressor";
  public static final String CREATE_REGION__COMPRESSOR__HELP = "The fully-qualified class name of the Compressor to use when compressing region entry values.  The default is no compression.";
  public static final String CREATE_REGION__MSG__INVALID_COMPRESSOR = "{0} is an invalid Compressor."; // leave space in the end

  public static final String CREATE_REGION__OFF_HEAP = "off-heap";
  public static final String CREATE_REGION__OFF_HEAP__HELP = "Causes the values of the region to be stored in off-heap memory. The default is on heap.";

  /* debug command */
  public static final String DEBUG = "debug";
  public static final String DEBUG__HELP = "Enable/Disable debugging output in GFSH.";
  public static final String DEBUG__STATE = "state";
  public static final String DEBUG__STATE__HELP = "ON or OFF to enable or disable debugging output.";
  public static final String DEBUG__MSG_DEBUG_STATE_IS = "Debug is ";
  public static final String DEBUG__MSG_0_INVALID_STATE_VALUE = "Invalid state value : {0}. It should be \"ON\" or \"OFF\" ";

  /* deploy command */
  public static final String DEPLOY = "deploy";
  public static final String DEPLOY__HELP = "Deploy JARs to a member or members.  Only one of either --jar or --dir may be specified.";
  public static final String DEPLOY__DIR = "dir";
  public static final String DEPLOY__DIR__HELP = "Directory from which to deploy the JARs.";
  public static final String DEPLOY__GROUP = "group";
  public static final String DEPLOY__GROUP__HELP = "Group(s) to which the specified JARs will be deployed. If not specified, deploy will occur on all members.";
  public static final String DEPLOY__JAR = "jar";
  public static final String DEPLOY__JAR__HELP = "Path of the JAR to deploy.";

  /* describe config command */
  public static final String DESCRIBE_CONFIG = "describe config";
  public static final String DESCRIBE_CONFIG__HELP = "Display configuration details of a member or members.";
  public static final String DESCRIBE_CONFIG__MEMBER = "member";
  public static final String DESCRIBE_CONFIG__MEMBER__HELP = "Name/Id of the member whose configuration will be described.";
  public static final String DESCRIBE_CONFIG__HIDE__DEFAULTS = "hide-defaults";
  public static final String DESCRIBE_CONFIG__HIDE__DEFAULTS__HELP = "Whether to hide configuration information for properties with the default value.";
  public static final String DESCRIBE_CONFIG__MEMBER__NOT__FOUND = "Member \"{0}\" not found";
  public static final String DESCRIBE_CONFIG__HEADER__TEXT = "Configuration of member : \"{0}\"";

  /* 'describe connection' command */
  public static final String DESCRIBE_CONNECTION = "describe connection";
  public static final String DESCRIBE_CONNECTION__HELP = "Display information about the current connection.";

  /* 'describe disk-store' command */
  public static final String DESCRIBE_DISK_STORE = "describe disk-store";
  public static final String DESCRIBE_DISK_STORE__HELP = "Display information about a member's disk store.";
  public static final String DESCRIBE_DISK_STORE__MEMBER = "member";
  public static final String DESCRIBE_DISK_STORE__MEMBER__HELP = "Name/Id of the member with the disk store to be described.";
  public static final String DESCRIBE_DISK_STORE__NAME = "name";
  public static final String DESCRIBE_DISK_STORE__NAME__HELP = "Name of the disk store to be described.";
  public static final String DESCRIBE_DISK_STORE__ERROR_MESSAGE = "An error occurred while collecting Disk Store information for member (%1$s) with disk store (%2$s) in the Geode cluster: %3$s";

  /* 'describe member' command */
  public static final String DESCRIBE_MEMBER = "describe member";
  public static final String DESCRIBE_MEMBER__HELP = "Display information about a member, including name, id, groups, regions, etc.";
  public static final String DESCRIBE_MEMBER__IDENTIFIER = "name";
  public static final String DESCRIBE_MEMBER__IDENTIFIER__HELP = "Name/Id of the member to be described.";
  public static final String DESCRIBE_MEMBER__MSG__NOT_FOUND = "Member \"{0}\" not found";
  public static final String DESCRIBE_MEMBER__MSG__INFO_FOR__0__COULD_NOT_BE_RETRIEVED = "Information for the member \"{0}\" could not be retrieved.";

  /* 'describe offline-disk-store' command */
  public static final String DESCRIBE_OFFLINE_DISK_STORE = "describe offline-disk-store";
  public static final String DESCRIBE_OFFLINE_DISK_STORE__HELP = "Display information about an offline disk store.";
  public static final String DESCRIBE_OFFLINE_DISK_STORE__DISKSTORENAME = "name";
  public static final String DESCRIBE_OFFLINE_DISK_STORE__DISKSTORENAME__HELP = "Name of the disk store to be described.";
  public static final String DESCRIBE_OFFLINE_DISK_STORE__REGIONNAME = "region";
  public static final String DESCRIBE_OFFLINE_DISK_STORE__REGIONNAME__HELP = "Name/Path of the region in the disk store to be described.";
  public static final String DESCRIBE_OFFLINE_DISK_STORE__DISKDIRS = "disk-dirs";
  public static final String DESCRIBE_OFFLINE_DISK_STORE__DISKDIRS__HELP = "Directories which contain the disk store files.";
  public static final String DESCRIBE_OFFLINE_DISK_STORE__PDX_TYPES = "pdx";
  public static final String DESCRIBE_OFFLINE_DISK_STORE__PDX_TYPES__HELP = "Display all the pdx types stored in the disk store";
  
  /* 'export offline-disk-store' command */
  public static final String EXPORT_OFFLINE_DISK_STORE = "export offline-disk-store";
  public static final String EXPORT_OFFLINE_DISK_STORE__HELP = "Export region data from an offline disk store into Geode snapshot files.";
  public static final String EXPORT_OFFLINE_DISK_STORE__DISKSTORENAME = "name";
  public static final String EXPORT_OFFLINE_DISK_STORE__DISKSTORENAME__HELP = "Name of the disk store to be exported.";
  public static final String EXPORT_OFFLINE_DISK_STORE__DIR = "dir";
  public static final String EXPORT_OFFLINE_DISK_STORE__DIR__HELP = "Directory to export snapshot files to.";
  public static final String EXPORT_OFFLINE_DISK_STORE__DISKDIRS = "disk-dirs";
  public static final String EXPORT_OFFLINE_DISK_STORE__DISKDIRS__HELP = "Directories which contain the disk store files.";
  public static final String EXPORT_OFFLINE_DISK_STORE__ERROR = "Error exporting disk store {0} is : {1}";
  public static final String EXPORT_OFFLINE_DISK_STORE__SUCCESS = "Exported all regions from disk store {0} to the directory {1}";

  /* 'describe region' command */
  public static final String DESCRIBE_REGION = "describe region";
  public static final String DESCRIBE_REGION__HELP = "Display the attributes and key information of a region.";
  public static final String DESCRIBE_REGION__NAME = "name";
  public static final String DESCRIBE_REGION__NAME__HELP = "Name/Path of the region to be described.";
  public static final String DESCRIBE_REGION__MSG__NOT_FOUND = "Region not found";
  public static final String DESCRIBE_REGION__MSG__ERROR = "Error";
  public static final String DESCRIBE_REGION__ATTRIBUTE__TYPE = "Type";
  public static final String DESCRIBE_REGION__MEMBER = "Member";
  public static final String DESCRIBE_REGION__ATTRIBUTE__NAME = "Name";
  public static final String DESCRIBE_REGION__ATTRIBUTE__VALUE = "Value";
  public static final String DESCRIBE_REGION__NONDEFAULT__COMMONATTRIBUTES__HEADER = "Non-Default Attributes Shared By {0}  ";
  public static final String DESCRIBE_REGION__NONDEFAULT__PERMEMBERATTRIBUTES__HEADER = "Non-Default Attributes Specific To The {0} ";
  public static final String DESCRIBE_REGION__HOSTING__MEMBER = "Hosting Members";
  public static final String DESCRIBE_REGION__ACCESSOR__MEMBER = "Accessor Members";
  public static final String DESCRIBE_REGION__ATTRIBUTE__TYPE__REGION = "Region";
  public static final String DESCRIBE_REGION__ATTRIBUTE__TYPE__PARTITION = "Partition";
  public static final String DESCRIBE_REGION__ATTRIBUTE__TYPE__EVICTION = "Eviction";

  /* 'destroy disk-store' command */
  public static final String DESTROY_DISK_STORE = "destroy disk-store";
  public static final String DESTROY_DISK_STORE__HELP = "Destroy a disk store, including deleting all files on disk used by the disk store. Data for closed regions previously using the disk store will be lost.";
  public static final String DESTROY_DISK_STORE__NAME = "name";
  public static final String DESTROY_DISK_STORE__NAME__HELP = "Name of the disk store that will be destroyed.";
  public static final String DESTROY_DISK_STORE__GROUP = "group";
  public static final String DESTROY_DISK_STORE__GROUP__HELP = "Group(s) of members on which the disk store will be destroyed. If no group is specified the disk store will be destroyed on all members.";
  public static final String DESTROY_DISK_STORE__ERROR_WHILE_DESTROYING_REASON_0 = "An error occurred while destroying the disk store: \"{0}\"";

  /* 'destroy function' command */
  public static final String DESTROY_FUNCTION = "destroy function";
  public static final String DESTROY_FUNCTION__HELP = "Destroy/Unregister a function. The default is for the function to be unregistered from all members.";
  public static final String DESTROY_FUNCTION__ID = "id";
  public static final String DESTROY_FUNCTION__ID__HELP = "ID of the function.";
  public static final String DESTROY_FUNCTION__ONGROUPS = "groups";
  public static final String DESTROY_FUNCTION__ONGROUPS__HELP = "Groups of members from which this function will be unregistered.";
  public static final String DESTROY_FUNCTION__ONMEMBER = "member";
  public static final String DESTROY_FUNCTION__ONMEMBER__HELP = "Name/Id of the member from which this function will be unregistered.";
  public static final String DESTROY_FUNCTION__MSG__CANNOT_EXECUTE = "Cannot execute";
  public static final String DESTROY_FUNCTION__MSG__PROVIDE_OPTION = "Provide either --groups or --member";

  /* 'destroy index' command */
  public static final String DESTROY_INDEX = "destroy index";
  public static final String DESTROY_INDEX__HELP = "Destroy/Remove the specified index.";
  public static final String DESTROY_INDEX__NAME = "name";
  public static final String DESTROY_INDEX__NAME__HELP = "Name of the index to remove.";
  public static final String DESTROY_INDEX__MEMBER = "member";
  public static final String DESTROY_INDEX__MEMBER__HELP = "Name/Id of the member from which the index will be removed.";
  public static final String DESTROY_INDEX__REGION = "region";
  public static final String DESTROY_INDEX__REGION__HELP = "Name/Path of the region from which the index will be removed.";
  public static final String DESTROY_INDEX__GROUP = "group";
  public static final String DESTROY_INDEX__GROUP__HELP = "Group of members from which the index will be removed.";
  public static final String DESTROY_INDEX__SUCCESS__MSG = "Index \"{0}\" successfully destroyed on the following members";
  public static final String DESTROY_INDEX__ON__REGION__SUCCESS__MSG = "Index \"{0}\" on region : \"{1}\" successfully destroyed on the following members";
  public static final String DESTROY_INDEX__ON__REGION__ONLY__SUCCESS__MSG = "Indexes on region : {0} successfully destroyed on the following members";
  public static final String DESTROY_INDEX__ON__MEMBERS__ONLY__SUCCESS__MSG = "Indexes successfully destroyed on the following members";
  public static final String DESTROY_INDEX__FAILURE__MSG = "Index \"{0}\" could not be destroyed for following reasons";
  public static final String DESTROY_INDEX__INDEX__NOT__FOUND = "Index named \"{0}\" not found";
  public static final String DESTROY_INDEX__REGION__NOT__FOUND = "Region \"{0}\" not found";
  public static final String DESTROY_INDEX__INVALID__NAME = "Invalid index name";
  public static final String DESTROY_INDEX__REASON_MESSAGE = "{0}.";
  public static final String DESTROY_INDEX__EXCEPTION__OCCURRED__ON = "Occurred on following members";
  public static final String DESTROY_INDEX__NUMBER__AND__MEMBER = "{0}. {1}";

  /* 'destroy region' command */
  public static final String DESTROY_REGION = "destroy region";
  public static final String DESTROY_REGION__HELP = "Destroy/Remove a region.";
  public static final String DESTROY_REGION__REGION = "name";
  public static final String DESTROY_REGION__REGION__HELP = "Name/Path of the region to be removed.";

  public static final String DESTROY_REGION__MSG__REGIONPATH_0_NOT_VALID = "Region path \"{0}\" is not valid.";
  public static final String DESTROY_REGION__MSG__COULDNOT_FIND_REGIONPATH_0_IN_GEODE = "Could not find a Region with Region path \"{0}\" in this Geode cluster. If region was recently created, please wait for at least {1} to allow the associated Management resources to be federated.";
  public static final String DESTROY_REGION__MSG__AND_ITS_SUBREGIONS = "and its subregions";
  public static final String DESTROY_REGION__MSG__REGION_0_1_DESTROYED = "\"{0}\" {1} destroyed successfully.";
  public static final String DESTROY_REGION__MSG__ERROR_OCCURRED_WHILE_DESTROYING_0_REASON_1 = "Error occurred while destroying region \"{0}\". Reason: {1}";
  public static final String DESTROY_REGION__MSG__UNKNOWN_RESULT_WHILE_DESTROYING_REGION_0_REASON_1 = "Unknown result while destroying region \"{0}\". Reason: {1}";
  public static final String DESTROY_REGION__MSG__COULDNOT_FIND_MEMBER_WITH_REGION_0 = "Could not find a Geode member which hosts a region with Region path \"{0}\"";
  public static final String DESTROY_REGION__MSG__SPECIFY_REGIONPATH_TO_DESTROY = "Please specify region path for the region to be destroyed.";
  public static final String DESTROY_REGION__MSG__ERROR_WHILE_DESTROYING_REGION_0_REASON_1 = "Error while destroying region {0}. Reason: {1}";

  /* disconnect command */
  public static final String DISCONNECT = "disconnect";
  public static final String DISCONNECT__HELP = "Close the current connection, if one is open.";
  public static final String DISCONNECT__MSG__DISCONNECTED = "Disconnected from : {0}";
  public static final String DISCONNECT__MSG__ERROR = "Error occurred while disconnecting: {0}";
  public static final String DISCONNECT__MSG__NOTCONNECTED = "Not connected!";

  /* echo command */
  public static final String ECHO = "echo";
  public static final String ECHO__HELP = "Echo the given text which may include system and user variables.";
  public static final String ECHO__STR = "string";
  public static final String ECHO__STR__HELP = "String to be echoed. For example, \"SYS_USER variable is set to ${SYS_USER}\".";
  public static final String ECHO__MSG__NO_GFSH_INSTANCE = "Could not get GFSH Instance";

  /* 'encrypt password' command */
  public static final String ENCRYPT = "encrypt password";
  public static final String ENCRYPT__HELP = "Encrypt a password for use in data source configuration.";
  public static final String ENCRYPT_STRING = "password";
  public static final String ENCRYPT_STRING__HELP = "Password to be encrypted.";

  /* 'execute function' command */
  public static final String EXECUTE_FUNCTION = "execute function";
  public static final String EXECUTE_FUNCTION__HELP = "Execute the function with the specified ID. By default will execute on all members.";
  public static final String EXECUTE_FUNCTION__ID = "id";
  public static final String EXECUTE_FUNCTION__ID__HELP = "ID of the function to execute.";
  public static final String EXECUTE_FUNCTION__ONGROUPS = "groups";
  public static final String EXECUTE_FUNCTION__ONGROUPS__HELP = "Groups of members on which the function will be executed.";
  public static final String EXECUTE_FUNCTION__ONMEMBER = "member";
  public static final String EXECUTE_FUNCTION__ONMEMBER__HELP = "Name/Id of the member on which the function will be executed.";
  public static final String EXECUTE_FUNCTION__ONREGION = "region";
  public static final String EXECUTE_FUNCTION__ONREGION__HELP = "Region on which the data dependent function will be executed.";
  public static final String EXECUTE_FUNCTION__ARGUMENTS = "arguments";
  public static final String EXECUTE_FUNCTION__ARGUMENTS__HELP = "Arguments to the function in comma separated String format.";
  public static final String EXECUTE_FUNCTION__RESULTCOLLECTOR = "result-collector";
  public static final String EXECUTE_FUNCTION__RESULTCOLLECTOR__HELP = "Fully qualified class name of the ResultCollector to instantiate for gathering results.";
  public static final String EXECUTE_FUNCTION__FILTER = "filter";
  public static final String EXECUTE_FUNCTION__FILTER__HELP = "Key list which causes the function to only be executed on members which have entries with these keys.";
  public static final String EXECUTE_FUNCTION__MSG__MISSING_FUNCTIONID = "Provide FunctionId";
  public static final String EXECUTE_FUNCTION__MSG__MISSING_OPTIONS = "Provide one of region/member/groups";
  public static final String EXECUTE_FUNCTION__MSG__OPTIONS = "Provide Only one of region/member/groups";
  public static final String EXECUTE_FUNCTION__MSG__NO_FUNCTION_FOR_FUNCTIONID = "For the functionId provided could not retrieve function. Function may not be registered";
  public static final String EXECUTE_FUNCTION__MSG__NO_FUNCTION_EXECUTION = "FunctionService could not create execution. Can not execute Function";  
  public static final String EXECUTE_FUNCTION__MSG__NO_ASSOCIATED_MEMBER = "Could not find a member matching";
  public static final String EXECUTE_FUNCTION__MSG__HAS_NO_MEMBER = "No member to execute on";
  public static final String EXECUTE_FUNCTION__MSG__COULD_NOT_EXECUTE_FUNCTION_0_ON_MEMBER_1_ERROR_2 = "Could not execute function :{0} on member : {1}. Details are : {2}";
  public static final String EXECUTE_FUNCTION__MSG__DOES_NOT_HAVE_FUNCTION_0_REGISTERED = "Function : {0} is not registered on member.";
  public static final String EXECUTE_FUNCTION__MSG__ERROR_IN_EXECUTING_ON_MEMBER_1_DETAILS_2 = "While executing function : {0} on member : {1} error occured : {2}";
  public static final String EXECUTE_FUNCTION__MSG__RESULT_COLLECTOR_0_NOT_FOUND_ERROR_1 = "ResultCollector : {0} not found. Error : {1}";
  public static final String EXECUTE_FUNCTION__MSG__MXBEAN_0_FOR_NOT_FOUND = "MXBean for : {0} not found. ";
  public static final String EXECUTE_FUNCTION__MSG__EXECUTING_0_ON_ENTIRE_DS = "Executing function : {0} on entire distributed system ";
  public static final String EXECUTE_FUNCTION__MSG__DS_HAS_NO_MEMBERS = "Distributed system has no members";
  public static final String EXECUTE_FUNCTION__MSG__ERROR_IN_EXECUTING_0_ON_REGION_1_DETAILS_2 = "While executing function : {0} on region : {1} error occured : {2}";
  public static final String EXECUTE_FUNCTION__MSG__ERROR_IN_RETRIEVING_EXECUTOR = "Could not retrieve executor";
  public static final String EXECUTE_FUNCTION__MSG__GROUPS_0_HAS_NO_MEMBERS = "Groups : {0} has no members";
  public static final String EXECUTE_FUNCTION__MSG__NO_ASSOCIATED_MEMBER_REGION = "Could not find a member associated with region ";
  public static final String EXECUTE_FUNCTION__MSG__COULD_NOT_RETRIEVE_ARGUMENTS = "Could not retrieve arguments";
  public static final String EXECUTE_FUNCTION__MSG__ERROR_IN_EXECUTING_0_ON_MEMBER_1_ON_REGION_2_DETAILS_3 = "While executing function : {0} on member : {1} one region : {2} error occured : {3}";
  public static final String EXECUTE_FUNCTION__MSG__MEMBER_SHOULD_NOT_HAVE_FILTER_FOR_EXECUTION = "Filters for executing on \"member\"/\"mebers of group\" is not supported.";

  /* exit command */
  public static final String EXIT = "exit";
  public static final String EXIT__HELP = "Exit GFSH and return control back to the calling process.";

  /* describe config command */
  public static final String EXPORT_CONFIG = "export config";
  public static final String EXPORT_CONFIG__HELP = "Export configuration properties for a member or members.";
  public static final String EXPORT_CONFIG__GROUP = "group";
  public static final String EXPORT_CONFIG__GROUP__HELP = "Group(s) of members whose configuration will be exported.";
  public static final String EXPORT_CONFIG__MEMBER = "member";
  public static final String EXPORT_CONFIG__MEMBER__HELP = "Name/Id of the member(s) whose configuration will be exported.";
  public static final String EXPORT_CONFIG__DIR = "dir";
  public static final String EXPORT_CONFIG__DIR__HELP = "Directory to which the exported configuration files will be written.";
  public static final String EXPORT_CONFIG__MSG__EXCEPTION = "Exception while exporting config: {0}";
  public static final String EXPORT_CONFIG__MSG__MEMBER_EXCEPTION = "Exception while exporting config for {0}: {0}";
  public static final String EXPORT_CONFIG__MSG__NOT_A_DIRECTORY = "{0} is not a directory";
  public static final String EXPORT_CONFIG__MSG__NOT_WRITEABLE = "{0} is not writeable";
  public static final String EXPORT_CONFIG__MSG__CANNOT_CREATE_DIR = "Directory {0} could not be created";

  /* 'export data' command */
  public static final String EXPORT_DATA = "export data";
  public static final String EXPORT_DATA__HELP = "Export user data from a region to a file.";
  public static final String EXPORT_DATA__REGION = "region";
  public static final String EXPORT_DATA__REGION__HELP = "Region from which data will be exported.";
  public static final String EXPORT_DATA__FILE = "file";
  public static final String EXPORT_DATA__FILE__HELP = "File to which the exported data will be written. The file must have an extension of \".gfd\".";
  public static final String EXPORT_DATA__MEMBER = "member";
  public static final String EXPORT_DATA__MEMBER__HELP = "Name/Id of a member which hosts the region. The data will be exported to the specified file on the host where the member is running.";
  public static final String EXPORT_DATA__MEMBER__NOT__FOUND = "Member {0} not found";
  public static final String EXPORT_DATA__REGION__NOT__FOUND = "Region {0} not found";
  public static final String EXPORT_DATA__SUCCESS__MESSAGE = "Data succesfully exported from region : {0} to file : {1} on host : {2}";

  /* export logs command */
  public static final String EXPORT_LOGS = "export logs";
  public static final String EXPORT_LOGS__HELP = "Export the log files for a member or members.";
  public static final String EXPORT_LOGS__DIR = "dir";
  public static final String EXPORT_LOGS__DIR__HELP = "Directory to which log files will be written.";
  public static final String EXPORT_LOGS__MEMBER = "member";
  public static final String EXPORT_LOGS__MEMBER__HELP = "Name/Id of the member whose log files will be exported.";
  public static final String EXPORT_LOGS__GROUP = "group";
  public static final String EXPORT_LOGS__GROUP__HELP = "Group of members whose log files will be exported.";
  public static final String EXPORT_LOGS__MSG__CANNOT_EXECUTE = "Cannot execute";
  public static final String EXPORT_LOGS__LOGLEVEL = LOG_LEVEL;
  public static final String EXPORT_LOGS__LOGLEVEL__HELP = "Minimum level of log entries to export. Valid values are: none, error, info, config , fine, finer and finest.  The default is \"info\".";
  public static final String EXPORT_LOGS__UPTO_LOGLEVEL = "only-log-level";
  public static final String EXPORT_LOGS__UPTO_LOGLEVEL__HELP = "Whether to only include those entries that exactly match the --log-level specified.";
  public static final String EXPORT_LOGS__STARTTIME = "start-time";
  public static final String EXPORT_LOGS__STARTTIME__HELP = "Log entries that occurred after this time will be exported. The default is no limit. Format: yyyy/MM/dd/HH/mm/ss/SSS/z OR yyyy/MM/dd";
  public static final String EXPORT_LOGS__ENDTIME = "end-time";
  public static final String EXPORT_LOGS__ENDTIME__HELP = "Log entries that occurred before this time will be exported. The default is no limit. Format: yyyy/MM/dd/HH/mm/ss/SSS/z OR yyyy/MM/dd";
  public static final String EXPORT_LOGS__MERGELOG = "merge-log";
  public static final String EXPORT_LOGS__MERGELOG__HELP = "Whether to merge logs after exporting to the target directory.";
  public static final String EXPORT_LOGS__MSG__CANNOT_MERGE = "Could not merge the files in target directory";
  public static final String EXPORT_LOGS__MSG__SPECIFY_ONE_OF_OPTION = "Specify one of group or member ID";
  public static final String EXPORT_LOGS__MSG__FUNCTION_EXCEPTION = "Error in executing function";
  public static final String EXPORT_LOGS__MSG__FILE_DOES_NOT_EXIST = "Does not exist";
  public static final String EXPORT_LOGS__MSG__TARGET_DIR_CANNOT_BE_CREATED = "Target Directory {0} cannot be created";
  public static final String EXPORT_LOGS__MSG__SPECIFY_ENDTIME = "Specify End Time.";
  public static final String EXPORT_LOGS__MSG__SPECIFY_STARTTIME = "Specify Start Time.";
  public static final String EXPORT_LOGS__MSG__INVALID_TIMERANGE = "Invalid Time Range.";
  public static final String EXPORT_LOGS__MSG__INVALID_MEMBERID = "Member : {0} is not valid.";
  public static final String EXPORT_LOGS__MSG__NO_GROUPMEMBER_FOUND = "Groups specified have no members";
  public static final String EXPORT_LOGS__MSG__FAILED_TO_EXPORT_LOG_FILES_FOR_MEMBER_0 = "Could not export log files for member {0}";

  /* export stack-trace command */
  public static final String EXPORT_STACKTRACE = "export stack-traces";
  public static final String EXPORT_STACKTRACE__HELP = "Export the stack trace for a member or members.";
  public static final String EXPORT_STACKTRACE__MEMBER = "member";
  public static final String EXPORT_STACKTRACE__MEMBER__HELP = "Name/Id of the member whose stack trace will be exported.";
  public static final String EXPORT_STACKTRACE__GROUP = "group";
  public static final String EXPORT_STACKTRACE__GROUP__HELP = "Group of members whose stack trace will be exported.";
  public static final String EXPORT_STACKTRACE_ALL_STACKS = "all-stacks";
  public static final String EXPORT_STACKTRACE_ALL_STACKS__HELP = "When set to true exports all the stackstraces.";
  public static final String EXPORT_STACKTRACE__FILE = "file";
  public static final String EXPORT_STACKTRACE__FILE__HELP = "Name of the file to which the stack traces will be written.";
  public static final String EXPORT_STACKTRACE__MEMBER__NOT__FOUND = "Member not found";
  public static final String EXPORT_STACKTRACE__SUCCESS = "stack-trace(s) exported to file: {0}";
  public static final String EXPORT_STACKTRACE__ERROR = "Error occured while showing stack-traces";
  public static final String EXPORT_STACKTRACE__HOST = "On host : ";
  public static final String EXPORT_STACKTRACE__INVALID_FILE_TYPE = "Invalid file extension. File must be a text file (.txt)";

  /* 'gc' command */
  public static final String GC = "gc";
  public static final String GC__HELP = "Force GC (Garbage Collection) on a member or members. The default is for garbage collection to occur on all caching members.";
  public static final String GC__MEMBER = "member";
  public static final String GC__MEMBER__HELP = "Name/Id of the member on which garbage collection will be done.";
  public static final String GC__GROUP = "group";
  public static final String GC__GROUP__HELP = "Group(s) of members on which garbage collection will be done.";
  public static final String GC__MSG__MEMBER_NOT_FOUND = "Member not found";
  public static final String GC__MSG__CANNOT_EXECUTE = "Cannot execute";
  public static final String GC__MSG__MEMBER_NAME = "Member ID/Name";
  public static final String GC__MSG__HEAP_SIZE_BEFORE_GC = "HeapSize (MB) Before GC";
  public static final String GC__MSG__HEAP_SIZE_AFTER_GC = "HeapSize(MB) After GC";
  public static final String GC__MSG__TOTAL_TIME_IN_GC = "Time Taken for GC in ms";


  /* get command */
  public static final String GET = "get";
  public static final String GET__HELP = "Display an entry in a region. If using a region whose key and value classes have been set, then specifying --key-class and --value-class is unnecessary.";
  public static final String GET__KEY = "key";
  public static final String GET__KEY__HELP = "String or JSON text from which to create the key.  Examples include: \"James\", \"100L\" and \"('id': 'l34s')\".";
  public static final String GET__KEYCLASS = "key-class";
  public static final String GET__KEYCLASS__HELP = "Fully qualified class name of the key's type. The default is the key constraint for the current region or String.";
  public static final String GET__LOAD = "load-on-cache-miss";
  public static final String GET__LOAD__HELP = "Explicitly enables or disables the use of any registered CacheLoaders on the specified Region when retrieving a value for the specified Key on Cache misses. (Default is true, or enabled)";
  public static final String GET__VALUEKLASS = "value-class";
  public static final String GET__VALUEKLASS__HELP = "Fully qualified class name of the value's type. The default is the value constraint for the current region or String.";
  public static final String GET__REGIONNAME = "region";
  public static final String GET__REGIONNAME__HELP = "Region from which to get the entry.";
  public static final String GET__MSG__REGIONNAME_EMPTY = "Region name is either empty or Null";
  public static final String GET__MSG__KEY_EMPTY = "Key is either empty or Null";
  public static final String GET__MSG__VALUE_EMPTY = "Value is either empty or Null";
  public static final String GET__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS = "Region <{0}> not found in any of the members";
  public static final String GET__MSG__REGION_NOT_FOUND = "Region <{0}> Not Found";
  public static final String GET__MSG__KEY_NOT_FOUND_REGION = "Key is not present in the region";

  /* help command */
  public static final String HELP = "help";
  public static final String HELP__HELP = "Display syntax and usage information for all commands or list all available commands if <command> isn't specified.";
  public static final String HELP__COMMAND = "command";
  public static final String HELP__COMMAND__HELP = "Name of the command for which help will be displayed.";
  public static final String PARAM_CONTEXT_HELP = "help:disable-string-converter";

  /* hint command */
  public static final String HINT = "hint";
  public static final String HINT__HELP = "Provide hints for a topic or list all available topics if \"topic\" isn't specified.";
  public static final String HINT__TOPICNAME = "topic";
  public static final String HINT__TOPICNAME__HELP = "Name of the topic for which hints will be displayed.";
  public static final String HINT__MSG__SHELL_NOT_INITIALIZED = "Shell is not initialized properly. Please restart the shell. Check gfsh-<timestamp>.log for errors.";
  public static final String HINT__MSG__UNKNOWN_TOPIC = "Unknown topic: {0}. Use " + HINT
      + "; to view the list of available topics.";
  public static final String HINT__MSG__TOPICS_AVAILABLE = "Hints are available for the following topics. Use \"" + HINT
      + " <topic-name>\" for a specific hint.";

  /* history command */
  public static final String HISTORY = "history";
  public static final String HISTORY__HELP = "Display or export previously executed GFSH commands.";
  public static final String HISTORY__FILE = "file";
  public static final String HISTORY__FILE__HELP = "File to which the history will be written.";
  public static final String HISTORY__MSG__FILE_NULL = "File should not be null";
  public static final String HISTORY__MSG__FILE_DOES_NOT_EXISTS = "File does not exist";
  public static final String HISTORY__MSG__FILE_SHOULD_NOT_BE_DIRECTORY = "File Should not be a directory";
  public static final String HISTORY__MSG__FILE_CANNOT_BE_WRITTEN = "Unable to write to the specified file";
  public static final String HISTORY__CLEAR = "clear";
  public static final String HISTORY__CLEAR__HELP = "Clears the history of GFSH commands. Takes value as true or false";
  public static final String HISTORY__MSG__DID_NOT_CLEAR_HISTORY = "Did not clear history of GFSH commands";
  public static final String HISTORY__MSG__CLEARED_HISTORY ="Successfully deleted history";


  /* 'import data' command */
  public static final String IMPORT_DATA = "import data";
  public static final String IMPORT_DATA__HELP = "Import user data from a file to a region.";
  public static final String IMPORT_DATA__REGION = "region";
  public static final String IMPORT_DATA__REGION__HELP = "Region into which data will be imported.";
  public static final String IMPORT_DATA__FILE = "file";
  public static final String IMPORT_DATA__FILE__HELP = "File from which the imported data will be read. The file must have an extension of \".gfd\".";
  public static final String IMPORT_DATA__MEMBER = "member";
  public static final String IMPORT_DATA__MEMBER__HELP = "Name/Id of a member which hosts the region. The data will be imported from the specified file on the host where the member is running.";
  public static final String IMPORT_DATA__MEMBER__NOT__FOUND = "Member {0} not found.";
  public static final String IMPORT_DATA__REGION__NOT__FOUND = "Region {0} not found.";
  public static final String IMPORT_DATA__SUCCESS__MESSAGE = "Data imported from file : {0} on host : {1} to region : {2}";

  /* 'list async-event-queues' command */
  public static final String LIST_ASYNC_EVENT_QUEUES = "list async-event-queues";
  public static final String LIST_ASYNC_EVENT_QUEUES__HELP = "Display the Async Event Queues for all members.";
  public static final String LIST_ASYNC_EVENT_QUEUES__ERROR_WHILE_LISTING_REASON_0 = "An error occurred while collecting queue information: \"{0}\"";
  public static final String LIST_ASYNC_EVENT_QUEUES__NO_QUEUES_FOUND_MESSAGE = "No Async Event Queues Found";


  /* 'list client' command */
  public static final String LIST_CLIENTS = "list clients";
  public static final String LIST_CLIENT__HELP = "Display list of connected clients";
  public static final String TOPIC_LIST = "Display list of connected clients";
  public static final String LIST_CLIENT_COULD_NOT_RETRIEVE_CLIENT_LIST_0 = "Could not retrieve list of clients. Reason : {0}";
  public static final String LIST_CLIENT_COULD_NOT_RETRIEVE_SERVER_LIST = "No cache-servers were observed.";
  public static final String LIST_COULD_NOT_RETRIEVE_CLIENT_LIST = "No clients were retrieved for cache-servers.";
  public static final String LIST_CLIENT_COLUMN_SERVERS = "Server Name / ID";
  public static final String LIST_CLIENT_COLUMN_Clients = "Client Name / ID";
  
  /* 'describe client' command */
  public static final String DESCRIBE_CLIENT = "describe client";
  public static final String DESCRIBE_CLIENT__HELP = "Display details of specified client";
  public static final String DESCRIBE_CLIENT__ID = "clientID";
  public static final String DESCRIBE_CLIENT__ID__HELP = "ID of a client for which details are needed";
  public static final String DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_SERVER_LIST = "No cache-servers were observed.";
  public static final String DESCRIBE_CLIENT__CLIENT__ID__NOT__FOUND__0 = "Specified Client ID {0} not present";
  public static final String DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_CLIENT_0 = "Could not retrieve client. Reason : {0}";
  public static final String DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_STATS_FOR_CLIENT_0 = "Could not retrieve stats for client : {0}";
  public static final String DESCRIBE_CLIENT_COULD_NOT_RETRIEVE_STATS_FOR_CLIENT_0_REASON_1 = "Could not retrieve stats for client : {0}. Reason : {1}";
  public static final String DESCRIBE_CLIENT_ERROR_FETCHING_STATS_0 = "Error occured while fetching stats. Reason : {0}";
  public static final String DESCRIBE_CLIENT_NO_MEMBERS = "DS has no members";
  public static final String DESCRIBE_CLIENT_COLUMN_PRIMARY_SERVERS = "Primary Servers";
  public static final String DESCRIBE_CLIENT_COLUMN_SECONDARY_SERVERS = "Secondary Servers";
  public static final String DESCRIBE_CLIENT_COLUMN_CPU = "CPU";
  public static final String DESCRIBE_CLIENT_COLUMN_LISTNER_CALLS = "Number of Cache Listner Calls";
  public static final String DESCRIBE_CLIENT_COLUMN_GETS = "Number of Gets";
  public static final String DESCRIBE_CLIENT_COLUMN_MISSES = "Number of Misses";
  public static final String DESCRIBE_CLIENT_COLUMN_PUTS = "Number of Puts";
  public static final String DESCRIBE_CLIENT_COLUMN_THREADS = "Number of Threads";
  public static final String DESCRIBE_CLIENT_COLUMN_PROCESS_CPU_TIME = "Process CPU Time (nanoseconds)";
  public static final String DESCRIBE_CLIENT_COLUMN_QUEUE_SIZE = "Queue size";
  public static final String DESCRIBE_CLIENT_COLUMN_UP_TIME = "UP Time (seconds)";
  public static final String DESCRIBE_CLIENT_COLUMN_DURABLE = "Is Durable";
  public static final String DESCRIBE_CLIENT_MIN_CONN = "Minimum Connections";
  public static final String DESCRIBE_CLIENT_MAX_CONN = "Maximum Connections";
  public static final String DESCRIBE_CLIENT_REDUDANCY = "Redudancy";
  public static final String DESCRIBE_CLIENT_CQs = "Num of CQs";
  
  
  
  /* list deployed command */
  public static final String LIST_DEPLOYED = "list deployed";
  public static final String LIST_DEPLOYED__HELP = "Display a list of JARs that were deployed to members using the \"deploy\" command.";
  public static final String LIST_DEPLOYED__GROUP = "group";
  public static final String LIST_DEPLOYED__GROUP__HELP = "Group(s) of members for which deployed JARs will be displayed.  If not specified, JARs for all members will be displayed.";
  public static final String LIST_DEPLOYED__NO_JARS_FOUND_MESSAGE = "No JAR Files Found";

  /* list function command */
  public static final String LIST_FUNCTION = "list functions";
  public static final String LIST_FUNCTION__HELP = "Display a list of registered functions. The default is to display functions for all members.";
  public static final String LIST_FUNCTION__MATCHES = "matches";
  public static final String LIST_FUNCTION__MATCHES__HELP = "Pattern that the function ID must match in order to be included. Uses Java pattern matching rules, not UNIX. For example, to match any character any number of times use \".*\" instead of \"*\".";
  public static final String LIST_FUNCTION__GROUP = "group";
  public static final String LIST_FUNCTION__GROUP__HELP = "Group(s) of members for which functions will be displayed.";
  public static final String LIST_FUNCTION__MEMBER = "member";
  public static final String LIST_FUNCTION__MEMBER__HELP = "Name/Id of the member(s) for which functions will be displayed.";
  public static final String LIST_FUNCTION__NO_FUNCTIONS_FOUND_ERROR_MESSAGE = "No Functions Found";

  public static final String LIST_GATEWAY = "list gateways";
  public static final String LIST_GATEWAY__HELP = "Display the Gateway Senders and Receivers for a member or members.";
  public static final String LIST_GATEWAY__GROUP = "group";
  public static final String LIST_GATEWAY__GROUP__HELP = "Group(s) of members for which Gateway Senders and Receivers will be displayed.";
  public static final String LIST_GATEWAY__MEMBER = "member";
  public static final String LIST_GATEWAY__MEMBER__HELP = "Member(s) for which Gateway Senders and Receivers will be displayed.";
  /* list index */
  public static final String LIST_INDEX = "list indexes";
  public static final String LIST_INDEX__HELP = "Display the list of indexes created for all members.";
  public static final String LIST_INDEX__ERROR_MESSAGE = "An error occurred while collecting all Index information across the Geode cluster: %1$s";
  public static final String LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE = "No Indexes Found";
  public static final String LIST_INDEX__STATS = "with-stats";
  public static final String LIST_INDEX__STATS__HELP = "Whether statistics should also be displayed.";

  /* list disk-store command */
  public static final String LIST_DISK_STORE = "list disk-stores";
  public static final String LIST_DISK_STORE__HELP = "Display disk stores for all members.";
  public static final String LIST_DISK_STORE__ERROR_MESSAGE = "An error occurred while collecting Disk Store information for all members across the Geode cluster: %1$s";
  public static final String LIST_DISK_STORE__DISK_STORES_NOT_FOUND_MESSAGE = "No Disk Stores Found";

  /* 'list member' command */
  public static final String LIST_MEMBER = "list members";
  public static final String LIST_MEMBER__HELP = "Display all or a subset of members.";
  public static final String LIST_MEMBER__GROUP = "group";
  public static final String LIST_MEMBER__GROUP__HELP = "Group name for which members will be displayed.";
  public static final String LIST_MEMBER__MSG__NO_MEMBER_FOUND = NO_MEMBERS_FOUND_MESSAGE;

  /* 'list region' command */
  public static final String LIST_REGION = "list regions";
  public static final String LIST_REGION__HELP = "Display regions of a member or members.";
  public static final String LIST_REGION__GROUP = "group";
  public static final String LIST_REGION__GROUP__HELP = "Group of members for which regions will be displayed.";
  public static final String LIST_REGION__MEMBER = "member";
  public static final String LIST_REGION__MEMBER__HELP = "Name/Id of the member for which regions will be displayed.";
  public static final String LIST_REGION__MSG__NOT_FOUND = "No Regions Found";
  public static final String LIST_REGION__MSG__ERROR = "Error occurred while fetching list of regions.";

  /* load-balance gateway-sender */
  public static final String LOAD_BALANCE_GATEWAYSENDER = "load-balance gateway-sender";
  public static final String LOAD_BALANCE_GATEWAYSENDER__HELP = "Cause the Gateway Sender to close its current connections so that it reconnects to its remote receivers in a more balanced fashion.";
  public static final String LOAD_BALANCE_GATEWAYSENDER__ID = "id";
  public static final String LOAD_BALANCE_GATEWAYSENDER__ID__HELP = "ID of the Gateway Sender.";

  /* locate entry command */
  public static final String LOCATE_ENTRY = "locate entry";
  public static final String LOCATE_ENTRY__HELP = "Identifies the location, including host, member and region, of entries that have the specified key.";
  public static final String LOCATE_ENTRY__KEY = "key";
  public static final String LOCATE_ENTRY__KEY__HELP = "String or JSON text from which to create a key.  Examples include: \"James\", \"100L\" and \"('id': 'l34s')\".";
  public static final String LOCATE_ENTRY__KEYCLASS = "key-class";
  public static final String LOCATE_ENTRY__KEYCLASS__HELP = "Fully qualified class name of the key's type. The default is java.lang.String.";
  public static final String LOCATE_ENTRY__VALUEKLASS = "value-class";
  public static final String LOCATE_ENTRY__VALUEKLASS__HELP = "Fully qualified class name of the value's type. The default is java.lang.String.";
  public static final String LOCATE_ENTRY__REGIONNAME = "region";
  public static final String LOCATE_ENTRY__REGIONNAME__HELP = "Region in which to locate values.";
  public static final String LOCATE_ENTRY__RECURSIVE = "recursive";
  public static final String LOCATE_ENTRY__RECURSIVE__HELP = "Whether to traverse regions and subregions recursively.";
  public static final String LOCATE_ENTRY__MSG__REGIONNAME_EMPTY = "Region name is either empty or Null";
  public static final String LOCATE_ENTRY__MSG__KEY_EMPTY = "Key is either empty or Null";
  public static final String LOCATE_ENTRY__MSG__VALUE_EMPTY = "Value is either empty or Null";
  public static final String LOCATE_ENTRY__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS = "Region <{0}> not found in any of the members";
  public static final String LOCATE_ENTRY__MSG__REGION_NOT_FOUND = "Region <{0}> Not Found";
  public static final String LOCATE_ENTRY__MSG__KEY_NOT_FOUND_REGION = "Key is not present in the region";

  /* 'netstat' command */
  public static final String NETSTAT = "netstat";
  public static final String NETSTAT__HELP = "Report network information and statistics via the \"netstat\" operating system command.";
  public static final String NETSTAT__MEMBER = "member";
  public static final String NETSTAT__MEMBER__HELP = "Name/Id of the member(s) on which to run the netstat command.";
  public static final String NETSTAT__GROUP = "group";
  public static final String NETSTAT__GROUP__HELP = "Group of members on which to run the netstat command.";
  public static final String NETSTAT__FILE = "file";
  public static final String NETSTAT__FILE__HELP = "Text file to which output from the netstat command will be written. A \".txt\" extention will be added if it's not already a part of the specified name.";
  public static final String NETSTAT__WITHLSOF = "with-lsof";
  public static final String NETSTAT__WITHLSOF__HELP = "Whether lsof (list open files) command output should also be displayed. Not applicable for \"Microsoft Windows(TM)\" hosts.";
  public static final String NETSTAT__MSG__FOR_HOST_1_OS_2_MEMBER_0 = "Host: {1}{3}OS: {2}{3}Member(s):{3} {0}"; // {3} for line separator
  public static final String NETSTAT__MSG__ERROR_OCCURRED_WHILE_EXECUTING_NETSTAT_ON_0 = "Error occurred while executing "
      + CliStrings.NETSTAT + " on {0}";
  public static final String NETSTAT__MSG__SAVED_OUTPUT_IN_0 = "Saved " + CliStrings.NETSTAT + " output in the file {0}.";
  public static final String NETSTAT__MSG__COULD_NOT_FIND_MEMBERS_0 = "Could not find member(s) with Id(s) or name(s): {0}.";
  public static final String NETSTAT__MSG__ONLY_ONE_OF_MEMBER_OR_GROUP_SHOULD_BE_SPECIFIED = "Only one of --group or --member should be specified.";
  public static final String NETSTAT__MSG__NOT_AVAILABLE_FOR_WINDOWS = "Not available for Windows.";
  public static final String NETSTAT__MSG__COULD_NOT_EXECUTE_0_REASON_1 = "Could not execute \"{0}\". Reason: {1}";
  public static final String NETSTAT__MSG__LSOF_NOT_IN_PATH = "lsof command not in current path";

  /* pause gateway-sender */
  public static final String PAUSE_GATEWAYSENDER = "pause gateway-sender";
  public static final String PAUSE_GATEWAYSENDER__ID = "id";;
  public static final String PAUSE_GATEWAYSENDER__MEMBER = "member";
  public static final String PAUSE_GATEWAYSENDER__GROUP = "group";
  public static final String PAUSE_GATEWAYSENDER__HELP = "Pause the Gateway Sender on a member or members.";
  public static final String PAUSE_GATEWAYSENDER__ID__HELP = "ID of the Gateway Sender.";
  public static final String PAUSE_GATEWAYSENDER__GROUP__HELP = "Group(s) of members on which to pause the Gateway Sender.";
  public static final String PAUSE_GATEWAYSENDER__MEMBER__HELP = "Name/Id of the member on which to pause the Gateway Sender.";

  /* put command */
  public static final String PUT = "put";
  public static final String PUT__HELP = "Add/Update an entry in a region. If using a region whose key and value classes have been set, then specifying --key-class and --value-class is unnecessary.";
  public static final String PUT__KEY = "key";
  public static final String PUT__KEY__HELP = "String or JSON text from which to create the key.  Examples include: \"James\", \"100L\" and \"('id': 'l34s')\".";
  public static final String PUT__VALUE = "value";
  public static final String PUT__VALUE__HELP = "String or JSON text from which to create the value.  Examples include: \"manager\", \"100L\" and \"('value': 'widget')\".";
  public static final String PUT__PUTIFABSENT = "skip-if-exists";
  public static final String PUT__PUTIFABSENT__HELP = "Skip the put operation when an entry with the same key already exists. The default is to overwrite the entry (false).";
  public static final String PUT__KEYCLASS = "key-class";
  public static final String PUT__KEYCLASS__HELP = "Fully qualified class name of the key's type. The default is java.lang.String.";
  public static final String PUT__VALUEKLASS = "value-class";
  public static final String PUT__VALUEKLASS__HELP = "Fully qualified class name of the value's type. The default is java.lang.String.";
  public static final String PUT__REGIONNAME = "region";
  public static final String PUT__REGIONNAME__HELP = "Region into which the entry will be put.";
  public static final String PUT__MSG__REGIONNAME_EMPTY = "Region name is either empty or Null";
  public static final String PUT__MSG__KEY_EMPTY = "Key is either empty or Null";
  public static final String PUT__MSG__VALUE_EMPTY = "Value is either empty or Null";
  public static final String PUT__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS = "Region <{0}> not found in any of the members";
  public static final String PUT__MSG__REGION_NOT_FOUND = "Region <{0}> Not Found";
  public static final String PUT__MSG__KEY_NOT_FOUND_REGION = "Key is not present in the region";

  public static final String QUERY = "query";
  public static final String QUERY__HELP = "Run the specified OQL query as a single quoted string and display the results in one or more pages."
      + " Limit will default to the value stored in the \""
      + Gfsh.ENV_APP_FETCH_SIZE
      + "\" variable."
      + " Page size will default to the value stored in the \"" + Gfsh.ENV_APP_COLLECTION_LIMIT + "\" variable.";
  public static final String QUERY__QUERY = "query";
  public static final String QUERY__STEPNAME = "step-name";
  public static final String QUERY__STEPNAME__DEFAULTVALUE = "ALL";
  public static final String QUERY__INTERACTIVE = "interactive";
  public static final String QUERY__QUERY__HELP = "The OQL string.";
  public static final String QUERY__INTERACTIVE__HELP = "Whether or not this query is interactive. If false then all results will be displayed at once.";
  public static final String QUERY__MSG__QUERY_EMPTY = "Query is either empty or Null";
  public static final String QUERY__MSG__INVALID_QUERY = "Query is invalid due for error : <{0}>";
  public static final String QUERY__MSG__REGIONS_NOT_FOUND = "Cannot find regions <{0}> in any of the members";
  public static final String QUERY__MSG__NOT_SUPPORTED_ON_MEMBERS = CliStrings.QUERY
      + " command should be used only from shell. Use QueryService API for running query inside Geode VMs";

  /* 'rebalance' command */
  public static final String REBALANCE = "rebalance";
  public static final String REBALANCE__HELP = "Rebalance partitioned regions. The default is for all partitioned regions to be rebalanced.";
  public static final String REBALANCE__INCLUDEREGION = "include-region";
  public static final String REBALANCE__INCLUDEREGION__HELP = "Partitioned regions to be included when rebalancing. Includes take precedence over excludes.";
  public static final String REBALANCE__EXCLUDEREGION = "exclude-region";
  public static final String REBALANCE__EXCLUDEREGION__HELP = "Partitioned regions to be excluded when rebalancing.";
  public static final String REBALANCE__TIMEOUT = "time-out";
  public static final String REBALANCE__TIMEOUT__HELP = "Time to wait (in seconds) before GFSH returns to a prompt while rebalancing continues in the background. The default is to wait for rebalancing to complete.";
  public static final String REBALANCE__SIMULATE = "simulate";
  public static final String REBALANCE__SIMULATE__HELP = "Whether to only simulate rebalancing. The --time-out parameter is not available when simulating.";
  public static final String REBALANCE__MSG__SIMULATED = "Simulated Successfully";
  public static final String REBALANCE__MSG__TIMEOUT = "Time out. Rebalance operation will continue on servers";
  public static final String REBALANCE__MSG__REBALANCED = "Rebalanced Successfully";
  public static final String REBALANCE__MSG__CANCELLED = "Rebalance Cancelled";
  public static final String REBALANCE__MSG__WAIT_INTERRUPTED = "Rebalance wait interrupted Cancelled";
  public static final String REBALANCE__MSG__NO_ASSOCIATED_DISTRIBUTED_MEMBER = "For the region {0}, no member was found";
  public static final String REBALANCE__MSG__NO_EXECUTION = "Could not execute for member:{0}";
  public static final String REBALANCE__MSG__TOTALBUCKETCREATEBYTES = "Total bytes in all redundant bucket copies created during this rebalance";
  public static final String REBALANCE__MSG__TOTALBUCKETCREATETIM = "Total time (in milliseconds) spent creating redundant bucket copies during this rebalance";
  public static final String REBALANCE__MSG__TOTALBUCKETCREATESCOMPLETED = "Total number of redundant copies created during this rebalance";
  public static final String REBALANCE__MSG__TOTALBUCKETTRANSFERBYTES = "Total bytes in buckets moved during this rebalance";
  public static final String REBALANCE__MSG__TOTALBUCKETTRANSFERTIME = "Total time (in milliseconds) spent moving buckets during this rebalance";
  public static final String REBALANCE__MSG__TOTALBUCKETTRANSFERSCOMPLETED = "Total number of buckets moved during this rebalance";
  public static final String REBALANCE__MSG__TOTALPRIMARYTRANSFERTIME = "Total time (in milliseconds) spent switching the primary state of buckets during this rebalance";
  public static final String REBALANCE__MSG__TOTALPRIMARYTRANSFERSCOMPLETED = "Total primaries transferred during this rebalance";
  public static final String REBALANCE__MSG__TOTALTIME = "Total time (in milliseconds) for this rebalance";
  public static final String REBALANCE__MSG__NO_REBALANCING_REGIONS_ON_DS = "Distributed system has no regions that can be rebalanced";
  public static final String REBALANCE__MSG__EXCEPTION_IN_REBALANCE_FOR_MEMBER_0_Exception = "Excpetion occured while rebalancing on member : {0} . Exception is ";
  public static final String REBALANCE__MSG__EXCEPTION_IN_REBALANCE_FOR_MEMBER_0_Exception_1 = "Excpetion occured while rebalancing on member : {0} . Exception is : {1}";
  public static final String REBALANCE__MSG__ERROR_IN_RETRIEVING_MBEAN = "Could not retrieve MBean for region : {0}";
  public static final String REBALANCE__MSG__NO_EXECUTION_FOR_REGION_0_ON_MEMBERS_1 = "Could not execute rebalance for region: {0} on members : {1} ";
  public static final String REBALANCE__MSG__NO_EXECUTION_FOR_REGION_0 = "Could not execute rebalance for region: {0}";
  public static final String REBALANCE__MSG__MEMBERS_MIGHT_BE_DEPARTED = " Reason : Members may be departed";
  public static final String REBALANCE__MSG__REASON = " Reason : ";
  public static final String REBALANCE__MSG__REBALANCE_WILL_CONTINUE = "Rebalance will continue in background";
  public static final String REBALANCE__MSG__REGION_NOT_ASSOCIATED_WITH_MORE_THAN_ONE_MEMBER = "No regions associated with more than 1 members";
  public static final String REBALANCE__MSG__EXCEPTION_OCCRED_WHILE_REBALANCING_0  = "Exception occured while rebelancing. Reason : {0}";

  /* remove command */
  public static final String REMOVE = "remove";
  public static final String REMOVE__HELP = "Remove an entry from a region. If using a region whose key class has been set, then specifying --key-class is unnecessary.";
  public static final String REMOVE__KEY = "key";
  public static final String REMOVE__KEY__HELP = "String or JSON text from which to create the key.  Examples include: \"James\", \"100L\" and \"('id': 'l34s')\".";
  public static final String REMOVE__KEYCLASS = "key-class";
  public static final String REMOVE__KEYCLASS__HELP = "Fully qualified class name of the key's type. The default is the key constraint for the current region or String.";
  public static final String REMOVE__REGION = "region";
  public static final String REMOVE__REGION__HELP = "Region from which to remove the entry.";
  public static final String REMOVE__ALL = "all";
  public static final String REMOVE__ALL__HELP = "Clears the region by removing all entries. Partitioned region does not support remove-all";
  public static final String REMOVE__MSG__REGIONNAME_EMPTY = "Region name is either empty or Null";
  public static final String REMOVE__MSG__KEY_EMPTY = "Key is either empty or Null";
  public static final String REMOVE__MSG__VALUE_EMPTY = "Value is either empty or Null";
  public static final String REMOVE__MSG__REGION_NOT_FOUND_ON_ALL_MEMBERS = "Region <{0}> not found in any of the members";
  public static final String REMOVE__MSG__REGION_NOT_FOUND = "Region <{0}> Not Found";
  public static final String REMOVE__MSG__KEY_NOT_FOUND_REGION = "Key is not present in the region";
  public static final String REMOVE__MSG__CLEARED_ALL_CLEARS = "Cleared all keys in the region";
  public static final String REMOVE__MSG__CLEAREALL_NOT_SUPPORTED_FOR_PARTITIONREGION = "Option --"+ REMOVE__ALL + " is not supported on partitioned region";

  /* resume gateway-sender */
  public static final String RESUME_GATEWAYSENDER = "resume gateway-sender";
  public static final String RESUME_GATEWAYSENDER__ID = "id";;
  public static final String RESUME_GATEWAYSENDER__MEMBER = "member";
  public static final String RESUME_GATEWAYSENDER__GROUP = "group";
  public static final String RESUME_GATEWAYSENDER__HELP = "Resume the Gateway Sender on a member or members.";
  public static final String RESUME_GATEWAYSENDER__ID__HELP = "ID of the Gateway Sender.";
  public static final String RESUME_GATEWAYSENDER__GROUP__HELP = "Group(s) of members on which to resume the Gateway Sender.";
  public static final String RESUME_GATEWAYSENDER__MEMBER__HELP = "Name/Id of the member on which to resume the Gateway Sender.";

  /* 'revoke missing-disk-store' command */
  public static final String REVOKE_MISSING_DISK_STORE = "revoke missing-disk-store";
  public static final String REVOKE_MISSING_DISK_STORE__HELP = "Instructs the member(s) of a distributed system to stop waiting for a disk store to be available. Only revoke a disk store if its files are lost as it will no longer be recoverable once revoking is initiated. Use the \"show missing-disk-store\" command to get descriptions of missing disk stores.";
  public static final String REVOKE_MISSING_DISK_STORE__ID = "id";
  public static final String REVOKE_MISSING_DISK_STORE__ID__HELP = "ID of the missing disk store to be revoked.";

  /* 'run' command */
  public static final String RUN = "run";
  public static final String RUN__HELP = "Execute a set of GFSH commands. Commands that normally prompt for additional input will instead use default values.";
  public static final String RUN__FILE = "file";
  public static final String RUN__FILE__HELP = "File containing the GFSH commands to execute.";
  public static final String RUN__QUIET = "quiet";
  public static final String RUN__QUIET__HELP = "Whether to show command output.";
  public static final String RUN__CONTINUEONERROR = "continue-on-error";
  public static final String RUN__CONTINUEONERROR__HELP = "Whether command execution should continue if an error is received.";

  public static final String RUN__MSG_FILE_NOT_FOUND = "Could not find specified script file: ";

  /* 'set variable' command */
  public static final String SET_VARIABLE = "set variable";
  public static final String SET_VARIABLE__HELP = "Set GFSH variables that can be used by commands. " +
                                                  "For example: if variable \"CACHE_SERVERS_GROUP\" is set then to use it with \""+CliStrings.LIST_MEMBER+"\", use \""+CliStrings.LIST_MEMBER+" --"+CliStrings.LIST_MEMBER__GROUP+"=${CACHE_SERVERS_GROUP}\". " +
                                                  "The \"echo\" command can be used to know the value of a variable.";
  public static final String SET_VARIABLE__VAR = "name";
  public static final String SET_VARIABLE__VAR__HELP = "Name for the variable. Name must only be composed of letters, numbers and the \"_\" character and may not start with a number.";
  public static final String SET_VARIABLE__VALUE = "value";
  public static final String SET_VARIABLE__VALUE__HELP = "Value that the variable will be set to.";

  public static final String USEFUL_VARIABLES = "[A] System Variables\n"
      + "1. SYS_USER          User name (read only)\n"
      + "2. SYS_USER_HOME     User's home directory (read only)\n"
      + "3. SYS_HOST_NAME     Host where GFSH is running (read only)\n"
      + "4. SYS_CLASSPATH     CLASSPATH of the GFSH JVM (read only)\n"
      + "5. SYS_JAVA_VERSION  Java version used by GFSH (read only)\n"
      + "6. SYS_OS            OS name for the host where GFSH is running (read only)\n"
      + "7. SYS_PWD           Current working directory (read only)\n\n"
      + "[B] Application Variables\n"
      + "1. APP_CONTEXT_PATH                Current context path (read only)\n"
      + "2. APP_FETCH_SIZE                  Fetch size used when querying. Valid values are: 1-100\n"
      + "3. APP_LAST_EXIT_STATUS            Numeric value for last command exit status. One of: 0 (success), 1 (error), 2 (crash) (read only)\n"
      + "4. APP_COLLECTION_LIMIT            Number of items in the embedded collection of a result to be iterated. Valid values are: 1-100.\n"
      + "5. APP_QUERY_RESULTS_DISPLAY_MODE  How command results should be shown. Valid values are: table and catalog.\n"
      + "6. APP_QUIET_EXECUTION.            Whether commands should be excuted in quiet mode. Valid values are: true and false.\n";

  /* 'sh' command */
  public static final String SH = "sh";
  public static final String SH__HELP = "Allows execution of operating system (OS) commands. Use '&' to return to gfsh prompt immediately. NOTE: Commands which pass output to another shell command are not currently supported.";
  public static final String SH__COMMAND = "command";
  public static final String SH__COMMAND__HELP = "The command to execute.";
  public static final String SH__USE_CONSOLE = "use-console";
  public static final String SH__USE_CONSOLE__HELP = "Useful on Unix systems for applications which need handle of console. Adds \"</dev/tty >/dev/tty\" to the user specified command.";

  /* show dead-lock command */
  public static final String SHOW_DEADLOCK = "show dead-locks";
  public static final String SHOW_DEADLOCK__HELP = "Display any deadlocks in the Geode distributed system.";
  public static final String SHOW_DEADLOCK__DEPENDENCIES__FILE = "file";
  public static final String SHOW_DEADLOCK__DEPENDENCIES__FILE__HELP = "Name of the file to which dependencies between members will be written.";
  public static final String SHOW_DEADLOCK__NO__DEADLOCK = "No deadlock was detected.";
  public static final String SHOW_DEADLOCK__DEADLOCK__DETECTED = "Deadlock detected.";
  public static final String SHOW_DEADLOCK__DEEPEST_FOUND = "No deadlock was detected.  Here is the deepest call chain that could be found";
  public static final String SHOW_DEADLOCK__DEPENDENCIES__REVIEW = "Please view the dependencies between the members in file : {0}";
  public static final String SHOW_DEADLOCK__ERROR = "Error";

  /* Show Log command */
  public static final String SHOW_LOG = "show log";
  public static final String SHOW_LOG_HELP = "Display the log for a member.";
  public static final String SHOW_LOG_MEMBER = "member";
  public static final String SHOW_LOG_MEMBER_HELP = "Name/Id of the member whose log file will be displayed.";
  public static final String SHOW_LOG_LINE_NUM = "lines";
  public static final String SHOW_LOG_LINE_NUM_HELP = "Number of lines from the log file to display. The maximum is 100.";
  public static final String SHOW_LOG_NO_LOG = "There is no log for this member";
  public static final String SHOW_LOG_MSG_MEMBER_NOT_FOUND = "Member not found";
  public static final String SHOW_LOG_MSG_INVALID_NUMBER = "Invalid number";
  public static final String SHOW_LOG_ERROR = "Error";

  /* show metrics */
  public static final String SHOW_METRICS = "show metrics";
  public static final String SHOW_METRICS__HELP = "Display or export metrics for the entire distributed system, a member or a region.";
  public static final String SHOW_METRICS__REGION = "region";
  public static final String SHOW_METRICS__REGION__HELP = "Name/Path of the region whose metrics will be displayed/exported.";
  public static final String SHOW_METRICS__MEMBER = "member";
  public static final String SHOW_METRICS__MEMBER__HELP = "Name/Id of the member whose metrics will be displayed/exported.";
  public static final String SHOW_METRICS__CATEGORY = "categories";
  public static final String SHOW_METRICS__CATEGORY__HELP = "Categories available based upon the parameters specified are:\n"
      + "- no parameters specified: cluster, cache, diskstore, query\n"
      + "- region specified: cluster, region, partition, diskstore, callback, eviction\n"
      + "- member specified: member, jvm, region, serialization, communication, function, transaction, diskstore, lock, eviction, distribution, offheap\n"
      + "- member and region specified: region, partition, diskstore, callback, eviction";
  public static final String SHOW_METRICS__FILE = "file";
  public static final String SHOW_METRICS__FILE__HELP = "Name of the file to which metrics will be written.";
  public static final String SHOW_METRICS__ERROR = "Unable to retrieve metrics : {0} ";
  public static final String SHOW_METRICS__TYPE__HEADER = "Category";
  public static final String SHOW_METRICS__METRIC__HEADER = "Metric";
  public static final String SHOW_METRICS__VALUE__HEADER = "Value";
  public static final String SHOW_METRICS__CACHESERVER__PORT = "port";
  public static final String SHOW_METRICS__CACHESERVER__PORT__HELP = "Port number of the Cache Server whose metrics are to be displayed/exported. This can only be used along with the --member parameter.";
  public static final String SHOW_METRICS__CANNOT__USE__CACHESERVERPORT = "If the --port parameter is specified, then the --member parameter must also be specified.";
  public static final String SHOW_METRICS__CACHE__SERVER__NOT__FOUND = "Metrics for the Cache Server with port : {0} and member : {1} not found.\n Please check the port number and the member name/id";

  /* 'show missing-disk-store' command */
  public static final String SHOW_MISSING_DISK_STORE = "show missing-disk-stores";
  public static final String SHOW_MISSING_DISK_STORE__HELP = "Display a summary of the disk stores that are currently missing from a distributed system.";
  public static final String SHOW_MISSING_DISK_STORE__ERROR_MESSAGE = "An error occurred while showing missing disk stores and missing colocated regions: %1$s";

  /* 'shutdown' command */
  public static final String SHUTDOWN = "shutdown";
  public static final String SHUTDOWN__HELP = "Stop all members.";
  public static final String SHUTDOWN__TIMEOUT = "time-out";
  public static final String SHUTDOWN__TIMEOUT__HELP = "Time to wait (in seconds) for a graceful shutdown. Should be at least 10 sec";
  public static final String INCLUDE_LOCATORS = "include-locators";
  public static final String INCLUDE_LOCATORS_HELP = "To shutdown locators specify this option as true. Default is false";
  public static final String SHUTDOWN__MSG__CANNOT_EXECUTE = "Cannot execute";
  public static final String SHUTDOWN__MSG__ERROR = "Exception occured while shutdown. Reason : {0}";  
  public static final String SHUTDOWN__MSG__MANAGER_NOT_FOUND = "Could not locate Manager.";
  public static final String SHUTDOWN__MSG__WARN_USER = "As a lot of data in memory will be lost, including possibly events in queues, do you really want to shutdown the entire distributed system?";
  public static final String SHUTDOWN__MSG__ABORTING_SHUTDOWN = "Aborting shutdown of the entire distributed system";
  public static final String SHUTDOWN__MSG__SHUTDOWN_ENTIRE_DS = "Shutting down entire distributed system";
  public static final String SHUTDOWN__MSG__IMPROPER_TIMEOUT = "time-out should not be less than 10 sec.";
  public static final String SHUTDOWN__MSG__CAN_NOT_SHUTDOWN_WITHIN_TIMEOUT = "Could not shutdown within timeout. Shutdown will continue in background";
  public static final String SHUTDOWN__MSG__NO_DATA_NODE_FOUND = "No data node found for stopping. Please specify --shutdown-locators option if you want locators to be stopped";
  
  public static final String SHUTDOWN_TIMEDOUT = "Shutdown command timedout. Please manually check node status";
  
  /* change log level */
  public static final String CHANGE_LOGLEVEL = "change loglevel";
  public static final String CHANGE_LOGLEVEL__HELP = "This command changes log-level run time on specified servers."; 
  public static final String CHANGE_LOGLEVEL__GROUPS = "groups";
  public static final String CHANGE_LOGLEVEL__GROUPS__HELP = "Groups of members to change the log-level";
  public static final String CHANGE_LOGLEVEL__MEMBER = "members";
  public static final String CHANGE_LOGLEVEL__MEMBER__HELP = "Name/Id of the member to change the log-level";
  public static final String CHANGE_LOGLEVEL__LOGLEVEL = "loglevel";
  public static final String CHANGE_LOGLEVEL__LOGLEVEL__HELP = "Log level to change to";
  public static final String CHANGE_LOGLEVEL__MSG__SPECIFY_GRP_OR_MEMBER = "Specify one of group or member";
  public static final String CHANGE_LOGLEVEL__MSG__INVALID_LOG_LEVEL = "Specified log-level is invalid";
  public static final String CHANGE_LOGLEVEL__MSG__SPECIFY_LOG_LEVEL = "Specify valid log-level";
  public static final String CHANGE_LOGLEVEL__MSG__CANNOT_EXECUTE = "Cannot execute change log-level.";
  public static final String CHANGE_LOGLEVEL__COLUMN_MEMBER = "Member";
  public static final String CHANGE_LOGLEVEL__COLUMN_STATUS = "Changed log-level";
  public static final String CHANGE_LOGLEVEL__MSG_NO_MEMBERS = "No members were observed for changing log-level.";

  /* 'sleep' command */
  public static final String SLEEP = "sleep";
  public static final String SLEEP__HELP = "Delay for a specified amount of time in seconds - floating point values are allowed.";
  public static final String SLEEP__TIME = "time";
  public static final String SLEEP__TIME__HELP = "Number of Seconds to sleep for.";

  /* create gateway-receiver */
  public static final String CREATE_GATEWAYRECEIVER = "create gateway-receiver";
  public static final String CREATE_GATEWAYRECEIVER__HELP = "Create the Gateway Receiver on a member or members.";
  public static final String CREATE_GATEWAYRECEIVER__GROUP = "group";
  public static final String CREATE_GATEWAYRECEIVER__GROUP__HELP = "Group(s) of members on which to create the Gateway Receiver.";
  public static final String CREATE_GATEWAYRECEIVER__MEMBER = "member";
  public static final String CREATE_GATEWAYRECEIVER__MEMBER__HELP = "Name/Id of the member on which to create the Gateway Receiver.";
  public static final String CREATE_GATEWAYRECEIVER__STARTPORT = "start-port";
  public static final String CREATE_GATEWAYRECEIVER__STARTPORT__HELP = "Starting value of the port range from which the GatewayReceiver's port will be chosen.";
  public static final String CREATE_GATEWAYRECEIVER__ENDPORT = "end-port";
  public static final String CREATE_GATEWAYRECEIVER__ENDPORT__HELP = "End value of the port range from which the GatewayReceiver's port will be chosen.";
  public static final String CREATE_GATEWAYRECEIVER__BINDADDRESS = "bind-address";
  public static final String CREATE_GATEWAYRECEIVER__BINDADDRESS__HELP = "The IP address or host name that the receiver's socket will listen on for client connections.";
  public static final String CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS = "maximum-time-between-pings";
  public static final String CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS__HELP = "The maximum amount of time between client pings.";
  public static final String CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE = SOCKET_BUFFER_SIZE;
  public static final String CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE__HELP = "The buffer size in bytes of the socket connection for this GatewayReceiver.";
  public static final String CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER = "gateway-transport-filter";
  public static final String CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER__HELP = "The fully qualified class names of GatewayTransportFilters (separated by comma) to be added to the GatewayReceiver. e.g. gateway-transport-filter=com.user.filters.MyFilter1,com.user.filters.MyFilters2";
  public static final String CREATE_GATEWAYRECEIVER__MSG__GATEWAYRECEIVER_CREATED_ON_0_ONPORT_1 = "GatewayReceiver created on member \"{0}\" and will listen on the port \"{1}\"";
  public static final String CREATE_GATEWAYRECEIVER__MANUALSTART = "manual-start";
  public static final String CREATE_GATEWAYRECEIVER__MANUALSTART__HELP = "Whether manual start is to be enabled or the receiver will start automatically after creation.";


  /* start gateway-receiver */
  public static final String START_GATEWAYRECEIVER = "start gateway-receiver";
  public static final String START_GATEWAYRECEIVER__MEMBER = "member";
  public static final String START_GATEWAYRECEIVER__GROUP = "group";
  public static final String START_GATEWAYRECEIVER__HELP = "Start the Gateway Receiver on a member or members.";
  public static final String START_GATEWAYRECEIVER__GROUP__HELP = "Group(s) of members on which to start the Gateway Receiver.";
  public static final String START_GATEWAYRECEIVER__MEMBER__HELP = "Name/Id of the member on which to start the Gateway Receiver.";

  /* create gateway-sender */
  public static final String CREATE_GATEWAYSENDER = "create gateway-sender";
  public static final String CREATE_GATEWAYSENDER__HELP = "Create the Gateway Sender on a member or members.";
  public static final String CREATE_GATEWAYSENDER__GROUP = "group";
  public static final String CREATE_GATEWAYSENDER__GROUP__HELP = "Group(s) of members on which to create the Gateway Sender.";
  public static final String CREATE_GATEWAYSENDER__MEMBER = "member";
  public static final String CREATE_GATEWAYSENDER__MEMBER__HELP = "Name/Id of the member on which to create the Gateway Sender.";
  public static final String CREATE_GATEWAYSENDER__ID = "id";
  public static final String CREATE_GATEWAYSENDER__ID__HELP = "Id of the GatewaySender.";
  public static final String CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID = "remote-distributed-system-id";
  public static final String CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID__HELP = "Id of the remote distributed system to which the sender will send events.";
  public static final String CREATE_GATEWAYSENDER__PARALLEL = "parallel";
  public static final String CREATE_GATEWAYSENDER__PARALLEL__HELP = "Whether this is Parallel GatewaySender.";
  public static final String CREATE_GATEWAYSENDER__MANUALSTART = "manual-start";
  public static final String CREATE_GATEWAYSENDER__MANUALSTART__HELP = "Whether manual start is to be enabled or the sender will start automatically after creation.";
  public static final String CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE = SOCKET_BUFFER_SIZE;
  public static final String CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE__HELP = "The buffer size of the socket connection between this GatewaySender and its receiving GatewayReceiver.";
  public static final String CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT = "socket-read-timeout";
  public static final String CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT__HELP = "The amount of time in milliseconds that a socket read between a sending GatewaySender and its receiving GatewayReceiver will block.";
  public static final String CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION = "enable-batch-conflation";
  public static final String CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION__HELP = "Whether batch conflation is to be enabled for a GatewaySender.";
  public static final String CREATE_GATEWAYSENDER__BATCHSIZE = "batch-size";
  public static final String CREATE_GATEWAYSENDER__BATCHSIZE__HELP = "The batch size for the GatewaySender.";
  public static final String CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL = "batch-time-interval";
  public static final String CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL__HELP = "The batch time interval for the GatewaySender.";
  public static final String CREATE_GATEWAYSENDER__ENABLEPERSISTENCE = "enable-persistence";
  public static final String CREATE_GATEWAYSENDER__ENABLEPERSISTENCE__HELP = "Whether persistence is to be enabled for the GatewaySender.";
  public static final String CREATE_GATEWAYSENDER__DISKSTORENAME = "disk-store-name";
  public static final String CREATE_GATEWAYSENDER__DISKSTORENAME__HELP = "The disk store name to be configured for overflow or persistence.";
  public static final String CREATE_GATEWAYSENDER__DISKSYNCHRONOUS = "disk-synchronous";
  public static final String CREATE_GATEWAYSENDER__DISKSYNCHRONOUS__HELP = "Whether writes to the disk in case of persistence are synchronous.";
  public static final String CREATE_GATEWAYSENDER__MAXQUEUEMEMORY = "maximum-queue-memory";
  public static final String CREATE_GATEWAYSENDER__MAXQUEUEMEMORY__HELP = "The maximum amount of memory (in MB) for a GatewaySender's queue.";
  public static final String CREATE_GATEWAYSENDER__ALERTTHRESHOLD = "alert-threshold";
  public static final String CREATE_GATEWAYSENDER__ALERTTHRESHOLD__HELP = "The alert threshold for entries in a GatewaySender's queue.";
  public static final String CREATE_GATEWAYSENDER__DISPATCHERTHREADS = "dispatcher-threads";
  public static final String CREATE_GATEWAYSENDER__DISPATCHERTHREADS__HELP = "The number of dispatcher threads working for this GatewaySender. When dispatcher threads is set to > 1, appropriate order policy is required to be set.";
  public static final String CREATE_GATEWAYSENDER__ORDERPOLICY = "order-policy";
  public static final String CREATE_GATEWAYSENDER__ORDERPOLICY__HELP = "The order policy followed while dispatching the events to remote distributed system. Order policy is set only when dispatcher threads are > 1. Possible values are 'THREAD', 'KEY', 'PARTITION'.";
  public static final String CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER = "gateway-event-filter";
  public static final String CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER__HELP = "The list of fully qualified class names of GatewayEventFilters (separated by comma) to be associated with the GatewaySender. This serves as a callback for users to filter out events before dispatching to remote distributed system. e.g gateway-event-filter=com.user.filters.MyFilter1,com.user.filters.MyFilters2";
  public static final String CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER = "gateway-transport-filter";
  public static final String CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER__HELP = "The fully qualified class name of GatewayTransportFilter to be added to the GatewaySender. ";
  public static final String CREATE_GATEWAYSENDER__MSG__GATEWAYSENDER_0_CREATED_ON_1 = "GatewaySender \"{0}\" created on \"{1}\"";
  public static final String CREATE_GATEWAYSENDER__MSG__COULDNOT_INSTANTIATE_CLASS_0_SPECIFIED_FOR_1 = "Could not instantiate class \"{0}\" specified for \"{1}\".";
  public static final String CREATE_GATEWAYSENDER__MSG__COULDNOT_ACCESS_CLASS_0_SPECIFIED_FOR_1 = "Could not access class \"{0}\" specified for \"{1}\".";

  /* stop gateway-reciver */
  public static final String START_GATEWAYSENDER = "start gateway-sender";
  public static final String START_GATEWAYSENDER__HELP = "Start the Gateway Sender on a member or members.";
  public static final String START_GATEWAYSENDER__ID = "id";
  public static final String START_GATEWAYSENDER__ID__HELP = "ID of the Gateway Sender.";
  public static final String START_GATEWAYSENDER__GROUP = "group";
  public static final String START_GATEWAYSENDER__GROUP__HELP = "Group(s) of members on which to start the Gateway Sender.";
  public static final String START_GATEWAYSENDER__MEMBER = "member";
  public static final String START_GATEWAYSENDER__MEMBER__HELP = "Name/Id of the member on which to start the Gateway Sender.";

  /* start gfmon command */
  public static final String START_PULSE = "start pulse";
  public static final String START_PULSE__ERROR = "An error occurred while launching Geode Pulse - %1$s";
  public static final String START_PULSE__HELP = "Open a new window in the default Web browser with the URL for the Pulse application.";
  public static final String START_PULSE__RUN = "Launched Geode Pulse";
  public static final String START_PULSE__URL = "url";
  public static final String START_PULSE__URL__HELP = "URL of the Pulse Web application.";
  public static final String START_PULSE__URL__NOTFOUND = "Could not find the URL for Geode Pulse.";

  /* 'start jsonsole' command */
  public static final String START_JCONSOLE = "start jconsole";
  public static final String START_JCONSOLE__HELP = "Start the JDK's JConsole tool in a separate process. JConsole will be launched, but connecting to Geode must be done manually.";
  public static final String START_JCONSOLE__CATCH_ALL_ERROR_MESSAGE = "An error occurred while launching JConsole = %1$s";
  public static final String START_JCONSOLE__CONNECT_BY_MEMBER_NAME_ID_ERROR_MESSAGE = "Connecting by the Geode member's name or ID is not currently supported.\nPlease specify the member as '<hostname|IP>[PORT].";
  public static final String START_JCONSOLE__INTERVAL = "interval";
  public static final String START_JCONSOLE__INTERVAL__HELP = "Update internal (in seconds). This parameter is passed as -interval to JConsole.";
  public static final String START_JCONSOLE__IO_EXCEPTION_MESSAGE = "An IO error occurred while launching JConsole.\nPlease ensure that JAVA_HOME is set to the JDK installation or the JDK bin directory is in the system PATH.";
  public static final String START_JCONSOLE__J = "J";
  public static final String START_JCONSOLE__J__HELP = "Arguments passed to the JVM on which JConsole will run.";
  public static final String START_JCONSOLE__NOT_FOUND_ERROR_MESSAGE = "JConsole could not be found.\nPlease ensure that JAVA_HOME is set to the JDK installation or the JDK bin directory is in the system PATH.";
  public static final String START_JCONSOLE__NOTILE = "notile";
  public static final String START_JCONSOLE__NOTILE__HELP = "Whether to initially tile windows for two or more connections. This parameter is passed as -notile to JConsole.";
  public static final String START_JCONSOLE__PLUGINPATH = "pluginpath";
  public static final String START_JCONSOLE__PLUGINPATH__HELP = "Directories or JAR files which are searched for JConsole plugins. The path should contain a provider-configuration file named:\n"
      + "    META-INF/services/com.sun.tools.jconsole.JConsolePlugin\n"
      + "containing one line for each plugin specifying the fully qualified class name of the class implementing the com.sun.tools.jconsole.JConsolePlugin class.";
  public static final String START_JCONSOLE__RUN = "Launched JConsole";
  public static final String START_JCONSOLE__VERSION = "version";
  public static final String START_JCONSOLE__VERSION__HELP = "Display the JConsole version information. This parameter is passed as -version to JConsole.";

  /* 'start jvisualvm command' */
  public static final String START_JVISUALVM = "start jvisualvm";
  public static final String START_JVISUALVM__HELP = "Start the JDK's Java VisualVM (jvisualvm) tool in a separate process. Java VisualVM will be launched, but connecting to Geode must be done manually.";
  public static final String START_JVISUALVM__ERROR_MESSAGE = "An error occurred while launching Java VisualVM - %1$s";
  public static final String START_JVISUALVM__EXPECTED_JDK_VERSION_ERROR_MESSAGE = "Java VisualVM was not bundled with the JDK until version 1.6.\nDownload and install Java VisualVM to the JDK bin directory separately.";
  public static final String START_JVISUALVM__J = "J";
  public static final String START_JVISUALVM__J__HELP = "Arguments passed to the JVM on which Java VisualVM will run.";
  public static final String START_JVISUALVM__NOT_FOUND_ERROR_MESSAGE = "Java VisualVM could not be found on this system.\nPlease ensure that \"jvisualvm\" is installed in the JDK bin directory and the JDK bin directory is in the system PATH.";
  public static final String START_JVISUALVM__RUN = "Launched JVisualVM";

  /* 'start locator' command */
  public static final String START_LOCATOR = "start locator";
  public static final String START_LOCATOR__HELP = "Start a Locator.";
  public static final String START_LOCATOR__BIND_ADDRESS = "bind-address";
  public static final String START_LOCATOR__BIND_ADDRESS__HELP = "IP address on which the Locator will be bound.  By default, the Locator is bound to all local addresses.";
  public static final String START_LOCATOR__CLASSPATH = "classpath";
  public static final String START_LOCATOR__CLASSPATH__HELP = "Location of user application classes required by the Locator. The user classpath is prepended to the Locator's classpath.";
  public static final String START_LOCATOR__DIR = "dir";
  public static final String START_LOCATOR__DIR__HELP = "Directory in which the Locator will be started and ran. The default is ./<locator-member-name>";
  public static final String START_LOCATOR__FORCE = "force";
  public static final String START_LOCATOR__FORCE__HELP = "Whether to allow the PID file from a previous Locator run to be overwritten.";
  public static final String START_LOCATOR__GROUP = "group";
  public static final String START_LOCATOR__GROUP__HELP = "Group(s) the Locator will be a part of.";
  public static final String START_LOCATOR__HOSTNAME_FOR_CLIENTS = "hostname-for-clients";
  public static final String START_LOCATOR__HOSTNAME_FOR_CLIENTS__HELP = "Hostname or IP address that will be sent to clients so they can connect to this Locator. The default is the bind-address of the Locator.";
  public static final String START_LOCATOR__INCLUDE_SYSTEM_CLASSPATH = "include-system-classpath";
  public static final String START_LOCATOR__INCLUDE_SYSTEM_CLASSPATH__HELP = "Includes the System CLASSPATH on the Locator's CLASSPATH. The System CLASSPATH is not included by default.";
  public static final String START_LOCATOR__LOCATORS = LOCATORS;
  public static final String START_LOCATOR__LOCATORS__HELP = "Sets the list of Locators used by this Locator to join the appropriate Geode cluster.";
  public static final String START_LOCATOR__LOG_LEVEL = LOG_LEVEL;
  public static final String START_LOCATOR__LOG_LEVEL__HELP = "Sets the level of output logged to the Locator log file.  Possible values for log-level include: finest, finer, fine, config, info, warning, severe, none.";
  public static final String START_LOCATOR__MCAST_ADDRESS = MCAST_ADDRESS;
  public static final String START_LOCATOR__MCAST_ADDRESS__HELP = "The IP address or hostname used to bind the UPD socket for multi-cast networking so the Locator can communicate with other members in the Geode cluster using a common multicast address and port.  If mcast-port is zero, then mcast-address is ignored.";
  public static final String START_LOCATOR__MCAST_PORT = MCAST_PORT;
  public static final String START_LOCATOR__MCAST_PORT__HELP = "Sets the port used for multi-cast networking so the Locator can communicate with other members of the Geode cluster.  A zero value disables mcast.";
  public static final String START_LOCATOR__MEMBER_NAME = "name";
  public static final String START_LOCATOR__MEMBER_NAME__HELP = "The member name to give this Locator in the Geode cluster.";
  public static final String START_LOCATOR__PORT = "port";
  public static final String START_LOCATOR__PORT__HELP = "Port the Locator will listen on.";
  public static final String START_LOCATOR__PROPERTIES = "properties-file";
  public static final String START_LOCATOR__PROPERTIES__HELP = "The gemfire.properties file for configuring the Locator's distributed system. The file's path can be absolute or relative to the gfsh working directory (--dir=)."; // TODO:GEODE-1466: update golden file to geode.properties
  public static final String START_LOCATOR__SECURITY_PROPERTIES = "security-properties-file";
  public static final String START_LOCATOR__SECURITY_PROPERTIES__HELP = "The gfsecurity.properties file for configuring the Locator's security configuration in the distributed system. The file's path can be absolute or relative to gfsh directory (--dir=).";
  public static final String START_LOCATOR__INITIALHEAP = "initial-heap";
  public static final String START_LOCATOR__INITIALHEAP__HELP = "Initial size of the heap in the same format as the JVM -Xms parameter.";
  public static final String START_LOCATOR__J = "J";
  public static final String START_LOCATOR__J__HELP = "Argument passed to the JVM on which the Locator will run. For example, --J=-Dfoo.bar=true will set the property \"foo.bar\" to \"true\".";
  public static final String START_LOCATOR__MAXHEAP = "max-heap";
  public static final String START_LOCATOR__MAXHEAP__HELP = "Maximum size of the heap in the same format as the JVM -Xmx parameter.";
  public static final String START_LOCATOR__GENERAL_ERROR_MESSAGE = "An error occurred while attempting to start a Locator in %1$s on %2$s: %3$s";
  public static final String START_LOCATOR__PROCESS_TERMINATED_ABNORMALLY_ERROR_MESSAGE = "The Locator process terminated unexpectedly with exit status %1$d. Please refer to the log file in %2$s for full details.%n%n%3$s";
  public static final String START_LOCATOR__RUN_MESSAGE = "Starting a Geode Locator in %1$s...";
  public static final String START_LOCATOR__MSG__COULD_NOT_CREATE_DIRECTORY_0_VERIFY_PERMISSIONS = "Could not create directory {0}. Please verify directory path or user permissions.";
  public static final String START_LOCATOR__CONNECT = "connect";
  public static final String START_LOCATOR__CONNECT__HELP = "When connect is set to false , Gfsh does not automatically connect to the locator which is started using this command.";
  public static final String START_LOCATOR__USE__0__TO__CONNECT = "Please use \"{0}\" to connect Gfsh to the locator.";
  public static final String START_LOCATOR__ENABLE__SHARED__CONFIGURATION = ENABLE_CLUSTER_CONFIGURATION;
  public static final String START_LOCATOR__ENABLE__SHARED__CONFIGURATION__HELP = "When " + START_LOCATOR__ENABLE__SHARED__CONFIGURATION + " is set to true, locator hosts and serves cluster configuration.";
  public static final String START_LOCATOR__LOAD__SHARED_CONFIGURATION__FROM__FILESYSTEM = "load-cluster-configuration-from-dir";
  public static final String START_LOCATOR__LOAD__SHARED_CONFIGURATION__FROM__FILESYSTEM__HELP = "When \" " + START_LOCATOR__LOAD__SHARED_CONFIGURATION__FROM__FILESYSTEM + " \" is set to true, the locator loads the cluster configuration from the \""+ SharedConfiguration.CLUSTER_CONFIG_ARTIFACTS_DIR_NAME + "\" directory.";  
  public static final String START_LOCATOR__CLUSTER__CONFIG__DIR = "cluster-config-dir";
  public static final String START_LOCATOR__CLUSTER__CONFIG__DIR__HELP = "Directory used by the cluster configuration service to store the cluster configuration on the filesystem";
  
  /* 'start manager' command */
  public static final String START_MANAGER = "start manager";
  public static final String START_MANAGER__HELP = "Start a Manager. Parameters --peer and --server will be removed for simplicity and Locator is always available for both.";
  public static final String START_MANAGER__MEMBERNAME = "name";
  public static final String START_MANAGER__MEMBERNAME__HELP = "Member name for this Manager service.";
  public static final String START_MANAGER__DIR = "dir";
  public static final String START_MANAGER__DIR__HELP = "Directory in which the Manager will be run. The default is the current directory.";
  public static final String START_MANAGER__CLASSPATH = "classpath";
  public static final String START_MANAGER__CLASSPATH__HELP = "Location of user classes required by the Manager. This path is appended to the current classpath.";
  public static final String START_MANAGER__PORT = "port";
  public static final String START_MANAGER__PORT__HELP = "Port the Manager will listen on for JMX-RMI client connections.";
  public static final String START_MANAGER__BIND_ADDRESS = "bind-address";
  public static final String START_MANAGER__BIND_ADDRESS__HELP = "IP address the Manager listen on for JMX-RMI client connections. The default is to bind to all local addresses.";
  public static final String START_MANAGER__GROUP = "group";
  public static final String START_MANAGER__GROUP__HELP = "Group(s) this Manager will be a part of.";
  public static final String START_MANAGER__MAXHEAP = "max-heap";
  public static final String START_MANAGER__MAXHEAP__HELP = "Maximum size of the heap in the same format as the JVM -Xmx parameter.";
  public static final String START_MANAGER__INITIALHEAP = "initial-heap";
  public static final String START_MANAGER__INITIALHEAP__HELP = "Initial size of the heap in the same format as the JVM -Xms parameter.";
  public static final String START_MANAGER__GEODEPROPS = "G";
  public static final String START_MANAGER__GEODEPROPS__HELP = "Geode property passed as a <name>=<value> pair.";
  public static final String START_MANAGER__J = "J";
  public static final String START_MANAGER__J__HELP = "Argument passed to the JVM on which the Locator will run. For example, --J=-Dfoo.bar=true will set the property \"foo.bar\" to \"true\".";

  /* 'start server' command */
  public static final String START_SERVER = "start server";
  public static final String START_SERVER__HELP = "Start a Geode Cache Server.";
  public static final String START_SERVER__ASSIGN_BUCKETS = "assign-buckets";
  public static final String START_SERVER__ASSIGN_BUCKETS__HELP = "Whether to assign buckets to the partitioned regions of the cache on server start.";
  public static final String START_SERVER__BIND_ADDRESS = "bind-address";
  public static final String START_SERVER__BIND_ADDRESS__HELP = "The IP address on which the Server will be bound.  By default, the Server is bound to all local addresses.";
  public static final String START_SERVER__CACHE_XML_FILE = CACHE_XML_FILE;
  public static final String START_SERVER__CACHE_XML_FILE__HELP = "Specifies the name of the XML file or resource to initialize the cache with when it is created.";
  public static final String START_SERVER__CLASSPATH = "classpath";
  public static final String START_SERVER__CLASSPATH__HELP = "Location of user application classes required by the Server. The user classpath is prepended to the Server's classpath.";
  public static final String START_SERVER__DIR = "dir";
  public static final String START_SERVER__DIR__HELP = "Directory in which the Cache Server will be started and ran. The default is ./<server-member-name>";
  public static final String START_SERVER__DISABLE_DEFAULT_SERVER = "disable-default-server";
  public static final String START_SERVER__DISABLE_DEFAULT_SERVER__HELP = "Whether the Cache Server will be started by default.";
  public static final String START_SERVER__DISABLE_EXIT_WHEN_OUT_OF_MEMORY = "disable-exit-when-out-of-memory";
  public static final String START_SERVER__DISABLE_EXIT_WHEN_OUT_OF_MEMORY_HELP = "Prevents the JVM from exiting when an OutOfMemoryError occurs.";
  public static final String START_SERVER__ENABLE_TIME_STATISTICS = ENABLE_TIME_STATISTICS;
  public static final String START_SERVER__ENABLE_TIME_STATISTICS__HELP = "Causes additional time-based statistics to be gathered for Geode operations.";
  public static final String START_SERVER__FORCE = "force";
  public static final String START_SERVER__FORCE__HELP = "Whether to allow the PID file from a previous Cache Server run to be overwritten.";
  public static final String START_SERVER__GROUP = "group";
  public static final String START_SERVER__GROUP__HELP = "Group(s) the Cache Server will be a part of.";
  public static final String START_SERVER__INCLUDE_SYSTEM_CLASSPATH = "include-system-classpath";
  public static final String START_SERVER__INCLUDE_SYSTEM_CLASSPATH__HELP = "Includes the System CLASSPATH on the Server's CLASSPATH. The System CLASSPATH is not included by default.";
  public static final String START_SERVER__INITIAL_HEAP = "initial-heap";
  public static final String START_SERVER__INITIAL_HEAP__HELP = "Initial size of the heap in the same format as the JVM -Xms parameter.";
  public static final String START_SERVER__J = "J";
  public static final String START_SERVER__J__HELP = "Argument passed to the JVM on which the server will run. For example, --J=-Dfoo.bar=true will set the system property \"foo.bar\" to \"true\".";
  public static final String START_SERVER__LOCATORS = LOCATORS;
  public static final String START_SERVER__LOCATORS__HELP = "Sets the list of Locators used by the Cache Server to join the appropriate Geode cluster.";
  public static final String START_SERVER__LOCK_MEMORY = ConfigurationProperties.LOCK_MEMORY;
  public static final String START_SERVER__LOCK_MEMORY__HELP = "Causes Geode to lock heap and off-heap memory pages into RAM. This prevents the operating system from swapping the pages out to disk, which can cause severe performance degradation. When you use this option, also configure the operating system limits for locked memory.";
  public static final String START_SERVER__LOCATOR_WAIT_TIME = "locator-wait-time";
  public static final String START_SERVER__LOCATOR_WAIT_TIME_HELP = "Sets the number of seconds the server will wait for a locator to become available during startup before giving up.";
  public static final String START_SERVER__LOG_LEVEL = LOG_LEVEL;
  public static final String START_SERVER__LOG_LEVEL__HELP = "Sets the level of output logged to the Cache Server log file.  Possible values for log-level include: finest, finer, fine, config, info, warning, severe, none.";
  public static final String START_SERVER__MAXHEAP = "max-heap";
  public static final String START_SERVER__MAXHEAP__HELP = "Maximum size of the heap in the same format as the JVM -Xmx parameter.";
  public static final String START_SERVER__MCAST_ADDRESS = MCAST_ADDRESS;
  public static final String START_SERVER__MCAST_ADDRESS__HELP = "The IP address or hostname used to bind the UPD socket for multi-cast networking so the Cache Server can communicate with other members in the Geode cluster.  If mcast-port is zero, then mcast-address is ignored.";
  public static final String START_SERVER__MCAST_PORT = MCAST_PORT;
  public static final String START_SERVER__MCAST_PORT__HELP = "Sets the port used for multi-cast networking so the Cache Server can communicate with other members of the Geode cluster.  A zero value disables mcast.";
  public static final String START_SERVER__NAME = "name";
  public static final String START_SERVER__NAME__HELP = "The member name to give this Cache Server in the Geode cluster.";
  public static final String START_SERVER__MEMCACHED_PORT = MEMCACHED_PORT;
  public static final String START_SERVER__MEMCACHED_PORT__HELP = "Sets the port that the Geode memcached service listens on for memcached clients.";
  public static final String START_SERVER__MEMCACHED_PROTOCOL = MEMCACHED_PROTOCOL;
  public static final String START_SERVER__MEMCACHED_PROTOCOL__HELP = "Sets the protocol that the Geode memcached service uses (ASCII or BINARY).";
  public static final String START_SERVER__MEMCACHED_BIND_ADDRESS = MEMCACHED_BIND_ADDRESS;
  public static final String START_SERVER__MEMCACHED_BIND_ADDRESS__HELP = "Sets the IP address the Geode memcached service listens on for memcached clients. The default is to bind to the first non-loopback address for this machine.";
  public static final String START_SERVER__OFF_HEAP_MEMORY_SIZE = ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
  public static final String START_SERVER__OFF_HEAP_MEMORY_SIZE__HELP = "The total size of off-heap memory specified as off-heap-memory-size=<n>[g|m]. <n> is the size. [g|m] indicates whether the size should be interpreted as gigabytes or megabytes. A non-zero size causes that much memory to be allocated from the operating system and reserved for off-heap use.";
  public static final String START_SERVER__PROPERTIES = "properties-file";
  public static final String START_SERVER__PROPERTIES__HELP = "The gemfire.properties file for configuring the Cache Server's distributed system. The file's path can be absolute or relative to the gfsh working directory."; // TODO:GEODE-1466: update golden file to geode.properties
  public static final String START_SERVER__REDIS_PORT = ConfigurationProperties.REDIS_PORT;
  public static final String START_SERVER__REDIS_PORT__HELP = "Sets the port that the Geode Redis service listens on for Redis clients.";
  public static final String START_SERVER__REDIS_BIND_ADDRESS = ConfigurationProperties.REDIS_BIND_ADDRESS;
  public static final String START_SERVER__REDIS_BIND_ADDRESS__HELP = "Sets the IP address the Geode Redis service listens on for Redis clients. The default is to bind to the first non-loopback address for this machine.";
  public static final String START_SERVER__REDIS_PASSWORD = ConfigurationProperties.REDIS_PASSWORD;
  public static final String START_SERVER__REDIS_PASSWORD__HELP = "Sets the authentication password for GeodeRedisServer"; // TODO:GEODE-1566: update golden file to GeodeRedisServer
  public static final String START_SERVER__SECURITY_PROPERTIES = "security-properties-file";
  public static final String START_SERVER__SECURITY_PROPERTIES__HELP = "The gfsecurity.properties file for configuring the Server's security configuration in the distributed system. The file's path can be absolute or relative to gfsh directory.";
  public static final String START_SERVER__REBALANCE = "rebalance";
  public static final String START_SERVER__REBALANCE__HELP = "Whether to initiate rebalancing across the Geode cluster.";
  public static final String START_SERVER__SERVER_BIND_ADDRESS = SERVER_BIND_ADDRESS;
  public static final String START_SERVER__SERVER_BIND_ADDRESS__HELP = "The IP address that this distributed system's server sockets in a client-server topology will be bound. If set to an empty string then all of the local machine's addresses will be listened on.";
  public static final String START_SERVER__SERVER_PORT = "server-port";
  public static final String START_SERVER__SERVER_PORT__HELP = "The port that the distributed system's server sockets in a client-server topology will listen on.  The default server-port is "
      + CacheServer.DEFAULT_PORT + ".";
  public static final String START_SERVER__SPRING_XML_LOCATION = "spring-xml-location";
  public static final String START_SERVER__SPRING_XML_LOCATION_HELP = "Specifies the location of a Spring XML configuration file(s) for bootstrapping and configuring a Geode Server.";
  public static final String START_SERVER__STATISTIC_ARCHIVE_FILE = STATISTIC_ARCHIVE_FILE;
  public static final String START_SERVER__STATISTIC_ARCHIVE_FILE__HELP = "The file that statistic samples are written to.  An empty string (default) disables statistic archival.";
  // public static final String START_SERVER__START_LOCATOR = "start-locator";
  // public static final String START_SERVER__START_LOCATOR__HELP =
  // "To start embedded Locator with given endpoints in the format: host[port]. If no endpoints are given defaults (localhost[10334]) are assumed.";
  public static final String START_SERVER__USE_CLUSTER_CONFIGURATION = USE_CLUSTER_CONFIGURATION;
  public static final String START_SERVER__USE_CLUSTER_CONFIGURATION__HELP = "When set to true, the server requests the configuration from locator's cluster configuration service.";
  public static final String START_SERVER__GENERAL_ERROR_MESSAGE = "An error occurred while attempting to start a Geode Cache Server: %1$s";
  public static final String START_SERVER__PROCESS_TERMINATED_ABNORMALLY_ERROR_MESSAGE = "The Cache Server process terminated unexpectedly with exit status %1$d. Please refer to the log file in %2$s for full details.%n%n%3$s";
  public static final String START_SERVER__RUN_MESSAGE = "Starting a Geode Server in %1$s...";
  public static final String START_SERVER__MSG__COULD_NOT_CREATE_DIRECTORY_0_VERIFY_PERMISSIONS = "Could not create directory {0}. Please verify directory path or user permissions.";
  
  public static final String START_SERVER__CRITICAL__HEAP__PERCENTAGE = "critical-heap-percentage";
  public static final String START_SERVER__CRITICAL__HEAP__HELP = "Set the percentage of heap at or above which the cache is considered in danger of becoming inoperable due to garbage collection pauses or out of memory exceptions"; 
  
  public static final String START_SERVER__EVICTION__HEAP__PERCENTAGE = "eviction-heap-percentage";
  public static final String START_SERVER__EVICTION__HEAP__PERCENTAGE__HELP = "Set the percentage of heap at or above which the eviction should begin on Regions configured for HeapLRU eviction. Changing this value may cause eviction to begin immediately." +
  "Only one change to this attribute or critical heap percentage will be allowed at any given time and its effect will be fully realized before the next change is allowed. This feature requires additional VM flags to perform properly. ";

  public static final String START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE = "critical-off-heap-percentage";
  public static final String START_SERVER__CRITICAL_OFF_HEAP__HELP = "Set the percentage of off-heap memory at or above which the cache is considered in danger of becoming inoperable due to out of memory exceptions"; 
  
  public static final String START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE = "eviction-off-heap-percentage";
  public static final String START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE__HELP = "Set the percentage of off-heap memory at or above which the eviction should begin on Regions configured for off-heap and HeapLRU eviction. Changing this value may cause eviction to begin immediately." +
  " Only one change to this attribute or critical off-heap percentage will be allowed at any given time and its effect will be fully realized before the next change is allowed.";
//cacheServer.setLoadPollInterval(loadPollInterval)
//cacheServer.setLoadProbe(loadProbe);
//cacheServer.setMaxConnections(maxCons);
//cacheServer.setMaximumMessageCount(maxMessageCount);
//cacheServer.setMaximumTimeBetweenPings(maximumTimeBetweenPings);
//cacheServer.setMaxThreads(maxThreads);
//cacheServer.setMessageTimeToLive(messageTimeToLive);
//cacheServer.setSocketBufferSize(socketBufferSize)
//cacheServer.setTcpNoDelay(noDelay)
  public static final String START_SERVER__HOSTNAME__FOR__CLIENTS = "hostname-for-clients";
  public static final String START_SERVER__HOSTNAME__FOR__CLIENTS__HELP = "Sets the ip address or host name that this cache server is to listen on for client connections." +
  "Setting a specific hostname-for-clients will cause server locators to use this value when telling clients how to connect to this cache server. This is useful in the case where the cache server may refer to itself with one hostname, but the clients need to use a different hostname to find the cache server."+
  "The value \"\" causes the bind-address to be given to clients."+
  "A null value will be treated the same as the default \"\".";

  
  public static final String START_SERVER__LOAD__POLL__INTERVAL = "load-poll-interval";
  public static final String START_SERVER__LOAD__POLL__INTERVAL__HELP ="Set the frequency in milliseconds to poll the load probe on this cache server"; 


  public static final String START_SERVER__MAX__CONNECTIONS = "max-connections";
  public static final String START_SERVER__MAX__CONNECTIONS__HELP = "Sets the maxium number of client connections allowed. When the maximum is reached the cache server will stop accepting connections";
  
  public static final String START_SERVER__MAX__THREADS = "max-threads";
  public static final String START_SERVER__MAX__THREADS__HELP = "Sets the maxium number of threads allowed in this cache server to service client requests. The default of 0 causes the cache server to dedicate a thread for every client connection";
  
  public static final String START_SERVER__MAX__MESSAGE__COUNT = "max-message-count";
  public static final String START_SERVER__MAX__MESSAGE__COUNT__HELP = "Sets maximum number of messages that can be enqueued in a client-queue.";
  
  public static final String START_SERVER__MESSAGE__TIME__TO__LIVE = "message-time-to-live";
  public static final String START_SERVER__MESSAGE__TIME__TO__LIVE__HELP = "Sets the time (in seconds ) after which a message in the client queue will expire";

  public static final String START_SERVER__SOCKET__BUFFER__SIZE = SOCKET_BUFFER_SIZE;
  public static final String START_SERVER__SOCKET__BUFFER__SIZE__HELP = "Sets the buffer size in bytes of the socket connection for this CacheServer. The default is 32768 bytes.";
  
  public static final String START_SERVER__TCP__NO__DELAY = "tcp-no-delay";
  public static final String START_SERVER__TCP__NO__DELAY__HELP = "Configures the tcpNoDelay setting of sockets used to send messages to clients. TcpNoDelay is enabled by default";
  
  
  /* start vsd command */
  public static final String START_VSD = "start vsd";
  public static final String START_VSD__FILE = "file";
  public static final String START_VSD__FILE__HELP = "File or directory from which to read the statistics archive(s).";
  public static final String START_VSD__ERROR_MESSAGE = "An error occurred while launching VSD - %1$s";
  public static final String START_VSD__HELP = "Start VSD in a separate process.";
  public static final String START_VSD__NOT_FOUND_ERROR_MESSAGE = "The location of VSD could not be found.  Please ensure VSD was properly installed under Geode home (%1$s).";
  public static final String START_VSD__RUN = "Launched Geode Visual Statistics Display (VSD) (see Geode log files for issues on start)";

  /* start databrowser command */
  public static final String START_DATABROWSER = "start data-browser";
  public static final String START_DATABROWSER__HELP = "Start Data Browser in a separate process.";
  public static final String START_DATABROWSER__NOT_FOUND_ERROR_MESSAGE = "The location of DataBrowser could not be found.  Please ensure DataBrowser was properly installed under Geode home (%1$s).";
  public static final String START_DATABROWSER__RUN = "Launched Geode DataBrowser (see Geode log files for issues on start)";
  public static final String START_DATABROWSER__ERROR = "An error occurred while launching DataBrowser - %1$s";

  /* status gateway-receiver */
  public static final String STATUS_GATEWAYRECEIVER = "status gateway-receiver";
  public static final String STATUS_GATEWAYRECEIVER__HELP = "Display the status of a Gateway Receiver.";
  public static final String STATUS_GATEWAYRECEIVER__GROUP = "group";
  public static final String STATUS_GATEWAYRECEIVER__GROUP__HELP = "Group(s) of Gateway Receivers for which to display status.";
  public static final String STATUS_GATEWAYRECEIVER__MEMBER = "member";
  public static final String STATUS_GATEWAYRECEIVER__MEMBER__HELP = "Name/Id of the Gateway Receiver for which to display status.";

  /* status gateway-sender */
  public static final String STATUS_GATEWAYSENDER = "status gateway-sender";
  public static final String STATUS_GATEWAYSENDER__HELP = "Display the status of a Gateway Sender.";
  public static final String STATUS_GATEWAYSENDER__ID = "id";
  public static final String STATUS_GATEWAYSENDER__ID__HELP = "ID of the Gateway Sender.";
  public static final String STATUS_GATEWAYSENDER__GROUP = "group";
  public static final String STATUS_GATEWAYSENDER__GROUP__HELP = "Group(s) of Gateway Senders for which to display status.";
  public static final String STATUS_GATEWAYSENDER__MEMBER = "member";
  public static final String STATUS_GATEWAYSENDER__MEMBER__HELP = "Name/Id of the Gateway Sender for which to display status.";

  /* 'status locator' command */
  public static final String STATUS_LOCATOR = "status locator";
  public static final String STATUS_LOCATOR__HELP = "Display the status of a Locator. Possible statuses are: started, online, offline or not responding.";
  public static final String STATUS_LOCATOR__DIR = "dir";
  public static final String STATUS_LOCATOR__DIR__HELP = "Working directory in which the Locator is running. The default is the current directory.";
  public static final String STATUS_LOCATOR__HOST = "host";
  public static final String STATUS_LOCATOR__HOST__HELP = "Hostname or IP address on which the Locator is running.";
  public static final String STATUS_LOCATOR__MEMBER = "name";
  public static final String STATUS_LOCATOR__MEMBER__HELP = "Member name or ID of the Locator in the Geode cluster.";
  public static final String STATUS_LOCATOR__PID = "pid";
  public static final String STATUS_LOCATOR__PID__HELP = "Process ID (PID) of the running Locator.";
  public static final String STATUS_LOCATOR__PORT = "port";
  public static final String STATUS_LOCATOR__PORT__HELP = "Port on which the Locator is listening. The default is 10334.";
  public static final String STATUS_LOCATOR__GENERAL_ERROR_MESSAGE = "An error occurred while attempting to determine the state of Locator on %1$s running in %2$s: %3$s";
  public static final String STATUS_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE = "No Locator with member name or ID {0} could be found.";

  /* 'status server' command */
  public static final String STATUS_SERVER = "status server";
  public static final String STATUS_SERVER__HELP = "Display the status of a Geode Cache Server.";
  public static final String STATUS_SERVER__DIR = "dir";
  public static final String STATUS_SERVER__DIR__HELP = "Working directory in which the Cache Server is running. The default is the current directory.";
  public static final String STATUS_SERVER__GENERAL_ERROR_MESSAGE = "An error occurred while attempting to determine the status of Geode Cache server: %1$s";
  public static final String STATUS_SERVER__MEMBER = "name";
  public static final String STATUS_SERVER__MEMBER__HELP = "Member name or ID of the Cache Server in the Geode cluster.";
  public static final String STATUS_SERVER__NO_SERVER_FOUND_FOR_MEMBER_ERROR_MESSAGE = "No Geode Cache Server with member name or ID {0} could be found.";
  public static final String STATUS_SERVER__PID = "pid";
  public static final String STATUS_SERVER__PID__HELP = "Process ID (PID) of the running Geode Cache Server.";

  /* stop gateway-reciver */
  public static final String STOP_GATEWAYRECEIVER = "stop gateway-receiver";
  public static final String STOP_GATEWAYRECEIVER__MEMBER = "member";
  public static final String STOP_GATEWAYRECEIVER__GROUP = "group";
  public static final String STOP_GATEWAYRECEIVER__HELP = "Stop the Gateway Receiver on a member or members.";
  public static final String STOP_GATEWAYRECEIVER__GROUP__HELP = "Group(s) of members on which to stop the Gateway Receiver.";
  public static final String STOP_GATEWAYRECEIVER__MEMBER__HELP = "Name/Id of the member on which to stop the Gateway Receiver.";

  /* stop gateway-sender */
  public static final String STOP_GATEWAYSENDER = "stop gateway-sender";
  public static final String STOP_GATEWAYSENDER__ID = "id";;
  public static final String STOP_GATEWAYSENDER__MEMBER = "member";
  public static final String STOP_GATEWAYSENDER__GROUP = "group";
  public static final String STOP_GATEWAYSENDER__HELP = "Stop the Gateway Sender on a member or members.";
  public static final String STOP_GATEWAYSENDER__ID__HELP = "ID of the Gateway Sender.";
  public static final String STOP_GATEWAYSENDER__GROUP__HELP = "Group(s) of members on which to stop the Gateway Sender.";
  public static final String STOP_GATEWAYSENDER__MEMBER__HELP = "Name/Id of the member on which to stop the Gateway Sender.";

  /* 'stop locator' command */
  public static final String STOP_LOCATOR = "stop locator";
  public static final String STOP_LOCATOR__HELP = "Stop a Locator.";
  public static final String STOP_LOCATOR__DIR = "dir";
  public static final String STOP_LOCATOR__DIR__HELP = "Working directory in which the Locator is running. The default is the current directory.";
  public static final String STOP_LOCATOR__MEMBER = "name";
  public static final String STOP_LOCATOR__MEMBER__HELP = "Member name or ID of the Locator in the Geode cluster.";
  public static final String STOP_LOCATOR__PID = "pid";
  public static final String STOP_LOCATOR__PID__HELP = "The process id (PID) of the running Locator.";
  public static final String STOP_LOCATOR__GENERAL_ERROR_MESSAGE = "An error occurred while attempting to stop a Locator: %1$s";
  public static final String STOP_LOCATOR__LOCATOR_IS_CACHE_SERVER_ERROR_MESSAGE = "The Locator identified by {0} is also a cache server and cannot be shutdown using 'stop locator'.  Please use 'stop server' instead.";
  public static final String STOP_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE = "No Locator with member name or ID {0} could be found.";
  public static final String STOP_LOCATOR__NOT_LOCATOR_ERROR_MESSAGE = "The Geode member identified by {0} is not a Locator and cannot be shutdown using 'stop locator'.";
  public static final String STOP_LOCATOR__SHUTDOWN_MEMBER_MESSAGE = "Locator {0} has been requested to stop.";
  public static final String STOP_LOCATOR__STOPPING_LOCATOR_MESSAGE = "Stopping Locator running in %1$s on %2$s as %3$s...%nProcess ID: %4$d%nLog File: %5$s";

  /* 'stop server' command */
  public static final String STOP_SERVER = "stop server";
  public static final String STOP_SERVER__HELP = "Stop a Geode Cache Server.";
  public static final String STOP_SERVER__DIR = "dir";
  public static final String STOP_SERVER__DIR__HELP = "Working directory in which the Cache Server is running. The default is the current directory.";
  public static final String STOP_SERVER__GENERAL_ERROR_MESSAGE = "An error occurred while attempting to stop a Cache Server: %1$s";
  public static final String STOP_SERVER__MEMBER_IS_NOT_SERVER_ERROR_MESSAGE = "Attempting to stop a Geode member that is not a Cache Server using 'stop server'; the operation is not permitted.";
  public static final String STOP_SERVER__MEMBER = "name";
  public static final String STOP_SERVER__MEMBER__HELP = "Member name or ID of the Cache Server in the Geode cluster.";
  public static final String STOP_SERVER__NO_SERVER_FOUND_FOR_MEMBER_ERROR_MESSAGE = "No Cache Server with member name or ID {0} could be found.";
  public static final String STOP_SERVER__PID = "pid";
  public static final String STOP_SERVER__PID__HELP = "Process ID (PID) of the running Geode Cache Server.";
  public static final String STOP_SERVER__SHUTDOWN_MEMBER_MESSAGE = "Cache Server {0} has been requested to stop.";
  public static final String STOP_SERVER__STOPPING_SERVER_MESSAGE = "Stopping Cache Server running in %1$s on %2$s as %3$s...%nProcess ID: %4$d%nLog File: %5$s";

  /* undeploy command */
  public static final String UNDEPLOY = "undeploy";
  public static final String UNDEPLOY__HELP = "Undeploy JARs from a member or members.";
  public static final String UNDEPLOY__GROUP = "group";
  public static final String UNDEPLOY__GROUP__HELP = "Group(s) of members from which to undeploy JARs. If not specified, undeploy will occur on all members.";
  public static final String UNDEPLOY__JAR = "jar";
  public static final String UNDEPLOY__JAR__HELP = "JAR(s) to be undeployed.  If not specified, all JARs will be undeployed.";
  public static final String UNDEPLOY__NO_JARS_FOUND_MESSAGE = "No JAR Files Found";

  /* 'upgrade offline-disk-store' command */
  public static final String UPGRADE_OFFLINE_DISK_STORE = "upgrade offline-disk-store";
  public static final String UPGRADE_OFFLINE_DISK_STORE__HELP = "Upgrade an offline disk store. If the disk store is large, additional memory may need to be allocated to the process using the --J=-Xmx??? parameter.";
  public static final String UPGRADE_OFFLINE_DISK_STORE__NAME = "name";
  public static final String UPGRADE_OFFLINE_DISK_STORE__NAME__HELP = "Name of the offline disk store to be upgraded.";
  public static final String UPGRADE_OFFLINE_DISK_STORE__DISKDIRS = "disk-dirs";
  public static final String UPGRADE_OFFLINE_DISK_STORE__DISKDIRS__HELP = "Directories where data for the disk store was previously written.";
  public static final String UPGRADE_OFFLINE_DISK_STORE__MAXOPLOGSIZE = "max-oplog-size";
  public static final String UPGRADE_OFFLINE_DISK_STORE__MAXOPLOGSIZE__HELP = "Maximum size (in megabytes) of the oplogs created by the upgrade.";
  public static final String UPGRADE_OFFLINE_DISK_STORE__J = "J";
  public static final String UPGRADE_OFFLINE_DISK_STORE__J__HELP = "Arguments passed to the Java Virtual Machine performing the upgrade operation on the disk store.";
  public static final String UPGRADE_OFFLINE_DISK_STORE__DISKSTORE_0_DOESNOT_EXIST = "Disk store \"{0}\" does not exist.";
  public static final String UPGRADE_OFFLINE_DISK_STORE__UPGRADE_ATTEMPTED_BUT_NOTHING_TO_UPGRADE = "Upgradation was attempted but nothing to upgrade.";
  public static final String UPGRADE_OFFLINE_DISK_STORE__ERROR_WHILE_UPGRADATION_REASON_0 = "Error occurred while perfoming disk store upgrade. Reason: \"{0}\"";
  public static final String UPGRADE_OFFLINE_DISK_STORE__MSG__CANNOT_LOCATE_0_DISKSTORE_IN_1 = "Cannot locate disk store \"{0}\" in directory : \"{1}\"";
  public static final String UPGRADE_OFFLINE_DISK_STORE__MSG__DISKSTORE_IN_USE_COMPACT_DISKSTORE_CAN_BE_USED = "The disk is currently being used by another process";
  public static final String UPGRADE_OFFLINE_DISK_STORE__MSG__CANNOT_ACCESS_DISKSTORE_0_FROM_1_CHECK_GFSH_LOGS = COMPACT_OFFLINE_DISK_STORE__MSG__CANNOT_ACCESS_DISKSTORE_0_FROM_1_CHECK_GFSH_LOGS;
  public static final String UPGRADE_OFFLINE_DISK_STORE__MSG__ERROR_WHILE_COMPACTING_DISKSTORE_0_WITH_1_REASON_2 = "Error occured while upgrading disk store={0} {1}. Reason: {2}";

  /* 'validate disk-store' command */
  public static final String VALIDATE_DISK_STORE = "validate offline-disk-store";
  public static final String VALIDATE_DISK_STORE__HELP = "Scan the contents of a disk store to verify that it has no errors.";
  public static final String VALIDATE_DISK_STORE__NAME = "name";
  public static final String VALIDATE_DISK_STORE__NAME__HELP = "Name of the disk store to be validated.";
  public static final String VALIDATE_DISK_STORE__DISKDIRS = "disk-dirs";
  public static final String VALIDATE_DISK_STORE__DISKDIRS__HELP = "Directories where data for the disk store was previously written.";
  public static final String VALIDATE_DISK_STORE__J = "J";
  public static final String VALIDATE_DISK_STORE__J__HELP = "Arguments passed to the Java Virtual Machine performing the compact operation on the disk store.";
  public static final String VALIDATE_DISK_STORE__MSG__NO_DIRS = VALIDATE_DISK_STORE__DISKDIRS + " is mandatory";
  public static final String VALIDATE_DISK_STORE__MSG__IO_ERROR = "Input/Output error in validating disk store {0} is : {1}";
  public static final String VALIDATE_DISK_STORE__MSG__ERROR = "Error in validating disk store {0} is : {1}";

  /* 'version' command */
  public static final String VERSION = "version";
  public static final String VERSION__HELP = "Display product version information.";
  public static final String VERSION__FULL = "full";
  public static final String VERSION__FULL__HELP = "Whether to show the full version information.";

  /* start gateway command messages */
  public static final String GATEWAY__MSG__OPTIONS = "Provide only one of member/group";
  public static final String GATEWAY_MSG_MEMBER_0_NOT_FOUND = "Member : {0} not found";
  public static final String GATEWAY_MSG_MEMBERS_NOT_FOUND = "Members not found";
  public static final String GATEWAY_ERROR = "Error";
  public static final String GATEWAY_OK = "OK";

  public static final String HEADER_GATEWAYS = "Gateways";
  public static final String SECTION_GATEWAY_SENDER = "GatewaySender Section";
  public static final String TABLE_GATEWAY_SENDER = "GatewaySender Table";
  public static final String HEADER_GATEWAY_SENDER = "GatewaySender";

  public static final String RESULT_GATEWAY_SENDER_ID = "GatewaySender Id";
  public static final String RESULT_HOST_MEMBER = "Member";
  public static final String RESULT_REMOTE_CLUSTER = "Remote Cluster Id";
  public static final String RESULT_TYPE = "Type";
  public static final String RESULT_STATUS = "Status";
  public static final String RESULT_QUEUED_EVENTS = "Queued Events";
  public static final String RESULT_RECEIVER = "Receiver Location";
  public static final String SENDER_PARALLEL = "Parallel";
  public static final String SENDER_SERIAL = "Serial";
  public static final String GATEWAY_RUNNING = "Running";
  public static final String GATEWAY_NOT_RUNNING = "Not Running";
  public static final String SENDER_PAUSED = "Paused";
  public static final String SENDER_PRIMARY = "Primary";
  public static final String RESULT_POLICY = "Runtime Policy";
  public static final String SENDER_SECONADRY = "Secondary";

  public static final String SECTION_GATEWAY_RECEIVER = "GatewayReceiver Section";
  public static final String TABLE_GATEWAY_RECEIVER = "GatewayReceiver Table";
  public static final String HEADER_GATEWAY_RECEIVER = "GatewayReceiver";
  public static final String RESULT_PORT = "Port";
  public static final String RESULT_SENDERS_COUNT = "Sender Count";
  public static final String RESULT_SENDER_CONNECTED = "Sender's Connected";
  public static final String SECTION_GATEWAY_SENDER_AVAILABLE = "Available GatewaySender Section";
  public static final String SECTION_GATEWAY_SENDER_NOT_AVAILABLE = "Not Available GatewaySender Section";
  public static final String SECTION_GATEWAY_RECEIVER_AVAILABLE = "Available GatewayReceiver Section";
  public static final String SECTION_GATEWAY_RECEIVER_NOT_AVAILABLE = "Not Available GatewayReceiver Section";

  public static final String GATEWAY_RECEIVER_IS_NOT_AVAILABLE_OR_STOPPED = "GatewayReceiver is not available or already stopped";
  public static final String GATEWAY_RECEIVER_IS_NOT_AVAILABLE_OR_STOPPED_ON_MEMBER = "GatewayReceiver is not available or already stopped on member {0}";
  public static final String GATEWAY_RECEIVER_IS_ALREADY_STARTED_ON_MEMBER_0 = "GatewayReceiver is already started on member {0}";
  public static final String GATEWAY_RECEIVER_IS_STARTED_ON_MEMBER_0 = "GatewayReceiver is started on member {0}";
  public static final String GATEWAY_RECEIVER_IS_NOT_AVAILABLE_ON_MEMBER_0 = "GatewayReceiver is not available on member {0}";

  public static final String GATEWAY_SENDER_IS_NOT_AVAILABLE = "GatewaySender is not available";
  public static final String GATEWAY_SENDER_0_IS_ALREADY_STARTED_ON_MEMBER_1 = "GatewaySender {0} is already started on member {1}";
  public static final String GATEWAY_SENDER_0_IS_NOT_AVAILABLE_ON_MEMBER_1 = "GatewaySender {0} is not available on member {1}";
  public static final String GATEWAY_SENDER_0_IS_STARTED_ON_MEMBER_1 = "GatewaySender {0} is started on member {1}";
  public static final String GATEWAY_SENDER_0_IS_ALREADY_PAUSED_ON_MEMBER_1 = "GatewaySender {0} is already paused on member {1}";
  public static final String GATEWAY_SENDER_0_IS_PAUSED_ON_MEMBER_1 = "GatewaySender {0} is paused on member {1}";
  public static final String GATEWAY_SENDER_0_IS_NOT_RUNNING_ON_MEMBER_1 = "GatewaySender {0} is not running on member {1}.";
  public static final String GATEWAY_SENDER_0_IS_RESUMED_ON_MEMBER_1 = "GatewaySender {0} is resumed on member {1}";
  public static final String GATEWAY_SENDER_0_IS_NOT_PAUSED_ON_MEMBER_1 = "GatewaySender {0} is not paused on member {1}";
  public static final String GATEWAY_SENDER_0_IS_STOPPED_ON_MEMBER_1 = "GatewaySender {0} is stopped on member {1}";
  public static final String GATEWAY_SENDER_0_IS_REBALANCED_ON_MEMBER_1 = "GatewaySender {0} is rebalanced on member {1}";
  public static final String GATEWAY_SENDER_0_IS_NOT_FOUND_ON_ANY_MEMBER = "GatewaySender {0} is not found on any member";
  public static final String GATEWAY_RECEIVER_IS_STOPPED_ON_MEMBER_0 = "GatewayReceiver is stopped on member {0}";
  public static final String GATEWAY_RECEIVER_IS_NOT_RUNNING_ON_MEMBER_0 = "GatewayReceiver is not running on member {0}";
  public static final String GATEWAYS_ARE_NOT_AVAILABLE_IN_CLUSTER = "GatewaySenders or GatewayRecievers are not available in cluster";
  public static final String GATEWAY_SENDER_0_COULD_NOT_BE_INVOKED_DUE_TO_1 = "Could not invoke start gateway sender {0} operation on members due to {1}";
  public static final String GATEWAY_SENDER_0_COULD_NOT_BE_STARTED_ON_MEMBER_DUE_TO_1= "Could not start gateway sender {0} on member due to {1}";
  /* end gateway command messages */

  /***
   * CQ Commands
   *
   */

  //List durable cqs

  public static final String LIST_DURABLE_CQS = "list durable-cqs";
  public static final String LIST_DURABLE_CQS__DURABLECLIENTID = DURABLE_CLIENT_ID;
  public static final String LIST_DURABLE_CQS__DURABLECLIENTID__HELP = "The id used to identify the durable client";
  public static final String LIST_DURABLE_CQS__HELP = "List durable client cqs associated with the specified durable client id.";
  public static final String LIST_DURABLE_CQS__MEMBER = "member";
  public static final String LIST_DURABLE_CQS__MEMBER__HELP = "Name/Id of the member for which the durable client is registered and durable cqs will be displayed.";
  public static final String LIST_DURABLE_CQS__GROUP = "group";
  public static final String LIST_DURABLE_CQS__GROUP__HELP = "Group of members for which the durable client is registered and durable cqs will be displayed.";
  public static final String LIST_DURABLE_CQS__NO__CQS__FOR__CLIENT = "No durable cqs found for durable-client-id : \"{0}\".";
  public static final String LIST_DURABLE_CQS__PER__MEMBER__MSG = "Durable cqs on member : \"{0}\".";
  public static final String LIST_DURABLE_CQS__FAILURE__MSG = "Errors while retrieving cqs for durable client \"{0}\".";
  public static final String LIST_DURABLE_CQS__EXCEPTION__OCCURRED__ON = "Members with exceptions while retrieving durable cqs.";
  public static final String LIST_DURABLE_CQS__NO__CQS__REGISTERED= "No durable cq's registered on this member.";
  public static final String LIST_DURABLE_CQS__NAME = "durable-cq-name";
  public static final String LIST_DURABLE_CQS__FAILURE__HEADER = "Unable to list durable-cqs for durable-client-id : \"{0}\" due to following reasons.";

  //Close Durable CQ's
  public static final String CLOSE_DURABLE_CQS = "close durable-cq";
  public static final String CLOSE_DURABLE_CQS__HELP = "Closes the durable cq registered by the durable client and drains events held for the durable cq from the subscription queue.";
  public static final String CLOSE_DURABLE_CQS__NAME = "durable-cq-name";
  public static final String CLOSE_DURABLE_CQS__NAME__HELP = "Name of the cq to be closed.";
  public static final String CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID = DURABLE_CLIENT_ID;
  public static final String CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID__HELP = "The id of the durable client";
  public static final String CLOSE_DURABLE_CQS__MEMBER = "member";
  public static final String CLOSE_DURABLE_CQS__MEMBER__HELP = "Name/Id of the member for which the durable client is registered and the cq to be closed.";
  public static final String CLOSE_DURABLE_CQS__GROUP = "group";
  public static final String CLOSE_DURABLE_CQS__GROUP__HELP = "Group of members for which the durable client is registered and the cq to be closed.";
  public static final String CLOSE_DURABLE_CQS__FAILURE__HEADER = "Could not close the durable-cq : \"{0}\" for the durable-client-id : \"{1}\" due to following reasons.";
  public static final String CLOSE_DURABLE_CQS__SUCCESS = "Closed the durable cq : \"{0}\" for the durable client : \"{1}\".";
  public static final String CLOSE_DURABLE_CQS__UNABLE__TO__CLOSE__CQ = "Unable to close the durable cq : \"{0}\" for the durable client : \"{1}\".";

  //Close Durable Clients
  public static final String CLOSE_DURABLE_CLIENTS = "close durable-client";
  public static final String CLOSE_DURABLE_CLIENTS__HELP = "Attempts to close the durable client, the client must be disconnected.";
  public static final String CLOSE_DURABLE_CLIENTS__CLIENT__ID = DURABLE_CLIENT_ID;
  public static final String CLOSE_DURABLE_CLIENTS__CLIENT__ID__HELP = "The id used to identify the durable client.";
  public static final String CLOSE_DURABLE_CLIENTS__MEMBER = "member";
  public static final String CLOSE_DURABLE_CLIENTS__MEMBER__HELP = "Name/Id of the member for which the durable client is to be closed.";
  public static final String CLOSE_DURABLE_CLIENTS__GROUP = "group";
  public static final String CLOSE_DURABLE_CLIENTS__GROUP__HELP = "Group of members for which the durable client is to be closed.";
  public static final String CLOSE_DURABLE_CLIENTS__FAILURE__HEADER = "Unable to close the durable client : \"{0}\" due to following reasons.";
  public static final String CLOSE_DURABLE_CLIENTS__SUCCESS = "Closed the durable client : \"{0}\".";

  public static final String COUNT_DURABLE_CQ_EVENTS = "show subscription-queue-size";
  public static final String COUNT_DURABLE_CQ_EVENTS__HELP = "Shows the number of events in the subscription queue.  If a cq name is provided, counts the number of events in the subscription queue for the specified cq.";
  public static final String COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID = DURABLE_CLIENT_ID;
  public static final String COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID__HELP = "The id used to identify the durable client.";
  public static final String COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME = "durable-cq-name";
  public static final String COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME__HELP = "The name that identifies the cq.";
  public static final String COUNT_DURABLE_CQ_EVENTS__MEMBER = "member";
  public static final String COUNT_DURABLE_CQ_EVENTS__MEMBER__HELP = "Name/Id of the member for which the subscription events are to be counted.";
  public static final String COUNT_DURABLE_CQ_EVENTS__GROUP = "group";
  public static final String COUNT_DURABLE_CQ_EVENTS__GROUP__HELP = "Group of members for which the subscription queue events are to be counted.";
  public static final String COUNT_DURABLE_CQ_EVENTS__DURABLE_CLIENT_NOT_FOUND = "No durable client found on server for durable client id {0}.";
  public static final String COUNT_DURABLE_CQ_EVENTS__DURABLE_CQ_NOT_FOUND = "No cq found on server for client with durable-client-id : {0} with cq-name : {1}.";
  public static final String COUNT_DURABLE_CQ_EVENTS__DURABLE_CQ_STATS_NOT_FOUND = "No cq stats found on server for durable client id {0} with cq name {1}.";
  public static final String COUNT_DURABLE_CQ_EVENTS__NO__CQS__REGISTERED = "No cq's registered on this member";
  public static final String COUNT_DURABLE_CQ_EVENTS__SUBSCRIPTION__QUEUE__SIZE__CQ = "subcription-queue-size for durable-cq : \"{0}\".";
  public static final String COUNT_DURABLE_CQ_EVENTS__SUBSCRIPTION__QUEUE__SIZE__CLIENT = "subcription-queue-size for durable-client : \"{0}\".";

  /***
   * Cluster Configuration commands 
   */
  
  public static final String EXPORT_SHARED_CONFIG = "export cluster-configuration";
  public static final String EXPORT_SHARED_CONFIG__HELP = "Exports the cluster configuration artifacts as a zip file.";
  public static final String EXPORT_SHARED_CONFIG__DIR = "dir";
  public static final String EXPORT_SHARED_CONFIG__DIR__HELP = "The directory in which the exported cluster configuration artifacts will be saved";
  public static final String EXPORT_SHARED_CONFIG__FILE = "zip-file-name";
  public static final String EXPORT_SHARED_CONFIG__FILE__HELP = "Name of the zip file containing the exported cluster configuration artifacts";
  public static final String EXPORT_SHARED_CONFIG__MSG__NOT_A_DIRECTORY = "{0} is not a directory.";
  public static final String EXPORT_SHARED_CONFIG__MSG__CANNOT_CREATE_DIR = "Directory {0} could not be created.";
  public static final String EXPORT_SHARED_CONFIG__MSG__NOT_WRITEABLE = "{0} is not writeable";
  public static final String EXPORT_SHARED_CONFIG__FILE__NAME = "cluster-config-{0}.zip";
  public static final String EXPORT_SHARED_CONFIG__DOWNLOAD__MSG = "Downloading cluster configuration : {0}";
  public static final String EXPORT_SHARED_CONFIG__LOCATOR_HEADER = "Locator";
  public static final String EXPORT_SHARED_CONFIG__LOCATOR__ERROR__MSG__HEADER = "Error";
  public static final String EXPORT_SHARED_CONFIG__UNABLE__TO__EXPORT__CONFIG = "Unable to export config";
  
  public static final String IMPORT_SHARED_CONFIG = "import cluster-configuration";
  public static final String IMPORT_SHARED_CONFIG__HELP = "Imports configuration into cluster configuration hosted at the locators";
  public static final String IMPORT_SHARED_CONFIG__ZIP = "zip-file-name";
  public static final String IMPORT_SHARED_CONFIG__ZIP__HELP = "The zip file containing the cluster configuration artifacts, which are to be imported";
  public static final String IMPORT_SHARED_CONFIG__DIR = "dir";
  public static final String IMPORT_SHARED_CONFIG__DIR__HELP = "The directory which contains the cluster configuration artifacts, which are to be imported.";
  public static final String IMPORT_SHARED_CONFIG__PRE__IMPORT__MSG = "Importing configuration into locator would overwrite the existing cluster configuration. Would you like to proceed ?";
  public static final String IMPORT_SHARED_CONFIG__CANNOT__IMPORT__MSG = "Cluster configuration cannot be imported when there are running data members present in the distributed system.\n" +
  		                                                          "Shutdown all the non locator members to import the cluster configuration, and restart them after the cluster configuration is successfully imported." ;
  public static final String NO_LOCATORS_WITH_SHARED_CONFIG = "No locators found with \"enable-cluster-configuration=true\".";
  public static final String IMPORT_SHARED_CONFIG__ARTIFACTS__COPIED = "Cluster configuration artifacts successfully copied";
  public static final String IMPORT_SHARED_CONFIG__SUCCESS__MSG = "Cluster configuration successfully imported";
  public static final String IMPORT_SHARED_CONFIG__PROVIDE__ZIP = "Parameter \"{0}\"  is required. Use \"help <command name>\" for assistance.";
  
  public static final String STATUS_SHARED_CONFIG = "status cluster-config-service";
  public static final String STATUS_SHARED_CONFIG_HELP = "Displays the status of cluster configuration service on all the locators with enable-cluster-configuration set to true.";
  public static final String STATUS_SHARED_CONFIG_NAME_HEADER = "Name";
  public static final String STATUS_SHARED_CONFIG_HOST_HEADER = "Hostname";
  public static final String STATUS_SHARED_CONFIG_PORT_HEADER = "Port";
  public static final String STATUS_SHARED_CONFIG_STATUS = "Status";
  
  
  public static final String CONFIGURE_PDX = "configure pdx";
  public static final String CONFIGURE_PDX__HELP = "Configures Geode's Portable Data eXchange for all the cache(s) in the cluster. This command would not take effect on the running members in the system.\n This command persists the pdx configuration in the locator with cluster configuration service. \n This command should be issued before starting any data members.";
  public static final String CONFIGURE_PDX__READ__SERIALIZED = "read-serialized";
  public static final String CONFIGURE_PDX__READ__SERIALIZED__HELP = "Set to true to have PDX deserialization produce a PdxInstance instead of an instance of the domain class";
  public static final String CONFIGURE_PDX__IGNORE__UNREAD_FIELDS = "ignore-unread-fields";
  public static final String CONFIGURE_PDX__IGNORE__UNREAD_FIELDS__HELP = "Control whether pdx ignores fields that were unread during deserialization. The default is to preserve unread fields be including their data during serialization. But if you configure the cache to ignore unread fields then their data will be lost during serialization."+
                                                                           "You should only set this attribute to true if you know this member will only be reading cache data. In this use case you do not need to pay the cost of preserving the unread fields since you will never be reserializing pdx data.";
  public static final String CONFIGURE_PDX__PERSISTENT = "persistent";
  public static final String CONFIGURE_PDX__PERSISTENT__HELP = "Control whether the type metadata for PDX objects is persisted to disk. The default for this setting is false. If you are using persistent regions with PDX then you must set this to true. If you are using a GatewaySender or AsyncEventQueue with PDX then you should set this to true";
  public static final String CONFIGURE_PDX__DISKSTORE = "disk-store";
  public static final String CONFIGURE_PDX__DISKSTORE__HELP = "Named disk store where the PDX type data will be stored";
  
  public static final String CONFIGURE_PDX__CHECK__PORTABILITY = "check-portability";
  public static final String CONFIGURE_PDX__CHECK__PORTABILITY__HELP = "if true then an serialization done by this serializer will throw an exception if the object it not portable to non-java languages.";
  
  public static final String CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES = "portable-auto-serializable-classes";
  public static final String CONFIGURE_PDX__PORTABLE__AUTO__SERIALIZER__CLASSES__HELP = "the patterns which are matched against domain class names to determine whether they should be serialized";
  
  public static final String CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES = "auto-serializable-classes";
  public static final String CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES__HELP = "the patterns which are matched against domain class names to determine whether they should be serialized, serialization done by the auto-serializer will throw an exception if the object of these classes are not portable to non-java languages";
  public static final String CONFIGURE_PDX__NORMAL__MEMBERS__WARNING = "The command would only take effect on new data members joining the distributed system. It won't affect the existing data members";
  public static final String CONFIGURE_PDX__ERROR__MESSAGE = "The autoserializer cannot support both portable and non-portable classes at the same time.";
  
  public static final String PDX_RENAME = "pdx rename";
  public static final String PDX_RENAME__HELP = "Renames PDX types in an offline disk store. \n Any pdx types that are renamed will be listed in the output. \n If no renames are done or the disk-store is online then this command will fail.";
  public static final String PDX_RENAME_OLD = "old";
  public static final String PDX_RENAME_OLD__HELP = "If a PDX type's fully qualified class name has a word that matches this text then it will be renamed. Words are delimited by '.' and '$'.";
  public static final String PDX_RENAME_NEW = "new";
  public static final String PDX_RENAME_NEW__HELP = "The text to replace the word that matched old";
  public static final String PDX_DISKSTORE = "disk-store";
  public static final String PDX_DISKSTORE__HELP = "Name of the disk store to operate on";
  public static final String PDX_DISKDIR = "disk-dirs";
  public static final String PDX_DISKDIR__HELP = "Directories which contain the disk store files";
  public static final String PDX_RENAME__SUCCESS = "Successfully renamed pdx types:\n{0}";
  public static final String PDX_RENAME__ERROR = "Error renaming pdx types : {0}";
  public static final String PDX_RENAME__EMPTY = "No Pdx types found to rename.";
  
  public static final String PDX_DELETE_FIELD = "pdx delete-field";
  public static final String PDX_DELETE_FIELD__HELP = "Deletes a field from a PDX type in an offline disk store. \n Any pdx types with a field deleted by this command will be listed in the output. \n If no deletes are done or the disk-store is online then this command will fail.";
  public static final String PDX_CLASS = "class";
  public static final String PDX_CLASS__HELP = "The fully qualified class name of the pdx type to delete the field from.";
  public static final String PDX_FIELD = "field";
  public static final String PDX_FIELD__HELP = "Field name to delete";
  public static final String PDX_DELETE_FIELD__SUCCESS = "Successfully deleted field in types:\n{0}";
  public static final String PDX_DELETE_FIELD__ERROR = "Error deleting field : {0}";
  public static final String PDX_DELETE__EMPTY = "Field to be deleted not found in the class.";


  public static final String START_SERVER__REST_API = "start-rest-api";
  public static final String START_SERVER__REST_API__HELP = "When set to true, will start the REST API service.";
  public static final String START_SERVER__HTTP_SERVICE_PORT = "http-service-port";
  public static final String START_SERVER__HTTP_SERVICE_PORT__HELP = "Port on which HTTP Service will listen on";
  public static final String START_SERVER__HTTP_SERVICE_BIND_ADDRESS = "http-service-bind-address";
  public static final String START_SERVER__HTTP_SERVICE_BIND_ADDRESS__HELP = "The IP address on which the HTTP Service will be bound.  By default, the Server is bound to all local addresses.";
  /**
   * Creates a MessageFormat with the given pattern and uses it to format the given argument.
   *
   * @param pattern
   *          the pattern to be substituted with given argument
   * @param argument
   *          an object to be formatted and substituted
   * @return formatted string
   * @throws IllegalArgumentException
   *           if the pattern is invalid, or the argument is not of the type expected by the format element(s) that use
   *           it.
   */
  public static String format(String pattern, Object argument) {
    return format(pattern, new Object[] { argument });
  }

  /**
   * Creates a MessageFormat with the given pattern and uses it to format the given arguments.
   *
   * @param pattern
   *          the pattern to be substituted with given arguments
   * @param arguments
   *          an array of objects to be formatted and substituted
   * @return formatted string
   * @throws IllegalArgumentException
   *           if the pattern is invalid, or if an argument in the <code>arguments</code> array is not of the type
   *           expected by the format element(s) that use it.
   */
  public static String format(String pattern, Object... arguments) {
    return MessageFormat.format(pattern, arguments);
  }
  
  
  public static final String IGNORE_INTERCEPTORS = "ignoreInterCeptors";
  
}

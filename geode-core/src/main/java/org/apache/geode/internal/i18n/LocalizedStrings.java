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

package org.apache.geode.internal.i18n;

import org.apache.geode.i18n.StringId;


/**
 * This interface defines all of the {@link StringId} that
 * are used for internationalization, aka i18n.
 * <p>
 * <b>No code other than StringId creation belongs here</b>
 * <p>
 * It is an interface so that classes that need to access a StringId
 * can simply add <code>implements LocalizedStrings</code> and then
 * access the StringIds directly
 * <code>
 * public class SomeClass implements LocalizedStrings {
 * public logStuff() {
 * getLogger().info(SomeClass_SOME_STRINGID);
 * }
 * }
 * </code>
 * @since GemFire 6.0
 */
public class LocalizedStrings {

  /**
   * reserved space for commonly used strings, messageId 0-1024
   **/
  public static final StringId EMPTY = new StringId(0, "");
  public static final StringId ONE_ARG = StringId.LITERAL;
  //alias for use in temporarily adding debugging to the product
  public static final StringId DEBUG = ONE_ARG;

  public static final StringId DONT_RELEASE = ONE_ARG;

  //alias for use in the test tree
  public static final StringId TESTING = ONE_ARG;
  public static final StringId TWO_ARG = new StringId(2, "{0} {1}");
  public static final StringId TWO_ARG_COLON = new StringId(3, "{0} : {1}");
  public static final StringId ERROR = new StringId(4, "ERROR");
  public static final StringId CACHE_IS_CLOSING = new StringId(1021, "Cache is closing");
  public static final StringId NOT_A_REAL_GEMFIREVM = new StringId(1022, "Not a real GemFireVM");
  public static final StringId SHOULDNT_INVOKE = new StringId(1023, "Should not be invoked");
  public static final StringId UNSUPPORTED_AT_THIS_TIME = new StringId(1024, "Unsupported at this time");

  /**
   * Gemfire strings, messageId 1025-15000
   **/
  public static final StringId AbstractHealthEvaluator_OKAY_HEALTH__0 = new StringId(1025, "OKAY_HEALTH:  {0}");
  public static final StringId AbstractHealthEvaluator_POOR_HEALTH__0 = new StringId(1026, "POOR_HEALTH:  {0}");
  public static final StringId AbstractRegion_CACHECALLBACK_CLOSE_EXCEPTION = new StringId(1027, "CacheCallback close exception");
  public static final StringId PoolManagerImpl_POOL_NAMED_0_ALREADY_EXISTS = new StringId(1028, "A pool named \"{0}\" already exists");
  public static final StringId AcceptorImpl_CACHE_SERVER_CONNECTION_LISTENER_BOUND_TO_ADDRESS_0_WITH_BACKLOG_1 = new StringId(1029, "Cache server connection listener bound to address {0} with backlog {1}.");
  public static final StringId AcceptorImpl_CACHE_SERVER_FAILED_ACCEPTING_CLIENT_CONNECTION_DUE_TO_SOCKET_TIMEOUT = new StringId(1030, "Cache server: failed accepting client connection due to socket timeout.");
  public static final StringId AcceptorImpl_CACHE_SERVER_FAILED_ACCEPTING_CLIENT_CONNECTION__0 = new StringId(1031, "Cache server: failed accepting client connection  {0}");
  public static final StringId AcceptorImpl_CACHE_SERVER_ON_PORT_0_IS_SHUTTING_DOWN = new StringId(1032, "Cache server on port {0} is shutting down.");
  public static final StringId AcceptorImpl_CACHE_SERVER_TIMED_OUT_WAITING_FOR_HANDSHAKE_FROM__0 = new StringId(1033, "Cache server: timed out waiting for handshake from  {0}");
  public static final StringId AcceptorImpl_CACHE_SERVER_UNEXPECTED_EXCEPTION = new StringId(1034, "Cache server: Unexpected Exception");
  public static final StringId AcceptorImpl_CACHE_SERVER_UNEXPECTED_IOEXCEPTION_FROM_ACCEPT = new StringId(1035, "Cache server: Unexpected IOException from accept");
  public static final StringId AcceptorImpl_EXCEEDED_MAX_CONNECTIONS_0 = new StringId(1036, "exceeded max-connections {0}");
  public static final StringId AcceptorImpl_IGNORING = new StringId(1037, "ignoring");
  public static final StringId AcceptorImpl_IGNORING_EVENT_ON_SELECTOR_KEY__0 = new StringId(1038, "ignoring event on selector key  {0}");
  public static final StringId CacheClientNotifier_CACHECLIENTNOTIFIER_A_PREVIOUS_CONNECTION_ATTEMPT_FROM_THIS_CLIENT_IS_STILL_BEING_PROCESSED__0 = new StringId(1039, "A previous connection attempt from this client is still being processed: {0}");
  public static final StringId AcceptorImpl_REJECTED_CONNECTION_FROM_0_BECAUSE_CURRENT_CONNECTION_COUNT_OF_1_IS_GREATER_THAN_OR_EQUAL_TO_THE_CONFIGURED_MAX_OF_2 = new StringId(1040, "Rejected connection from {0} because current connection count of {1} is greater than or equal to the configured max of {2}");
  public static final StringId AcceptorImpl_SELECTOR_ENABLED = new StringId(1041, "SELECTOR enabled");
  public static final StringId AcceptorImpl_UNEXPECTED = new StringId(1042, "unexpected");
  public static final StringId AdminDistributedSystem_COULD_NOT_SET_THE_GEMFIRE_VM = new StringId(1043, "Could not set the GemFire VM.");
  public static final StringId AdminDistributedSystemImpl_ADDING_NEW_APPLICATION_FOR__0 = new StringId(1044, "Adding new Application for  {0}");
  public static final StringId AdminDistributedSystemImpl_ADDING_NEW_CACHESERVER_FOR__0 = new StringId(1045, "Adding new CacheServer for  {0}");
  public static final StringId AdminDistributedSystemImpl_WHILE_GETTING_CANONICAL_FILE = new StringId(1046, "While getting canonical file");
  public static final StringId AdminRequest_RESPONSE_TO__0__WAS_CANCELLED = new StringId(1047, "Response to  {0}  was cancelled.");
  public static final StringId AdminWaiters_COULD_NOT_SEND_REQUEST_0 = new StringId(1048, "Could not send request.{0}");
  public static final StringId AdminWaiters_REQUEST_SEND_TO_0_WAS_CANCELLED_1 = new StringId(1049, "Request sent to {0} was cancelled. {1}");
  public static final StringId AdminWaiters_REQUEST_SENT_TO_0_FAILED_SINCE_MEMBER_DEPARTED_1 = new StringId(1050, "Request sent to {0} failed since member departed.{1}");
  public static final StringId AgentImpl_0__IS_ALREADY_REGISTERED = new StringId(1051, "{0}  is already registered.");
  public static final StringId AgentImpl_AGENT_HAS_STOPPED = new StringId(1052, "Agent has stopped");
  public static final StringId AgentImpl_AUTO_CONNECT_FAILED__0 = new StringId(1053, "auto connect failed:  {0}");
  public static final StringId AgentImpl_FAILED_TO_START_HTTPADAPTOR__0 = new StringId(1054, "Failed to start HttpAdaptor:  {0}");
  public static final StringId AgentImpl_FAILED_TO_START_RMICONNECTORSERVER = new StringId(1055, "Failed to start RMIConnectorServer:");
  public static final StringId AgentImpl_FAILED_TO_START_SNMPADAPTOR__0 = new StringId(1056, "Failed to start SnmpAdaptor:  {0}");
  public static final StringId AgentImpl_GEMFIRE_JMX_AGENT_IS_RUNNING = new StringId(1057, "GemFire JMX Agent is running...");
  public static final StringId AgentImpl_HTTPADAPTOR_ALREADY_REGISTERED_AS__0 = new StringId(1058, "HttpAdaptor already registered as  {0}");
  public static final StringId AgentImpl_HTTP_ADAPTOR_LISTENING_ON_ADDRESS__0 = new StringId(1059, "HTTP adaptor listening on address:  {0}");
  public static final StringId AgentImpl_HTTP_ADAPTOR_LISTENING_ON_PORT__0 = new StringId(1060, "HTTP adaptor listening on port:  {0}");
  public static final StringId AgentImpl_INCORRECT_NULL_HOSTNAME = new StringId(1061, "Incorrect null hostname");
  public static final StringId AgentImpl_INCORRECT_PORT_VALUE__0 = new StringId(1062, "Incorrect port value  {0}");
  public static final StringId AgentImpl_RMICONNECTORSERVER_ALREADY_REGISTERED_AS__0 = new StringId(1063, "RMIConnectorServer already registered as  {0}");
  public static final StringId AgentImpl_SNMPADAPTOR_ALREADY_REGISTERED_AS__0 = new StringId(1064, "SnmpAdaptor already registered as  {0}");
  public static final StringId AgentImpl_STOPPING_JMX_AGENT = new StringId(1065, "Stopping JMX agent");
  public static final StringId AgentImpl_XSLTPROCESSOR_ALREADY_REGISTERED_AS__0 = new StringId(1066, "XsltProcessor already registered as  {0}");
  public static final StringId AbstractRegion_THE_CONNECTION_POOL_0_HAS_NOT_BEEN_CREATED = new StringId(1067, "The connection pool \"{0}\" has not been created");

  public static final StringId AttributesFactory_ADDCACHELISTENER_PARAMETER_WAS_NULL = new StringId(1069, "addCacheListener parameter was null");
  public static final StringId AttributesFactory_AN_EVICTION_CONTROLLER_WITH_LOCAL_DESTROY_EVICTION_ACTION_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION = new StringId(1070, "An Eviction Controller with local destroy eviction action is incompatible with distributed replication");

  public static final StringId AttributesFactory_CONCURRENCYLEVEL_MUST_BE_0 = new StringId(1072, "concurrencyLevel must be > 0");
  public static final StringId AttributesFactory_DATAPOLICY_MUST_NOT_BE_NULL = new StringId(1073, "dataPolicy must not be null");
  public static final StringId AttributesFactory_DATA_POLICIES_OTHER_THAN_0_ARE_NOT_ALLOWED_IN_PARTITIONED_REGIONS = new StringId(1074, "Data policies other than {0} are not allowed in  partitioned regions.");
  public static final StringId AttributesFactory_DATA_POLICIES_OTHER_THAN_0_ARE_NOT_SUPPORTED_FOR_PARTITIONED_REGIONS = new StringId(1075, "Data policies other than {0} are not supported for Partitioned Regions");
  public static final StringId AttributesFactory_DATA_POLICY_0_IS_NOT_ALLOWED_FOR_A_PARTITIONED_REGION_DATAPOLICIES_OTHER_THAN_1_ARE_NOT_ALLOWED = new StringId(1076, "Data policy {0} is not allowed for a partitioned region. DataPolicies other than {1} are not allowed.");
  public static final StringId AttributesFactory_DIR_SIZE_CANNOT_BE_NEGATIVE_0 = new StringId(1077, "Dir size cannot be negative : {0}");

  public static final StringId AttributesFactory_EXPIRATIONACTIONLOCAL_DESTROY_ON_THE_ENTRIES_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION = new StringId(1079, "ExpirationAction.LOCAL_DESTROY on the entries is incompatible with distributed replication");
  public static final StringId AttributesFactory_EXPIRATIONACTIONLOCAL_INVALIDATE_ON_THE_ENTRIES_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION = new StringId(1080, "ExpirationAction.LOCAL_INVALIDATE on the entries is incompatible with distributed replication");
  public static final StringId AttributesFactory_EXPIRATIONACTIONLOCAL_INVALIDATE_ON_THE_REGION_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION = new StringId(1081, "ExpirationAction.LOCAL_INVALIDATE on the region is incompatible with distributed replication");

  public static final StringId AttributesFactory_IDLETIMEOUT_MUST_NOT_BE_NULL = new StringId(1085, "idleTimeout must not be null");
  public static final StringId AttributesFactory_IF_THE_DATA_POLICY_IS_0_THEN_ENTRY_EXPIRATION_IS_NOT_ALLOWED = new StringId(1086, "If the data policy is {0} then entry expiration is not allowed.");
  public static final StringId AttributesFactory_IF_THE_DATA_POLICY_IS_0_THEN_EVICTION_IS_NOT_ALLOWED = new StringId(1087, "If the data policy is {0} then eviction is not allowed.");
  public static final StringId AttributesFactory_IF_THE_MEMBERSHIP_ATTRIBUTES_HAS_REQUIRED_ROLES_THEN_SCOPE_MUST_NOT_BE_LOCAL = new StringId(1088, "If the membership attributes has required roles then scope must not be LOCAL.");
  public static final StringId AttributesFactory_INITCACHELISTENERS_PARAMETER_HAD_A_NULL_ELEMENT = new StringId(1089, "initCacheListeners parameter had a null element");
  public static final StringId AttributesFactory_INITIALCAPACITY_MUST_BE_0 = new StringId(1090, "initialCapacity must be >= 0");
  public static final StringId AttributesFactory_KEYCONSTRAINT_MUST_NOT_BE_A_PRIMITIVE_TYPE = new StringId(1091, "keyConstraint must not be a primitive type");
  public static final StringId AttributesFactory_LOADFACTOR_MUST_BE_0_VALUE_IS_0 = new StringId(1092, "loadFactor must be > 0, value is {0}");
  public static final StringId AttributesFactory_MIRRORTYPE_MUST_NOT_BE_NULL = new StringId(1093, "mirrorType must not be null");
  public static final StringId AttributesFactory_MORE_THAN_ONE_CACHE_LISTENER_EXISTS = new StringId(1094, "More than one cache listener exists.");
  public static final StringId AttributesFactory_NO_MIRROR_TYPE_CORRESPONDS_TO_DATA_POLICY_0 = new StringId(1095, "No mirror type corresponds to data policy \"{0}\".");
  public static final StringId AttributesFactory_NUMBER_OF_DISKSIZES_IS_0_WHICH_IS_NOT_EQUAL_TO_NUMBER_OF_DISK_DIRS_WHICH_IS_1 = new StringId(1096, " Number of diskSizes is {0} which is not equal to number of disk Dirs which is {1}");
  public static final StringId AttributesFactory_PARTITIONATTRIBUTES_LOCALMAXMEMORY_MUST_NOT_BE_NEGATIVE = new StringId(1097, "PartitionAttributes localMaxMemory must not be negative.");
  public static final StringId AttributesFactory_SCOPETYPE_MUST_NOT_BE_NULL = new StringId(1098, "scopeType must not be null");
  public static final StringId AttributesFactory_SETLOCKGRANTERTRUE_IS_NOT_ALLOWED_IN_PARTITIONED_REGIONS = new StringId(1099, "setLockGranter(true) is not allowed in Partitioned Regions.");
  public static final StringId AttributesFactory_SETTING_SCOPE_ON_A_PARTITIONED_REGIONS_IS_NOT_ALLOWED = new StringId(1100, "Setting Scope on a Partitioned Regions is not allowed.");
  public static final StringId AttributesFactory_STATISTICS_MUST_BE_ENABLED_FOR_EXPIRATION = new StringId(1101, "Statistics must be enabled for expiration");
  public static final StringId AttributesFactory_TIMETOLIVE_MUST_NOT_BE_NULL = new StringId(1102, "timeToLive must not be null");
  public static final StringId AttributesFactory_TOTAL_SIZE_OF_PARTITION_REGION_MUST_BE_0 = new StringId(1103, "Total size of partition region must be > 0.");
  public static final StringId AttributesFactory_VALUECONSTRAINT_MUST_NOT_BE_A_PRIMITIVE_TYPE = new StringId(1104, "valueConstraint must not be a primitive type");
  public static final StringId BaseCommand_0_CONNECTION_DISCONNECT_DETECTED_BY_EOF = new StringId(1105, "{0}: connection disconnect detected by EOF.");
  public static final StringId BaseCommand_0_EOFEXCEPTION_DURING_A_WRITE_OPERATION_ON_REGION__1_KEY_2_MESSAGEID_3 = new StringId(1106, "{0}: EOFException during a write operation on region : {1} key: {2} messageId: {3}");
  public static final StringId BaseCommand_0_QUERYSTRING_IS_1 = new StringId(1107, "{0} : QueryString is: {1}.");
  public static final StringId BaseCommand_0_UNEXPECTED_ERROR_ON_SERVER = new StringId(1108, "{0} : Unexpected Error on server");
  public static final StringId BaseCommand_0_UNEXPECTED_EXCEPTION = new StringId(1109, "{0}: Unexpected Exception");
  public static final StringId BaseCommand_0_UNEXPECTED_EXCEPTION_DURING_OPERATION_ON_REGION_1_KEY_2_MESSAGEID_3 = new StringId(1110, "{0}: Unexpected Exception during operation on region: {1} key: {2} messageId: {3}");
  public static final StringId BaseCommand_0_UNEXPECTED_IOEXCEPTION = new StringId(1111, "{0}: Unexpected IOException: ");
  public static final StringId BaseCommand_0_UNEXPECTED_IOEXCEPTION_DURING_OPERATION_FOR_REGION_1_KEY_2_MESSID_3 = new StringId(1112, "{0}: Unexpected IOException during operation for region: {1} key: {2} messId: {3}");
  public static final StringId BaseCommand_0_UNEXPECTED_SHUTDOWNEXCEPTION = new StringId(1113, "{0}: Unexpected ShutdownException: ");
  public static final StringId BaseCommand_0_UNEXPECTED_SHUTDOWNEXCEPTION_DURING_OPERATION_ON_REGION_1_KEY_2_MESSAGEID_3 = new StringId(1114, "{0}: Unexpected ShutdownException during operation on region: {1} key: {2} messageId: {3}");
  public static final StringId BaseCommand_0_UNEXPECTED_THREADINTERRUPTEDEXCEPTION = new StringId(1115, "{0}: Unexpected ThreadInterruptedException: ");
  public static final StringId BaseCommand_0_UNEXPECTED_THREADINTERRUPTEDEXCEPTION_DURING_OPERATION_ON_REGION_1_KEY_2_MESSAGEID_3 = new StringId(1116, "{0}: Unexpected ThreadInterruptedException during operation on region: {1} key: {2} messageId: {3}");
  public static final StringId BaseCommand_UNKNOWN_QUERY_EXCEPTION = new StringId(1117, "Uknown query Exception.");
  public static final StringId BaseCommand_SEVERE_CACHE_EXCEPTION_0 = new StringId(1118, "Severe cache exception : {0}");
  public static final StringId BaseCommand_UNEXPECTED_QUERYINVALIDEXCEPTION_WHILE_PROCESSING_QUERY_0 = new StringId(1119, "Unexpected QueryInvalidException while processing query {0}");
  public static final StringId LocalRegion_THE_REGION_0_WAS_CONFIGURED_TO_USE_OFF_HEAP_MEMORY_BUT_OFF_HEAP_NOT_CONFIGURED = new StringId(1120, "The region {0} was configured to use off heap memory but no off heap memory was configured");

  public static final StringId CacheServerImpl_CACHESERVER_CONFIGURATION___0 = new StringId(1122, "CacheServer Configuration:   {0}");
  public static final StringId CacheServerImpl_FORCING_NOTIFYBYSUBSCRIPTION_TO_SUPPORT_DYNAMIC_REGIONS = new StringId(1123, "Forcing notifyBySubscription to support dynamic regions");

  public static final StringId BucketAdvisor_ATTEMPTED_TO_CLOSE_BUCKETADVISOR_THAT_IS_ALREADY_CLOSED = new StringId(1131, "Attempted to close BucketAdvisor that is already CLOSED");
  public static final StringId AgentImpl_COULD_NOT_TAIL_0_BECAUSE_1 = new StringId(1132, "Could not tail \"{0}\" because: {1}");
  public static final StringId SystemAdmin_USED_TO_SPECIFY_A_HOST_NAME_OR_IP_ADDRESS_TO_GIVE_TO_CLIENTS_SO_THEY_CAN_CONNECT_TO_A_LOCATOR = new StringId(1133, "Used to specify a host name or IP address to give to clients so they can connect to a locator.");
  public static final StringId BucketAdvisor_BUCKETADVISOR_WAS_NOT_CLOSED_PROPERLY = new StringId(1134, "BucketAdvisor was not closed properly.");
  public static final StringId BucketBackupMessage_BUCKETBACKUPMESSAGE_DATA_STORE_NOT_CONFIGURED_FOR_THIS_MEMBER = new StringId(1135, "BucketBackupMessage: data store not configured for this member");

  public static final StringId CacheClientNotifier_0_REGISTERCLIENT_EXCEPTION_ENCOUNTERED_IN_REGISTRATION_1 = new StringId(1140, "{0} :registerClient: Exception encountered in registration {1}");
  public static final StringId CacheClientNotifier_CACHECLIENTNOTIFIER_KEEPING_PROXY_FOR_DURABLE_CLIENT_NAMED_0_FOR_1_SECONDS_2 = new StringId(1141, "CacheClientNotifier: Keeping proxy for durable client named {0} for {1} seconds {2}.");
  public static final StringId CacheClientNotifier_CACHECLIENTNOTIFIER_THE_REQUESTED_DURABLE_CLIENT_HAS_THE_SAME_IDENTIFIER__0__AS_AN_EXISTING_DURABLE_CLIENT__1__DUPLICATE_DURABLE_CLIENTS_ARE_NOT_ALLOWED = new StringId(1142, "CacheClientNotifier: The requested durable client has the same identifier ( {0} ) as an existing durable client ( {1} ). Duplicate durable clients are not allowed.");
  public static final StringId CacheClientNotifier_CACHECLIENTNOTIFIER_UNSUCCESSFULLY_REGISTERED_CLIENT_WITH_IDENTIFIER__0 = new StringId(1143, "CacheClientNotifier: Unsuccessfully registered client with identifier  {0}");
  public static final StringId CacheClientNotifier_CANNOT_NOTIFY_CLIENTS_TO_PERFORM_OPERATION_0_ON_EVENT_1 = new StringId(1144, "CacheClientNotifier: Cannot notify clients to perform operation {0} on event {1}");
  public static final StringId CacheClientNotifier_EXCEPTION_OCCURRED_WHILE_PROCESSING_CQS = new StringId(1145, "Exception occurred while processing CQs");

  public static final StringId CacheClientNotifier_UNABLE_TO_CLOSE_CQS_FOR_THE_CLIENT__0 = new StringId(1147, "Unable to close CQs for the client:  {0}");
  public static final StringId CacheClientNotifier_UNABLE_TO_GET_THE_CQSERVICE_WHILE_CLOSING_THE_DEAD_PROXIES = new StringId(1148, "Unable to get the CqService while closing the dead proxies");
  public static final StringId CacheClientProxy_0_AN_UNEXPECTED_IOEXCEPTION_OCCURRED_SO_THE_PROXY_WILL_BE_CLOSED = new StringId(1149, "{0}: An unexpected IOException occurred so the proxy will be closed.");
  public static final StringId CacheClientProxy_0_CANCELLING_EXPIRATION_TASK_SINCE_THE_CLIENT_HAS_RECONNECTED = new StringId(1150, "{0}: Cancelling expiration task since the client has reconnected.");
  public static final StringId CacheClientProxy_0_COULD_NOT_STOP_MESSAGE_DISPATCHER_THREAD = new StringId(1151, "{0}: Could not stop message dispatcher thread.");
  public static final StringId CacheClientProxy_0_EXCEPTION_IN_CLOSING_THE_UNDERLYING_HAREGION_OF_THE_HAREGIONQUEUE = new StringId(1152, "{0}: Exception in closing the underlying HARegion of the HARegionQueue");
  public static final StringId CacheClientProxy_0_EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_ADD_MESSAGE_TO_QUEUE = new StringId(1153, "{0}: Exception occurred while attempting to add message to queue");
  public static final StringId CacheClientProxy_0_POSSIBILITY_OF_NOT_BEING_ABLE_TO_SEND_SOME_OR_ALL_THE_MESSAGES_TO_CLIENTS_TOTAL_MESSAGES_CURRENTLY_PRESENT_IN_THE_LIST_1 = new StringId(1154, "{0} Possibility of not being able to send some or all of the messages to clients. Total messages currently present in the list {1}.");
  public static final StringId CacheClientProxy_0_PROXY_CLOSING_DUE_TO_SOCKET_BEING_CLOSED_LOCALLY = new StringId(1155, "{0}: Proxy closing due to socket being closed locally.");
  public static final StringId CacheClientProxy_0_PROXY_CLOSING_DUE_TO_UNEXPECTED_BROKEN_PIPE_ON_SOCKET_CONNECTION = new StringId(1156, "{0}: Proxy closing due to unexpected broken pipe on socket connection.");
  public static final StringId CacheClientProxy_0_PROXY_CLOSING_DUE_TO_UNEXPECTED_RESET_BY_PEER_ON_SOCKET_CONNECTION = new StringId(1157, "{0}: Proxy closing due to unexpected reset by peer on socket connection.");
  public static final StringId CacheClientProxy_0_PROXY_CLOSING_DUE_TO_UNEXPECTED_RESET_ON_SOCKET_CONNECTION = new StringId(1158, "{0}: Proxy closing due to unexpected reset on socket connection.");
  public static final StringId CacheClientProxy_0__AN_UNEXPECTED_EXCEPTION_OCCURRED = new StringId(1159, "{0} : An unexpected Exception occurred");
  public static final StringId CacheClientProxy_0__EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_ADD_MESSAGE_TO_QUEUE = new StringId(1160, "{0} : Exception occurred while attempting to add message to queue");
  public static final StringId CacheClientProxy_0__PAUSING_PROCESSING = new StringId(1161, "{0} : Pausing processing");
  public static final StringId CacheClientProxy_0__RESUMING_PROCESSING = new StringId(1162, "{0} : Resuming processing");
  public static final StringId CacheClientProxy_0__THE_EXPIRATION_TASK_HAS_FIRED_SO_THIS_PROXY_IS_BEING_TERMINATED = new StringId(1163, "{0} : The expiration task has fired, so this proxy is being terminated.");

  public static final StringId CacheClientProxy_PROBLEM_CAUSED_BY_BROKEN_PIPE_ON_SOCKET = new StringId(1166, "Problem caused by broken pipe on socket.");
  public static final StringId CacheClientProxy_PROBLEM_CAUSED_BY_MESSAGE_QUEUE_BEING_CLOSED = new StringId(1167, "Problem caused by message queue being closed.");
  public static final StringId CacheClientUpdater_0_CAUGHT_FOLLOWING_EXECPTION_WHILE_ATTEMPTING_TO_CREATE_A_SERVER_TO_CLIENT_COMMUNICATION_SOCKET_AND_WILL_EXIT_1 = new StringId(1168, "{0}: Caught following exception while attempting to create a server-to-client communication socket and will exit: {1}");
  public static final StringId CacheClientUpdater_0_CONNECTION_WAS_REFUSED = new StringId(1169, "{0} connection was refused");
  public static final StringId CacheClientUpdater_SSL_NEGOTIATION_FAILED_WITH_ENDPOINT_0 = new StringId(1170, "SSL negotiation failed with endpoint: {0}");

  public static final StringId CacheClientUpdater_0_RECEIVED_AN_UNSUPPORTED_MESSAGE_TYPE_1 = new StringId(1172, "{0}: Received an unsupported message (type={1})");
  public static final StringId CacheClientUpdater_0__1__2 = new StringId(1173, "{0} :  {1} : {2}");
  public static final StringId CacheClientUpdater_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_DESTROY_ENTRY_REGION_0_KEY_1 = new StringId(1174, "The following exception occurred while attempting to destroy entry (region: {0} key: {1})");

  public static final StringId CacheClientUpdater_FAILED_TO_INVOKE_CQ_DISPATCHER_ERROR___0 = new StringId(1177, "Failed to invoke CQ Dispatcher. Error :  {0}");

  public static final StringId CacheFactory_0_AN_OPEN_CACHE_ALREADY_EXISTS = new StringId(1179, "{0}: An open cache already exists.");
  public static final StringId InternalDistributedSystem_shutdownHook_shuttingdown = new StringId(1180, "VM is exiting - shutting down distributed system");
  public static final StringId GroupMembershipService_entered_into_membership_in_group_0_with_id_1 = new StringId(1181, "Finished joining (took {0}ms).");
  public static final StringId CacheServerLauncher_CACHE_SERVER_ERROR = new StringId(1182, "Cache server error");
  public static final StringId CacheXmlParser_XML_PARSER_CHARACTERS_APPENDED_CHARACTER_DATA_0 = new StringId(1183, "XML Parser characters, appended character data: {0}");
  public static final StringId CacheXmlParser_XML_PARSER_CHARACTERS_NEW_CHARACTER_DATA_0 = new StringId(1184, "XML Parser characters, new character data: {0}");
  public static final StringId CacheXmlParser_XML_PARSER_CREATEDECLARABLE_CLASS_NAME_0 = new StringId(1185, "XML Parser createDeclarable class name: {0}");
  public static final StringId CacheXmlParser_XML_PARSER_CREATEDECLARABLE_PROPERTIES__0 = new StringId(1186, "XML Parser createDeclarable properties:  {0}");
  public static final StringId ClearRegion_0_THE_INPUT_REGION_NAME_FOR_THE_CLEAR_REGION_REQUEST_IS_NULL = new StringId(1187, "{0}: The input region name for the clear region request is null");
  public static final StringId ClearRegion_THE_INPUT_REGION_NAME_FOR_THE_CLEAR_REGION_REQUEST_IS_NULL = new StringId(1188, " The input region name for the clear region request is null");
  public static final StringId ClearRegion_WAS_NOT_FOUND_DURING_CLEAR_REGION_REGUEST = new StringId(1189, " was not found during clear region request");
  public static final StringId ClientHealtMonitor_0_IS_BEING_TERMINATED_BECAUSE_ITS_CLIENT_TIMEOUT_OF_1_HAS_EXPIRED = new StringId(1190, "{0} is being terminated because its client timeout of {1} has expired.");
  public static final StringId ClientHealthMonitor_0_AN_UNEXPECTED_EXCEPTION_OCCURRED = new StringId(1191, "{0}: An unexpected Exception occurred");
  public static final StringId ClientHealthMonitor_CLIENTHEALTHMONITORTHREAD_MAXIMUM_ALLOWED_TIME_BETWEEN_PINGS_0 = new StringId(1192, "ClientHealthMonitorThread maximum allowed time between pings: {0}");
  public static final StringId ClientHealthMonitor_CLIENTHEALTHMONITOR_REGISTERING_CLIENT_WITH_MEMBER_ID_0 = new StringId(1193, "ClientHealthMonitor: Registering client with member id {0}");
  public static final StringId ClientHealthMonitor_CLIENTHEALTHMONITOR_UNREGISTERING_CLIENT_WITH_MEMBER_ID_0 = new StringId(1194, "ClientHealthMonitor: Unregistering client with member id {0}");
  public static final StringId ClientHealthMonitor_CLIENT_HEALTH_MONITOR_THREAD_DISABLED_DUE_TO_MAXIMUMTIMEBETWEENPINGS_SETTING__0 = new StringId(1195, "Client health monitor thread disabled due to maximumTimeBetweenPings setting:  {0}");
  public static final StringId ClientHealthMonitor_MONITORING_CLIENT_WITH_MEMBER_ID_0_IT_HAD_BEEN_1_MS_SINCE_THE_LATEST_HEARTBEAT_MAX_INTERVAL_IS_2_TERMINATED_CLIENT = new StringId(1196, "Monitoring client with member id {0}. It had been {1} ms since the latest heartbeat. Max interval is {2}. Terminated client.");
  public static final StringId ClientHealthMonitor_UNEXPECTED_INTERRUPT_EXITING = new StringId(1197, "Unexpected interrupt, exiting");
  public static final StringId ClientProxyMembershipID_UNABLE_TO_DESERIALIZE_MEMBERSHIP_ID = new StringId(1198, "Unable to deserialize membership id");
  public static final StringId DiskStore_IS_USED_IN_NONPERSISTENT_REGION = new StringId(1199, "Only regions with persistence or overflow to disk can specify DiskStore");
  public static final StringId DiskRegion_COMPLEXDISKREGIONGETNEXTDIR_MAX_DIRECTORY_SIZE_WILL_GET_VIOLATED__GOING_AHEAD_WITH_THE_SWITCHING_OF_OPLOG_ANY_WAYS_CURRENTLY_AVAILABLE_SPACE_IN_THE_DIRECTORY_IS__0__THE_CAPACITY_OF_DIRECTORY_IS___1 = new StringId(1200, "Even though the configured directory size limit has been exceeded a new oplog will be created because compaction is enabled. The configured limit is {1}. The current space used in the directory by this disk store is {0}.");

  public static final StringId AttributesFactory_CLONENOTSUPPORTEDEXCEPTION_THROWN_IN_CLASS_THAT_IMPLEMENTS_CLONEABLE = new StringId(1213, "CloneNotSupportedException thrown in class that implements cloneable.");

  public static final StringId CqQueryImpl_CQ_IS_CLOSED_CQNAME_0 = new StringId(1215, "CQ is closed, CqName : {0}");

  public static final StringId Connection_DISCONNECTED_AS_A_SLOWRECEIVER = new StringId(1226, "Disconnected as a slow-receiver");

  public static final StringId ConnectionTable_FAILED_TO_ACCEPT_CONNECTION_FROM_0_BECAUSE_1 = new StringId(1240, "Failed to accept connection from {0} because: {1}");

  public static final StringId Connection_0_ASYNC_CONFIGURATION_RECEIVED_1 = new StringId(1243, "{0} async configuration received {1}.");
  public static final StringId Connection_0_ERROR_READING_MESSAGE = new StringId(1244, "{0} Error reading message");
  public static final StringId Connection_0_EXCEPTION_IN_CHANNEL_READ = new StringId(1245, "{0} exception in channel read");
  public static final StringId Connection_0_EXCEPTION_RECEIVED = new StringId(1246, "{0} exception received");
  public static final StringId Connection_0_STRAY_INTERRUPT_READING_MESSAGE = new StringId(1247, "{0} Stray interrupt reading message");
  public static final StringId Connection_0_SUCCESSFULLY_REESTABLISHED_CONNECTION_TO_PEER_1 = new StringId(1248, "{0}: Successfully reestablished connection to peer {1}");
  public static final StringId Connection_ACK_READ_EXCEPTION = new StringId(1249, "ack read exception");
  public static final StringId Connection_ACK_READ_EXCEPTION_0 = new StringId(1250, "ack read exception: {0}");

  public static final StringId Connection_ALLOCATING_LARGER_NETWORK_READ_BUFFER_NEW_SIZE_IS_0_OLD_SIZE_WAS_1 = new StringId(1254, "Allocating larger network read buffer, new size is {0} old size was {1}.");
  public static final StringId Connection_BLOCKED_FOR_0_MS_WHICH_IS_LONGER_THAN_THE_MAX_OF_1_MS_ASKING_SLOW_RECEIVER_2_TO_DISCONNECT = new StringId(1255, "Blocked for {0}ms which is longer than the max of {1}ms, asking slow receiver {2} to disconnect.");
  public static final StringId Connection_CLASSNOTFOUND_DESERIALIZING_MESSAGE_0 = new StringId(1256, "ClassNotFound deserializing message: {0}");
  public static final StringId Connection_CONNECTION_ATTEMPTING_RECONNECT_TO_PEER__0 = new StringId(1257, "Connection: Attempting reconnect to peer  {0}");
  public static final StringId Connection_CONNECTION_FAILED_TO_CONNECT_TO_PEER_0_BECAUSE_1 = new StringId(1258, "Connection: shared={0} ordered={1} failed to connect to peer {2} because: {3}");
  public static final StringId Connection_CONNECTION_HANDSHAKE_FAILED_TO_CONNECT_TO_PEER_0_BECAUSE_1 = new StringId(1259, "Connection: shared={0} ordered={1} handshake failed to connect to peer {2} because: {3}");
  public static final StringId Connection_DETECTED_OLD_VERSION_PRE_5_0_1_OF_GEMFIRE_OR_NONGEMFIRE_DURING_HANDSHAKE_DUE_TO_INITIAL_BYTE_BEING_0 = new StringId(1260, "Detected old version (pre 5.0.1) of GemFire or non-GemFire during handshake due to initial byte being {0}");
  public static final StringId Connection_END_OF_FILE_ON_ACK_STREAM = new StringId(1261, "end of file on ack stream");
  public static final StringId Connection_ERROR_DESERIALIZING_MESSAGE = new StringId(1262, "Error deserializing message");
  public static final StringId Connection_ERROR_DESERIALIZING_P2P_HANDSHAKE_MESSAGE = new StringId(1263, "Error deserializing P2P handshake message");
  public static final StringId Connection_ERROR_DESERIALIZING_P2P_HANDSHAKE_REPLY = new StringId(1264, "Error deserializing P2P handshake reply");
  public static final StringId Connection_ERROR_DISPATCHING_MESSAGE = new StringId(1265, "Error dispatching message");
  public static final StringId Connection_EXCEPTION_FLUSHING_BATCH_SEND_BUFFER_0 = new StringId(1266, "Exception flushing batch send buffer: {0}");
  public static final StringId Connection_FAILED_HANDLING_CHUNK_MESSAGE = new StringId(1267, "Failed handling chunk message");
  public static final StringId Connection_FAILED_HANDLING_END_CHUNK_MESSAGE = new StringId(1268, "Failed handling end chunk message");
  public static final StringId Connection_FAILED_SETTING_CHANNEL_TO_BLOCKING_MODE_0 = new StringId(1269, "Failed setting channel to blocking mode {0}");
  public static final StringId Connection_FINISHED_WAITING_FOR_REPLY_FROM_0 = new StringId(1270, "Finished waiting for reply from {0}");
  public static final StringId Connection_IOEXCEPTION_DESERIALIZING_MESSAGE = new StringId(1271, "IOException deserializing message");
  public static final StringId Connection_OWNER_SHOULD_NOT_BE_NULL = new StringId(1272, "\"owner\" should not be null");
  public static final StringId Connection_P2P_PUSHER_EXCEPTION_0 = new StringId(1273, "P2P pusher exception: {0}");
  public static final StringId Connection_QUEUED_BYTES_0_EXCEEDS_MAX_OF_1_ASKING_SLOW_RECEIVER_2_TO_DISCONNECT = new StringId(1274, "Queued bytes {0} exceeds max of {1}, asking slow receiver {2} to disconnect.");
  public static final StringId Connection_SOCKET_0_IS_1_INSTEAD_OF_THE_REQUESTED_2 = new StringId(1275, "Socket {0} is {1} instead of the requested {2}.");
  public static final StringId Connection_THROWABLE_DESERIALIZING_P2P_HANDSHAKE_REPLY = new StringId(1276, "Throwable deserializing P2P handshake reply");
  public static final StringId Connection_THROWABLE_DISPATCHING_MESSAGE = new StringId(1277, "Throwable dispatching message");
  public static final StringId Connection_TIMED_OUT_WAITING_FOR_READERTHREAD_ON_0_TO_FINISH = new StringId(1278, "Timed out waiting for readerThread on {0} to finish.");
  public static final StringId Connection_UNABLE_TO_GET_INPUT_STREAM = new StringId(1279, "Unable to get input stream");
  public static final StringId Connection_UNABLE_TO_GET_P2P_CONNECTION_STREAMS = new StringId(1280, "Unable to get P2P connection streams");
  public static final StringId Connection_UNEXPECTED_FAILURE_DESERIALIZING_MESSAGE = new StringId(1281, "Unexpected failure deserializing message");
  public static final StringId Connection_UNKNOWN_HANDSHAKE_REPLY_CODE_0 = new StringId(1282, "Unknown handshake reply code: {0}");
  public static final StringId Connection_UNKNOWN_HANDSHAKE_REPLY_CODE_0_NIOMESSAGELENGTH_1_PROCESSORTYPE_2 = new StringId(1283, "Unknown handshake reply code: {0} nioMessageLength={1} processorType={2}");
  public static final StringId Connection_UNKNOWN_P2P_MESSAGE_TYPE_0 = new StringId(1284, "Unknown P2P message type: {0}");
  public static final StringId Connection_UNKNOWN_PROCESSOR_TYPE_0 = new StringId(1285, "Unknown processor type: {0}");
  public static final StringId ContainsKeyValueMess_PARTITIONED_REGION_0_IS_NOT_CONFIGURED_TO_STORE_DATA = new StringId(1286, "Partitioned Region {0} is not configured to store data");
  // ok to reuse 1287
  public static final StringId ContainsKey_0_THE_INPUT_KEY_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL = new StringId(1288, "{0}: The input key for the containsKey request is null");
  public static final StringId ContainsKey_0_THE_INPUT_REGION_NAME_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL = new StringId(1289, "{0}: The input region name for the containsKey request is null");
  public static final StringId ContainsKey_THE_INPUT_KEY_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL = new StringId(1290, " The input key for the containsKey request is null");
  public static final StringId ContainsKey_THE_INPUT_REGION_NAME_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL = new StringId(1291, " The input region name for the containsKey request is null");
  public static final StringId ContainsKey_WAS_NOT_FOUND_DURING_CONTAINSKEY_REQUEST = new StringId(1292, " was not found during containsKey request");
  public static final StringId ContextImpl_CONTEXTIMPL_LOOKUP_ERROR_WHILE_CREATING_USERTRANSACTION_OBJECT = new StringId(1293, "ContextImpl::lookup::Error while creating UserTransaction object");
  public static final StringId ContextImpl_CONTEXTIMPL_LOOKUP_ERROR_WHILE_LOOKING_UP_0 = new StringId(1294, "ContextImpl::lookup::Error while looking up {0}");
  public static final StringId CqAttributesFactory_EXCEPTION_CLOSING_CQ_LISTENER_ERROR_0 = new StringId(1295, "Exception closing CQ Listener Error: {0}");
  public static final StringId CqAttributesFactory_EXCEPTION_OCCURED_WHILE_CLOSING_CQ_LISTENER_ERROR_0 = new StringId(1296, "Exception occurred while closing CQ Listener Error: {0}");
  public static final StringId CqAttributesFactory_RUNTIME_EXCEPTION_OCCURED_CLOSING_CQ_LISTENER_ERROR_0 = new StringId(1297, "Runtime Exception occurred closing CQ Listener Error: {0}");
  public static final StringId CqAttributesFactory_RUNTIME_EXCEPTION_OCCURED_WHILE_CLOSING_CQ_LISTENER_ERROR_0 = new StringId(1298, "Runtime Exception occurred while closing CQ Listener Error: {0}");
  public static final StringId CqQueryImpl_FAILED_TO_STORE_CONTINUOUS_QUERY_IN_THE_REPOSITORY_CQNAME_0_1 = new StringId(1299, "Failed to store Continuous Query in the repository. CqName: {0} {1}");

  public static final StringId PRHARRedundancyProvider_0_IN_THE_PARTITIONED_REGION_REGION_NAME_1 = new StringId(1301, "{0} Region name = {1}");
  public static final StringId CqQueryImpl_CLASS_NOT_FOUND_EXCEPTION_THE_ANTLRJAR_OR_THE_SPCIFIED_CLASS_MAY_BE_MISSING_FROM_SERVER_SIDE_CLASSPATH_ERROR_0 = new StringId(1302, "Class not found exception. The antlr.jar or the spcified class may be missing from server side classpath. Error : {0}");
  public static final StringId CqQueryImpl_CQ_IS_NOT_IN_RUNNING_STATE_STOP_CQ_DOES_NOT_APPLY_CQNAME_0 = new StringId(1303, "CQ is not in running state, stop CQ does not apply, CqName : {0}");
  public static final StringId CqQueryImpl_CQ_QUERIES_CANNOT_HAVE_MORE_THAN_ONE_ITERATOR_IN_THE_FROM_CLAUSE = new StringId(1304, "CQ queries cannot have more than one iterator in the FROM clause");
  public static final StringId CqQueryImpl_CQ_QUERIES_DO_NOT_SUPPORT_ORDER_BY = new StringId(1305, "CQ queries do not support ORDER BY");
  public static final StringId CqQueryImpl_CQ_QUERIES_DO_NOT_SUPPORT_PROJECTIONS = new StringId(1306, "CQ queries do not support projections");
  public static final StringId CqQueryImpl_CQ_QUERIES_MUST_BE_A_SELECT_STATEMENT_ONLY = new StringId(1307, "CQ queries must be a select statement only");
  public static final StringId CqQueryImpl_CQ_QUERIES_MUST_HAVE_A_REGION_PATH_ONLY_AS_THE_FIRST_ITERATOR_IN_THE_FROM_CLAUSE = new StringId(1308, "CQ queries must have a region path only as the first iterator in the FROM clause");
  public static final StringId CqQueryImpl_CQ_QUERIES_MUST_REFERENCE_ONE_AND_ONLY_ONE_REGION = new StringId(1309, "CQ queries must reference one and only one region");
  public static final StringId CqQueryImpl_ERROR_WHILE_PARSING_THE_QUERY_ERROR_0 = new StringId(1310, "Error while parsing the query. Error : {0}");
  public static final StringId PRHARRedundancyProvider_UNABLE_TO_FIND_ANY_MEMBERS_TO_HOST_A_BUCKET_IN_THE_PARTITIONED_REGION_0 = new StringId(1311, "Unable to find any members to host a bucket in the partitioned region. {0}.{1}");
  public static final StringId CqQueryImpl_EXCEPTION_OCCOURED_IN_THE_CQLISTENER_OF_THE_CQ_CQNAME_0_ERROR_1 = new StringId(1312, "Exception occurred in the CqListener of the CQ, CqName : {0} Error : {1}");

  public static final StringId CqQueryImpl_FAILED_TO_CLOSE_THE_CQ_CQNAME_0_ERROR_FROM_LAST_ENDPOINT_1 = new StringId(1314, "Failed to close the cq. CqName: {0}. Error from last endpoint: {1}");

  public static final StringId CqQueryImpl_FAILED_TO_STOP_THE_CQ_CQNAME_0_ERROR_FROM_LAST_SERVER_1 = new StringId(1316, "Failed to stop the cq. CqName :{0} Error from last server: {1}");

  public static final StringId CqQueryImpl_REGION_ON_WHICH_QUERY_IS_SPECIFIED_NOT_FOUND_LOCALLY_REGIONNAME_0 = new StringId(1318, "Region on which query is specified not found locally, regionName: {0}");
  public static final StringId CqQueryImpl_REGION__0_SPECIFIED_WITH_CQ_NOT_FOUND_CQNAME_1 = new StringId(1319, "Region : {0} specified with cq not found. CqName: {1}");
  public static final StringId CqQueryImpl_RUNTIMEEXCEPTION_OCCOURED_IN_THE_CQLISTENER_OF_THE_CQ_CQNAME_0_ERROR_1 = new StringId(1320, "RuntimeException occurred in the CqListener of the CQ, CqName : {0} Error : {1}");
  public static final StringId CqQueryImpl_SELECT_DISTINCT_QUERIES_NOT_SUPPORTED_IN_CQ = new StringId(1321, "select DISTINCT queries not supported in CQ");

  public static final StringId CqQueryImpl_THE_WHERE_CLAUSE_IN_CQ_QUERIES_CANNOT_REFER_TO_A_REGION = new StringId(1323, "The WHERE clause in CQ queries cannot refer to a region");
  public static final StringId CqQueryImpl_UNABLE_TO_CREATE_CQ_0_ERROR__1 = new StringId(1324, "Unable to create cq {0} Error : {1}");

  public static final StringId CqQueryImpl_CQ_IS_IN_RUNNING_STATE_CQNAME_0 = new StringId(1327, "CQ is in running state, CqName : {0}");

  public static final StringId CqService_CLIENT_SIDE_NEWCQ_METHOD_INVOCATION_ON_SERVER = new StringId(1329, "client side newCq() method invocation on server.");
  public static final StringId CqService_CQ_NOT_FOUND_FAILED_TO_CLOSE_THE_SPECIFIED_CQ_0 = new StringId(1330, "CQ Not found, Failed to close the specified CQ {0}");

  public static final StringId CqService_CQ_NOT_FOUND_IN_THE_CQ_META_REGION_CQNAME_0 = new StringId(1332, "CQ not found in the cq meta region, CqName: {0}");
  public static final StringId CqService_CQ_WITH_THE_GIVEN_NAME_ALREADY_EXISTS_CQNAME_0 = new StringId(1333, "CQ with the given name already exists. CqName : {0}");

  public static final StringId RemoteGfManagerAgent_LISTENER_THREW_AN_EXCEPTION = new StringId(1335, "Listener threw an exception.");
  public static final StringId PRHARRedundancyProvider_FOUND_A_MEMBER_TO_HOST_A_BUCKET = new StringId(1336, "Found a member to host a bucket.");
  public static final StringId CqService_EXCEPTION_IN_THE_CQLISTENER_OF_THE_CQ_CQNAME_0_ERROR__1 = new StringId(1337, "Exception in the CqListener of the CQ, CqName: {0} Error : {1}");
  public static final StringId CqService_EXCEPTION_WHILE_REGISTERING_CQ_ON_SERVER_CQNAME___0 = new StringId(1338, "Exception while registering CQ on server. CqName :  {0}");
  public static final StringId CqService_FAILED_TO_CLOSE_CQ__0___1 = new StringId(1339, "Failed to close CQ {0} {1}");

  public static final StringId CqService_INVALID_CQ_MONITOR_REQUEST_RECEIVED = new StringId(1345, "Invalid CQ Monitor request received.");

  public static final StringId CqService_NULL_ARGUMENT_0 = new StringId(1348, "Null argument \"{0}\"");
  public static final StringId CqService_RUNTIME_EXCEPTION_IN_THE_CQLISTENER_OF_THE_CQ_CQNAME_0_ERROR__1 = new StringId(1349, "Runtime Exception in the CqListener of the CQ, CqName: {0} Error : {1}");
  public static final StringId CqService_SERVER_SIDE_EXECUTECQ_METHOD_IS_CALLED_ON_CLIENT_CQNAME_0 = new StringId(1350, "Server side executeCq method is called on client. CqName : {0}");

  public static final StringId CqQueryImpl_FAILED_TO_CLOSE_THE_CQ_CQNAME_0_THE_SERVER_ENDPOINTS_ON_WHICH_THIS_CQ_WAS_REGISTERED_WERE_NOT_FOUND = new StringId(1354, "Failed to close the cq. CqName: {0}. The server endpoints on which this cq was registered were not found.");

  public static final StringId CreateRegionProcessor_MORE_THAN_ONE_EXCEPTION_THROWN_IN__0 = new StringId(1356, "More than one exception thrown in  {0}");
  public static final StringId CreateRegion_0_THE_INPUT_PARENT_REGION_NAME_FOR_THE_CREATE_REGION_REQUEST_IS_NULL = new StringId(1357, "{0}: The input parent region name for the create region request is null");
  public static final StringId CreateRegion_0_THE_INPUT_REGION_NAME_FOR_THE_CREATE_REGION_REQUEST_IS_NULL = new StringId(1358, "{0}: The input region name for the create region request is null");
  public static final StringId CreateRegion_THE_INPUT_PARENT_REGION_NAME_FOR_THE_CREATE_REGION_REQUEST_IS_NULL = new StringId(1359, " The input parent region name for the create region request is null");
  public static final StringId CreateRegion_THE_INPUT_REGION_NAME_FOR_THE_CREATE_REGION_REQUEST_IS_NULL = new StringId(1360, " The input region name for the create region request is null");
  public static final StringId DLockGrantor_DEBUGHANDLESUSPENDTIMEOUTS_SLEEPING_FOR__0 = new StringId(1361, "debugHandleSuspendTimeouts sleeping for  {0}");

  public static final StringId DLockGrantor_DLOCKGRANTORTHREAD_WAS_UNEXPECTEDLY_INTERRUPTED = new StringId(1363, "DLockGrantorThread was unexpectedly interrupted");

  public static final StringId DLockGrantor_PROCESSING_OF_POSTREMOTERELEASELOCK_THREW_UNEXPECTED_RUNTIMEEXCEPTION = new StringId(1367, "Processing of postRemoteReleaseLock threw unexpected RuntimeException");

  public static final StringId DLockGrantor_RELEASED_REGULAR_LOCK_WITH_WAITING_READ_LOCK_0 = new StringId(1369, "Released regular lock with waiting read lock: {0}");

  public static final StringId DLockRequestProcessor_DLOCKREQUESTMESSAGEPROCESS_CAUGHT_THROWABLE = new StringId(1373, "[DLockRequestMessage.process] Caught throwable:");
  public static final StringId DLockRequestProcessor_FAILED_TO_FIND_PROCESSOR_FOR__0 = new StringId(1374, "Failed to find processor for {0}");
  public static final StringId DLockRequestProcessor_HANDLED_LOCAL_ORPHANED_GRANT = new StringId(1375, "Handled local orphaned grant.");
  public static final StringId DLockRequestProcessor_HANDLED_ORPHANED_GRANT_WITHOUT_RELEASE = new StringId(1376, "Handled orphaned grant without release.");
  public static final StringId DLockRequestProcessor_HANDLED_ORPHANED_GRANT_WITH_RELEASE = new StringId(1377, "Handled orphaned grant with release.");
  public static final StringId DLockRequestProcessor_MORE_THAN_ONE_EXCEPTION_THROWN_IN__0 = new StringId(1378, "More than one exception thrown in {0}");
  public static final StringId DLockRequestProcessor_NO_PROCESSOR_FOUND_FOR_DLOCKRESPONSEMESSAGE__0 = new StringId(1379, "No processor found for DLockResponseMessage: {0}");
  public static final StringId DLockRequestProcessor_RELEASING_LOCAL_ORPHANED_GRANT_FOR_0 = new StringId(1380, "Releasing local orphaned grant for {0}.");
  public static final StringId DLockRequestProcessor_RELEASING_ORPHANED_GRANT_FOR__0 = new StringId(1381, "Releasing orphaned grant for  {0}");
  public static final StringId DLockRequestProcessor_WAITING_TO_PROCESS_DLOCKRESPONSEMESSAGE = new StringId(1382, "Waiting to process DLockResponseMessage");
  public static final StringId DLockService_DEBUG_GRANTOR_REPORTS_NOT_HOLDER_FOR_0 = new StringId(1383, "DEBUG: Grantor reports NOT_HOLDER for {0}");
  public static final StringId DLockService_DEBUG_LOCKINTERRUPTIBLY_HAS_GONE_HOT_AND_LOOPED_0_TIMES = new StringId(1384, "DEBUG: lockInterruptibly has gone hot and looped [0] times");

  public static final StringId DLockService_FAILED_TO_NOTIFY_GRANTOR_OF_DESTRUCTION_WITHIN_0_ATTEMPTS = new StringId(1386, "Failed to notify grantor of destruction within {0} attempts.");
  public static final StringId DLockService_GRANTOR_CREATION_WAS_ABORTED_BUT_GRANTOR_WAS_NOT_DESTROYED = new StringId(1387, "Grantor creation was aborted but grantor was not destroyed");
  public static final StringId DLockService_GRANTOR_IS_STILL_INITIALIZING = new StringId(1388, "Grantor is still initializing");
  public static final StringId DLockService_GRANTOR_REPORTS_REENTRANT_LOCK_NOT_HELD_0 = new StringId(1389, "Grantor reports reentrant lock not held: {0}");
  public static final StringId DLockService_LOCK_WAS_INTERRUPTED = new StringId(1390, "lock() was interrupted");

  public static final StringId DLockToken_ATTEMPTING_TO_USE_DESTROYED_TOKEN_0 = new StringId(1392, "Attempting to use destroyed token: {0}");
  public static final StringId DataSerializer_CLASS_0_DOES_NOT_HAVE_A_ZEROARGUMENT_CONSTRUCTOR = new StringId(1393, "Class {0} does not have a zero-argument constructor.");
  public static final StringId DataSerializer_CLASS_0_DOES_NOT_HAVE_A_ZEROARGUMENT_CONSTRUCTOR_IT_IS_AN_INNER_CLASS_OF_1_SHOULD_IT_BE_A_STATIC_INNER_CLASS = new StringId(1394, "Class {0} does not have a zero-argument constructor. It is an inner class of {1}. Should it be a static inner class?");
  public static final StringId DataSourceFactory_DATASOURCEFACTORYGETMANAGEDDATASOURCEMANAGED_CONNECTION_FACTORY_CLASS_IS_NOT_AVAILABLE = new StringId(1395, "DataSourceFactory::getManagedDataSource:Managed Connection factory class is not available");
  public static final StringId DataSourceFactory_DATASOURCEFACTORYGETMANAGEDDATASOURCE_EXCEPTION_IN_CREATING_MANAGED_CONNECTION_FACTORY_EXCEPTION_STRING_0 = new StringId(1396, "DataSourceFactory::getManagedDataSource: Exception in creating managed connection factory. Exception string = {0}");
  public static final StringId DataSourceFactory_DATASOURCEFACTORYGETPOOLEDDATASOURCECONNECTIONPOOLDATASOURCE_CLASS_NAME_FOR_THE_RESOURCEMANAGER_IS_NOT_AVAILABLE = new StringId(1397, "DataSourceFactory::getPooledDataSource:ConnectionPoolDataSource class name for the ResourceManager is not available");
  public static final StringId DataSourceFactory_DATASOURCEFACTORYGETSIMPLEDATASOURCEJDBC_DRIVER_IS_NOT_AVAILABLE = new StringId(1398, "DataSourceFactory::getSimpleDataSource:JDBC Driver is not available");
  public static final StringId DataSourceFactory_DATASOURCEFACTORYGETSIMPLEDATASOURCEURL_STRING_TO_DATABASE_IS_NULL = new StringId(1399, "DataSourceFactory::getSimpleDataSource:URL String to Database is null");
  public static final StringId DataSourceFactory_DATASOURCEFACTORYGETTRANXDATASOURCEXADATASOURCE_CLASS_NAME_FOR_THE_RESOURCEMANAGER_IS_NOT_AVAILABLE = new StringId(1400, "DataSourceFactory::getTranxDataSource:XADataSource class name for the ResourceManager is not available");
  public static final StringId DataSourceFactory_DATASOURCEFACTORY_GETPOOLEDDATASOURCE_EXCEPTION_CREATING_CONNECTIONPOOLDATASOURCE_EXCEPTION_STRING_0 = new StringId(1401, "DataSourceFactory::getPooledDataSource:Exception creating ConnectionPoolDataSource.Exception string={0}");
  public static final StringId DataSourceFactory_DATASOURCEFACTORY_GETSIMPLEDATASOURCE_EXCEPTION_WHILE_CREATING_GEMFIREBASICDATASOURCE_EXCEPTION_STRING_0 = new StringId(1402, "DataSourceFactory::getSimpleDataSource:Exception while creating GemfireBasicDataSource.Exception String={0}");
  public static final StringId DataSourceFactory_DATASOURCEFACTORY_GETTRANXDATASOURCE_EXCEPTION_IN_CREATING_GEMFIRETRANSACTIONDATASOURCE__EXCEPTION_STRING_0 = new StringId(1403, "DataSourceFactory::getTranxDataSource:Exception in creating GemFireTransactionDataSource. Exception string={0}");

  public static final StringId DebuggerSupport_DEBUGGER_CONTINUING = new StringId(1405, "DEBUGGER: Continuing");
  public static final StringId DebuggerSupport_WAITING_FOR_DEBUGGER_TO_ATTACH_0 = new StringId(1406, "DEBUGGER: Waiting for Java debugger to attach... {0}");

  public static final StringId DefaultQueryService_EXCEPTION_REMOVING_INDEX___0 = new StringId(1408, "Exception removing index :  {0}");
  public static final StringId DefaultQueryService_EXCEPTION_WHILE_CREATING_INDEX_ON_PR_DEFAULT_QUERY_PROCESSOR = new StringId(1409, "Exception while creating index on pr default query processor.");

  public static final StringId Default_0_UNKNOWN_MESSAGE_TYPE_1_WITH_TX_2_FROM_3 = new StringId(1412, "{0}: Unknown message type ({1}) with tx: {2} from {3}");
  public static final StringId DestroEntryMessage_FAILED_ATTEMPT_TO_DESTROY_OR_INVALIDATE_ENTRY_0_1_FROM_CONSOLE_AT_2 = new StringId(1413, "Failed attempt to destroy or invalidate entry {0} {1} from console at {2}");
  public static final StringId DestroRegionMessage_FAILED_ATTEMPT_TO_DESTROY_OR_INVALIDATE_REGION_0_FROM_CONSOLE_AT_1 = new StringId(1414, "Failed attempt to destroy or invalidate region {0} from console at {1}");
  public static final StringId DestroyRegionOperation_CACHEWRITER_SHOULD_NOT_HAVE_BEEN_CALLED = new StringId(1415, "CacheWriter should not have been called");
  public static final StringId DestroyRegionOperation_DISTRIBUTEDLOCK_SHOULD_NOT_HAVE_BEEN_ACQUIRED = new StringId(1416, "DistributedLock should not have been acquired");
  public static final StringId DestroyRegionOperation_EXCEPTION_WHILE_PROCESSING__0_ = new StringId(1417, "Exception while processing [ {0} ]");
  public static final StringId DestroyRegionOperation_GOT_TIMEOUT_WHEN_TRYING_TO_RECREATE_REGION_DURING_REINITIALIZATION_1 = new StringId(1418, "Got timeout when trying to recreate region during re-initialization: {1}");
  public static final StringId DestroyRegionOperation_REGION_DESTRUCTION_MESSAGE_IMPLEMENTATION_IS_IN_BASICPROCESS__NOT_THIS_METHOD = new StringId(1419, "Region Destruction message implementation is in basicProcess, not this method");
  public static final StringId DestroyRegion_0_THE_INPUT_REGION_NAME_FOR_THE_DESTROY_REGION_REQUEST_IS_NULL = new StringId(1420, "{0}: The input region name for the destroy region request is null");
  public static final StringId Destroy_0_DURING_ENTRY_DESTROY_NO_ENTRY_WAS_FOUND_FOR_KEY_1 = new StringId(1421, "{0}: during entry destroy no entry was found for key {1}");
  public static final StringId Destroy_0_THE_INPUT_KEY_FOR_THE_DESTROY_REQUEST_IS_NULL = new StringId(1422, "{0}: The input key for the destroy request is null");
  public static final StringId Destroy_0_THE_INPUT_REGION_NAME_FOR_THE_DESTROY_REQUEST_IS_NULL = new StringId(1423, "{0}: The input region name for the destroy request is null");
  public static final StringId Destroy_0_UNEXPECTED_EXCEPTION = new StringId(1424, "{0}: Unexpected Exception");
  public static final StringId AcceptorImpl_IGNORING_MAX_THREADS_DUE_TO_JROCKIT_NIO_BUG = new StringId(1425, "Ignoring max-threads setting and using zero instead due to JRockit NIO bugs.  See GemFire bug #40198");
  public static final StringId AbstractDistributionConfig_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1 = new StringId(1426, "The bind-address \"{0}\" is not a valid address for this machine.  These are the valid addresses for this machine: {1}");
  public static final StringId AcceptorImpl_IGNORING_MAX_THREADS_DUE_TO_WINDOWS_IPV6_BUG = new StringId(1427, "Ignoring max-threads setting and using zero instead due to Java bug 6230761: NIO does not work with IPv6 on Windows.  See GemFire bug #40472");

  public static final StringId DirectChannel_0_SECONDS_HAVE_ELAPSED_WHILE_WAITING_FOR_REPLY_FROM_1_ON_2_WHOSE_CURRENT_MEMBERSHIP_LIST_IS_3 = new StringId(1430, "{0} seconds have elapsed while waiting for reply from {1} on {2} whose current membership list is: [{3}]");

  public static final StringId DirectChannel_GEMFIRE_P2P_LISTENER_STARTED_ON__0 = new StringId(1432, "GemFire P2P Listener started on  {0}");

  public static final StringId DirectChannel_UNABLE_TO_INITIALIZE_DIRECT_CHANNEL_BECAUSE__0 = new StringId(1434, "Unable to initialize direct channel because:  {0}");
  public static final StringId DirectChannel_UNEXPECTED_TIMEOUT_WHILE_WAITING_FOR_ACK_FROM__0 = new StringId(1435, "Unexpected timeout while waiting for ack from  {0}");
  public static final StringId DirectChannel_VIEW_NO_LONGER_HAS_0_AS_AN_ACTIVE_MEMBER_SO_WE_WILL_NO_LONGER_WAIT_FOR_IT = new StringId(1436, "View no longer has {0} as an active member, so we will no longer wait for it.");
  public static final StringId DirectChannel_WHILE_PULLING_A_MESSAGE = new StringId(1437, "While pulling a message");
  public static final StringId TXState_CANNOT_COMMIT_REMOTED_TRANSACTION = new StringId(1438, "Cannot commit a transaction being run on behalf of a remote thread");

  public static final StringId DistributedCacheOperation_EXCEPTION_OCCURRED_WHILE_PROCESSING__0 = new StringId(1441, "Exception occurred while processing  {0}");

  public static final StringId DistributedCacheOperation_WAITFORACKIFNEEDED_EXCEPTION = new StringId(1443, "waitForAckIfNeeded: exception");
  public static final StringId DistributedRegion_ATTEMPT_TO_ACQUIRE_DISTRIBUTED_LOCK_FOR_0_FAILED_AFTER_WAITING_1_SECONDS = new StringId(1444, "Attempt to acquire distributed lock for {0} failed after waiting {1} seconds.");
  public static final StringId DistributedRegion_DLS_DESTROY_MAY_HAVE_FAILED_FOR_0 = new StringId(1445, "DLS destroy may have failed for {0}");
  public static final StringId DistributedRegion_EXCEPTION_OCCURRED_IN_REGIONMEMBERSHIPLISTENER = new StringId(1446, "Exception occurred in RegionMembershipListener");
  public static final StringId DistributedRegion_NO_REPLICATED_REGION_FOUND_FOR_EXECUTING_FUNCTION_0 = new StringId(1447, "No Replicated Region found for executing function : {0}.");
  public static final StringId DistributedRegion_TIMED_OUT_AFTER_WAITING_0_SECONDS_FOR_THE_DISTRIBUTED_LOCK_FOR_1 = new StringId(1448, "Timed out after waiting {0} seconds for the distributed lock for {1}.");
  public static final StringId DistributedRegion_UNEXPECTED_EXCEPTION = new StringId(1449, "Unexpected exception:");

  public static final StringId DistributionAdvisor_UNEXPECTED_EXCEPTION = new StringId(1456, "Unexpected exception:");
  public static final StringId DistributionChannel_ATTEMPTING_A_SEND_TO_A_DISCONNECTED_DISTRIBUTIONMANAGER = new StringId(1457, "Attempting a send to a disconnected DistributionManager");
  public static final StringId DistributionLocatorImpl_DONE_WAITING_FOR_LOCATOR = new StringId(1458, "Done waiting for locator");
  public static final StringId DistributionLocator_COULD_NOT_BIND_LOCATOR_TO__0__1 = new StringId(1459, "Could not bind locator to {0}[{1}]");
  public static final StringId DistributionLocator_COULD_NOT_START_LOCATOR = new StringId(1460, "Could not start locator");
  public static final StringId DistributionLocator_LOCATOR_STOPPED = new StringId(1461, "Locator stopped");
  public static final StringId DistributionManager_0_IS_THE_ELDER_AND_THE_ONLY_MEMBER = new StringId(1462, "{0} is the elder and the only member.");

  public static final StringId DistributionManager_ADMINISTRATION_MEMBER_AT_0_CLOSED_1 = new StringId(1464, "Administration member at {0} closed: {1}");
  public static final StringId DistributionManager_ADMINISTRATION_MEMBER_AT_0_CRASHED_1 = new StringId(1465, "Administration member at {0} crashed: {1}");
  public static final StringId DistributionManager_ADMITTING_MEMBER_0_NOW_THERE_ARE_1_NONADMIN_MEMBERS = new StringId(1466, "Admitting member <{0}>. Now there are {1} non-admin member(s).");
  public static final StringId DistributionManager_AT_LEAST_ONE_EXCEPTION_OCCURRED = new StringId(1467, "At least one Exception occurred.");
  public static final StringId DistributionManager_CHANGING_ELDER_FROM_0_TO_1 = new StringId(1468, "Changing Elder from {0} to {1}.");
  public static final StringId DistributionManager_CLOBBERTHREAD_THREAD_REFUSED_TO_DIE__0 = new StringId(1469, "clobberThread: Thread refused to die:  {0}");
  public static final StringId DistributionManager_DAEMON_THREADS_ARE_SLOW_TO_STOP_CULPRITS_INCLUDE_0 = new StringId(1470, "Daemon threads are slow to stop; culprits include: {0}");
  public static final StringId DistributionManager_DIDNT_HEAR_BACK_FROM_ANY_OTHER_SYSTEM_I_AM_THE_FIRST_ONE = new StringId(1471, "Did not hear back from any other system. I am the first one.");
  public static final StringId DistributionManager_DISTRIBUTIONMANAGER_0_STARTED_ON_1_THERE_WERE_2_OTHER_DMS_3_4_5 = new StringId(1472, "DistributionManager {0} started on {1}. There were {2} other DMs. others: {3} {4} {5}");
  public static final StringId DistributionManager_DISTRIBUTIONMANAGER_STOPPED_IN_0_MS = new StringId(1473, "DistributionManager stopped in {0}ms.");
  public static final StringId DistributionManager_DMMEMBERSHIP_ADMITTING_NEW_ADMINISTRATION_MEMBER__0_ = new StringId(1474, "DMMembership: Admitting new administration member < {0} >.");

  public static final StringId DistributionManager_ELDER__0__IS_NOT_CURRENTLY_AN_ACTIVE_MEMBER_SELECTING_NEW_ELDER = new StringId(1476, "Elder < {0} > is not currently an active member; selecting new elder.");
  public static final StringId DistributionManager_EXCEPTION_WHILE_CALLING_MEMBERSHIP_LISTENER_FOR_EVENT__0 = new StringId(1477, "Exception while calling membership listener for event:  {0}");
  public static final StringId DistributionManager_FAILED_SENDING_SHUTDOWN_MESSAGE_TO_PEERS_TIMEOUT = new StringId(1478, "Failed sending shutdown message to peers (timeout)");
  public static final StringId DistributionManager_FORCING_AN_ELDER_JOIN_EVENT_SINCE_A_STARTUP_RESPONSE_WAS_NOT_RECEIVED_FROM_ELDER__0_ = new StringId(1479, "Forcing an elder join event since a startup response was not received from elder  {0} .");
  public static final StringId DistributionManager_FORCING_THREAD_STOP_ON__0_ = new StringId(1480, "Forcing thread stop on < {0} >");

  public static final StringId DistributionManager_INITIAL_MEMBERSHIPMANAGER_VIEW___0 = new StringId(1482, "Initial (distribution manager) view =  {0}");
  public static final StringId DistributionManager_MARKING_DISTRIBUTIONMANAGER_0_AS_CLOSED = new StringId(1483, "Marking DistributionManager {0} as closed.");
  public static final StringId DistributionManager_MARKING_THE_SERIALQUEUEDEXECUTOR_WITH_ID__0__USED_BY_THE_MEMBER__1__TO_BE_UNUSED = new StringId(1484, "Marking the SerialQueuedExecutor with id : {0}  used by the member  {1}  to be unused.");
  public static final StringId DistributionManager_MEMBER_AT_0_GRACEFULLY_LEFT_THE_DISTRIBUTED_CACHE_1 = new StringId(1485, "Member at {0} gracefully left the distributed cache: {1}");
  public static final StringId DistributionManager_MEMBER_AT_0_UNEXPECTEDLY_LEFT_THE_DISTRIBUTED_CACHE_1 = new StringId(1486, "Member at {0} unexpectedly left the distributed cache: {1}");
  public static final StringId DistributionManager_NEWLY_SELECTED_ELDER_IS_NOW__0_ = new StringId(1487, "Newly selected elder is now < {0} >");
  public static final StringId DistributionManager_NEW_ADMINISTRATION_MEMBER_DETECTED_AT_0 = new StringId(1488, "New administration member detected at {0}.");
  public static final StringId DistributionManager_NOW_CLOSING_DISTRIBUTION_FOR__0 = new StringId(1489, "Now closing distribution for {0}");
  public static final StringId DistributionManager_SHUTTING_DOWN_DISTRIBUTIONMANAGER_0_1 = new StringId(1490, "Shutting down DistributionManager {0}. {1}");
  public static final StringId DistributionManager_STARTING_DISTRIBUTIONMANAGER_0_1 = new StringId(1491, "Starting DistributionManager {0}. {1}");
  public static final StringId DistributionManager_STILL_AWAITING_0_RESPONSES_FROM_1 = new StringId(1492, "Still awaiting {0} response(s) from: {1}.");
  public static final StringId DistributionManager_STOPPED_WAITING_FOR_STARTUP_REPLY_FROM_0_BECAUSE_THE_PEER_DEPARTED_THE_VIEW = new StringId(1493, "Stopped waiting for startup reply from <{0}> because the peer departed the view.");
  public static final StringId DistributionManager_STOPPED_WAITING_FOR_STARTUP_REPLY_FROM_0_BECAUSE_THE_REPLY_WAS_FINALLY_RECEIVED = new StringId(1494, "Stopped waiting for startup reply from <{0}> because the reply was finally received.");
  public static final StringId DistributionManager_TASK_FAILED_WITH_EXCEPTION = new StringId(1495, "Task failed with exception");
  public static final StringId DistributionManager_UNCAUGHT_EXCEPTION_CALLING_READYFORMESSAGES = new StringId(1496, "Uncaught exception calling readyForMessages");
  public static final StringId DistributionManager_UNCAUGHT_EXCEPTION_PROCESSING_MEMBER_EVENT = new StringId(1497, "Uncaught exception processing member event");
  public static final StringId DistributionManager_UNEXPECTED_INTERRUPTEDEXCEPTION = new StringId(1498, "Unexpected InterruptedException");

  public static final StringId DistributionManager_WHILE_PUSHING_MESSAGE_0_TO_1 = new StringId(1500, "While pushing message <{0}> to {1}");
  public static final StringId DistributionManager_WHILE_SENDING_SHUTDOWN_MESSAGE = new StringId(1501, "While sending shutdown message");
  public static final StringId DistributionMessage_0__SCHEDULE_REJECTED = new StringId(1502, "{0}  schedule() rejected");
  public static final StringId DistributionMessage_UNCAUGHT_EXCEPTION_PROCESSING__0 = new StringId(1503, "Uncaught exception processing  {0}");


  public static final StringId DynamicRegionFactory_DYNAMICREGIONLISTENER__0__THREW_EXCEPTION_ON_AFTERREGIONCREATED = new StringId(1508, "DynamicRegionListener {0} threw exception on afterRegionCreated");
  public static final StringId DynamicRegionFactory_DYNAMICREGIONLISTENER__0__THREW_EXCEPTION_ON_AFTERREGIONDESTROYED = new StringId(1509, "DynamicRegionListener {0} threw exception on afterRegionDestroyed");
  public static final StringId DynamicRegionFactory_DYNAMICREGIONLISTENER__0__THREW_EXCEPTION_ON_BEFOREREGIONCREATED = new StringId(1510, "DynamicRegionListener {0} threw exception on beforeRegionCreated");
  public static final StringId DynamicRegionFactory_DYNAMICREGIONLISTENER__0__THREW_EXCEPTION_ON_BEFOREREGIONDESTROYED = new StringId(1511, "DynamicRegionListener {0} threw exception on beforeRegionDestroyed");
  public static final StringId DynamicRegionFactory_ERROR_ATTEMPTING_TO_LOCALLY_CREATE_DYNAMIC_REGION__0 = new StringId(1512, "Error attempting to locally create Dynamic Region: {0}");
  public static final StringId DynamicRegionFactory_ERROR_ATTEMPTING_TO_LOCALLY_DESTROY_DYNAMIC_REGION__0 = new StringId(1513, "Error attempting to locally destroy Dynamic Region: {0}");
  public static final StringId DynamicRegionFactory_ERROR_DESTROYING_DYNAMIC_REGION__0 = new StringId(1514, "Error destroying Dynamic Region ''{0}''");
  public static final StringId DynamicRegionFactory_ERROR_INITIALIZING_DYNAMICREGIONFACTORY = new StringId(1515, "Error initializing DynamicRegionFactory");
  public static final StringId DynamicRegionFactory_ERROR__COULD_NOT_FIND_A_REGION_NAMED___0_ = new StringId(1516, "Error -- Could not find a region named: ''{0}''");
  public static final StringId ElderState_ELDERSTATE_PROBLEM_DM_0_BUT_SYSTEM_DISTRIBUTIONMANAGER_1 = new StringId(1517, "ElderState problem: dm={0}, but system DistributionManager={1}");
  public static final StringId ElderState_ELDERSTATE_PROBLEM_SYSTEM_0 = new StringId(1518, "ElderState problem: system={0}");
  public static final StringId ElderState_ELDERSTATE_PROBLEM_SYSTEM_DISTRIBUTIONMANAGER_0 = new StringId(1519, "ElderState problem: system DistributionManager={0}");

  public static final StringId EntryEventImpl_DATASTORE_FAILED_TO_CALCULATE_SIZE_OF_NEW_VALUE = new StringId(1540, "DataStore failed to calculate size of new value");
  public static final StringId EntryEventImpl_DATASTORE_FAILED_TO_CALCULATE_SIZE_OF_OLD_VALUE = new StringId(1541, "DataStore failed to calculate size of old value");
  public static final StringId ExpirationScheduler_SCHEDULING__0__TO_FIRE_IN__1__MS = new StringId(1542, "Scheduling  {0}  to fire in  {1}  ms");
  public static final StringId ExpiryTask_EXCEPTION_IN_EXPIRATION_TASK = new StringId(1543, "Exception in expiration task");

  public static final StringId FetchEntriesMessage_FETCHKEYSMESSAGE_DATA_STORE_NOT_CONFIGURED_FOR_THIS_MEMBER = new StringId(1545, "FetchKeysMessage: data store not configured for this member");

  public static final StringId FetchKeysMessage_FETCHKEYSMESSAGE_DATA_STORE_NOT_CONFIGURED_FOR_THIS_MEMBER = new StringId(1547, "FetchKeysMessage: data store not configured for this member");
  public static final StringId ForceDisconnectOperation_DISCONNECT_FORCED_BY__0__BECAUSE_WE_WERE_TOO_SLOW = new StringId(1548, "Disconnect forced by  {0}  because we were too slow.");
  public static final StringId GLOBALTRANSACTION__ENLISTRESOURCE__ERROR_WHILE_ENLISTING_XARESOURCE_0_1 = new StringId(1549, "GlobalTransaction::enlistResource::error while enlisting XAResource {0} {1}");

  public static final StringId GatewayEventRemoteDispatcher_0_USING_1 = new StringId(1552, "{0}: Using {1}");
  public static final StringId GatewayEventRemoteDispatcher_0_USING_1_AFTER_2_FAILED_CONNECT_ATTEMPTS = new StringId(1553, "{0}: Using {1} after {2} failed connect attempts");

  public static final StringId GatewayEventRemoteDispatcher_A_BATCHEXCEPTION_OCCURRED_PROCESSING_EVENT__0 = new StringId(1556, "A BatchException occurred processing events. Index of Array of Exception : {0}");
  public static final StringId GatewayEventRemoteDispatcher_NO_AVAILABLE_CONNECTION_WAS_FOUND_BUT_THE_FOLLOWING_ACTIVE_SERVERS_EXIST_0 = new StringId(1557, "No available connection was found, but the following active servers exist: {0}");
  public static final StringId GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH = new StringId(1558, "Stopping the processor because the following exception occurred while processing a batch:");

  public static final StringId GatewayEventRemoteDispatcher_THERE_ARE_NO_ACTIVE_SERVERS = new StringId(1560, "There are no active servers.");
  public static final StringId GatewayEventRemoteDispatcher_THE_EVENT_BEING_PROCESSED_WHEN_THE_BATCHEXCEPTION_OCCURRED_WAS__0 = new StringId(1561, "The event being processed when the BatchException occurred was:  {0}");

  public static final StringId GatewayEventRemoteDispatcher__0___COULD_NOT_CONNECT = new StringId(1564, "{0}: Could not connect.");

  public static final StringId GatewaySender_0_IS_BECOMING_PRIMARY_GATEWAY_Sender = new StringId(1566, "{0} is becoming primary gateway Sender.");

  public static final StringId GatewaySenderAdvisor_0_THE_THREAD_TO_OBTAIN_THE_FAILOVER_LOCK_WAS_INTERRUPTED__THIS_GATEWAY_SENDER_WILL_NEVER_BECOME_THE_PRIMARY = new StringId(1568, "{0}: The thread to obtain the failover lock was interrupted. This gateway sender will never become the primary.");

  public static final StringId SerialGatewaySenderImpl_0__STARTING_AS_PRIMARY = new StringId(1570, "{0} : Starting as primary");

  public static final StringId GatewaySender_COULD_NOT_STOP_LOCK_OBTAINING_THREAD_DURING_GATEWAY_SENDER_STOP = new StringId(1573, "Could not stop lock obtaining thread during gateway sender stop");

  public static final StringId GatewayImpl_0__MARKING__1__EVENTS_AS_POSSIBLE_DUPLICATES = new StringId(1578, "{0} : Marking  {1}  events as possible duplicates");

  public static final StringId GatewayImpl_0__WAITING_FOR_FAILOVER_COMPLETION = new StringId(1580, "{0} : Waiting for failover completion");
  public static final StringId GatewayImpl_0__WAITING_TO_BECOME_PRIMARY_GATEWAY = new StringId(1581, "{0} : Waiting to become primary gateway");
  public static final StringId GatewayImpl_ABOUT_TO_PROCESS_THE_MESSAGE_QUEUE_BUT_NOT_THE_PRIMARY = new StringId(1582, "About to process the message queue but not the primary.");

  public static final StringId GatewayImpl_DESTROYING_GATEWAYEVENTDISPATCHER_WITH_ACTIVELY_QUEUED_DATA = new StringId(1584, "Destroying GatewayEventDispatcher with actively queued data.");

  public static final StringId SerialGatewaySenderImpl_STARTED__0 = new StringId(1586, "Started  {0}");
  public static final StringId GatewayImpl_STOPPED__0 = new StringId(1587, "Stopped  {0}");
  public static final StringId SerialGatewaySenderImpl_0__STARTING_AS_SECONDARY_BECAUSE_PRIMARY_GATEWAY_SENDER_IS_AVAIALABLE_ON_MEMBER_2 = new StringId(1588, "{0} starting as secondary because primary gateway sender is available on member :{1}");
  public static final StringId GemFireBasicDataSource_AN_EXCEPTION_WAS_CAUGHT_WHILE_TRYING_TO_LOAD_THE_DRIVER = new StringId(1589, "An Exception was caught while trying to load the driver. {0}");
  public static final StringId GemFireBasicDataSource_GEMFIREBASICDATASOURCE_GETCONNECTION_URL_FOR_THE_DATASOURCE_NOT_AVAILABLE = new StringId(1590, "GemFireBasicDataSource::getConnection:Url for the DataSource not available");
  public static final StringId GemFireCache_0__NOW_CLOSING = new StringId(1591, "{0} : Now closing.");
  public static final StringId GemFireCache_INITIALIZING_CACHE_USING__0__1 = new StringId(1592, "Initializing cache using \"{0}\":{1}");
  public static final StringId GemFireCache_FAILED_TO_GET_THE_CQSERVICE_TO_CLOSE_DURING_CACHE_CLOSE_1 = new StringId(1593, "Failed to get the CqService, to close during cache close (1).");
  public static final StringId GemFireCache_FAILED_TO_GET_THE_CQSERVICE_TO_CLOSE_DURING_CACHE_CLOSE_2 = new StringId(1594, "Failed to get the CqService, to close during cache close (2).");
  public static final StringId GemFireCache_WHILE_READING_CACHE_XML_0_1 = new StringId(1595, "While reading Cache XML {0}. {1}");

  public static final StringId GemFireConnPooledDataSource_EXCEPTION_CREATING_GEMFIRECONNECTIONPOOLMANAGER = new StringId(1620, "An exception was caught while creating a GemFireConnectionPoolManager. {0}");
  public static final StringId CqQueryImpl_FAILED_TO_REMOVE_CONTINUOUS_QUERY_FROM_THE_REPOSITORY_CQNAME_0_ERROR_1 = new StringId(1621, "Failed to remove Continuous Query From the repository. CqName: {0} Error : {1}");

  public static final StringId GemFireStatSampler_ARCHIVING_STATISTICS_TO__0_ = new StringId(1633, "Archiving statistics to \"{0}\".");
  public static final StringId GemFireStatSampler_COULD_NOT_OPEN_STATISTIC_ARCHIVE_0_CAUSE_1 = new StringId(1634, "Could not open statistic archive {0}. Cause: {1}");
  public static final StringId GemFireStatSampler_COULD_NOT_RENAME_0_TO_1 = new StringId(1635, "Could not rename \"{0}\" to \"{1}\".");
  public static final StringId GemFireStatSampler_DISABLING_STATISTIC_ARCHIVAL = new StringId(1636, "Disabling statistic archival.");
  public static final StringId GemFireStatSampler_RENAMED_OLD_EXISTING_ARCHIVE_TO__0_ = new StringId(1637, "Renamed old existing archive to \"{0}\".");
  public static final StringId GemFireStatSampler_STATISTIC_ARCHIVE_CLOSE_FAILED_BECAUSE__0 = new StringId(1638, "Statistic archive close failed because: {0}");

  public static final StringId GlobalTransaction_GLOBALTRANSACTIONENLISTRESOURCEONLY_ONE_RESOUCE_MANAGER_SUPPORTED = new StringId(1644, "GlobalTransaction::enlistResource::Only one Resouce Manager supported");
  public static final StringId GlobalTransaction_GLOBALTRANSACTION_CONSTRUCTOR_ERROR_WHILE_TRYING_TO_CREATE_XID_DUE_TO_0 = new StringId(1645, "GlobalTransaction::Constructor::Error while trying to create Xid due to {0}");
  public static final StringId GlobalTransaction_GLOBATRANSACTION_EXPIREGTX_ERROR_OCCURED_WHILE_REMOVING_TRANSACTIONAL_MAPPINGS_0 = new StringId(1646, "GlobaTransaction::expireGTX:Error occurred while removing transactional mappings {0}");
  public static final StringId GlobalTransaction_TRANSACTION_0_HAS_TIMED_OUT = new StringId(1647, "Transaction {0} has timed out.");
  public static final StringId GrantorRequestProcessor_GRANTORREQUESTPROCESSOR_ELDERSYNCWAIT_THE_CURRENT_ELDER_0_IS_WAITING_FOR_THE_NEW_ELDER_1 = new StringId(1648, "GrantorRequestProcessor.elderSyncWait: The current Elder {0} is waiting for the new Elder {1}.");
  public static final StringId HARegionQueue_DACEREMOVEEVENTANDSETSEQUENCEID_SINCE_THE_EVENT_WAS_SUCCESSULY_REMOVED_BY_TAKE_OPERATION_IT_SHOULD_HAVE_EXISTED_IN_THE_REGION = new StringId(1649, "DACE::removeEventAndSetSequenceID: Since the event was successuly removed by a take operation, it should have existed in the region");
  public static final StringId HARegionQueue_DISPATCHEDANDCURRENTEVENTSEXPIREORUPDATE_UNEXPECTEDLY_ENCOUNTERED_EXCEPTION_WHILE_REMOVING_EXPIRY_ENTRY_FOR_THREADIDENTIFIER_0_AND_EXPIRY_VALUE_1 = new StringId(1650, "DispatchedAndCurrentEvents::expireOrUpdate: Unexpectedly encountered exception while removing expiry entry for ThreadIdentifier={0} and expiry value={1}");
  public static final StringId HARegionQueue_DISPATCHEDANDCURRENTEVENTSEXPIREORUPDATE_UNEXPECTEDLY_ENCOUNTERED_EXCEPTION_WHILE_UPDATING_EXPIRY_ID_FOR_THREADIDENTIFIER_0 = new StringId(1651, "DispatchedAndCurrentEvents::expireOrUpdate: Unexpectedly encountered exception while updating expiry ID for ThreadIdentifier={0}");
  public static final StringId HARegionQueue_EXCEPTION_OCCURED_WHILE_TRYING_TO_SET_THE_LAST_DISPATCHED_ID = new StringId(1652, "Exception occurred while trying to set the last dispatched id");
  public static final StringId HARegionQueue_HAREGIONQUEUECREATECACHELISTNEREXCEPTION_IN_THE_EXPIRY_THREAD = new StringId(1653, "HAREgionQueue::createCacheListner::Exception in the expiry thread");

  public static final StringId HARegionQueue_HAREGIONQUEUE_AND_ITS_DERIVED_CLASS_DO_NOT_SUPPORT_THIS_OPERATION = new StringId(1655, "HARegionQueue and its derived class do not support this operation ");
  public static final StringId ConnectionTable_THE_DISTRIBUTED_SYSTEM_IS_SHUTTING_DOWN = new StringId(1656, "The distributed system is shutting down");
  public static final StringId HARegionQueue_INTERRUPTEDEXCEPTION_OCCURED_IN_QUEUEREMOVALTHREAD_WHILE_WAITING = new StringId(1657, "InterruptedException occurred in QueueRemovalThread  while waiting ");
  public static final StringId HealthMonitorImpl_UNEXPECTED_STOP_OF_HEALTH_MONITOR = new StringId(1658, "Unexpected stop of health monitor");

  public static final StringId HARegionQueue_THE_QUEUEREMOVALTHREAD_IS_DONE = new StringId(1660, "The QueueRemovalThread is done.");
  public static final StringId HighPriorityAckedMessage_0_THERE_ARE_STILL_1_OTHER_THREADS_ACTIVE_IN_THE_HIGH_PRIORITY_THREAD_POOL = new StringId(1661, "{0}: There are still {1} other threads active in the high priority thread pool.");
  public static final StringId HostStatSampler_HOSTSTATSAMPLER_THREAD_COULD_NOT_BE_STOPPED = new StringId(1662, "HostStatSampler thread could not be stopped during shutdown.");
  public static final StringId HostStatSampler_STATISIC_ARCHIVER_SHUTDOWN_FAILED_BECAUSE__0 = new StringId(1663, "Statistic archiver shutdown failed because:  {0}");
  public static final StringId HostStatSampler_STATISTIC_ARCHIVER_SHUTDOWN_FAILED_BECAUSE__0 = new StringId(1664, "Statistic archiver shutdown failed because:  {0}");
  public static final StringId HostStatSampler_STATISTIC_ARCHIVER_SHUTTING_DOWN_BECAUSE__0 = new StringId(1665, "Statistic archiver shutting down because:  {0}");

  public static final StringId IndexCreationMsg_REGION_IS_LOCALLY_DESTROYED_THROWING_REGIONDESTROYEDEXCEPTION_FOR__0 = new StringId(1673, "Region is locally destroyed, throwing RegionDestroyedException for  {0}");
  public static final StringId CqQueryImpl_FAILED_TO_STOP_THE_CQ_CQNAME_0_THE_SERVER_ENDPOINTS_ON_WHICH_THIS_CQ_WAS_REGISTERED_WERE_NOT_FOUND = new StringId(1676, "Failed to stop the cq. CqName: {0}. The server endpoints on which this cq was registered were not found.");

  public static final StringId InternalDataSerializer_COULD_NOT_LOAD_DATASERIALIZER_CLASS_0 = new StringId(1679, "Could not load DataSerializer class: {0}");
  public static final StringId InternalDataSerializer_REGISTER_DATASERIALIZER_0_OF_CLASS_1 = new StringId(1680, "Register DataSerializer {0} of class {1}");
  public static final StringId InternalDistributedSystem_CONNECTLISTENER_THREW = new StringId(1681, "ConnectListener threw...");
  public static final StringId InternalDistributedSystem_DISCONNECTLISTENERSHUTDOWN_THREW = new StringId(1682, "DisconnectListener/Shutdown threw...");
  public static final StringId InternalDistributedSystem_DISCONNECT_LISTENER_IGNORED_ITS_INTERRUPT__0 = new StringId(1683, "Disconnect listener ignored its interrupt: {0}");

  public static final StringId InternalDistributedSystem_DISCONNECT_LISTENER_STILL_RUNNING__0 = new StringId(1685, "Disconnect listener still running: {0}");
  public static final StringId InternalDistributedSystem_EXCEPTION_OCCURED_WHILE_TRYING_TO_CONNECT_THE_SYSTEM_DURING_RECONNECT = new StringId(1686, "Exception occurred while trying to connect the system during reconnect");
  public static final StringId InternalDistributedSystem_EXCEPTION_OCCURED_WHILE_TRYING_TO_CREATE_THE_CACHE_DURING_RECONNECT = new StringId(1687, "Exception occurred while trying to create the cache during reconnect");
  public static final StringId InternalDistributedSystem_INTERRUPTED_WHILE_PROCESSING_DISCONNECT_LISTENER = new StringId(1688, "Interrupted while processing disconnect listener");

  public static final StringId GemFireCacheImpl_RUNNING_IN_LOCAL_MODE = new StringId(1690, "Running in local mode since no locators were specified.");
  public static final StringId InternalDistributedSystem_SHUTDOWNLISTENER__0__THREW = new StringId(1691, "ShutdownListener < {0} > threw...");
  public static final StringId InternalDistributedSystem_STARTUP_CONFIGURATIONN_0 = new StringId(1692, "Startup Configuration:\n {0}");

  public static final StringId InternalInstantiator_CLASS_0_DOES_NOT_HAVE_A_TWOARGUMENT_CLASS_INT_CONSTRUCTOR = new StringId(1694, "Class {0} does not have a two-argument (Class, int) constructor.");
  public static final StringId InternalInstantiator_CLASS_0_DOES_NOT_HAVE_A_TWOARGUMENT_CLASS_INT_CONSTRUCTOR_IT_IS_AN_INNER_CLASS_OF_1_SHOULD_IT_BE_A_STATIC_INNER_CLASS = new StringId(1695, "Class {0} does not have a two-argument (Class, int) constructor. It is an inner class of {1}. Should it be a static inner class?");
  public static final StringId InternalInstantiator_COULD_NOT_LOAD_INSTANTIATED_CLASS_0 = new StringId(1696, "Could not load instantiated class: {0}");
  public static final StringId InternalInstantiator_COULD_NOT_LOAD_INSTANTIATOR_CLASS_0 = new StringId(1697, "Could not load instantiator class: {0}");
  public static final StringId InternalLocator_0__IS_STOPPED = new StringId(1698, "{0}  is stopped");
  public static final StringId InternalLocator_COULD_NOT_STOP__0__IN_60_SECONDS = new StringId(1699, "Could not stop  {0}  in 60 seconds");

  public static final StringId InternalLocator_INTERRUPTED_WHILE_STOPPING__0 = new StringId(1701, "Interrupted while stopping  {0}");
  public static final StringId InternalLocator_LOCATOR_STARTED_ON__0 = new StringId(1702, "Locator started on  {0}");
  public static final StringId StartupMessage_REJECTED_NEW_SYSTEM_NODE_0_BECAUSE_MCAST_PORT_1_DOES_NOT_MATCH_THE_DISTRIBUTED_SYSTEM_2_IT_IS_ATTEMPTING_TO_JOIN = new StringId(1703, "Rejected new system node {0} because its mcast-port {1} does not match the mcast-port {2} of the distributed system it is attempting to join. To fix this make sure the \"mcast-port\" gemfire property is set the same on all members of the same distributed system.");
  public static final StringId StartupMessage_REJECTED_NEW_SYSTEM_NODE_0_BECAUSE_MCAST_ADDRESS_1_DOES_NOT_MATCH_THE_DISTRIBUTED_SYSTEM_2_IT_IS_ATTEMPTING_TO_JOIN = new StringId(1704, "Rejected new system node {0} because its mcast-address {1} does not match the mcast-address {2} of the distributed system it is attempting to join. To fix this make sure the \"mcast-address\" gemfire property is set the same on all members of the same distributed system.");
  public static final StringId ReplyProcessor21_UNKNOWN_DSFID_ERROR = new StringId(1705, "Exception received due to missing DSFID {0} on remote node \"{1}\" " + "running version \"{2}\".");

  public static final StringId InternalLocator_STARTING_DISTRIBUTED_SYSTEM = new StringId(1707, "Starting distributed system");

  public static final StringId InternalLocator_STOPPING__0 = new StringId(1709, "Stopping {0}");
  public static final StringId InternalLocator_USING_EXISTING_DISTRIBUTED_SYSTEM__0 = new StringId(1710, "Using existing distributed system:  {0}");
  public static final StringId Invalid_0_INVALID_MESSAGE_TYPE_WITH_TX_1_FROM_2 = new StringId(1711, "{0}: INVALID message type with tx: {1} from {2}");

  public static final StringId JCAConnectionManagerImpl_EXCEPTION_CAUGHT_WHILE_INITIALIZING = new StringId(1713, "JCAConnectionManagerImpl::Constructor: An exception was caught while initialising due to {0}");
  public static final StringId GroupMembershipService_JOINED_TOOK__0__MS = new StringId(1714, "Joined the distributed system (took  {0}  ms)");
  public static final StringId GroupMembershipService_FAILED_TO_SEND_MESSAGE_0_TO_MEMBER_1_VIEW_2 = new StringId(1715, "Failed to send message <{0}> to member <{1}> view = {2}");


  public static final StringId GroupMembershipService_MEMBERSHIP_ERROR_HANDLING_STARTUP_EVENT = new StringId(1720, "Membership: Error handling startup event");
  public static final StringId GroupMembershipService_MEMBERSHIP_FAULT_WHILE_PROCESSING_VIEW_ADDITION_OF__0 = new StringId(1721, "Membership: Fault while processing view addition of  {0}");
  public static final StringId GroupMembershipService_MEMBERSHIP_FAULT_WHILE_PROCESSING_VIEW_REMOVAL_OF__0 = new StringId(1722, "Membership: Fault while processing view removal of  {0}");

  public static final StringId GroupMembershipService_MEMBERSHIP_IGNORING_SURPRISE_CONNECT_FROM_SHUNNED_MEMBER_0 = new StringId(1726, "Membership: Ignoring surprise connect from shunned member <{0}>");

  public static final StringId GroupMembershipService_MEMBERSHIP_PROCESSING_ADDITION__0_ = new StringId(1730, "Membership: Processing addition < {0} >");

  public static final StringId JNDIInvoker_JNDIINVOKERMAPTRANSACTIONSNAMINGEXCEPTION_WHILE_BINDING_TRANSACTIONMANAGERUSERTRANSACTION_TO_APPLICATION_SERVER_JNDI_TREE = new StringId(1737, "JNDIInvoker::mapTransactions::NamingException while binding TransactionManager/UserTransaction to Application Server JNDI Tree");
  public static final StringId JNDIInvoker_JNDIINVOKERMAPTRANSACTIONSNAMINGEXCEPTION_WHILE_BINDING_TRANSACTIONMANAGERUSERTRANSACTION_TO_GEMFIRE_JNDI_TREE = new StringId(1738, "JNDIInvoker::mapTransactions::NamingException while binding TransactionManager/UserTransaction to GemFire JNDI Tree");
  public static final StringId JNDIInvoker_JNDIINVOKERMAPTRANSACTIONSSYSTEMEXCEPTION_WHILE_BINDING_TRANSACTIONMANAGERUSERTRANSACTION_TO_APPLICATION_SERVER_JNDI_TREE = new StringId(1739, "JNDIInvoker::mapTransactions::SystemException while binding TransactionManager/UserTransaction to Application Server JNDI Tree");
  public static final StringId JNDIInvoker_JNDIINVOKERMAPTRANSACTIONSSYSTEMEXCEPTION_WHILE_BINDING_USERTRANSACTION_TO_GEMFIRE_JNDI_TREE = new StringId(1740, "JNDIInvoker::mapTransactions::SystemException while binding UserTransaction to GemFire JNDI Tree");
  public static final StringId JNDIInvoker_JNDIINVOKER_DOTRANSACTIONLOOKUP_FOUND_WEBSPHERE_TRANSACTIONMANAGER_FACTORY_CLASS_0_BUT_COULDNT_INVOKE_ITS_STATIC_GETTRANSACTIONMANAGER_METHOD = new StringId(1741, "JNDIInvoker::doTransactionLookup::Found WebSphere TransactionManager factory class [{0}], but could not invoke its static ''getTransactionManager'' method");
  public static final StringId JNDIInvoker_JNDIINVOKER_MAPDATASOURCE_0_WHILE_BINDING_1_TO_JNDI_CONTEXT = new StringId(1742, "JNDIInvoker::mapDataSource::{0} while binding {1} to JNDI Context");
  public static final StringId GemFireStatSampler_OS_STATISTICS_FAILED_TO_INITIALIZE_PROPERLY_SOME_STATS_MAY_BE_MISSING_SEE_BUGNOTE_37160 = new StringId(1743, "OS statistics failed to initialize properly, some stats may be missing. See bugnote #37160.");
  public static final StringId GemFireStatSampler_OS_STATISTIC_COLLECTION_DISABLED_BY_OSSTATSDISABLED_SYSTEM_PROPERTY = new StringId(1744, "OS statistic collection disabled by setting the \"osStatsDisabled\" system property to true.");
  public static final StringId KeySet_0_THE_INPUT_REGION_NAME_FOR_THE_KEY_SET_REQUEST_IS_NULL = new StringId(1745, "{0}: The input region name for the key set request is null");
  public static final StringId DataSerializer_ENUM_TO_SERIALIZE_IS_NULL = new StringId(1746, "The enum constant to serialize is null");
  public static final StringId DataSerializer_ENUM_CLASS_TO_DESERIALIZE_IS_NULL = new StringId(1747, "the enum class to deserialize is null");

  public static final StringId LocalRegion_0_EVENT_NOT_DISPATCHED_DUE_TO_REJECTED_EXECUTION = new StringId(1766, "{0} Event not dispatched due to rejected execution");
  public static final StringId LocalRegion_0_OPERATIONS_ARE_NOT_ALLOWED_BECAUSE_THIS_THREAD_HAS_AN_ACTIVE_TRANSACTION = new StringId(1767, "{0} operations are not allowed because this thread has an active transaction");
  public static final StringId LocalRegion_EXCEPTION_IN_EXPIRATION_TASK = new StringId(1768, "Exception in expiration task");
  public static final StringId LocalRegion_EXCEPTION_OCCURRED_IN_CACHELISTENER = new StringId(1769, "Exception occurred in CacheListener");
  public static final StringId LocalRegion_EXCEPTION_OCCURRED_WHILE_CLOSING_CQS_ON_REGION_DESTORY = new StringId(1770, "Exception occurred while closing CQs on region destroy.");
  public static final StringId LocalRegion_REGIONENTRY_SHOULD_NOT_BE_NULL = new StringId(1771, "regionEntry should not be null");
  public static final StringId LocalRegion_REGIONENTRY_WAS_CREATED_WITH_TRANSACTION_THAT_IS_NO_LONGER_ACTIVE = new StringId(1772, "Region.Entry was created with transaction that is no longer active.");
  public static final StringId LocalRegion_REGION_COLLECTION_WAS_CREATED_WITH_TRANSACTION_0_THAT_IS_NO_LONGER_ACTIVE = new StringId(1773, "Region collection was created with transaction {0} that is no longer active.");
  public static final StringId LocalRegion_STATISTICS_DISABLED_FOR_REGION_0 = new StringId(1774, "Statistics disabled for region ''{0}''");
  public static final StringId LocalRegion_THIS_ITERATOR_DOES_NOT_SUPPORT_MODIFICATION = new StringId(1775, "This iterator does not support modification");

  public static final StringId LocalRegion_VALUE_FOR_CONTAINSVALUEVALUE_CANNOT_BE_NULL = new StringId(1778, "Value for \"containsValue(value)\" cannot be null");
  public static final StringId LocalStatisticsFactory_STATISTIC_COLLECTION_IS_DISABLED_USE_DSTATSDISABLEFALSE_TO_TURN_ON_STATISTICS = new StringId(1779, "Statistic collection is disabled: use: -Dstats.disable=false to turn on statistics.");
  public static final StringId LogFileParser_MISSING_TIME_STAMP = new StringId(1780, "MISSING TIME STAMP");
  public static final StringId MX4JModelMBean_CANNOT_FIND_OPERATIONS_PARAMETER_CLASSES = new StringId(1781, "Cannot find operation''s parameter classes");
  public static final StringId MX4JModelMBean_CANNOT_STORE_MODELMBEAN_AFTER_OPERATION_INVOCATION = new StringId(1782, "Cannot store ModelMBean after operation invocation");
  public static final StringId MX4JModelMBean_CANNOT_STORE_MODELMBEAN_AFTER_SETATTRIBUTE = new StringId(1783, "Cannot store ModelMBean after setAttribute");

  public static final StringId CqService_TIMEOUT_WHILE_TRYING_TO_GET_CQ_FROM_META_REGION_CQNAME_0 = new StringId(1785, "Timeout while trying to get CQ from meta region, CqName: {0}");
  public static final StringId ManagedEntityController_A_REMOTE_COMMAND_MUST_BE_SPECIFIED_TO_OPERATE_ON_A_MANAGED_ENTITY_ON_HOST_0 = new StringId(1786, "A remote command must be specified to operate on a managed entity on host \"{0}\"");
  public static final StringId ManagedEntityController_COULD_NOT_DETERMINE_IF_MANAGED_ENTITY_WAS_RUNNING_0 = new StringId(1787, "Could not determine if managed entity was running: {0}");
  public static final StringId ManagedEntityController_EXECUTING_REMOTE_COMMAND_0_IN_DIRECTORY_1 = new StringId(1788, "Executing remote command: {0} in directory {1}");
  public static final StringId ManagedEntityController_OUTPUT_OF_0_IS_1 = new StringId(1789, "Output of \"{0}\" is {1}");
  public static final StringId ManagedEntityController_REMOTE_EXECUTION_OF_0_FAILED = new StringId(1790, "Remote execution of \"{0}\" failed.");
  public static final StringId ManagedEntityController_RESULT_OF_EXECUTING_0_IS_1 = new StringId(1791, "Result of executing \"{0}\" is {1}");
  public static final StringId ManagedEntityController_WHILE_EXECUTING_0 = new StringId(1792, "While executing \"{0}\"");
  public static final StringId ManagerLogWriter_COULD_NOT_CHECK_DISK_SPACE_ON_0_BECAUSE_JAVAIOFILELISTFILES_RETURNED_NULL_THIS_COULD_BE_CAUSED_BY_A_LACK_OF_FILE_DESCRIPTORS = new StringId(1793, "Could not check disk space on \"{0}\" because java.io.File.listFiles returned null. This could be caused by a lack of file descriptors.");
  public static final StringId ManagerLogWriter_COULD_NOT_DELETE_INACTIVE__0___1_ = new StringId(1794, "Could not delete inactive {0} \"{1}\".");
  public static final StringId ManagerLogWriter_COULD_NOT_FREE_SPACE_IN_0_DIRECTORY_THE_SPACE_USED_IS_1_WHICH_EXCEEDS_THE_CONFIGURED_LIMIT_OF_2 = new StringId(1795, "Could not free space in {0} directory.  The space used is {1} which exceeds the configured limit of {2}.");

  public static final StringId ManagerLogWriter_DELETED_INACTIVE__0___1_ = new StringId(1797, "Deleted inactive  {0}  \"{1}\".");
  public static final StringId ManagerLogWriter_SWITCHING_TO_LOG__0 = new StringId(1798, "Switching to log {0}");


  public static final StringId ManagerLogWriter_UNABLE_TO_REMOVE_CONSOLE_WITH_ID_0_FROM_ALERT_LISTENERS = new StringId(1799, "Unable to remove console with id {0} from alert listeners.");

  public static final StringId Message_RPL_NEG_LEN__0 = new StringId(1800, "rpl: neg len:  {0}");
  public static final StringId InternalLocator_DISCONNECTING_DISTRIBUTED_SYSTEM_FOR_0 = new StringId(1801, "Disconnecting distributed system for {0}");
  public static final StringId Oplog_COULD_NOT_DELETE__0_ = new StringId(1802, "Could not delete \"{0}\".");
  public static final StringId DiskInitFile_FAILED_INIT_FILE_WRITE_BECAUSE_0 = new StringId(1803, "Failed writing data to initialization file because: {0}");
  public static final StringId DataSerializer_CLASS_0_NOT_ENUM = new StringId(1804, "Class {0} is not an enum");
  public static final StringId Oplog_PARTIAL_RECORD = new StringId(1805, "Detected a partial record in oplog file. Partial records can be caused by an abnormal shutdown in which case this warning can be safely ignored. They can also be caused by the oplog file being corrupted.");

  public static final StringId NewLRUClockHand_ADDING_ANODE_TO_LRU_LIST = new StringId(1806, "adding a Node to lru list: {0}");

  public static final StringId NewLRUClockHand_DISCARDING_EVICTED_ENTRY = new StringId(1808, "discarding evicted entry");

  public static final StringId NewLRUClockHand_GREEDILY_PICKING_AN_AVAILABLE_ENTRY = new StringId(1810, "greedily picking an available entry");

  public static final StringId NewLRUClockHand_REMOVING_TRANSACTIONAL_ENTRY_FROM_CONSIDERATION = new StringId(1814, "removing transactional entry from consideration");
  public static final StringId NewLRUClockHand_RETURNING_UNUSED_ENTRY = new StringId(1815, "returning unused entry: {0}");

  public static final StringId NewLRUClockHand_SKIPPING_RECENTLY_USED_ENTRY = new StringId(1817, "skipping recently used entry {0}");
  public static final StringId NewLRUClockHand_UNLINKENTRY_CALLED = new StringId(1818, "unlinkEntry called for {0}");

  public static final StringId Oplog_FAILED_READING_FILE_DURING_RECOVERY_FROM_0 = new StringId(1820, "Failed to read file during recovery from \"{0}\"");
  public static final StringId Oplog_NO_VALUE_WAS_FOUND_FOR_ENTRY_WITH_DISK_ID_0_ON_A_REGION_WITH_SYNCHRONOUS_WRITING_SET_TO_1 = new StringId(1821, "No value was found for entry with disk Id  {0} on a region  with synchronous writing set to {1}");

  public static final StringId Oplog_OPLOGBASICGET_ERROR_IN_READING_THE_DATA_FROM_DISK_FOR_DISK_ID_HAVING_DATA_AS_0 = new StringId(1823, "Oplog::basicGet: Error in reading the data from disk for Disk ID {0}");

  public static final StringId Oplog_OPLOGGETNOBUFFEREXCEPTION_IN_RETRIEVING_VALUE_FROM_DISK_FOR_DISKID_0 = new StringId(1826, "Oplog::getNoBuffer:Exception in retrieving value from disk for diskId={0}");

  public static final StringId GroupMembershipService_THIS_DISTRIBUTED_SYSTEM_IS_SHUTTING_DOWN = new StringId(1836, "This distributed system is shutting down.");
  public static final StringId PRHARedundancyProvider_EXCEPTION_CREATING_PARTITION_ON__0 = new StringId(1837, "Exception creating partition on  {0}");
  public static final StringId AbstractDistributionConfig_ACK_SEVERE_ALERT_THRESHOLD_NAME = new StringId(1838, "The number of seconds a distributed message can wait for acknowledgment past {0} before it ejects unresponsive members from the distributed system.  Defaults to \"{1}\".  Legal values are in the range [{2}..{3}].");
  public static final StringId AbstractDistributionConfig_CLIENT_CONFLATION_PROP_NAME = new StringId(1839, "Client override for server queue conflation setting");
  public static final StringId PRHARRedundancyProvider_ALLOCATE_ENOUGH_MEMBERS_TO_HOST_BUCKET = new StringId(1840, "allocate enough members to host bucket.");
  public static final StringId PRHARedundancyProvider_TIME_OUT_WAITING_0_MS_FOR_CREATION_OF_BUCKET_FOR_PARTITIONED_REGION_1_MEMBERS_REQUESTED_TO_CREATE_THE_BUCKET_ARE_2 = new StringId(1841, "Time out waiting {0} ms for creation of bucket for partitioned region {1}. Members requested to create the bucket are: {2}");

  public static final StringId PUT_0_FAILED_TO_PUT_ENTRY_FOR_REGION_1_KEY_2_VALUE_3 = new StringId(1843, "{0}: Failed to put entry for region {1} key {2} value {3}");
  public static final StringId PUT_0_UNEXPECTED_EXCEPTION = new StringId(1844, "{0}: Unexpected Exception");
  public static final StringId PartitionMessage_MEMBERDEPARTED_GOT_NULL_MEMBERID_CRASHED_0 = new StringId(1845, "memberDeparted got null memberId crashed={0}");
  public static final StringId PartitionRegion_NO_VM_AVAILABLE_FOR_GETENTRY_IN_0_ATTEMPTS = new StringId(1846, "No VM available for getEntry in {0} attempts");
  public static final StringId PartitionRegion_NO_VM_AVAILABLE_FOR_GET_IN_0_ATTEMPTS = new StringId(1847, "No VM available for get in {0} attempts");

  public static final StringId PartitionedRegionDataStore_ASSERTION_ERROR_CREATING_BUCKET_IN_REGION = new StringId(1849, "Assertion error creating bucket in region");

  public static final StringId PartitionedRegionDataStore_CREATEBUCKETREGION_CREATING_BUCKETID_0_NAME_1 = new StringId(1851, "createBucketRegion: Creating bucketId = {0} name = {1}.");
  public static final StringId PartitionedRegionDataStore_EXCPETION__IN_BUCKET_INDEX_CREATION_ = new StringId(1852, "Excpetion  in bucket index creation : {0}");
  public static final StringId PRHARRedundancyProvider_CONFIGURED_REDUNDANCY_LEVEL_COULD_NOT_BE_SATISFIED_0 = new StringId(1853, "Configured Redundancy Level Could Not be Satisfied. {0} to satisfy redundancy for the region.{1}");
  public static final StringId PartitionedRegionDataStore_PARTITIONEDREGION_0_CAUGHT_UNEXPECTED_EXCEPTION_DURING_CLEANUP = new StringId(1854, "PartitionedRegion {0}: caught unexpected exception during data cleanup");
  public static final StringId MemberFunctionExecutor_NO_MEMBER_FOUND_FOR_EXECUTING_FUNCTION_0 = new StringId(1855, "No member found for executing function : {0}.");

  public static final StringId PartitionedRegionDataStore_PARTITIONED_REGION_0_HAS_EXCEEDED_LOCAL_MAXIMUM_MEMORY_CONFIGURATION_2_MB_CURRENT_SIZE_IS_3_MB = new StringId(1857, "Partitioned Region {0} has exceeded local maximum memory configuration {2} Mb, current size is {3} Mb");
  public static final StringId PartitionedRegionDataStore_PARTITIONED_REGION_0_IS_AT_OR_BELOW_LOCAL_MAXIMUM_MEMORY_CONFIGURATION_2_MB_CURRENT_SIZE_IS_3_MB = new StringId(1858, "Partitioned Region {0} is at or below local maximum memory configuration {2} Mb, current size is {3} Mb");

  public static final StringId FunctionService_BUCKET_MIGRATED_TO_ANOTHER_NODE = new StringId(1860, "Bucket migrated to another node. Please retry.");
  public static final StringId PartitionedRegionDataStore_VERIFIED_NODELIST_FOR_BUCKETID_0_IS_1 = new StringId(1861, "Verified nodelist for bucketId={0} is {1}");
  public static final StringId PartitionedRegionHelper_DATALOSS___0____CURRENT_MEMBERSHIP_LIST___1 = new StringId(1862, "DATALOSS (  {0}  ) :: Current Membership List =  {1}");
  public static final StringId PartitionedRegionHelper_DATALOSS___0____NODELIST_FROM_PRCONFIG___1 = new StringId(1863, "DATALOSS (  {0}  ) :: NodeList from prConfig =  {1}");
  public static final StringId PartitionedRegionHelper_DATALOSS___0____SIZE_OF_NODELIST_AFTER_VERIFYBUCKETNODES_FOR_BUKID___1__IS_0 = new StringId(1864, "DATALOSS (  {0}  ) :: Size of nodeList After verifyBucketNodes for bukId =  {1}  is 0");
  public static final StringId PartitionedRegionHelper_GOT_ENTRYNOTFOUNDEXCEPTION_IN_DESTROY_OP_FOR_ALLPRREGION_KEY_0 = new StringId(1865, "Got EntryNotFoundException in destroy Op for allPRRegion key = {0}");
  public static final StringId PartitionedRegion_0_IS_USING_PRID_1_FOR_2_BUT_THIS_PROCESS_IS_USING_PRID_3 = new StringId(1866, "{0} is using PRID {1} for {2} but this process is using PRID {3}");
  public static final StringId PartitionedRegion_0_IS_USING_PRID_1_FOR_2_BUT_THIS_PROCESS_MAPS_THAT_PRID_TO_3 = new StringId(1867, "{0} is using PRID {1} for {2} but this process maps that PRID to {3}");

  public static final StringId PartitionedRegion_CAUGHT_EXCEPTION_WHILE_TRYING_TO_UNLOCK_DURING_REGION_DESTRUCTION = new StringId(1869, "Caught exception while trying to unlock during Region destruction.");
  public static final StringId PartitionedRegion_CREATED_INDEX_LOCALLY_SENDING_INDEX_CREATION_MESSAGE_TO_ALL_MEMBERS_AND_WILL_BE_WAITING_FOR_RESPONSE_0 = new StringId(1870, "Created index locally, sending index creation message to all members, and will be waiting for response {0}.");
  public static final StringId PartitionedRegion_DONE_WATING_FOR_REMOVE_INDEX = new StringId(1871, "Done wating for remove index...");
  public static final StringId PartitionedRegion_FAILED_REGISTRATION_PRID_0_NAMED_1 = new StringId(1872, "FAILED_REGISTRATION prId={0} named {1}");
  public static final StringId PartitionedRegion_FORCEREATTEMPT_EXCEPTION___0 = new StringId(1873, "ForceReattempt exception :  {0}");
  public static final StringId PartitionedRegion_NEWLY_ADDED_MEMBER_TO_THE_PR_IS_AN_ACCESSOR_AND_WILL_NOT_RECEIVE_INDEX_INFORMATION_0 = new StringId(1874, "Newly added member to the PR is an accessor and will not receive index information : {0}");

  public static final StringId PartitionedRegion_NO_VM_AVAILABLE_FOR_CONTAINS_KEY_IN_1_ATTEMPTS = new StringId(1877, "No VM available for contains key in {1} attempts");
  public static final StringId PartitionedRegion_NO_VM_AVAILABLE_FOR_CONTAINS_VALUE_FOR_KEY_IN_1_ATTEMPTS = new StringId(1878, "No VM available for contains value for key in {1} attempts");
  public static final StringId ExecuteFunction_CANNOT_SPECIFY_0_FOR_DATA_INDEPENDENT_FUNCTIONS = new StringId(1879, "Cannot specify {0} for data independent functions");
  public static final StringId PartitionedRegion_NO_VM_AVAILABLE_FOR_INVALIDATE_IN_0_ATTEMPTS = new StringId(1880, "No VM available for invalidate in {0} attempts.");

  public static final StringId PartitionedRegion_PARTITIONEDREGION_0_CLEANUP_PROBLEM_DESTROYING_BUCKET_1 = new StringId(1882, "PartitionedRegion {0}: cleanUp problem destroying bucket {1}");
  public static final StringId PartitionedRegion_PARTITIONEDREGION_CLEANUPFAILEDINITIALIZATION_FAILED_TO_CLEAN_THE_PARTIONREGION_ALLPARTITIONEDREGIONS = new StringId(1883, "PartitionedRegion#cleanupFailedInitialization: Failed to clean the PartionRegion allPartitionedRegions");
  public static final StringId PartitionedRegion_PARTITIONEDREGION_CLEANUPFAILEDINITIALIZATION_FAILED_TO_CLEAN_THE_PARTIONREGION_DATA_STORE = new StringId(1884, "PartitionedRegion#cleanupFailedInitialization(): Failed to clean the PartionRegion data store");
  public static final StringId PartitionedRegion_PARTITIONEDREGION_SENDDESTROYREGIONMESSAGE_CAUGHT_EXCEPTION_DURING_DESTROYREGIONMESSAGE_SEND_AND_WAITING_FOR_RESPONSE = new StringId(1885, "PartitionedRegion#sendDestroyRegionMessage: Caught exception during DestroyRegionMessage send and waiting for response");
  public static final StringId PartitionedRegion_PARTITIONED_REGION_0_IS_BORN_WITH_PRID_1_IDENT_2 = new StringId(1886, "Partitioned Region {0} is born with prId={1} ident:{2}");
  public static final StringId PartitionedRegion_PARTITIONED_REGION_0_IS_CREATED_WITH_PRID_1 = new StringId(1887, "Partitioned Region {0} is created with prId={1}");
  public static final StringId PartitionedRegion_PARTITIONED_REGION_0_WITH_PRID_1_CLOSED = new StringId(1888, "Partitioned Region {0} with prId={1} closed.");
  public static final StringId PartitionedRegion_PARTITIONED_REGION_0_WITH_PRID_1_CLOSING = new StringId(1889, "Partitioned Region {0} with prId={1} closing.");
  public static final StringId PartitionedRegion_PARTITIONED_REGION_0_WITH_PRID_1_IS_BEING_DESTROYED = new StringId(1890, "Partitioned Region {0} with prId={1} is being destroyed.");
  public static final StringId PartitionedRegion_PARTITIONED_REGION_0_WITH_PRID_1_IS_DESTROYED = new StringId(1891, "Partitioned Region {0} with prId={1} is destroyed.");
  public static final StringId PartitionedRegion_PRVIRTUALPUT_RETURNING_FALSE_WHEN_IFNEW_AND_IFOLD_ARE_BOTH_FALSE = new StringId(1892, "PR.virtualPut returning false when ifNew and ifOld are both false");
  public static final StringId PartitionedRegion_RELEASEPRIDLOCK_UNLOCKING_0_CAUGHT_AN_EXCEPTION = new StringId(1893, "releasePRIDLock: unlocking {0} caught an exception");

  public static final StringId PartitionedRegion_REMOVING_ALL_THE_INDEXES_ON_THIS_PARITITION_REGION__0 = new StringId(1899, "Removing all the indexes on this paritition region  {0}");
  public static final StringId PartitionedRegion_SENDING_REMOVEINDEX_MESSAGE_TO_ALL_THE_PARTICIPATING_PRS = new StringId(1900, "Sending removeIndex message to all the participating prs.");
  public static final StringId PartitionedRegion_STACK_TRACE = new StringId(1901, "stack trace");

  public static final StringId PartitionedRegion_THIS_INDEX__0_IS_NOT_ON_THIS_PARTITONED_REGION___1 = new StringId(1903, "This index  {0} is not on this partitoned region :  {1}");
  public static final StringId PartitionedRegion_THIS_IS_AN_ACCESSOR_VM_AND_DOESNT_CONTAIN_DATA = new StringId(1904, "This is an accessor vm and doesnt contain data");

  public static final StringId PartitionedRegion_TOTAL_SIZE_IN_PARTITIONATTRIBUTES_IS_INCOMPATIBLE_WITH_GLOBALLY_SET_TOTAL_SIZE_SET_THE_TOTAL_SIZE_TO_0MB = new StringId(1908, "Total size in PartitionAttributes is incompatible with globally set total size. Set the total size to {0}MB.");

  public static final StringId ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_CREATE_REQUEST_1_FOR_2_EVENTS = new StringId(1913, "{0}: Caught exception processing batch create request {1} for {2} events");
  public static final StringId ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_DESTROY_REQUEST_1_CONTAINING_2_EVENTS = new StringId(1914, "{0}: Caught exception processing batch destroy request {1} containing {2} events");
  public static final StringId ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_REQUEST_1_CONTAINING_2_EVENTS = new StringId(1915, "{0}: Caught exception processing batch request {1} containing {2} events");
  public static final StringId ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_REQUEST_1_CONTAINING_2_EVENTS = new StringId(1916, "{0}: Caught exception processing batch update request {1} containing {2} events");
  public static final StringId ProcessBatch_0_DURING_BATCH_DESTROY_NO_ENTRY_WAS_FOUND_FOR_KEY_1 = new StringId(1917, "{0}: during batch destroy no entry was found for key {1}");
  public static final StringId ProcessBatch_0_FAILED_TO_CREATE_OR_UPDATE_ENTRY_FOR_REGION_1_KEY_2_VALUE_3_CALLBACKARG_4 = new StringId(1918, "{0}: Failed to create or update entry for region {1} key {2} value {3} callbackArg {4}");
  public static final StringId ProcessBatch_0_FAILED_TO_UPDATE_ENTRY_FOR_REGION_1_KEY_2_VALUE_3_AND_CALLBACKARG_4 = new StringId(1919, "{0}: Failed to update entry for region {1}, key {2}, value {3}, and callbackArg {4}");
  public static final StringId GroupMembershipService_Invalid_Surprise_Member = new StringId(1920, "attempt to add old member: {0} as surprise member to {1}");
  public static final StringId ProcessBatch_0_THE_INPUT_KEY_FOR_THE_BATCH_DESTROY_REQUEST_1_IS_NULL = new StringId(1921, "{0}: The input key for the batch destroy request {1} is null");
  public static final StringId ProcessBatch_0_THE_INPUT_KEY_FOR_THE_BATCH_UPDATE_REQUEST_1_IS_NULL = new StringId(1922, "{0}: The input key for the batch update request {1} is null");
  public static final StringId ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_CREATE_REQUEST_1_IS_NULL = new StringId(1923, "{0}: The input region name for the batch create request {1} is null");
  public static final StringId ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_DESTROY_REQUEST_1_IS_NULL = new StringId(1924, "{0}: The input region name for the batch destroy request {1} is null");
  public static final StringId ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_UPDATE_REQUEST_1_IS_NULL = new StringId(1925, "{0}: The input region name for the batch update request {1} is null");
  public static final StringId ProcessBatch_0_WROTE_BATCH_EXCEPTION = new StringId(1926, "{0}: Wrote batch exception: ");
  public static final StringId ProcessBatch_RECEIVED_PROCESS_BATCH_REQUEST_0_OUT_OF_ORDER_THE_ID_OF_THE_LAST_BATCH_PROCESSED_WAS_1_THIS_BATCH_REQUEST_WILL_BE_PROCESSED_BUT_SOME_MESSAGES_MAY_HAVE_BEEN_LOST = new StringId(1927, "Received process batch request {0} out of order. The id of the last batch processed was {1}. This batch request will be processed, but some messages may have been lost.");
  public static final StringId ProcessBatch_RECEIVED_PROCESS_BATCH_REQUEST_0_THAT_HAS_ALREADY_BEEN_OR_IS_BEING_PROCESSED_GEMFIRE_GATEWAY_APPLYRETRIES_IS_SET_SO_THIS_BATCH_WILL_BE_PROCESSED_ANYWAY = new StringId(1928, "Received process batch request {0} that has already been or is being processed. gemfire.gateway.ApplyRetries is set, so this batch will be processed anyway.");
  public static final StringId ProcessBatch_RECEIVED_PROCESS_BATCH_REQUEST_0_THAT_HAS_ALREADY_BEEN_OR_IS_BEING_PROCESSED__THIS_PROCESS_BATCH_REQUEST_IS_BEING_IGNORED = new StringId(1929, "Received process batch request {0} that has already been or is being processed. This process batch request is being ignored.");

  public static final StringId ProcessBatch_WAS_NOT_FOUND_DURING_BATCH_CREATE_REQUEST_0 = new StringId(1931, "Region {0} was not found during batch create request {1}");

  public static final StringId Processbatch_0_UNKNOWN_ACTION_TYPE_1_FOR_BATCH_FROM_2 = new StringId(1934, "{0}: Unknown action type ({1}) for batch from {2}");

  public static final StringId SimpleStatSampler_STATSSAMPLERATE_0 = new StringId(1938, "stats.sample-rate= {0}");

  public static final StringId Put_0_ATTEMPTED_TO_PUT_A_NULL_VALUE_FOR_EXISTING_KEY_1 = new StringId(1940, "{0}: Attempted to put a null value for existing key {1}");
  public static final StringId Put_0_THE_INPUT_KEY_FOR_THE_PUT_REQUEST_IS_NULL = new StringId(1941, "{0} The input key for the put request is null");
  public static final StringId Put_0_THE_INPUT_REGION_NAME_FOR_THE_PUT_REQUEST_IS_NULL = new StringId(1942, "{0} The input region name for the put request is null");
  public static final StringId Put_ATTEMPTED_TO_PUT_A_NULL_VALUE_FOR_EXISTING_KEY_0 = new StringId(1943, "Attempted to put a null value for existing key {0}");
  public static final StringId Put_THE_INPUT_KEY_FOR_THE_PUT_REQUEST_IS_NULL = new StringId(1944, " The input key for the put request is null");
  public static final StringId Put_THE_INPUT_REGION_NAME_FOR_THE_PUT_REQUEST_IS_NULL = new StringId(1945, " The input region name for the put request is null");

  public static final StringId QueueRemovalMessage_QUEUEREMOVALMESSAGEPROCESSEXCEPTION_IN_PROCESSING_THE_LAST_DISPTACHED_SEQUENCE_ID_FOR_A_HAREGIONQUEUES_DACE_THE_PROBLEM_IS_WITH_EVENT_ID__0_FOR_HAREGION_WITH_NAME_1 = new StringId(1948, "QueueRemovalMessage::process:Exception in processing the last disptached sequence ID for a HARegionQueue''s DACE. The problem is with event ID ={0} for HARegion with name={1}");

  public static final StringId QueueRemovalMessage_QUEUE_FOUND_DESTROYED_WHILE_PROCESSING_THE_LAST_DISPTACHED_SEQUENCE_ID_FOR_A_HAREGIONQUEUES_DACE_THE_EVENT_ID_IS_0_FOR_HAREGION_WITH_NAME_1 = new StringId(1950, "Queue found destroyed while processing the last disptached sequence ID for a HARegionQueue''s DACE. The event ID is {0} for HARegion with name={1}");
  public static final StringId RegionAdvisor_FAILED_TO_PROCESS_ALL_QUEUED_BUCKETPROFILES_FOR_0 = new StringId(1951, "Failed to process all queued BucketProfiles for {0}");

  public static final StringId TXManager_NO_WRITER_ON_CLIENT = new StringId(1953, "A TransactionWriter cannot be registered on a client");
  public static final StringId Version_REMOTE_VERSION_NOT_SUPPORTED = new StringId(1954, "Peer or client version with ordinal {0} not supported. Highest known version is {1}");

  public static final StringId RegisterInterestList_0_REGION_NAMED_1_WAS_NOT_FOUND_DURING_REGISTER_INTEREST_LIST_REQUEST = new StringId(1960, "{0}: Region named {1} was not found during register interest list request.");
  public static final StringId RegisterInterestList_THE_INPUT_LIST_OF_KEYS_FOR_THE_REGISTER_INTEREST_REQUEST_IS_EMPTY = new StringId(1961, "The input list of keys for the register interest request is empty.");
  public static final StringId RegisterInterestList_THE_INPUT_LIST_OF_KEYS_IS_EMPTY_AND_THE_INPUT_REGION_NAME_IS_NULL_FOR_THE_REGISTER_INTEREST_REQUEST = new StringId(1962, "The input list of keys is empty and the input region name is null for the register interest request.");
  public static final StringId RegisterInterest_0_REGION_NAMED_1_WAS_NOT_FOUND_DURING_REGISTER_INTEREST_REQUEST = new StringId(1963, "{0}: Region named {1} was not found during register interest request.");
  public static final StringId RegisterInterest_THE_INPUT_KEY_FOR_THE_REGISTER_INTEREST_REQUEST_IS_NULL = new StringId(1964, "The input key for the register interest request is null");
  public static final StringId RegisterInterest_THE_INPUT_REGION_NAME_FOR_THE_REGISTER_INTEREST_REQUEST_IS_NULL = new StringId(1965, "The input region name for the register interest request is null.");
  public static final StringId RemoteGfManagerAgent_ABORTED__0 = new StringId(1966, "aborted  {0}");
  public static final StringId RemoteGfManagerAgent_IGNORING_STRANGE_INTERRUPT = new StringId(1967, "Ignoring strange interrupt");
  public static final StringId RemoteGfManagerAgent_JOINPROCESSOR_CAUGHT_EXCEPTION = new StringId(1968, "JoinProcessor caught exception...");
  public static final StringId RemoveIndexesMessage_REGION_IS_LOCALLY_DESTROYED_THROWING_REGIONDESTROYEDEXCEPTION_FOR__0 = new StringId(1969, "Region is locally destroyed, throwing RegionDestroyedException for  {0}");
  public static final StringId RemoveIndexesMessage_REMOVE_INDEXES_MESSAGE_GOT_THE_PR__0 = new StringId(1970, "Remove indexes message got the pr  {0}");
  public static final StringId RemoveIndexesMessage_TRYING_TO_GET_PR_WITH_ID___0 = new StringId(1971, "Trying to get pr with id :  {0}");
  public static final StringId RemoveIndexesMessage_WILL_REMOVE_THE_INDEXES_ON_THIS_PR___0 = new StringId(1972, "Will remove the indexes on this pr :  {0}");

  public static final StringId ReplyProcessor21_0_SEC_HAVE_ELAPSED_WHILE_WAITING_FOR_REPLIES_1_ON_2_WHOSE_CURRENT_MEMBERSHIP_LIST_IS_3 = new StringId(1974, "{0} seconds have elapsed while waiting for replies: {1} on {2} whose current membership list is: [{3}]");
  public static final StringId ReplyProcessor21_EXCEPTION_RECEIVED_IN_REPLYMESSAGE_ONLY_ONE_EXCEPTION_IS_PASSED_BACK_TO_CALLER_THIS_EXCEPTION_IS_LOGGED_ONLY = new StringId(1975, "Exception received in ReplyMessage. Only one exception is passed back to caller. This exception is logged only.");
  public static final StringId ReplyProcessor21_RECEIVED_REPLY_FROM_MEMBER_0_BUT_WAS_NOT_EXPECTING_ONE_MORE_THAN_ONE_REPLY_MAY_HAVE_BEEN_RECEIVED_THE_REPLY_THAT_WAS_NOT_EXPECTED_IS_1 = new StringId(1976, "Received reply from member {0} but was not expecting one. More than one reply may have been received. The reply that was not expected is: {1}");

  public static final StringId ReplyProcessor21_VIEW_NO_LONGER_HAS_0_AS_AN_ACTIVE_MEMBER_SO_WE_WILL_NO_LONGER_WAIT_FOR_IT = new StringId(1978, "View no longer has {0} as an active member, so we will no longer wait for it.");
  public static final StringId ReplyProcessor21_WAIT_FOR_REPLIES_COMPLETED_1 = new StringId(1979, "{0} wait for replies completed");
  public static final StringId ReplyProcessor21_WAIT_FOR_REPLIES_TIMING_OUT_AFTER_0_SEC = new StringId(1980, "wait for replies timing out after {0} seconds");
  public static final StringId TXStateProxyImpl_Distributed_Region_In_Client_TX = new StringId(1981, "Distributed region {0} is being used in a client-initiated transaction.  The transaction will only affect servers and this client.  To keep from seeing this message use ''local'' scope in client regions used in transactions.");
  public static final StringId Request_THE_INPUT_KEY_FOR_THE_GET_REQUEST_IS_NULL = new StringId(1982, "The input key for the get request is null.");
  public static final StringId Request_THE_INPUT_REGION_NAME_AND_KEY_FOR_THE_GET_REQUEST_ARE_NULL = new StringId(1983, "The input region name and key for the get request are null.");
  public static final StringId Request_THE_INPUT_REGION_NAME_FOR_THE_GET_REQUEST_IS_NULL = new StringId(1984, "The input region name for the get request is null.");

  public static final StringId ServerConnection_0_HANDSHAKE_ACCEPT_FAILED_ON_SOCKET_1_2 = new StringId(1988, "{0}: Handshake accept failed on socket {1}: {2}");

  public static final StringId ServerConnection_0_RECEIVED_UNKNOWN_HANDSHAKE_REPLY_CODE_1 = new StringId(1991, "{0}: Received Unknown handshake reply code: {1}");
  public static final StringId ServerConnection_RECEIVED_UNKNOWN_HANDSHAKE_REPLY_CODE = new StringId(1992, "Received Unknown handshake reply code.");
  public static final StringId ServerConnection_0_UNEXPECTED_CANCELLATION = new StringId(1993, "{0}: Unexpected cancellation: ");

  public static final StringId LocalRegion_THE_FOLLOWING_EXCEPTION_OCCURRED_ATTEMPTING_TO_GET_KEY_0 = new StringId(1995, "The following exception occurred attempting to get key={0}");

  public static final StringId SystemAdmin_LRU_OPTION_HELP = new StringId(1998, "-lru=<type> Sets region''s lru algorithm. Valid types are: none, lru-entry-count, lru-heap-percentage, or lru-memory-size");
  public static final StringId SystemAdmin_LRUACTION_OPTION_HELP = new StringId(1999, "-lruAction=<action> Sets the region''s lru action. Valid actions are: none, overflow-to-disk, local-destroy");
  public static final StringId SystemAdmin_LRULIMIT_OPTION_HELP = new StringId(2000, "-lruLimit=<int> Sets the region''s lru limit. Valid values are >= 0");
  public static final StringId SystemAdmin_CONCURRENCYLEVEL_OPTION_HELP = new StringId(2001, "-concurrencyLevel=<int> Sets the region''s concurrency level. Valid values are >= 0");
  public static final StringId SystemAdmin_INITIALCAPACITY_OPTION_HELP = new StringId(2002, "-initialCapacity=<int> Sets the region''s initial capacity. Valid values are >= 0");
  public static final StringId SystemAdmin_LOADFACTOR_OPTION_HELP = new StringId(2003, "-loadFactor=<float> Sets the region''s load factory. Valid values are >= 0.0");
  public static final StringId SystemAdmin_STATISTICSENABLED_OPTION_HELP = new StringId(2004, "-statisticsEnabled=<boolean> Sets the region''s statistics enabled. Value values are true or false");
  public static final StringId SystemAdmin_MONITOR_OPTION_HELP = new StringId(2005, "-monitor Causes the stats command to keep periodically checking its statistic archives for updates.");

  public static final StringId CacheFactory_0_EXISTING_CACHE_WITH_DIFFERENT_CACHE_CONFIG = new StringId(2006, "Existing cache has different cache configuration, it has:\n{0}");
  public static final StringId LonerDistributionmanager_CHANGING_PORT_FROM_TO = new StringId(2007, "Updating membership port.  Port changed from {0} to {1}.  ID is now {2}");
  public static final StringId ManagerLogWriter_ROLLING_CURRENT_LOG_TO_0 = new StringId(2008, "Rolling current log to {0}");

  public static final StringId ExecuteFunction66_TRANSACTIONAL_FUNCTION_WITHOUT_RESULT = new StringId(2009, " Function invoked within transactional context, but hasResults() is false; ordering of transactional operations cannot be guaranteed.  This message is only issued once by a server.");
  public static final StringId TXManagerImpl_TRANSACTION_ACTIVE_CANNOT_RESUME = new StringId(2010, "Cannot resume transaction, current thread has an active transaction");
  public static final StringId TXManagerImpl_UNKNOWN_TRANSACTION_OR_RESUMED = new StringId(2011, "Trying to resume unknown transaction, or transaction resumed by another thread");

  public static final StringId Connection_ATTEMPT_TO_CONNECT_TIMED_OUT_AFTER_0_MILLISECONDS = new StringId(2012, "Attempt timed out after {0} milliseconds");
  public static final StringId TXManagerImpl_EXCEPTION_IN_TRANSACTION_TIMEOUT = new StringId(2013, "Exception occurred while rolling back timed out transaction {0}");
  public static final StringId LocalRegion_SERVER_HAS_CONCURRENCY_CHECKS_ENABLED_0_BUT_CLIENT_HAS_1_FOR_REGION_2 = new StringId(2014, "Server has concurrencyChecksEnabled {0} but client has {1} for region {2}");
  public static final StringId CreateRegionProcessor_CANNOT_CREATE_REGION_0_CCENABLED_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_REGION_CCENABLED_2 = new StringId(2015, "Cannot create region {0} concurrency-checks-enabled={1} because another cache ({2}) has the same region concurrency-checks-enabled={3}");
  public static final StringId PartitionedRegion_ENABLING_CONCURRENCY_CHECKS_FOR_PERSISTENT_PR = new StringId(2016, "Turning on concurrency checks for region: {0} since it has been configured to persist data to disk.");
  public static final StringId CreateRegionProcessor_CANNOT_CREATE_REGION_0_WITH_PERSISTANCE_TRUE_PERSISTENT_MEMBERS_B4_NON_PERSISTENT = new StringId(2017, "Cannot create region {0} DataPolicy withPersistence=true because another cache ({1}) has the same region DataPolicy withPersistence=false." + " Persistent members must be started before non-persistent members");
  public static final StringId AbstractRegionMap_ATTEMPT_TO_REMOVE_TOMBSTONE = new StringId(2018, "Internal product error: attempt to directly remove a versioned tombstone from region entry map");
  public static final StringId HostStatSampler_STATISTICS_SAMPLING_THREAD_DETECTED_A_WAKEUP_DELAY_OF_0_MS_INDICATING_A_POSSIBLE_RESOURCE_ISSUE = new StringId(2019, "Statistics sampling thread detected a wakeup delay of {0} ms, indicating a possible resource issue. Check the GC, memory, and CPU statistics.");
  public static final StringId ServerConnection_0__UNEXPECTED_EXCEPTION = new StringId(2020, "{0} : Unexpected Exception");
  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_GATEWAYCONFLICTRESOLVER = new StringId(2021, "A {0} is not an instance of a GatewayConflictResolver.");
  public static final StringId GemFireCacheImpl_TOMBSTONE_ERROR = new StringId(2022, "Unexpected exception while processing tombstones");
  public static final StringId FunctionService_NO_MEMBERS_FOUND_IN_GROUPS = new StringId(2023, "No members found in group(s) {0}");
  public static final StringId DistributionManager_Time_Skew_Warning = new StringId(2024, "The clock for this machine may be more than 5 minutes different than the negotiated cache time received from {0}");
  public static final StringId DistributionManager_Cache_Time = new StringId(2025, "The negotiated cache time from {0} is {1}.  Delta from the system clock is {2} milliseconds.");
  public static final StringId RemoteMessage_REGION_0_NOT_COLOCATED_WITH_TRANSACTION = new StringId(2026, "Region {0} not colocated with other regions in transaction");
  public static final StringId ServerConnection_BATCH_IDS_ARE_OUT_OF_ORDER_SETTING_LATESTBATCHID_TO_0_IT_WAS_1 = new StringId(2027, "Batch IDs are out of order. Setting latestBatchId to:{0}. It was:{1}");
  public static final StringId DistributionManager_Cache_Time_Offset_Skew_Warning = new StringId(2028, "New cache time offset calculated is off more than {0} ms from earlier offset.");
  // ok to reuse 2029
  public static final StringId CacheXmlParser_UNKNOWN_INDEX_TYPE = new StringId(2030, "Unknown index type defined as {0}, will be set to range index");
  public static final StringId TXStateStub_LOCAL_DESTROY_NOT_ALLOWED_IN_TRANSACTION = new StringId(2031, "localDestroy() is not allowed in a transaction");
  public static final StringId TXStateStub_LOCAL_INVALIDATE_NOT_ALLOWED_IN_TRANSACTION = new StringId(2032, "localInvalidate() is not allowed in a transaction");

  public static final StringId GatewayEventRemoteDispatcher_A_BATCHEXCEPTION_OCCURRED_PROCESSING_PDX_EVENT__0 = new StringId(2036, "A BatchException occurred processing PDX events. Index of array of Exception : {0}");

  public static final StringId SizeMessage_SIZEMESSAGE_REGION_NOT_FOUND_FOR_THIS_MEMBER = new StringId(2046, "SizeMessage: Region {0} not found for this member");

  public static final StringId LocalRegion_EXCEPTION_OCCURRED_IN_CONFLICTRESOLVER = new StringId(2048, "Exception occurred in GatewayConflictResolver");

  public static final StringId SingleWriteSingleReadRegionQueue_0_DURING_FAILOVER_DETECTED_THAT_KEYS_HAVE_WRAPPED = new StringId(2050, "{0}: During failover, detected that keys have wrapped tailKey={1} headKey={2}");

  public static final StringId SingleWriteSingleReadRegionQueue_0_THE_QUEUE_REGION_NAMED_1_COULD_NOT_BE_CREATED = new StringId(2055, "{0}: The queue region named {1} could not be created");
  public static final StringId SingleWriteSingleReadRegionQueue_UNEXPECTED_EXCEPTION_DURING_INIT_OF_0 = new StringId(2056, "Unexpected Exception during init of {0}");

  public static final StringId SizeMessage_SIZEMESSAGE_DATA_STORE_NOT_CONFIGURED_FOR_THIS_MEMBER = new StringId(2059, "SizeMessage: data store not configured for this member");
  public static final StringId SocketCreator_SSL_CONNECTION_FROM_PEER_0 = new StringId(2060, "SSL Connection from peer {0}");
  public static final StringId SocketCreator_SSL_ERROR_IN_AUTHENTICATING_PEER_0_1 = new StringId(2061, "SSL Error in authenticating peer {0}[{1}].");
  public static final StringId SocketCreator_SSL_ERROR_IN_AUTHENTICATING_PEER = new StringId(2619, "SSL Error in authenticating peer.");
  public static final StringId SocketCreator_SSL_ERROR_IN_CONNECTING_TO_PEER_0_1 = new StringId(1125, "SSL Error in connecting to peer {0}[{1}].");

  public static final StringId StartupMessage_ILLEGALARGUMENTEXCEPTION_WHILE_REGISTERING_AN_INSTANTIATOR_0 = new StringId(2065, "IllegalArgumentException while registering an Instantiator: {0}");
  public static final StringId StartupMessage_ILLEGALARGUMENTEXCEPTION_WHILE_REGISTERING_A_DATASERIALIZER_0 = new StringId(2066, "IllegalArgumentException while registering a DataSerializer: {0}");
  public static final StringId StartupMessage_REJECTED_NEW_SYSTEM_NODE_0_BECAUSE_ISMCASTENABLED_1_DOES_NOT_MATCH_THE_DISTRIBUTED_SYSTEM_IT_IS_ATTEMPTING_TO_JOIN = new StringId(2067, "Rejected new system node {0} because mcast was {1} which does not match the distributed system it is attempting to join. To fix this make sure the \"mcast-port\" gemfire property is set the same on all members of the same distributed system.");
  public static final StringId StartupMessage_REJECTED_NEW_SYSTEM_NODE_0_BECAUSE_ISTCPDISABLED_1_DOES_NOT_MATCH_THE_DISTRIBUTED_SYSTEM_IT_IS_ATTEMPTING_TO_JOIN = new StringId(2068, "Rejected new system node {0} because isTcpDisabled={1} does not match the distributed system it is attempting to join.");

  public static final StringId StartupMessage_STARTUPMESSAGE_DM_0_HAS_STARTED_PROCESSOR_1_WITH_DISTRIBUTED_SYSTEM_ID_2 = new StringId(2070, "StartupMessage DM {0} has started. processor = {1}. with distributed system id : {2}");
  public static final StringId StartupOperation_MEMBERSHIP_RECEIVED_CONNECTION_FROM_0_BUT_RECEIVED_NO_STARTUP_RESPONSE_AFTER_1_MS = new StringId(2071, "Membership: received connection from <{0}> but received no startup response after {1} ms.");
  public static final StringId StartupOperation_MEMBERSHIP_STARTUP_TIMED_OUT_AFTER_WAITING_0_MILLISECONDS_FOR_RESPONSES_FROM_1 = new StringId(2072, "Membership: startup timed out after waiting {0} milliseconds for responses from {1}");

  public static final StringId StateFlushOperation_0__EXCEPTION_CAUGHT_WHILE_DETERMINING_CHANNEL_STATE = new StringId(2074, "{0}  Exception caught while determining channel state");
  public static final StringId StateFlushOperation_0__THROWABLE_CAUGHT_WHILE_DETERMINING_CHANNEL_STATE = new StringId(2075, "{0}  Throwable caught while determining channel state");

  public static final StringId StateFlushOperation_EXCEPTION_CAUGHT_WHILE_WAITING_FOR_CHANNEL_STATE = new StringId(2077, "Exception caught while waiting for channel state");

  public static final StringId StateFlushOperation_STATE_FLUSH_TERMINATED_WITH_EXCEPTION = new StringId(2081, "state flush terminated with exception");

  public static final StringId TCPConduit_0_IS_1_INSTEAD_OF_THE_REQUESTED_2 = new StringId(2083, "{0} is {1} instead of the requested {2}");
  public static final StringId TCPConduit_ABANDONED_BECAUSE_SHUTDOWN_IS_IN_PROGRESS = new StringId(2084, "Abandoned because shutdown is in progress");
  public static final StringId TCPConduit_ATTEMPTING_TCPIP_RECONNECT_TO__0 = new StringId(2085, "Attempting TCP/IP reconnect to  {0}");
  public static final StringId TCPConduit_ENDING_RECONNECT_ATTEMPT_BECAUSE_0_HAS_DISAPPEARED = new StringId(2086, "Ending reconnect attempt because {0} has disappeared.");
  public static final StringId TCPConduit_ENDING_RECONNECT_ATTEMPT_TO_0_BECAUSE_SHUTDOWN_HAS_STARTED = new StringId(2087, "Ending reconnect attempt to {0} because shutdown has started.");
  public static final StringId TCPConduit_ERROR_SENDING_MESSAGE_TO_0_WILL_REATTEMPT_1 = new StringId(2088, "Error sending message to {0} (will reattempt): {1}");
  public static final StringId TCPConduit_EXCEPTION_CREATING_SERVERSOCKET = new StringId(2089, "While creating ServerSocket on port {0} with address {1}");
  public static final StringId TCPConduit_EXCEPTION_PARSING_P2PIDLECONNECTIONTIMEOUT = new StringId(2090, "exception parsing p2p.idleConnectionTimeout");
  public static final StringId TCPConduit_EXCEPTION_PARSING_P2PTCPBUFFERSIZE = new StringId(2091, "exception parsing p2p.tcpBufferSize");
  public static final StringId TCPConduit_FAILED_TO_ACCEPT_CONNECTION_FROM_0_BECAUSE_1 = new StringId(2092, "Failed to accept connection from {0} because {1}");
  public static final StringId TCPConduit_FAILED_TO_SET_LISTENER_RECEIVERBUFFERSIZE_TO__0 = new StringId(2093, "Failed to set listener receiverBufferSize to  {0}");
  public static final StringId TCPConduit_INHIBITACCEPTOR = new StringId(2094, "p2p.test.inhibitAcceptor was found to be set, inhibiting incoming tcp/ip connections");
  public static final StringId TCPConduit_PEER_HAS_DISAPPEARED_FROM_VIEW = new StringId(2095, "Peer has disappeared from view: {0}");

  public static final StringId TCPConduit_SUCCESSFULLY_RECONNECTED_TO_MEMBER_0 = new StringId(2099, "Successfully reconnected to member {0}");
  public static final StringId TCPConduit_UNABLE_TO_SET_LISTENER_PRIORITY__0 = new StringId(2100, "unable to set listener priority:  {0}");
  public static final StringId TCPConduit_UNABLE_TO_SHUT_DOWN_LISTENER_WITHIN_0_MS_UNABLE_TO_INTERRUPT_SOCKET_ACCEPT_DUE_TO_JDK_BUG_GIVING_UP = new StringId(2101, "Unable to shut down listener within {0}ms.  Unable to interrupt socket.accept() due to JDK bug. Giving up.");
  public static final StringId TIMED_OUT_WAITING_FOR_ACKS = new StringId(2102, "Timed out waiting for ACKS.");
  public static final StringId TXCommitMessage_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER = new StringId(2103, "Exception occurred in TransactionListener");
  public static final StringId TXCommitMessage_NEW_MEMBERS_FOR_REGION_0_ORIG_LIST_1_NEW_LIST_2 = new StringId(2104, "New members for Region: {0} orig list: {1} new list: {2}");
  public static final StringId PartitionedRegion_INDEX_CREATION_FAILED_ROLLING_UPGRADE = new StringId(2105, "Indexes should not be created when there are older versions of gemfire in the cluster.");

  public static final StringId TXManagerImpl_EXCEPTION_OCCURRED_IN_TRANSACTIONLISTENER = new StringId(2107, "Exception occurred in TransactionListener");

  public static final StringId TXOriginatorRecoveryProcessor_PROCESSTXORIGINATORRECOVERYMESSAGE = new StringId(2109, "[processTXOriginatorRecoveryMessage]");
  public static final StringId TXOriginatorRecoveryProcessor_PROCESSTXORIGINATORRECOVERYMESSAGE_LOCALLY_PROCESS_REPLY = new StringId(2110, "[processTXOriginatorRecoveryMessage] locally process reply");
  public static final StringId TXOriginatorRecoveryProcessor_PROCESSTXORIGINATORRECOVERYMESSAGE_SEND_REPLY = new StringId(2111, "[processTXOriginatorRecoveryMessage] send reply");
  public static final StringId TXOriginatorRecoveryProcessor_PROCESSTXORIGINATORRECOVERYMESSAGE_THROWABLE = new StringId(2112, "[processTXOriginatorRecoveryMessage] throwable:");
  public static final StringId TXRecoverGrantorMessageProcessor_MORE_THAN_ONE_EXCEPTION_THROWN_IN__0 = new StringId(2113, "More than one exception thrown in  {0}");
  public static final StringId TXRecoverGrantorMessageProcessor_TXRECOVERGRANTORMESSAGEPROCESSORPROCESS_THROWABLE = new StringId(2114, "[TXRecoverGrantorMessageProcessor.process] throwable:");
  public static final StringId TailLogResponse_ERROR_OCCURRED_WHILE_READING_SYSTEM_LOG__0 = new StringId(2115, "Error occurred while reading system log:  {0}");
  public static final StringId TransactionManagerImpl_BEGIN__SYSTEMEXCEPTION_DUE_TO_0 = new StringId(2116, "SystemException due to {0}");
  public static final StringId TransactionManagerImpl_EXCEPTION_IN_NOTIFY_AFTER_COMPLETION_DUE_TO__0 = new StringId(2117, "Exception in notify after completion due to {0}");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPLCLEANUPEXCEPTION_WHILE_CLEANING_THREAD_BEFORE_RE_STATRUP = new StringId(2118, "Exception While cleaning thread before re statrup");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPLRESUMETRANSACTION_RESUMED = new StringId(2119, "Transaction resumed");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPLSUSPENDTRANSACTION_SUSPENDED = new StringId(2120, "Transaction suspended");
  public static final StringId TransactionManagerImpl_TRANSACTIONTIMEOUTTHREAD__RUN_EXCEPTION_OCCURRED_WHILE_INSPECTING_GTX_FOR_EXPIRY = new StringId(2121, "Exception occurred while inspecting global transaction for expiry");
  public static final StringId TxFarSideTracker_WAITING_TO_COMPLETE_ON_MESSAGE_0_CAUGHT_AN_INTERRUPTED_EXCEPTION = new StringId(2122, "Waiting to complete on message {0} caught an interrupted exception");
  public static final StringId UNCAUGHT_EXCEPTION_IN_THREAD_0 = new StringId(2123, "Uncaught exception in thread {0}");
  public static final StringId UNEXPECTED_EXCEPTION = new StringId(2124, "unexpected exception");
  public static final StringId UnRegisterInterestList_THE_INPUT_LIST_OF_KEYS_FOR_THE_UNREGISTER_INTEREST_REQUEST_IS_EMPTY = new StringId(2125, "The input list of keys for the unregister interest request is empty.");
  public static final StringId UnRegisterInterestList_THE_INPUT_LIST_OF_KEYS_IS_EMPTY_AND_THE_INPUT_REGION_NAME_IS_NULL_FOR_THE_UNREGISTER_INTEREST_REQUEST = new StringId(2126, "The input list of keys is empty and the input region name is null for the unregister interest request.");
  public static final StringId UnRegisterInterest_THE_INPUT_KEY_FOR_THE_UNREGISTER_INTEREST_REQUEST_IS_NULL = new StringId(2127, "The input key for the unregister interest request is null.");
  public static final StringId UnRegisterInterest_THE_INPUT_REGION_NAME_AND_KEY_FOR_THE_UNREGISTER_INTEREST_REQUEST_ARE_NULL = new StringId(2128, "The input region name and key for the unregister interest request are null.");
  public static final StringId UnRegisterInterest_THE_INPUT_REGION_NAME_FOR_THE_UNREGISTER_INTEREST_REQUEST_IS_NULL = new StringId(2129, "The input region name for the unregister interest request is null.");

  public static final StringId ASTLiteral_DAY_MUST_BE_131_IN_DATE_LITERAL = new StringId(2131, "Day must be 1..31 in DATE literal");
  public static final StringId ASTLiteral_DAY_MUST_BE_131_IN_TIMESTAMP_LITERAL = new StringId(2132, "Day must be 1..31 in TIMESTAMP literal");
  public static final StringId ASTLiteral_HOUR_MUST_BE_023_IN_TIMESTAMP_LITERAL = new StringId(2133, "Hour must be 0..23 in TIMESTAMP literal");
  public static final StringId ASTLiteral_HOUR_MUST_BE_023_IN_TIME_LITERAL = new StringId(2134, "Hour must be 0..23 in TIME literal");
  public static final StringId ASTLiteral_ILLEGAL_FORMAT_FOR_CHAR_LITERAL_A_CHARACTER_LITERAL_MUST_HAVE_EXACTLY_ONE_CHARACTER = new StringId(2135, "Illegal format for CHAR literal. A character literal must have exactly one character");
  public static final StringId ASTLiteral_ILLEGAL_FORMAT_FOR_DATE_LITERAL_0_EXPECTED_FORMAT_IS_YYYYMMDD = new StringId(2136, "Illegal format for DATE literal:  {0} . Expected format is yyyy-mm-dd");
  public static final StringId ASTLiteral_ILLEGAL_FORMAT_FOR_TIMESTAMP_LITERAL_0_EXPECTED_FORMAT_IS_YYYYMMDD_HHMMSSFFFFFFFFF = new StringId(2137, "Illegal format for TIMESTAMP literal:  {0} . Expected format is yyyy-mm-dd hh:mm:ss.fffffffff");
  public static final StringId ASTLiteral_ILLEGAL_FORMAT_FOR_TIME_LITERAL_0_EXPECTED_FORMAT_IS_HHMMSS = new StringId(2138, "Illegal format for TIME literal:  {0} . Expected format is hh:mm:ss");
  public static final StringId ASTLiteral_MINUTE_MUST_BE_059_IN_TIMESTAMP_LITERAL = new StringId(2139, "Minute must be 0..59 in TIMESTAMP literal");
  public static final StringId ASTLiteral_MINUTE_MUST_BE_059_IN_TIME_LITERAL = new StringId(2140, "Minute must be 0..59 in TIME literal");
  public static final StringId ASTLiteral_MONTH_MUST_BE_112_IN_DATE_LITERAL = new StringId(2141, "Month must be 1..12 in DATE literal");
  public static final StringId ASTLiteral_MONTH_MUST_BE_112_IN_TIMESTAMP_LITERAL = new StringId(2142, "Month must be 1..12 in TIMESTAMP literal");
  public static final StringId ASTLiteral_NEGATIVE_NUMBERS_NOT_ALLOWED_IN_TIMESTAMP_LITERAL = new StringId(2143, "Negative numbers not allowed in TIMESTAMP literal");
  public static final StringId ASTLiteral_SECOND_MUST_BE_059_IN_TIMESTAMP_LITERAL = new StringId(2144, "Second must be 0..59 in TIMESTAMP literal");
  public static final StringId ASTLiteral_SECOND_MUST_BE_059_IN_TIME_LITERAL = new StringId(2145, "Second must be 0..59 in TIME literal");
  public static final StringId ASTLiteral_UNABLE_TO_PARSE_DOUBLE_0 = new StringId(2146, "Unable to parse double  {0}");
  public static final StringId ASTLiteral_UNABLE_TO_PARSE_FLOAT_0 = new StringId(2147, "Unable to parse float  {0}");
  public static final StringId ASTLiteral_UNABLE_TO_PARSE_INTEGER_0 = new StringId(2148, "unable to parse integer:  {0}");
  public static final StringId ASTUnsupported_UNSUPPORTED_FEATURE_0 = new StringId(2149, "Unsupported feature:  {0}");
  public static final StringId AbstractCompiledValue_BOOLEAN_VALUE_EXPECTED_NOT_TYPE_0 = new StringId(2150, "boolean value expected, not type '' {0} ''");
  public static final StringId AbstractCompiledValue_GOT_NULL_AS_A_CHILD_FROM_0 = new StringId(2151, "Got null as a child from  {0}");
  public static final StringId AbstractConfig_0_VALUE_1_MUST_BE_A_NUMBER = new StringId(2152, "\"{0}\" value \"{1}\" must be a number.");
  public static final StringId AbstractConfig_0_VALUE_1_MUST_BE_A_VALID_HOST_NAME_2 = new StringId(2153, "{0}  value \"{1}\" must be a valid host name.  {2}");
  public static final StringId AbstractConfig_0_VALUE_1_MUST_BE_COMPOSED_OF_AN_INTEGER_A_FLOAT_AND_AN_INTEGER = new StringId(2154, "{0}  value \"{1}\" must be composed of an integer, a float, and an integer");
  public static final StringId AbstractConfig_0_VALUE_1_MUST_HAVE_THREE_ELEMENTS_SEPARATED_BY_COMMAS = new StringId(2155, "{0}  value \"{1}\" must have three elements separated by commas");

  public static final StringId AbstractConfig_THE_0_CONFIGURATION_ATTRIBUTE_CAN_NOT_BE_SET_FROM_THE_COMMAND_LINE_SET_1_FOR_EACH_INDIVIDUAL_PARAMETER_INSTEAD = new StringId(2157, "The \"{0}\" configuration attribute can not be set from the command line. Set \"{1}\" for each individual parameter instead.");
  public static final StringId AbstractConfig_UNHANDLED_ATTRIBUTE_NAME_0 = new StringId(2158, "unhandled attribute name \"{0}\".");
  public static final StringId AbstractConfig_UNHANDLED_ATTRIBUTE_TYPE_0_FOR_1 = new StringId(2159, "unhandled attribute type  {0}  for \"{1}\".");
  public static final StringId AbstractConfig_UNKNOWN_CONFIGURATION_ATTRIBUTE_NAME_0_VALID_ATTRIBUTE_NAMES_ARE_1 = new StringId(2160, "Unknown configuration attribute name \"{0}\". Valid attribute names are:  {1} .");

  public static final StringId AbstractDistributionConfig_0_VALUE_1_MUST_BE_OF_TYPE_2 = new StringId(2162, "{0}  value \"{1}\" must be of type  {2}");
  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_BYTEALLOWANCE_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2 = new StringId(2163, "Could not set \"{0}.byteAllowance\" to \"{1}\" because its value can not be less than \"{2}\"");
  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGEBLOCKMS_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2 = new StringId(2164, "Could not set \"{0}.rechargeBlockMs\" to \"{1}\" because its value can not be greater than \"{2}\"");
  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGEBLOCKMS_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2 = new StringId(2165, "Could not set \"{0}.rechargeBlockMs\" to \"{1}\" because its value can not be less than \"{2}\"");
  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGETHRESHOLD_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2 = new StringId(2166, "Could not set \"{0}.rechargeThreshold\" to \"{1}\" because its value can not be greater than \"{2}\"");
  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGETHRESHOLD_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2 = new StringId(2167, "Could not set \"{0}.rechargeThreshold\" to \"{1}\" because its value can not be less than \"{2}\"");
  public static final StringId UNCAUGHT_EXCEPTION_IN_THREAD_0_THIS_MESSAGE_CAN_BE_DISREGARDED_IF_IT_OCCURED_DURING_AN_APPLICATION_SERVER_SHUTDOWN_THE_EXCEPTION_MESSAGE_WAS_1 = new StringId(2168, "Uncaught exception in thread {0} this message can be disregarded if it occurred during an Application Server shutdown. The Exception message was: {1}");

  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE = new StringId(2170, "Could not set \"{0}\" to \"{1}\" because its value must be 0 when \"{2}\" is true.");
  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_FALSE_WHEN_2_IS_NOT_0 = new StringId(2171, "Could not set \"{0}\" to \"{1}\" because its value must be false when \"{2}\" is not 0.");
  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_IT_WAS_NOT_A_MULTICAST_ADDRESS = new StringId(2172, "Could not set \"{0}\" to \"{1}\" because it was not a multicast address.");
  public static final StringId AbstractDistributionConfig_INVALID_LOCATOR_0 = new StringId(2173, "Invalid locator:  {0}");
  public static final StringId AbstractDistributionConfig_INVALID_LOCATOR_0_HOST_NAME_WAS_EMPTY = new StringId(2174, "Invalid locator \"{0}\". Host name was empty.");
  public static final StringId AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0 = new StringId(2175, "unhandled attribute name \"{0}\".");
  public static final StringId AbstractDistributionConfig_UNKNOWN_LOCATOR_BIND_ADDRESS_0 = new StringId(2176, "Unknown locator bind address:  {0}");
  public static final StringId AbstractDistributionConfig_UNKNOWN_LOCATOR_HOST_0 = new StringId(2177, "Unknown locator host:  {0}");

  public static final StringId AbstractLRURegionMap_UNKNOWN_EVICTION_ACTION_0 = new StringId(2179, "Unknown eviction action:  {0}");
  public static final StringId AbstractPoolCache_ABSTRACTPOOLEDCACHEGETPOOLEDCONNECTIONFROMPOOLLOGIN_TIMEOUT_EXCEEDED = new StringId(2180, "AbstractPooledCache::getPooledConnectionFromPool:Login time-out exceeded");
  public static final StringId AbstractRegionEntry_AN_IOEXCEPTION_WAS_THROWN_WHILE_SERIALIZING = new StringId(2181, "An IOException was thrown while serializing.");
  public static final StringId AbstractRegionEntry_CANNOT_GET_VALUE_ON_DISK_FOR_A_REGION_THAT_DOES_NOT_ACCESS_THE_DISK = new StringId(2182, "Cannot get value on disk for a region that does not access the disk.");
  public static final StringId AbstractRegionEntry_DURING_THE_GII_PUT_OF_ENTRY_THE_REGION_GOT_CLEARED_SO_ABORTING_THE_OPERATION = new StringId(2183, "During the GII put of entry, the region got cleared so aborting the operation");
  public static final StringId AbstractRegion_ADDCACHELISTENER_PARAMETER_WAS_NULL = new StringId(2184, "addCacheListener parameter was null");
  public static final StringId AbstractRegion_CANNOT_SET_IDLE_TIMEOUT_WHEN_STATISTICS_ARE_DISABLED = new StringId(2185, "Cannot set idle timeout when statistics are disabled.");
  public static final StringId AbstractRegion_CANNOT_SET_LOCK_GRANTOR_WHEN_SCOPE_IS_NOT_GLOBAL = new StringId(2186, "Cannot set lock grantor when scope is not global");
  public static final StringId AbstractRegion_CANNOT_SET_TIME_TO_LIVE_WHEN_STATISTICS_ARE_DISABLED = new StringId(2187, "Cannot set time to live when statistics are disabled");
  public static final StringId AbstractRegion_0_ACTION_IS_INCOMPATIBLE_WITH_THIS_REGIONS_DATA_POLICY = new StringId(2188, "{0} action is incompatible with this region''s data policy.");
  public static final StringId AbstractRegion_IDLETIMEOUT_MUST_NOT_BE_NULL = new StringId(2189, "idleTimeout must not be null");
  public static final StringId AbstractRegion_INITCACHELISTENERS_PARAMETER_HAD_A_NULL_ELEMENT = new StringId(2190, "initCacheListeners parameter had a null element");
  public static final StringId AbstractRegion_INTERESTTYPEFILTER_CLASS_NOT_YET_SUPPORTED = new StringId(2191, "InterestType.FILTER_CLASS not yet supported");
  public static final StringId AbstractRegion_INTERESTTYPEOQL_QUERY_NOT_YET_SUPPORTED = new StringId(2192, "InterestType.OQL_QUERY not yet supported");
  public static final StringId AbstractRegion_MAP_CANNOT_BE_NULL = new StringId(2193, "map cannot be null");
  public static final StringId AbstractRegion_MORE_THAN_ONE_CACHE_LISTENER_EXISTS = new StringId(2194, "More than one cache listener exists.");
  public static final StringId AbstractRegion_NO_MIRROR_TYPE_CORRESPONDS_TO_DATA_POLICY_0 = new StringId(2195, "No mirror type corresponds to data policy \"{0}\"");
  public static final StringId AbstractRegion_REGULAR_EXPRESSION_ARGUMENT_WAS_NOT_A_STRING = new StringId(2196, "regular expression argument was not a String");
  public static final StringId AbstractRegion_REMOVECACHELISTENER_PARAMETER_WAS_NULL = new StringId(2197, "removeCacheListener parameter was null");
  public static final StringId AbstractRegion_SELECTVALUE_EXPECTS_RESULTS_OF_SIZE_1_BUT_FOUND_RESULTS_OF_SIZE_0 = new StringId(2198, "selectValue expects results of size 1, but found results of size  {0}");
  public static final StringId AbstractRegion_STATISTICS_DISABLED_FOR_REGION_0 = new StringId(2199, "Statistics disabled for region '' {0} ''");

  public static final StringId EntryExpiryTask_ERROR_CALCULATING_EXPIRATION_0 = new StringId(2201, "Error calculating expiration {0}");
  public static final StringId AbstractRegion_TIMETOLIVE_MUST_NOT_BE_NULL = new StringId(2202, "timeToLive must not be null");
  public static final StringId AbstractRegion_UNSUPPORTED_INTEREST_TYPE_0 = new StringId(2203, "Unsupported interest type:  {0}");
  public static final StringId AcceptorImpl_INTERRUPTED = new StringId(2204, "interrupted");
  public static final StringId AcceptorImpl_SELECTOR_THREAD_POOLING_CAN_NOT_BE_USED_WITH_CLIENTSERVER_SSL_THE_SELECTOR_CAN_BE_DISABLED_BY_SETTING_MAXTHREADS0 = new StringId(2205, "Selector thread pooling can not be used with client/server SSL. The selector can be disabled by setting max-threads=0.");
  public static final StringId AddHealthListenerRequest_NULL_GEMFIREHEALTHCONFIG = new StringId(2206, "Null GemFireHealthConfig");
  public static final StringId AdminDistributedSystemFactory_PLEASE_USE_DISTRIBUTEDSYSTEMCONFIGSETBINDADDRESS_INSTEAD = new StringId(2207, "Please use DistributedSystemConfig.setBindAddress instead.");
  public static final StringId AdminDistributedSystemImpl_0_DID_NOT_START_AFTER_1_MS = new StringId(2208, "{0}  did not start after  {1}  ms");
  public static final StringId AdminDistributedSystemImpl_0_DID_NOT_STOP_AFTER_1_SECONDS = new StringId(2209, "{0}  did not stop after  {1}  seconds.");
  public static final StringId AdminDistributedSystemImpl_BY_INTERRUPT = new StringId(2210, "by interrupt");
  public static final StringId AdminDistributedSystemImpl_CONNECT_HAS_NOT_BEEN_INVOKED_ON_THIS_ADMINDISTRIBUTEDSYSTEM = new StringId(2211, "connect() has not been invoked on this AdminDistributedSystem.");
  public static final StringId AdminDistributedSystemImpl_FUTURE_CANCELLED_DUE_TO_SHUTDOWN = new StringId(2212, "Future cancelled due to shutdown");
  public static final StringId AdminDistributedSystemImpl_GFMANAGERAGENT_MUST_NOT_BE_NULL = new StringId(2213, "GfManagerAgent must not be null");
  public static final StringId AdminDistributedSystemImpl_INTERRUPTED = new StringId(2214, "Interrupted");
  public static final StringId AdminDistributedSystemImpl_INTERRUPTED_WHILE_WAITING_FOR_0_TO_START = new StringId(2215, "Interrupted while waiting for  {0}  to start.");
  public static final StringId AdminDistributedSystemImpl_INTERRUPTED_WHILE_WAITING_FOR_0_TO_STOP = new StringId(2216, "Interrupted while waiting for  {0}  to stop.");
  public static final StringId AdminDistributedSystemJmxImpl_GFMANAGERAGENT_MUST_NOT_BE_NULL = new StringId(2217, "GfManagerAgent must not be null");
  public static final StringId AdminRegion_SHOULD_NOT_BE_CALLED = new StringId(2218, "Should not be called");
  public static final StringId AdminWaiters_REQUEST_FAILED = new StringId(2219, "Request failed.");
  public static final StringId AgentConfigImpl_AN_AGENTCONFIG_OBJECT_CANNOT_BE_MODIFIED_AFTER_IT_HAS_BEEN_USED_TO_CREATE_AN_AGENT = new StringId(2220, "An AgentConfig object cannot be modified after it has been used to create an Agent.");
  public static final StringId AgentConfigImpl_FAILED_READING_0 = new StringId(2221, "Failed reading {0}");

  public static final StringId AgentConfigImpl_HTTPBINDADDRESS_MUST_NOT_BE_NULL = new StringId(2223, "HttpBindAddress must not be null");
  public static final StringId AgentConfigImpl_0_MUST_BE_ZERO_OR_AN_INTEGER_BETWEEN_1_AND_2 = new StringId(2224, "{0} must be zero or an integer between {1} and {2}.");
  public static final StringId AgentConfigImpl_LOCATOR_WORKINGDIRECTORY_MUST_NOT_BE_NULL = new StringId(2225, "Locator WorkingDirectory must not be null");

  public static final StringId AgentConfigImpl_SPECIFIED_PROPERTIES_FILE_DOES_NOT_EXIST_0 = new StringId(2229, "Specified properties file does not exist:  {0}");
  public static final StringId AgentConfigImpl_UNKNOWN_CONFIG_PROPERTY_0 = new StringId(2230, "Unknown config property:  {0}");
  public static final StringId AgentImpl_AGENTCONFIG_MUST_NOT_BE_NULL = new StringId(2231, "AgentConfig must not be null");
  public static final StringId AgentImpl_SNMPDIRECTORY_DOES_NOT_EXIST = new StringId(2232, "snmp-directory does not exist");
  public static final StringId AgentImpl_SNMPDIRECTORY_MUST_BE_SPECIFIED_BECAUSE_SNMP_IS_ENABLED = new StringId(2233, "snmp-directory must be specified because SNMP is enabled");
  public static final StringId AgentImpl_THE_DIRECTORY_0_DOES_NOT_EXIST = new StringId(2234, "The directory  {0}  does not exist.");
  public static final StringId AgentImpl_THE_FILE_0_IS_A_DIRECTORY = new StringId(2235, "The file  {0}  is a directory.");

  public static final StringId AgentLauncher_JMX_AGENT_EXISTS_BUT_WAS_NOT_SHUTDOWN = new StringId(2237, "JMX Agent exists but was not shutdown.");

  public static final StringId AgentLauncher_THE_INPUT_WORKING_DIRECTORY_DOES_NOT_EXIST_0 = new StringId(2239, "The input working directory does not exist:  {0}");
  public static final StringId AgentLauncher_UNKNOWN_ARGUMENT_0 = new StringId(2240, "Unknown argument:  {0}");
  public static final StringId AlertLevel_THERE_IS_NO_ALERT_LEVEL_0 = new StringId(2241, "There is no alert level \"{0}\"");
  public static final StringId AlertLevel_UNKNOWN_ALERT_SEVERITY_0 = new StringId(2242, "Unknown alert severity:  {0}");
  public static final StringId ArchiveSplitter_ARCHIVE_VERSION_0_IS_NO_LONGER_SUPPORTED = new StringId(2243, "Archive version:  {0}  is no longer supported.");
  public static final StringId ArchiveSplitter_UNEXPECTED_TOKEN_BYTE_VALUE_0 = new StringId(2244, "Unexpected token byte value:  {0}");
  public static final StringId ArchiveSplitter_UNEXPECTED_TYPECODE_VALUE_0 = new StringId(2245, "unexpected typeCode value  {0}");
  public static final StringId ArchiveSplitter_UNSUPPORTED_ARCHIVE_VERSION_0_THE_SUPPORTED_VERSION_IS_1 = new StringId(2246, "Unsupported archive version:  {0} .  The supported version is:  {1} .");
  public static final StringId Assert_GET_STACK_TRACE = new StringId(2247, "get Stack trace");
  public static final StringId Atomic50StatisticsImpl_ATOMICS_DO_NOT_SUPPORT_DOUBLE_STATS = new StringId(2248, "Atomics do not support double stats");
  public static final StringId Atomic50StatisticsImpl_DIRECT_ACCESS_NOT_ON_ATOMIC50 = new StringId(2249, "direct access not on Atomic50");
  public static final StringId Atomic50StatisticsImpl_DOUBLE_STATS_NOT_ON_ATOMIC50 = new StringId(2250, "double stats not on Atomic50");
  public static final StringId AttributeDescriptor_FIELD_0_IN_CLASS_1_IS_NOT_ACCESSIBLE_TO_THE_QUERY_PROCESSOR = new StringId(2251, "Field '' {0} '' in class '' {1} '' is not accessible to the query processor");
  public static final StringId AttributeDescriptor_METHOD_0_IN_CLASS_1_IS_NOT_ACCESSIBLE_TO_THE_QUERY_PROCESSOR = new StringId(2252, "Method '' {0} '' in class '' {1} '' is not accessible to the query processor");
  public static final StringId AttributeDescriptor_NO_PUBLIC_ATTRIBUTE_NAMED_0_WAS_FOUND_IN_CLASS_1 = new StringId(2253, "No public attribute named '' {0} '' was found in class  {1}");

  public static final StringId AvailablePort_UNKNOWN_PROTOCOL_0 = new StringId(2258, "Unknown protocol:  {0}");

  public static final StringId BaseCommand_UNKNOWN_RESULT_TYPE_0 = new StringId(2269, "Unknown result type:  {0}");

  public static final StringId CacheServerImpl_A_CACHE_SERVERS_CONFIGURATION_CANNOT_BE_CHANGED_ONCE_IT_IS_RUNNING = new StringId(2275, "A cache server''s configuration cannot be changed once it is running.");

  public static final StringId BucketAdvisor_CANNOT_CHANGE_FROM_0_TO_1 = new StringId(2285, "Cannot change from  {0}  to  {1}");
  public static final StringId BucketRegion_THIS_SHOULD_NEVER_BE_CALLED_ON_0 = new StringId(2286, "This should never be called on  {0}");
  public static final StringId BucketSizeMessage_FAILED_SENDING_0 = new StringId(2287, "Failed sending < {0} >");
  public static final StringId BucketSizeMessage_NO_DATASTORE_IN_0 = new StringId(2288, "no datastore in  {0}");

  public static final StringId CacheClientNotifier_CACHECLIENTPROXY_FOR_THIS_CLIENT_IS_NO_LONGER_ON_THE_SERVER_SO_REGISTERINTEREST_OPERATION_IS_UNSUCCESSFUL = new StringId(2291, "CacheClientProxy for this client is no longer on the server , so registerInterest operation is unsuccessful");
  public static final StringId CacheClientNotifier_CLIENTPROXYMEMBERSHIPID_OBJECT_COULD_NOT_BE_CREATED_EXCEPTION_OCCURRED_WAS_0 = new StringId(2292, "ClientProxyMembershipID object could not be created. Exception occurred was {0}");
  public static final StringId CacheClientNotifier_EXCEPTION_OCCURRED_WHILE_TRYING_TO_REGISTER_INTEREST_DUE_TO_0 = new StringId(2293, "Exception occurred while trying to register interest due to :  {0}");
  public static final StringId CacheClientNotifier_THE_CACHE_CLIENT_NOTIFIER_DOES_NOT_SUPPORT_OPERATIONS_OF_TYPE_0 = new StringId(2294, "The cache client notifier does not support operations of type  {0}");
  public static final StringId CacheClientProxy_CLASS_0_COULD_NOT_BE_INSTANTIATED = new StringId(2295, "Class  {0}  could not be instantiated.");
  public static final StringId CacheClientProxy_CLASS_0_NOT_FOUND_IN_CLASSPATH = new StringId(2296, "Class  {0}  not found in classpath.");
  public static final StringId CacheClientProxy_EXCEPTION_OCCURRED_WHILE_TRYING_TO_CREATE_A_MESSAGE_QUEUE = new StringId(2297, "Exception occurred while trying to create a message queue.");
  public static final StringId GatewayEventRemoteDispatcher_0_COULD_NOT_CONNECT_1 = new StringId(2298, "{0} : Could not connect. {1}");

  public static final StringId CacheCollector_UNABLE_TO_MIX_REGION_AND_ENTRY_SNAPSHOTS_IN_CACHECOLLECTOR = new StringId(2300, "Unable to mix region and entry snapshots in CacheCollector.");
  public static final StringId CacheCreation_ATTRIBUTES_FOR_0_DO_NOT_MATCH = new StringId(2301, "Attributes for  {0}  do not match");

  public static final StringId CacheCreation_CACHESERVERS_SIZE = new StringId(2304, "cacheServers size");
  public static final StringId CacheCreation_CACHE_SERVER_0_NOT_FOUND = new StringId(2305, "cache server {0} not found");
  public static final StringId CacheCreation_NAMEDATTRIBUTES_SIZE = new StringId(2306, "namedAttributes size");
  public static final StringId CacheCreation_NO_ATTRIBUTES_FOR_0 = new StringId(2307, "No attributes for  {0}");
  public static final StringId CacheCreation_NO_ROOT_0 = new StringId(2308, "no root  {0}");
  public static final StringId CacheCreation_REGIONS_DIFFER = new StringId(2309, "regions differ");
  public static final StringId CacheCreation_ROOTS_SIZE = new StringId(2310, "roots size");
  public static final StringId CacheCreation_SAMECONFIG = new StringId(2311, "!sameConfig");
  public static final StringId CacheCreation_THE_MESSAGESYNCINTERVAL_PROPERTY_FOR_CACHE_CANNOT_BE_NEGATIVE = new StringId(2312, "The ''message-sync-interval'' property for cache cannot be negative");
  public static final StringId CacheCreation_TXLISTENER = new StringId(2313, "txListener");
  public static final StringId CacheDisplay_INVALID_INSPECTIONTYPE_PASSED_TO_CACHEDISPLAYGETCACHEDOBJECTDISPLAY = new StringId(2314, "Invalid inspectionType passed to CacheDisplay.getCachedObjectDisplay");
  public static final StringId CacheDistributionAdvisor_ILLEGAL_REGION_CONFIGURATION_FOR_MEMBERS_0 = new StringId(2315, "Illegal Region Configuration for members:  {0}");
  public static final StringId CacheFactory_A_CACHE_HAS_NOT_YET_BEEN_CREATED = new StringId(2316, "A cache has not yet been created.");
  public static final StringId CacheFactory_A_CACHE_HAS_NOT_YET_BEEN_CREATED_FOR_THE_GIVEN_DISTRIBUTED_SYSTEM = new StringId(2317, "A cache has not yet been created for the given distributed system.");
  public static final StringId CacheFactory_THE_CACHE_HAS_BEEN_CLOSED = new StringId(2318, "The cache has been closed.");

  public static final StringId CacheServerLauncher_A_0_IS_ALREADY_RUNNING_IN_DIRECTORY_1_2 = new StringId(2323, "A {0} is already running in directory \"{1}\"\n {2}");

  public static final StringId CacheServerLauncher_INTERNAL_ERROR_SHOULDNT_REACH_HERE = new StringId(2325, "internal error.. should not reach here.");
  public static final StringId CacheServerLauncher_NO_AVAILABLE_STATUS = new StringId(2326, "No available status.");
  public static final StringId CacheServerLauncher_THE_INPUT_WORKING_DIRECTORY_DOES_NOT_EXIST_0 = new StringId(2327, "The input working directory does not exist:  {0}");
  public static final StringId CacheServerLauncher_UNKNOWN_ARGUMENT_0 = new StringId(2328, "Unknown argument:  {0}");
  public static final StringId CacheTransactionManagerCreation_GETTING_A_TRANSACTIONID_NOT_SUPPORTED = new StringId(2329, "Getting a TransactionId not supported");
  public static final StringId CacheTransactionManagerCreation_MORE_THAN_ONE_TRANSACTION_LISTENER_EXISTS = new StringId(2330, "more than one transaction listener exists");
  public static final StringId CacheTransactionManagerCreation_TRANSACTIONS_NOT_SUPPORTED = new StringId(2331, "Transactions not supported");

  public static final StringId CacheXmlGenerator_AN_EXCEPTION_WAS_THROWN_WHILE_GENERATING_XML = new StringId(2333, "An Exception was thrown while generating XML.");
  public static final StringId CacheXmlGenerator_UNKNOWN_DATA_POLICY_0 = new StringId(2334, "Unknown data policy:  {0}");
  public static final StringId CacheXmlGenerator_UNKNOWN_EXPIRATIONACTION_0 = new StringId(2335, "Unknown ExpirationAction:  {0}");

  public static final StringId CacheXmlGenerator_UNKNOWN_INTERESTPOLICY_0 = new StringId(2337, "Unknown InterestPolicy:  {0}");
  public static final StringId CacheXmlGenerator_UNKNOWN_MIRROR_TYPE_0 = new StringId(2338, "Unknown mirror type:  {0}");
  public static final StringId CacheXmlGenerator_UNKNOWN_SCOPE_0 = new StringId(2339, "Unknown scope:  {0}");
  public static final StringId CacheXmlParser_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_1 = new StringId(2340, "{0}  must be defined in the context of  {1}");
  public static final StringId CacheXmlParser_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_REGIONATTRIBUTES = new StringId(2341, "{0}  must be defined in the context of region-attributes.");
  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_CACHELISTENER = new StringId(2342, "A  {0}  is not an instance of a CacheListener.");
  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_CACHELOADER = new StringId(2343, "A  {0}  is not an instance of a CacheLoader.");
  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_CACHEWRITER = new StringId(2344, "A  {0}  is not an instance of a CacheWriter.");

  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_OBJECTSIZER = new StringId(2346, "A  {0}  is not an instance of a ObjectSizer.");
  public static final StringId CacheXmlParser_A_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_REGIONATTRIBUTES = new StringId(2347, "A  {0}  must be defined in the context of region-attributes.");
  public static final StringId CacheXmlParser_A_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_REGIONATTRIBUTES_OR_1 = new StringId(2348, "A  {0}  must be defined in the context of region-attributes or  {1}");
  public static final StringId CacheXmlParser_A_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_REGIONATTRIBUTES_OR_PARTITIONATTRIBUTES = new StringId(2349, "A  {0}  must be defined in the context of region-attributes or partition-attributes.");
  public static final StringId CacheXmlParser_A_CACHEEXCEPTION_WAS_THROWN_WHILE_PARSING_XML = new StringId(2350, "A CacheException was thrown while parsing XML.");
  public static final StringId CacheXmlParser_A_CACHELOADER_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_REGIONATTRIBUTES = new StringId(2351, "A cache-loader must be defined in the context of region-attributes.");
  public static final StringId CacheXmlParser_CACHEXMLPARSERSTARTFUNCTIONALINDEXINDEX_CREATION_ATTRIBUTE_NOT_CORRECTLY_SPECIFIED = new StringId(2352, "CacheXmlParser::startFunctionalIndex:Index creation attribute not correctly specified.");
  public static final StringId CacheXmlParser_CACHEXMLPARSERSTARTPRIMARYKEYINDEXPRIMARYKEY_INDEX_CREATION_FIELD_IS_NULL = new StringId(2353, "CacheXmlParser::startPrimaryKeyIndex:Primary-Key Index creation field is null.");
  public static final StringId CacheXmlParser_CLASS_0_IS_NOT_AN_INSTANCE_OF_DECLARABLE = new StringId(2354, "Class \"{0}\" is not an instance of Declarable.");
  public static final StringId CacheXmlParser_NO_CACHE_ELEMENT_SPECIFIED = new StringId(2355, "No cache element specified.");
  public static final StringId CacheXmlParser_ONLY_A_PARAMETER_IS_ALLOWED_IN_THE_CONTEXT_OF_0 = new StringId(2356, "Only a parameter is allowed in the context of  {0}");

  public static final StringId CacheXmlParser_UNKNOWN_DATA_POLICY_0 = new StringId(2358, "Unknown data policy:  {0}");
  public static final StringId CacheXmlParser_UNKNOWN_EXPIRATION_ACTION_0 = new StringId(2359, "Unknown expiration action:  {0}");

  public static final StringId CacheXmlParser_UNKNOWN_INTERESTPOLICY_0 = new StringId(2361, "Unknown interest-policy:  {0}");
  public static final StringId CacheXmlParser_UNKNOWN_MIRROR_TYPE_0 = new StringId(2362, "Unknown mirror type:  {0}");
  public static final StringId CacheXmlParser_UNKNOWN_SCOPE_0 = new StringId(2363, "Unknown scope:  {0}");
  public static final StringId CacheXmlParser_UNKNOWN_XML_ELEMENT_0 = new StringId(2364, "Unknown XML element \"{0}\"");
  public static final StringId CacheXml_DTD_NOT_FOUND_0 = new StringId(2365, "DTD not found:  {0}");

  public static final StringId CachedDeserializableFactory_COULD_NOT_CALCULATE_SIZE_OF_OBJECT = new StringId(2367, "Could not calculate size of object");

  public static final StringId CancellationRegistry_NULL_CONSOLE = new StringId(2371, "Null Console!");
  public static final StringId ChunkedMessage_CHUNK_READ_ERROR_CONNECTION_RESET = new StringId(2372, "Chunk read error (connection reset)");
  public static final StringId ChunkedMessage_DEAD_CONNECTION = new StringId(2373, "Dead Connection");
  public static final StringId ChunkedMessage_INVALID_MESSAGE_TYPE_0_WHILE_READING_HEADER = new StringId(2374, "Invalid message type  {0}  while reading header");
  public static final StringId ClientHealthMonitor_STACK_TRACE_0 = new StringId(2375, "stack trace {0}");

  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_TRANSACTION_WRITER = new StringId(2377, "A  {0}  is not an instance of a TransactionWriter.");
  public static final StringId ClientProxyMembershipID_ATTEMPTING_TO_HANDSHAKE_WITH_CACHESERVER_BEFORE_CREATING_DISTRIBUTEDSYSTEM_AND_CACHE = new StringId(2378, "Attempting to handshake with CacheServer before creating DistributedSystem and Cache.");
  public static final StringId ClientProxyMembershipID_HANDSHAKE_IDENTITY_LENGTH_IS_TOO_BIG = new StringId(2379, "HandShake identity length is too big");
  public static final StringId ClientProxyMembershipID_UNABLE_TO_SERIALIZE_IDENTITY = new StringId(2380, "Unable to serialize identity");
  public static final StringId ClientProxyMembershipID_UNEXPECTED_EOF_REACHED_DISTRIBUTED_MEMBERSHIPID_COULD_NOT_BE_READ = new StringId(2381, "Unexpected EOF reached. Distributed MembershipID could not be read");
  public static final StringId ClientProxyMembershipID_UNEXPECTED_EOF_REACHED_UNIQUE_ID_COULD_NOT_BE_READ = new StringId(2382, "Unexpected EOF reached. Unique ID could not be read");
  public static final StringId CacheCreation_ATTRIBUTES_FOR_DISKSTORE_0_DO_NOT_MATCH = new StringId(2383, "Attributes for disk store {0} do not match");
  public static final StringId Collaboration_COLLABORATION_HAS_NO_CURRENT_TOPIC = new StringId(2384, "Collaboration has no current topic");
  public static final StringId Collaboration_NOT_IMPLEMENTED = new StringId(2385, "Not implemented");
  public static final StringId Collaboration_TOPIC_MUST_BE_SPECIFIED = new StringId(2386, "Topic must be specified");
  public static final StringId CompiledFunction_UNSUPPORTED_FUNCTION_WAS_USED_IN_THE_QUERY = new StringId(2387, "UnSupported function was used in the query");
  public static final StringId CompiledIn_IN_OPERATOR_CHECK_FOR_NULL_IN_PRIMITIVE_ARRAY = new StringId(2388, "IN operator, check for null IN primitive array");
  public static final StringId CompiledIn_OPERAND_OF_IN_CANNOT_BE_INTERPRETED_AS_A_COLLECTION_IS_INSTANCE_OF_0 = new StringId(2389, "Operand of IN cannot be interpreted as a Collection. Is instance of  {0}");
  public static final StringId CompiledIndexOperation_INDEX_EXPRESSION_MUST_BE_AN_INTEGER_FOR_LISTS_OR_ARRAYS = new StringId(2390, "index expression must be an integer for lists or arrays");
  public static final StringId CompiledIndexOperation_INDEX_EXPRESSION_NOT_SUPPORTED_ON_OBJECTS_OF_TYPE_0 = new StringId(2391, "index expression not supported on objects of type  {0}");
  public static final StringId CompiledIteratorDef_AN_ITERATOR_DEFINITION_MUST_BE_A_COLLECTION_TYPE_NOT_A_0 = new StringId(2392, "An iterator definition must be a collection type, not a  {0}");
  public static final StringId CompiledIteratorDef_EXCEPTION_IN_EVALUATING_THE_COLLECTION_EXPRESSION_IN_GETRUNTIMEITERATOR_EVEN_THOUGH_THE_COLLECTION_IS_INDEPENDENT_OF_ANY_RUNTIMEITERATOR = new StringId(2393, "Exception in evaluating the Collection Expression in getRuntimeIterator() even though the Collection is independent of any RuntimeIterator");
  public static final StringId CompiledIteratorDef_NOT_TO_BE_EVALUATED_DIRECTLY = new StringId(2394, "Not to be evaluated directly");

  public static final StringId CompiledJunction_ANDOR_OPERANDS_MUST_BE_OF_TYPE_BOOLEAN_NOT_TYPE_0 = new StringId(2396, "AND/OR operands must be of type boolean, not type '' {0} ''");
  public static final StringId CompiledJunction_INTERMEDIATERESULTS_CAN_NOT_BE_NULL = new StringId(2397, "intermediateResults can not be null");
  public static final StringId CompiledJunction_LITERAL_ANDLITERAL_OR_OPERANDS_MUST_BE_OF_TYPE_BOOLEAN_NOT_TYPE_0 = new StringId(2398, "LITERAL_and/LITERAL_or operands must be of type boolean, not type '' {0} ''");
  public static final StringId CompiledNegation_0_CANNOT_BE_NEGATED = new StringId(2399, "{0}  cannot be negated");
  public static final StringId CompiledOperation_COULD_NOT_RESOLVE_METHOD_NAMED_0 = new StringId(2400, "Could not resolve method named '' {0} ''");
  public static final StringId CompiledRegion_REGION_NOT_FOUND_0 = new StringId(2401, "Region not found:  {0}");

  public static final StringId CompiledSelect_RESULT_SET_DOES_NOT_MATCH_WITH_ITERATOR_DEFINITIONS_IN_FROM_CLAUSE = new StringId(2403, "Result Set does not match with iterator definitions in from clause");
  public static final StringId CompiledSelect_THE_WHERE_CLAUSE_WAS_TYPE_0_INSTEAD_OF_BOOLEAN = new StringId(2404, "The WHERE clause was type '' {0} '' instead of boolean");
  public static final StringId CompiledUnaryMinus_0_CANNOT_BE_UNARY_MINUS = new StringId(2405, "{0}  cannot be unary minus");
  public static final StringId DiskRegion_DISK_IS_FULL_COMPACTION_IS_DISABLED_NO_SPACE_CAN_BE_CREATED = new StringId(2406, "Disk is full and compaction is disabled. No space can be created");

  public static final StringId CompoundEntrySnapshot_ALL_SNAPSHOTS_IN_A_COMPOUND_SNAPSHOT_MUST_HAVE_THE_SAME_NAME = new StringId(2408, "All snapshots in a compound snapshot must have the same name");
  public static final StringId CompoundRegionSnapshot_ALL_SNAPSHOTS_IN_A_COMPOUND_SNAPSHOT_MUST_HAVE_THE_SAME_NAME = new StringId(2409, "All snapshots in a compound snapshot must have the same name");
  public static final StringId ConfigurationParameterImpl_0_IS_NOT_A_MODIFIABLE_CONFIGURATION_PARAMETER = new StringId(2410, "{0}  is not a modifiable configuration parameter");
  public static final StringId ConfigurationParameterImpl_CONFIGURATIONPARAMETER_DESCRIPTION_MUST_BE_SPECIFIED = new StringId(2411, "ConfigurationParameter description must be specified");
  public static final StringId ConfigurationParameterImpl_CONFIGURATIONPARAMETER_NAME_MUST_BE_SPECIFIED = new StringId(2412, "ConfigurationParameter name must be specified");
  public static final StringId ConfigurationParameterImpl_SETTING_ARRAY_VALUE_FROM_DELIMITED_STRING_IS_NOT_SUPPORTED = new StringId(2413, "Setting array value from delimited string is not supported");
  public static final StringId ConfigurationParameterImpl_UNABLE_TO_SET_0_TO_NULL_VALUE = new StringId(2414, "Unable to set  {0}  to null value");
  public static final StringId ConfigurationParameterImpl_UNABLE_TO_SET_TYPE_0_WITH_TYPE_1 = new StringId(2415, "Unable to set type  {0}  with type  {1}");
  public static final StringId ConfigurationParameterJmxImpl_REMOTE_MUTATION_OF_CONFIGURATIONPARAMETER_IS_CURRENTLY_UNSUPPORTED = new StringId(2416, "Remote mutation of ConfigurationParameter is currently unsupported.");

  public static final StringId ConnectionTable_CONNECTION_TABLE_IS_CLOSED = new StringId(2427, "Connection table is closed");
  public static final StringId Connection_CONNECTIONTABLE_IS_NULL = new StringId(2428, "ConnectionTable is null.");
  public static final StringId Connection_CONNECTION_HANDSHAKE_WITH_0_TIMED_OUT_AFTER_WAITING_1_MILLISECONDS = new StringId(2429, "Connection handshake with  {0}  timed out after waiting  {1}  milliseconds.");
  public static final StringId Connection_CONNECTION_IS_CLOSED = new StringId(2430, "connection is closed");
  public static final StringId Connection_DETECTED_OLD_VERSION_PRE_501_OF_GEMFIRE_OR_NONGEMFIRE_DURING_HANDSHAKE_DUE_TO_INITIAL_BYTE_BEING_0 = new StringId(2431, "Detected old version (pre 5.0.1) of GemFire or non-GemFire during handshake due to initial byte being  {0}");
  public static final StringId Connection_DETECTED_WRONG_VERSION_OF_GEMFIRE_PRODUCT_DURING_HANDSHAKE_EXPECTED_0_BUT_FOUND_1 = new StringId(2432, "Detected wrong version of GemFire product during handshake. Expected  {0}  but found  {1}");
  public static final StringId Connection_FORCED_DISCONNECT_SENT_TO_0 = new StringId(2433, "Forced disconnect sent to  {0}");
  public static final StringId Connection_HANDSHAKE_FAILED = new StringId(2434, "Handshake failed");
  public static final StringId Connection_MEMBER_LEFT_THE_GROUP = new StringId(2435, "Member {0}  left the group");
  public static final StringId Connection_NOT_CONNECTED_TO_0 = new StringId(2436, "Not connected to  {0}");
  public static final StringId Connection_NULL_CONNECTIONTABLE = new StringId(2437, "Null ConnectionTable");
  public static final StringId Connection_SOCKET_HAS_BEEN_CLOSED = new StringId(2438, "socket has been closed");
  public static final StringId Connection_TCP_MESSAGE_EXCEEDED_MAX_SIZE_OF_0 = new StringId(2439, "tcp message exceeded max size of  {0}");
  public static final StringId Connection_UNABLE_TO_READ_DIRECT_ACK_BECAUSE_0 = new StringId(2440, "Unable to read direct ack because:  {0}");
  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_COMPRESSOR = new StringId(2441, "A  {0}  is not an instance of a Compressor.");
  public static final StringId ContainsKeyValueMessage_ENOUNTERED_PRLOCALLYDESTROYEDEXCEPTION = new StringId(2442, "Enountered PRLocallyDestroyedException");
  public static final StringId ContainsKeyValueMessage_FAILED_SENDING_0 = new StringId(2443, "Failed sending < {0} >");
  public static final StringId ContainsKeyValueMessage_NO_RETURN_VALUE_RECEIVED = new StringId(2444, "no return value received");
  public static final StringId ContainsKeyValueMessage_PARTITIONED_REGION_0_ON_1_IS_NOT_CONFIGURED_TO_STORE_DATA = new StringId(2445, "Partitioned Region  {0}  on  {1}  is not configured to store data");
  public static final StringId ContextImpl_ADDTOENVIRONMENTSTRING_KEY_OBJECT_VALUE_IS_NOT_IMPLEMENTED = new StringId(2446, "addToEnvironment(String key, Object value) is not implemented");
  public static final StringId ContextImpl_CAN_NOT_DESTROY_NONEMPTY_CONTEXT = new StringId(2447, "Can not destroy non-empty Context!");
  public static final StringId ContextImpl_CAN_NOT_FIND_0 = new StringId(2448, "Can not find  {0}");
  public static final StringId ContextImpl_CAN_NOT_INVOKE_OPERATIONS_ON_DESTROYED_CONTEXT = new StringId(2449, "Can not invoke operations on destroyed context!");
  public static final StringId ContextImpl_EMPTY_INTERMEDIATE_COMPONENTS_ARE_NOT_SUPPORTED = new StringId(2450, "Empty intermediate components are not supported!");
  public static final StringId ContextImpl_EXPECTED_CONTEXTIMPL_BUT_FOUND_0 = new StringId(2451, "Expected ContextImpl but found  {0}");
  public static final StringId ContextImpl_EXPECTED_CONTEXT_BUT_FOUND_0 = new StringId(2452, "Expected Context but found  {0}");
  public static final StringId ContextImpl_GETENVIRONMENT_IS_NOT_IMPLEMENTED = new StringId(2453, "getEnvironment() is not implemented");
  public static final StringId ContextImpl_GETNAMEINNAMESPACE_IS_NOT_IMPLEMENTED = new StringId(2454, "getNameInNamespace() is not implemented");
  public static final StringId ContextImpl_LOOKUPLINKNAME_NAME_IS_NOT_IMPLEMENTED = new StringId(2455, "lookupLink(Name name) is not implemented");
  public static final StringId ContextImpl_LOOKUPLINKSTRING_NAME_IS_NOT_IMPLEMENTED = new StringId(2456, "lookupLink(String name) is not implemented");
  public static final StringId ContextImpl_MULTIPLE_NAME_SYSTEMS_ARE_NOT_SUPPORTED = new StringId(2457, "Multiple name systems are not supported!");
  public static final StringId ContextImpl_NAME_0_IS_ALREADY_BOUND = new StringId(2458, "Name  {0}  is already bound!");
  public static final StringId ContextImpl_NAME_0_NOT_FOUND = new StringId(2459, "Name  {0}  not found");
  public static final StringId ContextImpl_NAME_0_NOT_FOUND_IN_THE_CONTEXT = new StringId(2460, "Name  {0} not found in the context!");
  public static final StringId ContextImpl_NAME_CAN_NOT_BE_EMPTY = new StringId(2461, "Name can not be empty!");
  public static final StringId ContextImpl_REMOVEFROMENVIRONMENTSTRING_KEY_IS_NOT_IMPLEMENTED = new StringId(2462, "removeFromEnvironment(String key) is not implemented");
  public static final StringId ContextImpl_RENAMENAME_NAME1_NAME_NAME2_IS_NOT_IMPLEMENTED = new StringId(2463, "rename(Name name1, Name name2) is not implemented");
  public static final StringId ContextImpl_RENAMESTRING_NAME1_STRING_NAME2_IS_NOT_IMPLEMENTED = new StringId(2464, "rename(String name1, String name2) is not implemented");

  public static final StringId CopyHelper_CLONE_FAILED = new StringId(2466, "Clone failed.");
  public static final StringId CopyHelper_COPY_FAILED_ON_INSTANCE_OF_0 = new StringId(2467, "Copy failed on instance of  {0}");
  public static final StringId CqAttributesFactory_ADDCQLISTENER_PARAMETER_WAS_NULL = new StringId(2468, "addCqListener parameter was null");
  public static final StringId CqAttributesFactory_INITCQLISTENERS_PARAMETER_HAD_A_NULL_ELEMENT = new StringId(2469, "initCqListeners parameter had a null element");
  public static final StringId CqAttributesFactory_MORE_THAN_ONE_CQLISTENER_EXISTS = new StringId(2470, "More than one Cqlistener exists.");
  public static final StringId CqAttributesFactory_REMOVECQLISTENER_PARAMETER_WAS_NULL = new StringId(2471, "removeCqListener parameter was null");
  public static final StringId CqAttributesMutatorImpl_NOT_YET_SUPPORTED = new StringId(2472, "Not yet supported.");
  public static final StringId CqConflatable_SETLATESTVALUE_SHOULD_NOT_BE_USED = new StringId(2473, "setLatestValue should not be used");
  public static final StringId CqListenerImpl_NOT_YET_SUPPORTED = new StringId(2474, "Not yet supported.");
  public static final StringId ExecuteFunction_IOEXCEPTION_WHILE_SENDING_RESULT_CHUNK = new StringId(2475, "IOException while sending the result chunk to client");
  public static final StringId CqService_CACHE_IS_NULL = new StringId(2476, "cache is null");
  public static final StringId DLockGrantor_DLOCKGRANTOR_OPERATION_ONLY_ALLOWED_WHEN_INITIALIZING_NOT_0 = new StringId(2477, "DLockGrantor operation only allowed when initializing, not  {0}");
  public static final StringId DLockGrantor_GRANTOR_IS_DESTROYED = new StringId(2478, "Grantor is destroyed");

  public static final StringId DLockGrantor_UNKNOWN_STATE_FOR_GRANTOR_0 = new StringId(2480, "Unknown state for grantor:  {0}");
  public static final StringId DLockService_0_ATTEMPTED_TO_REENTER_NONREENTRANT_LOCK_1 = new StringId(2481, "{0}  attempted to reenter non-reentrant lock {1}");
  public static final StringId DLockService_0_HAS_BEEN_DESTROYED = new StringId(2482, "{0}  has been destroyed");
  public static final StringId DLockService_ATTEMPTING_TO_UNLOCK_0_1_BUT_THIS_THREAD_DOESNT_OWN_THE_LOCK = new StringId(2483, "Attempting to unlock  {0}  :  {1} , but this thread does not own the lock.");
  public static final StringId DLockService_ATTEMPTING_TO_UNLOCK_0_1_BUT_THIS_THREAD_DOESNT_OWN_THE_LOCK_2 = new StringId(2484, "Attempting to unlock  {0}  :  {1} , but this thread does not own the lock.  {2}");
  public static final StringId DLockService_CURRENT_THREAD_HAS_ALREADY_LOCKED_ENTIRE_SERVICE = new StringId(2485, "Current thread has already locked entire service");
  public static final StringId DLockService_KEYIFFAILED_MUST_HAVE_A_LENGTH_OF_ONE_OR_GREATER = new StringId(2486, "keyIfFailed must have a length of one or greater");
  public static final StringId DLockService_LOCK_SERVICE_NAME_MUST_NOT_BE_NULL_OR_EMPTY = new StringId(2487, "Lock service name must not be null or empty");
  public static final StringId DLockService_SERVICE_NAMED_0_ALREADY_CREATED = new StringId(2488, "Service named {0} already created");
  public static final StringId DLockService_SERVICE_NAMED_0_IS_NOT_VALID = new StringId(2489, "Service named ''{0}'' is not valid");
  public static final StringId DLockService_SERVICE_NAMED_0_IS_RESERVED_FOR_INTERNAL_USE_ONLY = new StringId(2490, "Service named {0} is reserved for internal use only");
  public static final StringId DLockService_SERVICE_NAMED_0_NOT_CREATED = new StringId(2491, "Service named {0} not created");
  public static final StringId ExecuteFunction_IOEXCEPTION_WHILE_SENDING_LAST_CHUNK = new StringId(2492, "IOException while sending the last chunk to client");
  public static final StringId DLockToken_THIS_THREADS_LEASE_EXPIRED_FOR_THIS_LOCK = new StringId(2493, "This thread''s lease expired for this lock");
  public static final StringId DataPolicy_ONLY_0_DATAPOLICIES_MAY_BE_DEFINED = new StringId(2494, "Only {0} DataPolicies may be defined");
  public static final StringId DataPolicy_ORDINAL_0_IS_ALREADY_DEFINED_BY_1 = new StringId(2495, "Ordinal {0} is already defined by  {1}");
  public static final StringId DataSerializer_0_DOES_NOT_EXTEND_DATASERIALIZER = new StringId(2496, "{0}  does not extend DataSerializer.");
  public static final StringId DataSerializer_0_IS_NOT_DATASERIALIZABLE_AND_JAVA_SERIALIZATION_IS_DISALLOWED = new StringId(2497, "{0}  is not DataSerializable and Java Serialization is disallowed");
  public static final StringId DataSerializer_COULD_NOT_CREATE_AN_INSTANCE_OF_0 = new StringId(2498, "Could not create an instance of  {0} .");
  public static final StringId DataSerializer_COULD_NOT_DESERIALIZE_AN_INSTANCE_OF_0 = new StringId(2499, "Could not deserialize an instance of  {0}");

  public static final StringId DataSerializer_PROBELM_WHILE_SERIALIZING = new StringId(2502, "Probelm while serializing.");
  public static final StringId DataSerializer_REGION_0_COULD_NOT_BE_FOUND_WHILE_READING_A_DATASERIALIZER_STREAM = new StringId(2503, "Region '' {0} '' could not be found while reading a DataSerializer stream");
  public static final StringId DataSerializer_SERIALIZER_0_A_1_SAID_THAT_IT_COULD_SERIALIZE_AN_INSTANCE_OF_2_BUT_ITS_TODATA_METHOD_RETURNED_FALSE = new StringId(2504, "Serializer  {0}  (a  {1} ) said that it could serialize an instance of  {2} , but its toData() method returned false.");

  public static final StringId DataSerializer_SERIALIZER_0_IS_NOT_REGISTERED = new StringId(2506, "Serializer with Id {0} is not registered");

  public static final StringId DataSerializer_UNKNOWN_TIMEUNIT_TYPE_0 = new StringId(2510, "Unknown TimeUnit type:  {0}");

  public static final StringId DataSerializer_WHILE_READING_AN_INETADDRESS = new StringId(2512, "While reading an InetAddress");

  public static final StringId DefaultQueryService_CACHE_MUST_NOT_BE_NULL = new StringId(2514, "cache must not be null");
  public static final StringId DefaultQueryService_CQNAME_MUST_NOT_BE_NULL = new StringId(2515, "cqName must not be null");
  public static final StringId DefaultQueryService_DEFAULTQUERYSERVICECREATEINDEXFIRST_ITERATOR_OF_INDEX_FROM_CLAUSE_DOES_NOT_EVALUATE_TO_A_REGION_PATH_THE_FROM_CLAUSE_USED_FOR_INDEX_CREATION_IS_0 = new StringId(2516, "DefaultQueryService::createIndex:First Iterator of Index >From Clause does not evaluate to a Region Path. The from clause used for Index creation is  {0}");
  public static final StringId DefaultQueryService_INDEX_CREATION_IS_NOT_SUPPORTED_FOR_REGIONS_WHICH_OVERFLOW_TO_DISK_THE_REGION_INVOLVED_IS_0 = new StringId(2517, "The specified index conditions are not supported for regions which overflow to disk. The region involved is  {0}");
  public static final StringId DefaultQueryService_REGION_0_NOT_FOUND_FROM_1 = new StringId(2518, "Region '' {0} '' not found: from  {1}");
  public static final StringId DefaultQueryService_THE_QUERY_STRING_MUST_NOT_BE_EMPTY = new StringId(2519, "The query string must not be empty");
  public static final StringId DefaultQueryService_THE_QUERY_STRING_MUST_NOT_BE_NULL = new StringId(2520, "The query string must not be null");
  public static final StringId DefaultQuery_A_QUERY_ON_A_PARTITIONED_REGION_0_MAY_NOT_REFERENCE_ANY_OTHER_REGION_1 = new StringId(2521, "A query on a Partitioned Region ( {0} ) may not reference any other region if query is NOT executed within a Function");
  public static final StringId DefaultQuery_NOT_YET_IMPLEMENTED = new StringId(2522, "not yet implemented");
  public static final StringId DefaultQuery_PARAMETERS_CANNOT_BE_NULL = new StringId(2523, "''parameters'' cannot be null");
  public static final StringId DefaultQuery_QUERY_MUST_BE_A_SIMPLE_SELECT_WHEN_REFERENCING_A_PARTITIONED_REGION = new StringId(2524, "query must be a simple select when referencing a Partitioned Region");
  public static final StringId DefaultQuery_REGION_NOT_FOUND_0 = new StringId(2525, "Region not found:  {0}");
  public static final StringId DefaultQuery_THE_WHERE_CLAUSE_CANNOT_REFER_TO_A_REGION_WHEN_QUERYING_ON_A_PARTITIONED_REGION = new StringId(2526, "The WHERE clause cannot refer to a region when querying on a Partitioned Region");
  public static final StringId DefaultQuery_WHEN_QUERYING_A_PARTITIONEDREGION_THE_FIRST_FROM_CLAUSE_ITERATOR_MUST_NOT_CONTAIN_A_SUBQUERY = new StringId(2527, "When querying a PartitionedRegion, the first FROM clause iterator must not contain a subquery");
  public static final StringId DefaultQuery_WHEN_QUERYING_A_PARTITIONED_REGION_THE_FROM_CLAUSE_ITERATORS_OTHER_THAN_THE_FIRST_ONE_MUST_NOT_REFERENCE_ANY_REGIONS = new StringId(2528, "When querying a Partitioned Region, the FROM clause iterators other than the first one must not reference any regions");
  public static final StringId DefaultQuery_WHEN_QUERYING_A_PARTITIONED_REGION_THE_ORDERBY_ATTRIBUTES_MUST_NOT_REFERENCE_ANY_REGIONS = new StringId(2529, "When querying a Partitioned Region, the order-by attributes must not reference any regions");
  public static final StringId DefaultQuery_WHEN_QUERYING_A_PARTITIONED_REGION_THE_PROJECTIONS_MUST_NOT_REFERENCE_ANY_REGIONS = new StringId(2530, "When querying a Partitioned Region, the projections must not reference any regions");
  public static final StringId DestroyMessage_FAILED_SENDING_0 = new StringId(2531, "Failed sending < {0} >");
  public static final StringId DirectChannel_COMMUNICATIONS_DISCONNECTED = new StringId(2532, "communications disconnected");
  public static final StringId DirectChannel_SHUNNING_0 = new StringId(2533, "Member is being shunned: {0}");
  public static final StringId DirectChannel_UNKNOWN_ERROR_SERIALIZING_MESSAGE = new StringId(2534, "Unknown error serializing message");
  public static final StringId DiskEntry_AN_IOEXCEPTION_WAS_THROWN_WHILE_SERIALIZING = new StringId(2535, "An IOException was thrown while serializing.");
  public static final StringId DiskEntry_DISK_REGION_IS_NULL = new StringId(2536, "Disk region is null");
  public static final StringId DiskEntry_ENTRYS_VALUE_SHOULD_NOT_BE_NULL = new StringId(2537, "Entry''s value should not be null.");
  public static final StringId DiskId_FOR_OVERFLOW_ONLY_MODE_THE_KEYID_SHOULD_NOT_BE_QUERIED = new StringId(2538, "For overflow only mode the keyID should not be queried");
  public static final StringId DiskId_FOR_OVERFLOW_ONLY_MODE_THE_KEYID_SHOULD_NOT_BE_SET = new StringId(2539, "For overflow only mode the keyID should not be set");
  public static final StringId DefaultQueryService_HASH_INDEX_CREATION_IS_NOT_SUPPORTED_FOR_MULTIPLE_ITERATORS = new StringId(2540, "Hash Index is not supported with from clause having multiple iterators(collections).");
  public static final StringId DefaultQueryService_HASH_INDEX_CREATION_IS_NOT_SUPPORTED_FOR_ASYNC_MAINTENANCE = new StringId(2541, "Hash index is currently not supported for regions with Asynchronous index maintenance.");
  public static final StringId DefaultQueryService_OFF_HEAP_INDEX_CREATION_IS_NOT_SUPPORTED_FOR_ASYNC_MAINTENANCE_THE_REGION_IS_0 = new StringId(2542, "Asynchronous index maintenance is currently not supported for off-heap regions. The off-heap region is {0}");

  public static final StringId DiskRegion_CANT_PUT_A_KEYVALUE_PAIR_WITH_ID_0 = new StringId(2545, "Cannot put a key/value pair with id  {0}");
  public static final StringId DiskRegion_CLEAR_OPERATION_ABORTING_THE_ONGOING_ENTRY_0_OPERATION_FOR_ENTRY_WITH_DISKID_1 = new StringId(2546, "Clear operation aborting the ongoing Entry {0} operation for Entry with DiskId = {1}");
  public static final StringId DiskRegion_CLEAR_OPERATION_ABORTING_THE_ONGOING_ENTRY_DESTRUCTION_OPERATION_FOR_ENTRY_WITH_DISKID_0 = new StringId(2547, "Clear operation aborting the ongoing Entry destruction operation for Entry with DiskId = {0}");
  public static final StringId DiskRegion_DATA_FOR_DISKENTRY_HAVING_DISKID_AS_0_COULD_NOT_BE_OBTAINED_FROM_DISK_A_CLEAR_OPERATION_MAY_HAVE_DELETED_THE_OPLOGS = new StringId(2548, "Data  for DiskEntry having DiskId as  {0}  could not be obtained from Disk. A clear operation may have deleted the oplogs");
  public static final StringId DiskRegion_ENTRY_HAS_BEEN_CLEARED_AND_IS_NOT_PRESENT_ON_DISK = new StringId(2549, "Entry has been cleared and is not present on disk");
  public static final StringId DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED = new StringId(2550, "The DiskRegion has been closed or destroyed");
  public static final StringId DiskWriteAttributesFactory_QUEUE_SIZE_SPECIFIED_HAS_TO_BE_A_NONNEGATIVE_NUMBER_AND_THE_VALUE_GIVEN_0_IS_NOT_ACCEPTABLE = new StringId(2551, "Queue size specified has to be a non-negative number and the value given {0} is not acceptable");
  public static final StringId DiskWriteAttributesFactory_MAXIMUM_OPLOG_SIZE_SPECIFIED_HAS_TO_BE_A_NONNEGATIVE_NUMBER_AND_THE_VALUE_GIVEN_0_IS_NOT_ACCEPTABLE = new StringId(2552, "Maximum Oplog size specified has to be a non-negative number and the value given  {0}  is not acceptable");
  public static final StringId DiskWriteAttributesFactory_TIME_INTERVAL_SPECIFIED_HAS_TO_BE_A_NONNEGATIVE_NUMBER_AND_THE_VALUE_GIVEN_0_IS_NOT_ACCEPTABLE = new StringId(2553, "Time Interval specified has to be a non-negative number and the value given  {0}  is not acceptable");
  public static final StringId DiskWriteAttributesImpl_0_HAS_TO_BE_A_VALID_NUMBER_AND_NOT_1 = new StringId(2554, "{0} has to be a valid number and not {1}");
  public static final StringId DiskWriteAttributesImpl_0_HAS_TO_BE_POSITIVE_NUMBER_AND_THE_VALUE_GIVEN_1_IS_NOT_ACCEPTABLE = new StringId(2555, "{0} has to be positive number and the value given {1} is not acceptable");
  public static final StringId DiskWriteAttributesImpl_0_PROPERTY_HAS_TO_BE_TRUE_OR_FALSE_OR_NULL_AND_CANNOT_BE_1 = new StringId(2556, "{0} property has to be \"true\" or \"false\" or null and cannot be {1}");
  public static final StringId DiskWriteAttributesImpl_CANNOT_SET_MAXOPLOGS_SIZE_TO_INFINITY_0_IF_COMPACTION_IS_SET_TO_TRUE = new StringId(2557, "Cannot set maxOplogs size to infinity (0) if compaction is set to true");
  public static final StringId DiskWriteAttributesImpl_COMPACTION_CANNOT_BE_SET_TO_TRUE_IF_MAXOPLOGSIZE_IS_SET_TO_INFINITE_INFINITE_IS_REPRESENTED_BY_SIZE_ZERO_0 = new StringId(2558, "Compaction cannot be set to true if max-oplog-size is set to infinite (infinite is represented by size zero : 0)");
  public static final StringId DistributedMemberLock_NOT_IMPLEMENTED = new StringId(2559, "not implemented");
  public static final StringId DistributedRegion_DISTRIBUTION_LOCKS_ARE_ONLY_SUPPORTED_FOR_REGIONS_WITH_GLOBAL_SCOPE_NOT_0 = new StringId(2560, "Distribution locks are only supported for regions with GLOBAL scope, not  {0}");
  public static final StringId DistributedRegion_LOCALCLEAR_IS_NOT_SUPPORTED_ON_DISTRIBUTED_REPLICATED_REGIONS = new StringId(2561, "localClear is not supported on distributed replicated regions.");
  public static final StringId DistributedRegion_NEWCONDITION_UNSUPPORTED = new StringId(2562, "newCondition unsupported");
  public static final StringId DistributedRegion_NOT_ALLOWED_TO_DO_A_LOCAL_INVALIDATION_ON_A_REPLICATED_REGION = new StringId(2563, "Not allowed to do a local invalidation on a replicated region");
  public static final StringId DistributedRegion_REGION_HAS_NOT_BEEN_CONFIGURED_WITH_REQUIRED_ROLES = new StringId(2564, "Region has not been configured with required roles.");
  public static final StringId DistributedRegion_THIS_THREAD_HAS_SUSPENDED_ALL_LOCKING_FOR_THIS_REGION = new StringId(2565, "This thread has suspended all locking for this region");
  public static final StringId DistributedSystemConfigImpl_A_DISTRIBUTEDSYSTEMCONFIG_OBJECT_CANNOT_BE_MODIFIED_AFTER_IT_HAS_BEEN_USED_TO_CREATE_AN_ADMINDISTRIBUTEDSYSTEM = new StringId(2566, "A DistributedSystemConfig object cannot be modified after it has been used to create an AdminDistributedSystem.");
  public static final StringId DistributedSystemConfigImpl_DISTRIBUTIONCONFIG_MUST_NOT_BE_NULL = new StringId(2567, "DistributionConfig must not be null.");
  public static final StringId DistributedSystemConfigImpl_INVALID_BIND_ADDRESS_0 = new StringId(2568, "Invalid bind address:  {0}");
  public static final StringId DistributedSystemConfigImpl_LOGDISKSPACELIMIT_MUST_BE_AN_INTEGER_BETWEEN_0_AND_1 = new StringId(2569, "LogDiskSpaceLimit must be an integer between  {0}  and  {1}");
  public static final StringId DistributedSystemConfigImpl_LOGFILESIZELIMIT_MUST_BE_AN_INTEGER_BETWEEN_0_AND_1 = new StringId(2570, "LogFileSizeLimit must be an integer between  {0}  and  {1}");
  public static final StringId DistributedSystemConfigImpl_MCASTPORT_MUST_BE_AN_INTEGER_INCLUSIVELY_BETWEEN_0_AND_1 = new StringId(2571, "mcastPort must be an integer inclusively between  {0}  and  {1}");
  public static final StringId DistributedSystemConfigImpl_WHILE_PARSING_0 = new StringId(2572, "While parsing \"{0}\"");
  public static final StringId DistributedSystemHealthMonitor_COULD_NOT_GET_LOCALHOST = new StringId(2573, "Could not get localhost?!");
  public static final StringId DistributedSystemHealthMonitor_NOT_A_REAL_GEMFIREVM = new StringId(2574, "Not a real GemFireVM");
  public static final StringId DirectChannel_DIRECT_CHANNEL_HAS_BEEN_STOPPED = new StringId(2575, "Direct channel has been stopped");
  public static final StringId DiskWriteAttributesImpl_0_HAS_TO_BE_LESS_THAN_2_BUT_WAS_1 = new StringId(2576, "{0} has to be a number that does not exceed {2} so the value given {1} is not acceptable");
  public static final StringId DistributionAdvisor_MEMBERID_CANNOT_BE_NULL = new StringId(2577, "memberId cannot be null");
  public static final StringId DistributionChannel_I_NO_LONGER_HAVE_A_MEMBERSHIP_ID = new StringId(2578, "I no longer have a membership ID");
  public static final StringId DistributionConfigImpl_FAILED_READING_0 = new StringId(2580, "Failed reading  {0}");

  public static final StringId DistributionLocatorConfigImpl_INVALID_HOST_0 = new StringId(2583, "Invalid host \"{0}\"");
  public static final StringId DistributionLocatorConfigImpl_PORT_0_MUST_BE_AN_INTEGER_BETWEEN_1_AND_2 = new StringId(2584, "Port ( {0} ) must be an integer between  {1}  and  {2}");
  public static final StringId DistributionLocatorId_0_DOES_NOT_CONTAIN_A_VALID_PORT_NUMBER = new StringId(2585, "{0}  does not contain a valid port number");
  public static final StringId DistributionLocatorId_FAILED_GETTING_HOST_FROM_NAME_0 = new StringId(2586, "Failed getting host from name:  {0}");
  public static final StringId DistributionLocatorId_FAILED_GETTING_LOCAL_HOST = new StringId(2587, "Failed getting local host");
  public static final StringId DistributionLocatorId__0_IS_NOT_IN_THE_FORM_HOSTNAMEPORT = new StringId(2588, "\"{0}\" is not in the form hostname[port].");
  public static final StringId DistributionLocatorImpl_CAN_NOT_SET_THE_STATE_OF_A_LOCATOR = new StringId(2589, "Can not set the state of a locator.");
  public static final StringId DistributionLocator_THE_PORT_ARGUMENT_MUST_BE_GREATER_THAN_0_AND_LESS_THAN_65536 = new StringId(2590, "The -port= argument must be greater than 0 and less than 65536.");

  public static final StringId DistributionManager_COULD_NOT_PROCESS_INITIAL_VIEW = new StringId(2595, "Could not process initial view");
  public static final StringId DistributionManager_DISTRIBUTIONMANAGERS_SHOULD_NOT_BE_COPY_SHARED = new StringId(2596, "DistributionManagers should not be copy shared.");
  public static final StringId DistributionManager_MESSAGE_DISTRIBUTION_HAS_TERMINATED = new StringId(2597, "Message distribution has terminated");
  public static final StringId DistributionManager_NONE_OF_THE_GIVEN_MANAGERS_IS_IN_THE_CURRENT_MEMBERSHIP_VIEW = new StringId(2598, "None of the given managers is in the current membership view");
  public static final StringId DistributionManager_NO_STARTUP_REPLIES_FROM_0 = new StringId(2599, "No startup replies from:  {0}");
  public static final StringId DistributionManager_NO_VALID_ELDER_WHEN_SYSTEM_IS_SHUTTING_DOWN = new StringId(2600, "no valid elder when system is shutting down");
  public static final StringId DistributionManager_ONE_OR_MORE_PEERS_GENERATED_EXCEPTIONS_DURING_CONNECTION_ATTEMPT = new StringId(2601, "One or more peers generated exceptions during connection attempt");
  public static final StringId DistributionManager_POSSIBLE_DEADLOCK_DETECTED = new StringId(2602, "Possible deadlock detected.");
  public static final StringId DistributionManager_THERE_IS_ALREADY_AN_ADMIN_AGENT_ASSOCIATED_WITH_THIS_DISTRIBUTION_MANAGER = new StringId(2603, "There is already an Admin Agent associated with this distribution manager.");
  public static final StringId DistributionManager_THERE_WAS_NEVER_AN_ADMIN_AGENT_ASSOCIATED_WITH_THIS_DISTRIBUTION_MANAGER = new StringId(2604, "There was never an Admin Agent associated with this distribution manager.");
  public static final StringId DefaultQueryService_OFF_HEAP_INDEX_CREATION_IS_NOT_SUPPORTED_FOR_MULTIPLE_ITERATORS_THE_REGION_IS_0 = new StringId(2605, "From clauses having multiple iterators(collections) are not supported for off-heap regions. The off-heap region is {0}");

  public static final StringId DistributionMessage_RECIPIENTS_CAN_ONLY_BE_SET_ONCE = new StringId(2608, "Recipients can only be set once");
  public static final StringId DistributionMessage_UNEXPECTED_ERROR_SCHEDULING_MESSAGE = new StringId(2609, "Unexpected error scheduling message");
  public static final StringId DummyQRegion_NOT_YET_IMPLEMENTED = new StringId(2610, "Not yet implemented");

  public static final StringId DynamicRegionFactory_DYNAMIC_REGION_0_HAS_NOT_BEEN_CREATED = new StringId(2612, "Dynamic region \"{0}\" has not been created.");

  public static final StringId ElderState_CANNOT_FORCE_GRANTOR_RECOVERY_FOR_GRANTOR_THAT_IS_TRANSFERRING = new StringId(2614, "Cannot force grantor recovery for grantor that is transferring");

  public static final StringId EntryEventImpl_AN_IOEXCEPTION_WAS_THROWN_WHILE_DESERIALIZING = new StringId(2620, "An IOException was thrown while deserializing");
  public static final StringId EntryEventImpl_AN_IOEXCEPTION_WAS_THROWN_WHILE_SERIALIZING = new StringId(2621, "An IOException was thrown while serializing.");
  public static final StringId EntryEventImpl_A_CLASSNOTFOUNDEXCEPTION_WAS_THROWN_WHILE_TRYING_TO_DESERIALIZE_CACHED_VALUE = new StringId(2622, "A ClassNotFoundException was thrown while trying to deserialize cached value.");
  public static final StringId EntryEventImpl_MUST_NOT_SERIALIZE_0_IN_THIS_CONTEXT = new StringId(2623, "Must not serialize  {0}  in this context.");
  public static final StringId EntryValueNodeImpl_UNABLE_TO_BUILD_CACHE_VALUE_DISPLAY = new StringId(2624, "Unable to build cache value display");
  public static final StringId EntryValueNodeImpl_UNABLE_TO_SET_ACCESSIBILITY_OF_FIELD_OBJECTS_DURING_CACHE_VALUE_DISPLAY_CONSTRUCTION = new StringId(2625, "Unable to set accessibility of Field objects during cache value display construction");
  public static final StringId ExecutionContext_ATTRIBUTE_NAMED_0_IS_AMBIGUOUS_BECAUSE_IT_CAN_APPLY_TO_MORE_THAN_ONE_VARIABLE_IN_SCOPE = new StringId(2626, "Attribute named '' {0} '' is ambiguous because it can apply to more than one variable in scope.");
  public static final StringId ExecutionContext_METHOD_NAMED_0_WITH_1_ARGUMENTS_IS_AMBIGUOUS_BECAUSE_IT_CAN_APPLY_TO_MORE_THAN_ONE_VARIABLE_IN_SCOPE = new StringId(2627, "Method named '' {0} '' with  {1}  arguments is ambiguous because it can apply to more than one variable in scope.");
  public static final StringId ExecutionContext_THE_ATTRIBUTE_OR_METHOD_NAME_0_COULD_NOT_BE_RESOLVED = new StringId(2628, "The attribute or method name '' {0} '' could not be resolved");
  public static final StringId ExecutionContext_TOO_FEW_QUERY_PARAMETERS = new StringId(2629, "Too few query parameters");
  public static final StringId FacetsJCAConnectionManagerImpl_FACETSJCACONNECTIONMANAGERIMPLALLOCATECONNECTIONNO_VALID_CONNECTION_AVAILABLE = new StringId(2630, "FacetsJCAConnectionManagerImpl::allocateConnection::No valid Connection available");
  public static final StringId FetchEntriesMessage_ERROR_DESERIALIZING_KEYS = new StringId(2631, "Error deserializing keys");
  public static final StringId FetchEntriesMessage_FAILED_SENDING_0 = new StringId(2632, "Failed sending < {0} >");
  public static final StringId FetchEntriesMessage_NO_REPLIES_RECEIVED = new StringId(2633, "No replies received");
  public static final StringId FetchEntriesMessage_PEER_REQUESTS_REATTEMPT = new StringId(2634, "Peer requests reattempt");
  public static final StringId FetchEntriesMessage_UNABLE_TO_SEND_RESPONSE_TO_FETCHENTRIES_REQUEST = new StringId(2635, "Unable to send response to fetch-entries request");
  public static final StringId FetchEntryMessage_ENCOUNTERED_PRLOCALLYDESTROYED = new StringId(2636, "Encountered PRLocallyDestroyed");
  public static final StringId FetchEntryMessage_ENTRY_NOT_FOUND = new StringId(2637, "entry not found");
  public static final StringId FetchEntryMessage_FAILED_SENDING_0 = new StringId(2638, "Failed sending < {0} >");
  public static final StringId FetchHostResponse_COULD_NOT_FIND_GEMFIREJAR = new StringId(2639, "Could not find gemfire.jar.");
  public static final StringId FetchKeysMessage_ENCOUNTERED_PRLOCALLYDESTROYEDEXCEPTION = new StringId(2640, "Encountered PRLocallyDestroyedException");
  public static final StringId FetchKeysMessage_ERROR_DESERIALIZING_KEYS = new StringId(2641, "Error deserializing keys");
  public static final StringId FetchKeysMessage_FAILED_SENDING_0 = new StringId(2642, "Failed sending < {0} >");
  public static final StringId FetchKeysMessage_NO_REPLIES_RECEIVED = new StringId(2643, "No replies received");
  public static final StringId FetchKeysMessage_PEER_REQUESTS_REATTEMPT = new StringId(2644, "Peer requests reattempt");
  public static final StringId FetchKeysMessage_UNABLE_TO_SEND_RESPONSE_TO_FETCH_KEYS_REQUEST = new StringId(2645, "Unable to send response to fetch keys request");
  public static final StringId FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSEENTRIES_METHOD_CALLED_WITH_COMPILEDBINDARGUMENT = new StringId(2646, "FunctionalIndexCreationHelper::prepareFromClause:entries method called with CompiledBindArgument");
  public static final StringId FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSEFROM_CLAUSE_DOES_NOT_EVALUATE_TO_VALID_COLLECTION = new StringId(2647, "FunctionalIndexCreationHelper::prepareFromClause:From clause does not evaluate to valid collection");
  public static final StringId FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSEFROM_CLAUSE_IS_NEITHER_A_COMPILEDPATH_NOR_COMPILEDOPERATION = new StringId(2648, "FunctionalIndexCreationHelper::prepareFromClause:From clause is neither a CompiledPath nor CompiledOperation");
  public static final StringId FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSETOLIST_TOARRAY_NOT_SUPPORTED = new StringId(2649, "FunctionalIndexCreationHelper::prepareFromClause:toList and toArray not supported");
  public static final StringId FunctionalIndexCreationHelper_FUNCTIONALINDEXCREATIONHELPERPREPAREFROMCLAUSETOLIST_TOARRAY_NOT_SUPPORTED_YET = new StringId(2650, "FunctionalIndexCreationHelper::prepareFromClause:toList and toArray not supported yet");
  public static final StringId FunctionalIndexCreationHelper_INVALID_FROM_CLAUSE_0 = new StringId(2651, "Invalid FROM Clause : '' {0} ''");
  public static final StringId FunctionalIndexCreationHelper_INVALID_FROM_CLAUSE_0_SUBSEQUENT_ITERATOR_EXPRESSIONS_IN_FROM_CLAUSE_MUST_BE_DEPENDENT_ON_PREVIOUS_ITERATORS = new StringId(2652, "Invalid FROM Clause : '' {0} ''; subsequent iterator expressions in from clause must be dependent on previous iterators");
  public static final StringId FunctionalIndexCreationHelper_INVALID_INDEXED_EXPRESSION_0 = new StringId(2653, "Invalid indexed expression : '' {0} ''");
  public static final StringId FunctionalIndexCreationHelper_INVALID_PROJECTION_ATTRIBUTES_0 = new StringId(2654, "Invalid projection attributes : '' {0} ''");
  public static final StringId Functions_ELEMENT_APPLIED_TO_PARAMETER_OF_SIZE_0 = new StringId(2655, "element() applied to parameter of size  {0}");

  public static final StringId Functions_MALFORMED_DATE_FORMAT_STRING_AS_THE_FORMAT_IS_0 = new StringId(2657, "Malformed date format string as the format is  {0}");
  public static final StringId Functions_PARAMETERS_TO_THE_TO_DATE_FUNCTION_SHOULD_BE_STRICTLY_SIMPLE_STRINGS = new StringId(2658, "Parameters to the to_date function should be strictly simple strings");
  public static final StringId Functions_THE_ELEMENT_FUNCTION_CANNOT_BE_APPLIED_TO_AN_OBJECT_OF_TYPE_0 = new StringId(2659, "The ''element'' function cannot be applied to an object of type '' {0} ''");
  public static final StringId FutureResult_FUTURE_WAS_CANCELLED = new StringId(2660, "Future was cancelled");

  public static final StringId GatewayEventImpl_NO_EVENT_ID_IS_AVAILABLE_FOR_THIS_GATEWAY_EVENT = new StringId(2666, "No event id is available for this gateway event.");

  public static final StringId GemFireCache_ATTRIBUTES_MUST_NOT_BE_NULL = new StringId(2683, "Attributes must not be null");

  public static final StringId GemFireCache_CANNOT_CREATE_A_CACHE_IN_AN_ADMINONLY_VM = new StringId(2686, "Cannot create a Cache in an admin-only VM.");
  public static final StringId GemFireCache_COULD_NOT_FIND_A_REINITIALIZING_REGION_NAMED_0 = new StringId(2687, "Could not find a reinitializing region named  {0}");
  public static final StringId GemFireCache_CREATED_GEMFIRECACHE_0 = new StringId(2688, "Created GemFireCache  {0}");
  public static final StringId GemFireCache_DECLARATIVE_CACHE_XML_FILERESOURCE_0_DOES_NOT_EXIST = new StringId(2689, "Declarative Cache XML file/resource \"{0}\" does not exist.");
  public static final StringId GemFireCache_DECLARATIVE_XML_FILE_0_IS_NOT_A_FILE = new StringId(2690, "Declarative XML file \"{0}\" is not a file.");
  public static final StringId GemFireCache_DYNAMIC_REGION_INITIALIZATION_FAILED = new StringId(2691, "dynamic region initialization failed");
  public static final StringId GemFireCache_FOUND_AN_EXISTING_REINITALIZING_REGION_NAMED_0 = new StringId(2692, "Found an existing reinitalizing region named  {0}");
  public static final StringId GemFireCache_PATH_CANNOT_BE_0 = new StringId(2693, "path cannot be '' {0} ''");
  public static final StringId GemFireCache_PATH_CANNOT_BE_EMPTY = new StringId(2694, "path cannot be empty");
  public static final StringId GemFireCache_PATH_CANNOT_BE_NULL = new StringId(2695, "path cannot be null");
  public static final StringId GemFireCache_THE_MESSAGESYNCINTERVAL_PROPERTY_FOR_CACHE_CANNOT_BE_NEGATIVE = new StringId(2696, "The ''messageSyncInterval'' property for cache cannot be negative");
  public static final StringId GemFireConnPooledDataSource_GEMFIRECONNPOOLEDDATASOURCECONNECTIONPOOLDATASOURCE_CLASS_OBJECT_IS_NULL_OR_CONFIGUREDDATASOURCEPROPERTIES_OBJECT_IS_NULL = new StringId(2697, "GemFireConnPooledDataSource::ConnectionPoolDataSource class object is null or ConfiguredDataSourceProperties object is null");
  public static final StringId GemFireConnPooledDataSource_GEMFIRECONNPOOLEDDATASOURCEGETCONNECTIONNO_VALID_CONNECTION_AVAILABLE = new StringId(2698, "GemFireConnPooledDataSource::getConnection::No valid Connection available");
  public static final StringId GemFireConnPooledDataSource_GEMFIRECONNPOOLEDDATASOURCEGETCONNFROMCONNPOOLJAVASQLCONNECTION_OBTAINED_IS_INVALID = new StringId(2699, "GemFireConnPooledDataSource::getConnFromConnPool:java.sql.Connection obtained is invalid");
  public static final StringId GemFireHealthEvaluator_NULL_GEMFIREHEALTHCONFIG = new StringId(2700, "Null GemFireHealthConfig");
  public static final StringId GemFireHealthImpl_CANNOT_ACCESS_A_CLOSED_GEMFIREHEALTH_INSTANCE = new StringId(2701, "Cannot access a closed GemFireHealth instance.");
  public static final StringId GemFireHealthImpl_THERE_ARE_NO_GEMFIRE_COMPONENTS_ON_HOST_0 = new StringId(2702, "There are no GemFire components on host \"{0}\".");
  public static final StringId GemFireHealthImpl_THE_GEMFIREHEALTHCONFIG_FOR_FOR_0_CANNOT_SERVE_AS_THE_DEFAULT_HEALTH_CONFIG = new StringId(2703, "The GemFireHealthConfig for \"{0}\" cannot serve as the default health config.");
  public static final StringId GemFireLevel_UNEXPECTED_LEVEL_CODE_0 = new StringId(2704, "Unexpected level code  {0}");
  public static final StringId GemFireStatSampler_ILLEGAL_FIELD_TYPE_0_FOR_STATISTIC = new StringId(2705, "Illegal field type  {0}  for statistic");
  public static final StringId GemFireTransactionDataSource_GEMFIRECONNPOOLEDDATASOURCEGETCONNFROMCONNPOOLJAVASQLCONNECTION_OBTAINED_IS_INVALID = new StringId(2706, "GemFireConnPooledDataSource::getConnFromConnPool:java.sql.Connection obtained is invalid");
  public static final StringId GemFireTransactionDataSource_GEMFIRETRANSACTIONDATASOURCEGETCONNECTIONNO_VALID_CONNECTION_AVAILABLE = new StringId(2707, "GemFireTransactionDataSource::getConnection::No valid Connection available");
  public static final StringId GemFireTransactionDataSource_GEMFIRETRANSACTIONDATASOURCEREGISTERTRANXCONNECTION_EXCEPTION_IN_REGISTERING_THE_XARESOURCE_WITH_THE_TRANSACTIONEXCEPTION_OCCURED_0 = new StringId(2708, "GemFireTransactionDataSource-registerTranxConnection(). Exception in registering the XAResource with the Transaction.Exception occurred= {0}");
  public static final StringId GemFireTransactionDataSource_GEMFIRETRANSACTIONDATASOURCEXADATASOURCE_CLASS_OBJECT_IS_NULL_OR_CONFIGUREDDATASOURCEPROPERTIES_OBJECT_IS_NULL = new StringId(2709, "GemFireTransactionDataSource::XADataSource class object is null or ConfiguredDataSourceProperties object is null");
  public static final StringId GemFireVersion_COULD_NOT_DETERMINE_PRODUCT_LIB_DIRECTORY = new StringId(2710, "could not determine product lib directory");
  public static final StringId GemFireVersion_COULD_NOT_WRITE_0_BECAUSE_1 = new StringId(2711, "Could not write  {0}  because  {1}");

  public static final StringId GenerateMBeanHTML_DTD_NOT_FOUND_0 = new StringId(2713, "DTD not found:  {0}");
  public static final StringId GenerateMBeanHTML_PUBLIC_ID_0_SYSTEM_ID_1 = new StringId(2714, "Public Id: \"{0}\" System Id: \"{1}\"");

  public static final StringId GetMessage_FAILED_SENDING_0 = new StringId(2716, "Failed sending < {0} >");
  public static final StringId GetMessage_NO_RETURN_VALUE_RECEIVED = new StringId(2717, "no return value received");
  public static final StringId GetMessage_OPERATION_GOT_INTERRUPTED_DUE_TO_SHUTDOWN_IN_PROGRESS_ON_REMOTE_VM = new StringId(2718, "Operation got interrupted due to shutdown in progress on remote VM.");
  public static final StringId OnRegionsFunctions_THE_REGION_SET_FOR_ONREGIONS_HAS_NULL = new StringId(2719, "One or more region references added to the regions set is(are) null");
  public static final StringId OnRegionsFunctions_THE_REGION_SET_IS_EMPTY_FOR_ONREGIONS = new StringId(2720, "Regions set is empty for onRegions function execution");
  public static final StringId HARegionQueue_HARGNQTYPE_CAN_EITHER_BE_BLOCKING_0_OR_NON_BLOCKING_1 = new StringId(2721, "haRgnQType can either be BLOCKING ( {0} ) or NON BLOCKING ( {1} )");
  public static final StringId HARegion_CANNOT_SET_TIME_TO_LIVE_WHEN_STATISTICS_ARE_DISABLED = new StringId(2722, "Cannot set time to live when statistics are disabled");
  public static final StringId HARegion_TIMETOLIVE_ACTION_IS_INCOMPATIBLE_WITH_THIS_REGIONS_MIRROR_TYPE = new StringId(2723, "timeToLive action is incompatible with this region''s mirror type");
  public static final StringId HARegion_TIMETOLIVE_MUST_NOT_BE_NULL = new StringId(2724, "timeToLive must not be null");

  public static final StringId HandShake_CLIENTPROXYMEMBERSHIPID_CLASS_COULD_NOT_BE_FOUND_WHILE_DESERIALIZING_THE_OBJECT = new StringId(2726, "ClientProxyMembershipID class could not be found while deserializing the object");

  public static final StringId HandShake_HANDSHAKE_EOF_REACHED_BEFORE_CLIENT_CODE_COULD_BE_READ = new StringId(2728, "HandShake: EOF reached before client code could be read");

  public static final StringId HandShake_HANDSHAKE_REPLY_CODE_IS_NOT_OK = new StringId(2730, "HandShake reply code is not ok");

  public static final StringId HealthMonitorImpl_A_HEALTH_MONITOR_CAN_NOT_BE_STARTED_ONCE_IT_HAS_BEEN_STOPPED = new StringId(2735, "A health monitor can not be started once it has been stopped");

  public static final StringId HeapDataOutputStream_NOT_IN_WRITE_MODE = new StringId(2737, "not in write mode");
  public static final StringId HeapDataOutputStream_UNEXPECTED_0 = new StringId(2738, "unexpected  {0}");

  public static final StringId OnRegionsFunctions_NOT_SUPPORTED_FOR_CLIENT_SERVER = new StringId(2740, "FunctionService#onRegions() is not supported for cache clients in client server mode");
  public static final StringId HostStatHelper_HOSTSTATHELPER_NOT_ALLOWED_IN_PURE_JAVA_MODE = new StringId(2741, "HostStatHelper not allowed in pure java mode");
  public static final StringId HostStatHelper_UNEXPECTED_OS_STATS_FLAGS_0 = new StringId(2742, "Unexpected os stats flags  {0}");
  public static final StringId HostStatHelper_UNHANDLED_OSCODE_0_HOSTSTATHELPERNEWPROCESS = new StringId(2743, "unhandled osCode= {0}  HostStatHelper:newProcess");
  public static final StringId HostStatHelper_UNHANDLED_OSCODE_0_HOSTSTATHELPERNEWPROCESSSTATS = new StringId(2744, "unhandled osCode= {0}  HostStatHelper:newProcessStats");
  public static final StringId HostStatHelper_UNHANDLED_OSCODE_0_HOSTSTATHELPERNEWSYSTEM = new StringId(2745, "unhandled osCode= {0}  HostStatHelper:newSystem");
  public static final StringId HostStatHelper_UNSUPPORTED_OS_0_SUPPORTED_OSS_ARE_SUNOSSPARC_SOLARIS_LINUXX86_AND_WINDOWS = new StringId(2746, "Unsupported OS \"{0}\". Supported OSs are: SunOS(sparc Solaris), Linux(x86) and Windows.");
  public static final StringId HostStatSampler_STATISTICS_SAMPLING_THREAD_IS_ALREADY_RUNNING_INDICATING_AN_INCOMPLETE_SHUTDOWN_OF_A_PREVIOUS_CACHE = new StringId(2747, "Statistics sampling thread is already running, indicating an incomplete shutdown of a previous cache.");

  public static final StringId IndexCreationMsg_COULD_NOT_GET_PARTITIONED_REGION_FROM_ID_0_FOR_MESSAGE_1_RECEIVED_ON_MEMBER_2_MAP_3 = new StringId(2751, "Could not get Partitioned Region from Id  {0}  for message  {1}  received on member= {2}  map= {3}");
  public static final StringId IndexCreationMsg_REGION_IS_LOCALLY_DESTROYED_ON_0 = new StringId(2752, "Region is locally destroyed on  {0}");
  public static final StringId IndexCreationMsg_REMOTE_INDEX_CREAION_FAILED = new StringId(2753, "Remote Index Creation Failed");

  public static final StringId IndexManager_INDEX_DOES_NOT_BELONG_TO_THIS_INDEXMANAGER = new StringId(2755, "Index does not belong to this IndexManager");
  public static final StringId IndexManager_INDEX_NAMED_0_ALREADY_EXISTS = new StringId(2756, "Index named '' {0} '' already exists.");
  public static final StringId IndexManager_INVALID_ACTION = new StringId(2757, "Invalid action");

  public static final StringId IndexManager_SIMILAR_INDEX_EXISTS = new StringId(2759, "Similar Index Exists");
  public static final StringId InitialImageOperation_COULD_NOT_CALCULATE_SIZE_OF_OBJECT = new StringId(2760, "Could not calculate size of object");

  public static final StringId Instantiator_CLASS_0_DOES_NOT_IMPLEMENT_DATASERIALIZABLE = new StringId(2762, "Class  {0}  does not implement DataSerializable");
  public static final StringId Instantiator_CLASS_ID_0_MUST_NOT_BE_0 = new StringId(2763, "Class id  {0}  must not be 0.");

  public static final StringId InterestEvent_CLASSNOTFOUNDEXCEPTION_DESERIALIZING_VALUE = new StringId(2766, "ClassNotFoundException deserializing value");
  public static final StringId InterestEvent_IOEXCEPTION_DESERIALIZING_VALUE = new StringId(2767, "IOException deserializing value");
  public static final StringId InternalDataSerializer_A_DATASERIALIZER_OF_CLASS_0_IS_ALREADY_REGISTERED_WITH_ID_1_SO_THE_DATASERIALIZER_OF_CLASS_2_COULD_NOT_BE_REGISTERED = new StringId(2768, "A DataSerializer of class {0} is already registered with id {1} so the DataSerializer of class {2} could not be registered.");
  public static final StringId InternalDataSerializer_CANNOT_CREATE_A_DATASERIALIZER_WITH_ID_0 = new StringId(2769, "Cannot create a DataSerializer with id 0.");
  public static final StringId InternalDistributedMember_INTERNALDISTRIBUTEDMEMBERCOMPARETO_COMPARISON_BETWEEN_DIFFERENT_CLASSES = new StringId(2770, "InternalDistributedMember.compareTo(): comparison between different classes");

  public static final StringId InternalDistributedSystem_NOT_IMPLEMENTED_YET = new StringId(2773, "Not implemented yet");
  public static final StringId InternalDistributedSystem_NO_LISTENERS_PERMITTED_AFTER_SHUTDOWN_0 = new StringId(2774, "No listeners permitted after shutdown:  {0}");

  public static final StringId InternalDistributedSystem_SOME_REQUIRED_ROLES_MISSING = new StringId(2776, "Some required roles missing");

  public static final StringId InternalDistributedSystem_THIS_CONNECTION_TO_A_DISTRIBUTED_SYSTEM_HAS_BEEN_DISCONNECTED = new StringId(2778, "This connection to a distributed system has been disconnected.");

  public static final StringId InternalInstantiator_0_DOES_NOT_EXTEND_INSTANTIATOR = new StringId(2785, "{0}  does not extend Instantiator.");
  public static final StringId InternalInstantiator_CANNOT_REGISTER_A_NULL_INSTANTIATOR = new StringId(2786, "Cannot register a null Instantiator.");
  public static final StringId InternalInstantiator_CLASS_0_IS_ALREADY_REGISTERED_WITH_ID_1_SO_IT_CANNOT_BE_REGISTERED_WTH_ID_2 = new StringId(2787, "Class  {0}  is already registered with id {1} so it can not be registered with id {2}");
  public static final StringId InternalInstantiator_CLASS_0_WAS_NOT_REGISTERED_WITH_ID_1 = new StringId(2788, "Class  {0}  was not registered with id  {1}");
  public static final StringId InternalInstantiator_CLASS_ID_0_IS_ALREADY_REGISTERED_FOR_CLASS_1_SO_IT_COULD_NOT_BE_REGISTED_FOR_CLASS_2 = new StringId(2789, "Class id {0} is already registered for class {1} so it could not be registered for class {2}");
  public static final StringId InternalInstantiator_COULD_NOT_ACCESS_ZEROARGUMENT_CONSTRUCTOR_OF_0 = new StringId(2790, "Could not access zero-argument constructor of  {0}");
  public static final StringId InternalInstantiator_COULD_NOT_INSTANTIATE_AN_INSTANCE_OF_0 = new StringId(2791, "Could not instantiate an instance of  {0}");
  public static final StringId InternalInstantiator_WHILE_INSTANTIATING_AN_INSTANCE_OF_0 = new StringId(2792, "While instantiating an instance of  {0}");

  public static final StringId InternalRole_GETCOUNT_REQUIRES_A_CONNECTION_TO_THE_DISTRIBUTED_SYSTEM = new StringId(2795, "getCount requires a connection to the distributed system.");
  public static final StringId InternalRole_INTERNALROLECOMPARETO_COMPARISON_BETWEEN_DIFFERENT_CLASSES = new StringId(2796, "InternalRole.compareTo(): comparison between different classes");
  public static final StringId InternalRole_ISPRESENT_REQUIRES_A_CONNECTION_TO_THE_DISTRIBUTED_SYSTEM = new StringId(2797, "isPresent requires a connection to the distributed system.");
  public static final StringId InvalidateMessage_FAILED_SENDING_0 = new StringId(2798, "Failed sending < {0} >");
  public static final StringId InvalidateMessage_NO_RESPONSE_CODE_RECEIVED = new StringId(2799, "no response code received");
  public static final StringId JCAConnectionManagerImpl_JCACONNECTIONMANAGERIMPLALLOCATECONNECTIONNO_VALID_CONNECTION_AVAILABLE = new StringId(2800, "JCAConnectionManagerImpl::allocateConnection::No valid Connection available");
  public static final StringId MemberFactory_UNABLE_TO_CREATE_MEMBERSHIP_MANAGER = new StringId(2801, "Unable to create membership manager");
  public static final StringId Member_MEMBERCOMPARETO_COMPARISON_BETWEEN_DIFFERENT_CLASSES = new StringId(2802, "NetMember.compareTo(): comparison between different classes");
  public static final StringId GroupMembershipService_CANNOT_FIND_0 = new StringId(2803, "Cannot find  {0}");
  public static final StringId GroupMembershipService_CHANNEL_CLOSED = new StringId(2804, "Channel closed");

  public static final StringId LRUAlgorithm_LRU_EVICTION_CONTROLLER_0_ALREADY_CONTROLS_THE_CAPACITY_OF_1_IT_CANNOT_ALSO_CONTROL_THE_CAPACITY_OF_REGION_2 = new StringId(2817, "LRU eviction controller  {0}  already controls the capacity of  {1} .  It cannot also control the capacity of region  {2} .");
  public static final StringId LRUAlgorithm_LRU_STATS_IN_EVICTION_CONTROLLER_INSTANCE_SHOULD_NOT_BE_NULL = new StringId(2818, "lru stats in eviction controller instance should not be null");
  public static final StringId LRUCapacityController_MAXIMUM_ENTRIES_MUST_BE_POSITIVE = new StringId(2819, "Maximum entries must be positive");

  public static final StringId ListenerIdMap_ILLEGAL_INITIAL_CAPACITY_0 = new StringId(2838, "Illegal Initial Capacity: {0}");
  public static final StringId ListenerIdMap_ILLEGAL_LOAD_FACTOR_0 = new StringId(2839, "Illegal Load factor:  {0}");

  public static final StringId LocalRegion_A_REGION_WITH_SCOPELOCAL_CAN_ONLY_HAVE_SUBREGIONS_WITH_SCOPELOCAL = new StringId(2842, "A region with Scope.LOCAL can only have subregions with Scope.LOCAL");
  public static final StringId LocalRegion_CANNOT_DO_A_LOCAL_INVALIDATE_ON_A_REPLICATED_REGION = new StringId(2843, "Cannot do a local invalidate on a replicated region");

  public static final StringId LocalRegion_CANNOT_WRITE_A_REGION_THAT_IS_NOT_CONFIGURED_TO_ACCESS_DISKS = new StringId(2845, "Cannot write a region that is not configured to access disks.");
  public static final StringId LocalRegion_CANNOT_WRITE_A_REGION_WITH_DATAPOLICY_0_TO_DISK = new StringId(2846, "Cannot write a region with data-policy  {0}  to disk.");
  public static final StringId LocalRegion_CLASS_0_COULD_NOT_BE_INSTANTIATED = new StringId(2847, "Class  {0}  could not be instantiated.");
  public static final StringId LocalRegion_CLASS_0_NOT_FOUND_IN_CLASSPATH = new StringId(2848, "Class  {0}  not found in classpath.");
  public static final StringId LocalRegion_EXCEPTION_OCCURED_WHILE_RE_CREATING_INDEX_DATA_ON_CLEARED_REGION = new StringId(2849, "Exception occurred while re creating index data on cleared region.");
  public static final StringId LocalRegion_FAILED_ENLISTEMENT_WITH_TRANSACTION_0 = new StringId(2850, "Failed enlistement with transaction \"{0}\"");
  public static final StringId LocalRegion_INPUTSTREAM_MUST_NOT_BE_NULL = new StringId(2851, "InputStream must not be null.");
  public static final StringId LocalRegion_INTEREST_KEY_MUST_NOT_BE_NULL = new StringId(2852, "interest key must not be null");
  public static final StringId LocalRegion_INTEREST_LIST_RETRIEVAL_REQUIRES_A_POOL = new StringId(2853, "Interest list retrieval requires a pool.");
  public static final StringId LocalRegion_INTEREST_REGISTRATION_NOT_SUPPORTED_ON_REPLICATED_REGIONS = new StringId(2854, "Interest registration not supported on replicated regions");
  public static final StringId LocalRegion_INTEREST_REGISTRATION_REQUIRES_A_POOL = new StringId(2855, "Interest registration requires a Pool");
  public static final StringId LocalRegion_INTEREST_UNREGISTRATION_REQUIRES_A_POOL = new StringId(2856, "Interest unregistration requires a pool.");
  public static final StringId LocalRegion_KEY_0_DOES_NOT_SATISFY_KEYCONSTRAINT_1 = new StringId(2857, "key ( {0} ) does not satisfy keyConstraint ( {1} )");
  public static final StringId LocalRegion_KEY_CANNOT_BE_NULL = new StringId(2858, "key cannot be null");
  public static final StringId LocalRegion_NAME_CANNOT_BE_EMPTY = new StringId(2859, "name cannot be empty");
  public static final StringId LocalRegion_NAME_CANNOT_BE_NULL = new StringId(2860, "name cannot be null");
  public static final StringId LocalRegion_NAME_CANNOT_CONTAIN_THE_SEPARATOR_0 = new StringId(2861, "name cannot contain the separator '' {0} ''");
  public static final StringId LocalRegion_NOT_ALLOWED_TO_DO_A_LOCAL_DESTROY_ON_A_REPLICATED_REGION = new StringId(2862, "Not allowed to do a local destroy on a replicated region");
  public static final StringId LocalRegion_ONLY_SUPPORTED_FOR_GLOBAL_SCOPE_NOT_LOCAL = new StringId(2863, "Only supported for GLOBAL scope, not LOCAL");
  public static final StringId LocalRegion_PATH_SHOULD_NOT_BE_NULL = new StringId(2864, "path should not be null");
  public static final StringId LocalRegion_PATH_SHOULD_NOT_START_WITH_A_SLASH = new StringId(2865, "path should not start with a slash");
  public static final StringId LocalRegion_REGIONS_WITH_DATAPOLICY_0_DO_NOT_SUPPORT_LOADSNAPSHOT = new StringId(2866, "Regions with DataPolicy  {0}  do not support loadSnapshot.");
  public static final StringId LocalRegion_REGIONS_WITH_DATAPOLICY_0_DO_NOT_SUPPORT_SAVESNAPSHOT = new StringId(2867, "Regions with DataPolicy  {0}  do not support saveSnapshot.");

  public static final StringId LocalRegion_REGION_ATTRIBUTES_MUST_NOT_BE_NULL = new StringId(2869, "region attributes must not be null");
  public static final StringId LocalRegion_SERVER_KEYSET_REQUIRES_A_POOL = new StringId(2870, "Server keySet requires a pool.");

  public static final StringId LocalRegion_TX_NOT_IN_PROGRESS = new StringId(2872, "tx not in progress");
  public static final StringId LocalRegion_UNEXPECTED_SNAPSHOT_CODE_0_THIS_SNAPSHOT_WAS_PROBABLY_WRITTEN_BY_AN_EARLIER_INCOMPATIBLE_RELEASE = new StringId(2873, "Unexpected snapshot code \"{0}\". This snapshot was probably written by an earlier, incompatible, release.");
  public static final StringId LocalRegion_UNSUPPORTED_SNAPSHOT_VERSION_0_ONLY_VERSION_1_IS_SUPPORTED = new StringId(2874, "Unsupported snapshot version \"{0}\". Only version \"{1}\" is supported.");
  public static final StringId LocalRegion_VALUE_0_DOES_NOT_SATISFY_VALUECONSTRAINT_1 = new StringId(2875, "value ( {0} ) does not satisfy valueConstraint ( {1} )");
  public static final StringId LocalRegion_VALUE_CANNOT_BE_NULL = new StringId(2876, "Value cannot be null");
  public static final StringId LocalRegion_VALUE_MUST_NOT_BE_NULL = new StringId(2877, "value must not be null");
  public static final StringId LockGrantorId_LOCKGRANTORMEMBER_IS_NULL = new StringId(2878, "lockGrantorMember is null");
  public static final StringId LockGrantorId_SOMELOCKGRANTORID_MUST_NOT_BE_NULL = new StringId(2879, "someLockGrantorId must not be null");
  public static final StringId LonerDistributionManager_LONER_TRIED_TO_SEND_MESSAGE_TO_0 = new StringId(2880, "Loner tried to send message to  {0}");
  public static final StringId LonerDistributionManager_MEMBER_NOT_FOUND_IN_MEMBERSHIP_SET = new StringId(2881, "Member not found in membership set");

  public static final StringId LossAction_INVALID_LOSSACTION_NAME_0 = new StringId(2884, "Invalid LossAction name:  {0}");
  public static final StringId MBeanUtil_0_IS_NOT_A_MANAGEDRESOURCE = new StringId(2885, "{0}  is not a ManagedResource");
  public static final StringId MBeanUtil_FAILED_TO_GET_MBEAN_REGISTRY = new StringId(2886, "Failed to get MBean Registry");
  public static final StringId MBeanUtil_MANAGEDBEAN_IS_NULL = new StringId(2887, "ManagedBean is null");
  public static final StringId MBeanUtil_NOTIFICATIONLISTENER_IS_REQUIRED = new StringId(2888, "NotificationListener is required");
  public static final StringId MBeanUtil_REFRESHNOTIFICATIONTYPE_IS_REQUIRED = new StringId(2889, "RefreshNotificationType is required");
  public static final StringId MBeanUtil_REFRESHTIMER_HAS_NOT_BEEN_PROPERLY_INITIALIZED = new StringId(2890, "RefreshTimer has not been properly initialized.");

  public static final StringId MX4JModelMBean_ATTRIBUTE_0_IS_NOT_READABLE = new StringId(2892, "Attribute  {0}  is not readable");
  public static final StringId MX4JModelMBean_ATTRIBUTE_0_IS_NOT_WRITABLE = new StringId(2893, "Attribute  {0}  is not writable");
  public static final StringId MX4JModelMBean_ATTRIBUTE_CANNOT_BE_NULL = new StringId(2894, "Attribute cannot be null.");
  public static final StringId MX4JModelMBean_ATTRIBUTE_DESCRIPTOR_FOR_ATTRIBUTE_0_CANNOT_BE_NULL = new StringId(2895, "Attribute descriptor for attribute  {0}  cannot be null");
  public static final StringId MX4JModelMBean_ATTRIBUTE_LIST_CANNOT_BE_NULL = new StringId(2896, "Attribute list cannot be null.");
  public static final StringId MX4JModelMBean_ATTRIBUTE_NAMES_CANNOT_BE_DIFFERENT = new StringId(2897, "Attribute names cannot be different.");
  public static final StringId MX4JModelMBean_ATTRIBUTE_NAMES_CANNOT_BE_NULL = new StringId(2898, "Attribute names cannot be null.");
  public static final StringId MX4JModelMBean_ATTRIBUTE_NAME_CANNOT_BE_NULL = new StringId(2899, "Attribute name cannot be null.");
  public static final StringId MX4JModelMBean_CANNOT_FIND_MODELMBEANATTRIBUTEINFO_FOR_ATTRIBUTE_0 = new StringId(2900, "Cannot find ModelMBeanAttributeInfo for attribute  {0}");
  public static final StringId MX4JModelMBean_CANNOT_FIND_MODELMBEANOPERATIONINFO_FOR_OPERATION_0 = new StringId(2901, "Cannot find ModelMBeanOperationInfo for operation  {0}");
  public static final StringId MX4JModelMBean_COULD_NOT_FIND_TARGET = new StringId(2902, "Could not find target");
  public static final StringId MX4JModelMBean_INVALID_PERSIST_VALUE = new StringId(2903, "Invalid persist value");
  public static final StringId MX4JModelMBean_LISTENER_CANNOT_BE_NULL = new StringId(2904, "Listener cannot be null.");
  public static final StringId MX4JModelMBean_MANAGED_RESOURCE_CANNOT_BE_NULL = new StringId(2905, "Managed resource cannot be null.");
  public static final StringId MX4JModelMBean_MBEAN_DESCRIPTOR_CANNOT_BE_NULL = new StringId(2906, "MBean descriptor cannot be null");
  public static final StringId MX4JModelMBean_METHOD_NAME_CANNOT_BE_NULL = new StringId(2907, "Method name cannot be null.");
  public static final StringId MX4JModelMBean_MODELMBEANINFO_CANNOT_BE_NULL = new StringId(2908, "ModelMBeanInfo cannot be null.");
  public static final StringId MX4JModelMBean_MODELMBEANINFO_IS_INVALID = new StringId(2909, "ModelMBeanInfo is invalid.");
  public static final StringId MX4JModelMBean_MODELMBEANINFO_IS_NULL = new StringId(2910, "ModelMBeanInfo is null");
  public static final StringId MX4JModelMBean_MODELMBEANINFO_PARAMETER_CANT_BE_NULL = new StringId(2911, "ModelMBeanInfo parameter cannot be null.");
  public static final StringId MX4JModelMBean_MODELMBEAN_CANNOT_BE_REGISTERED_UNTIL_SETMODELMBEANINFO_HAS_BEEN_CALLED = new StringId(2912, "ModelMBean cannot be registered until setModelMBeanInfo has been called.");
  public static final StringId MX4JModelMBean_MX4JMODELMBEAN_IS_NOT_REGISTERED = new StringId(2913, "MX4JModelMBean is not registered.");
  public static final StringId MX4JModelMBean_NOTIFICATION_CANNOT_BE_NULL = new StringId(2914, "Notification cannot be null.");
  public static final StringId MX4JModelMBean_OPERATION_DESCRIPTOR_FIELD_ROLE_MUST_BE_OPERATION_NOT_0 = new StringId(2915, "Operation descriptor field ''role'' must be ''operation'', not  {0}");
  public static final StringId MX4JModelMBean_OPERATION_DESCRIPTOR_FOR_OPERATION_0_CANNOT_BE_NULL = new StringId(2916, "Operation descriptor for operation  {0}  cannot be null");
  public static final StringId MX4JModelMBean_RETURNED_TYPE_AND_DECLARED_TYPE_ARE_NOT_ASSIGNABLE = new StringId(2917, "Returned type and declared type are not assignable.");
  public static final StringId ManageBucketMessage_FAILED_SENDING_0 = new StringId(2918, "Failed sending < {0} >");
  public static final StringId ManagedEntityConfigImpl_COULD_NOT_DETERMINE_LOCALHOST = new StringId(2919, "Could not determine localhost?!");
  public static final StringId ManagedEntityConfigImpl_COULD_NOT_FIND_GEMFIREJAR = new StringId(2920, "Could not find gemfire.jar.");
  public static final StringId ManagedEntityConfigImpl_INVALID_HOST_0 = new StringId(2921, "Invalid host \"{0}\"");
  public static final StringId ManagedEntityConfigImpl_THIS_CONFIGURATION_CANNOT_BE_MODIFIED_WHILE_ITS_MANAGED_ENTITY_IS_RUNNING = new StringId(2922, "This configuration cannot be modified while its managed entity is running.");
  public static final StringId ManagedEntityConfigXmlGenerator_EXCEPTION_THROWN_WHILE_GENERATING_XML = new StringId(2923, "Exception thrown while generating XML.");
  public static final StringId ManagedEntityConfigXmlParser_UNKNOWN_XML_ELEMENT_0 = new StringId(2924, "Unknown XML element \"{0}\"");

  public static final StringId ManagedEntityConfigXml_DTD_NOT_FOUND_0 = new StringId(2926, "DTD not found:  {0}");
  public static final StringId ManagedEntityConfigXml_ERROR_WHILE_PARSING_XML = new StringId(2927, "Error while parsing XML.");
  public static final StringId ManagedEntityConfigXml_FATAL_ERROR_WHILE_PARSING_XML = new StringId(2928, "Fatal error while parsing XML.");
  public static final StringId ManagedEntityController_EXECUTION_COMMAND_IS_EMPTY = new StringId(2929, "Execution command is empty");
  public static final StringId ManagerInfo_0_1_IS_ALREADY_RUNNING = new StringId(2930, "{0} \"{1}\" is already running.");
  public static final StringId ManagerInfo_COULD_NOT_LOAD_FILE_0 = new StringId(2931, "Could not load file \"{0}\".");
  public static final StringId ManagerInfo_COULD_NOT_LOAD_FILE_0_BECAUSE_A_CLASS_COULD_NOT_BE_FOUND = new StringId(2932, "Could not load file \"{0}\" because a class could not be found.");
  public static final StringId ManagerInfo_COULD_NOT_LOAD_FILE_0_BECAUSE_THE_FILE_IS_EMPTY_WAIT_FOR_THE_1_TO_FINISH_STARTING = new StringId(2933, "Could not load file \"{0}\" because the file is empty. Wait for the {1} to finish starting.");
  public static final StringId ManagerInfo_COULD_NOT_WRITE_FILE_0 = new StringId(2934, "Could not write file \"{0}\".");
  public static final StringId ManagerInfo_ONLY_LOCATORS_ARE_SUPPORTED = new StringId(2935, "Only locators are supported");
  public static final StringId ManagerInfo_THE_INFO_FILE_0_DOES_NOT_EXIST = new StringId(2936, "The info file \"{0}\" does not exist.");
  public static final StringId ManagerInfo_UNKNOWN_STATUSNAME_0 = new StringId(2937, "Unknown statusName  {0}");
  public static final StringId ManagerInfo__0_DOES_NOT_EXIST_OR_IS_NOT_A_DIRECTORY = new StringId(2938, "\"{0}\" does not exist or is not a directory.");

  public static final StringId MemLRUCapacityController_COULD_NOT_CREATE_SIZER_INSTANCE_GIVEN_THE_CLASS_NAME_0 = new StringId(2940, "Could not create sizer instance given the class name \"{0}\"");
  public static final StringId MemLRUCapacityController_MEMLRUCONTROLLER_LIMIT_MUST_BE_POSTIVE_0 = new StringId(2941, "MemLRUController limit must be postive:  {0}");

  public static final StringId MembershipAttributes_ONE_OR_MORE_REQUIRED_ROLES_MUST_BE_SPECIFIED = new StringId(2943, "One or more required roles must be specified.");
  public static final StringId MergeLogFiles_NUMBER_OF_LOG_FILES_0_IS_NOT_THE_SAME_AS_THE_NUMBER_OF_LOG_FILE_NAMES_1 = new StringId(2944, "Number of log files ( {0} ) is not the same as the number of log file names ( {1} )");
  public static final StringId PRHARedundancyProvider_TIMED_OUT_ATTEMPTING_TO_0_IN_THE_PARTITIONED_REGION__1_WAITED_FOR_2_MS = new StringId(2945, "Timed out attempting to {0} in the partitioned region.\n{1}\nWaited for: {2} ms.\n");
  public static final StringId MessageFactory_CLASS_0_IS_NOT_A_DISTRIBUTIONMESSAGE = new StringId(2946, "Class  {0}  is not a DistributionMessage.");
  public static final StringId Message_DEAD_CONNECTION = new StringId(2947, "Dead Connection");
  public static final StringId Message_INVALID_MESSAGETYPE = new StringId(2948, "Invalid MessageType");
  public static final StringId Message_INVALID_MESSAGE_TYPE_0_WHILE_READING_HEADER = new StringId(2949, "Invalid message type  {0}  while reading header");
  public static final StringId Message_MESSAGE_SIZE_0_EXCEEDED_MAX_LIMIT_OF_1 = new StringId(2950, "Message size  {0}  exceeded max limit of  {1}");
  public static final StringId Message_OPERATION_TIMED_OUT_ON_SERVER_WAITING_ON_CONCURRENT_DATA_LIMITER_AFTER_WAITING_0_MILLISECONDS = new StringId(2951, "Operation timed out on server waiting on concurrent data limiter after waiting  {0}  milliseconds");
  public static final StringId Message_OPERATION_TIMED_OUT_ON_SERVER_WAITING_ON_CONCURRENT_MESSAGE_LIMITER_AFTER_WAITING_0_MILLISECONDS = new StringId(2952, "Operation timed out on server waiting on concurrent message limiter after waiting  {0}  milliseconds");
  public static final StringId Message_PART_LENGTH_0_AND_NUMBER_OF_PARTS_1_INCONSISTENT = new StringId(2953, "Part length ( {0} ) and number of parts ( {1} ) inconsistent");
  public static final StringId Message_THE_CONNECTION_HAS_BEEN_RESET_WHILE_READING_A_PART = new StringId(2954, "The connection has been reset while reading a part");
  public static final StringId Message_THE_CONNECTION_HAS_BEEN_RESET_WHILE_READING_THE_HEADER = new StringId(2955, "The connection has been reset while reading the header");
  public static final StringId Message_THE_CONNECTION_HAS_BEEN_RESET_WHILE_READING_THE_PAYLOAD = new StringId(2956, "The connection has been reset while reading the payload");
  public static final StringId MethodDispatch_METHOD_0_IN_CLASS_1_IS_NOT_ACCESSIBLE_TO_THE_QUERY_PROCESSOR = new StringId(2957, "Method '' {0} '' in class '' {1} '' is not accessible to the query processor");
  public static final StringId MethodDispatch_NO_APPLICABLE_AND_ACCESSIBLE_METHOD_NAMED_0_WAS_FOUND_IN_CLASS_1_FOR_THE_ARGUMENT_TYPES_2 = new StringId(2958, "No applicable and accessible method named '' {0} '' was found in class '' {1} '' for the argument types  {2}");
  public static final StringId MethodDispatch_TWO_OR_MORE_MAXIMALLY_SPECIFIC_METHODS_WERE_FOUND_FOR_THE_METHOD_NAMED_0_IN_CLASS_1_FOR_THE_ARGUMENT_TYPES_2 = new StringId(2959, "Two or more maximally specific methods were found for the method named '' {0} '' in class '' {1} '' for the argument types:  {2}");
  public static final StringId MsgDestreamer_FAILURE_DURING_MESSAGE_DESERIALIZATION = new StringId(2960, "failure during message deserialization");

  public static final StringId MsgOutputStream_STRING_TOO_LONG_FOR_JAVA_SERIALIZATION = new StringId(2962, "String too long for java serialization");
  public static final StringId MsgStreamer_AN_EXCEPTION_WAS_THROWN_WHILE_SERIALIZING = new StringId(2963, "An Exception was thrown while serializing.");

  public static final StringId LonerDistributionManager_Cache_Time = new StringId(2966, "The current cache time is {0}.  Delta from the system clock is {1} milliseconds.");

  public static final StringId OSProcess_COULD_NOT_CREATE_LOG_FILE_0_BECAUSE_1 = new StringId(2987, "Could not create log file \"{0}\" because: {1}.");
  public static final StringId OSProcess_EXISTS_NOT_ALLOWED_IN_PURE_JAVA_MODE = new StringId(2988, "exists not allowed in pure java mode");
  public static final StringId OSProcess_KILL_NOT_ALLOWED_IN_PURE_JAVA_MODE = new StringId(2989, "kill not allowed in pure java mode");
  public static final StringId OSProcess_NEED_WRITE_ACCESS_FOR_THE_LOG_FILE_0 = new StringId(2990, "Need write access for the log file \"{0}\".");

  public static final StringId OSProcess_SETCURRENTDIRECTORY_NOT_ALLOWED_IN_PURE_JAVA_MODE = new StringId(2992, "setCurrentDirectory not allowed in pure java mode");
  public static final StringId OSProcess_SHOULD_NOT_SEND_A_SIGNAL_TO_PID_0 = new StringId(2993, "Should not send a signal to pid  {0,number,#}");
  public static final StringId OSProcess_SHUTDOWN_NOT_ALLOWED_IN_PURE_JAVA_MODE = new StringId(2994, "shutdown not allowed in pure java mode");
  public static final StringId OSProcess_THE_EXECUTABLE_0_DOES_NOT_EXIST = new StringId(2995, "the executable \"{0}\" does not exist");
  public static final StringId OSProcess_THE_LOG_FILE_0_WAS_NOT_A_NORMAL_FILE = new StringId(2996, "The log file \"{0}\" was not a normal file.");
  public static final StringId OSProcess_WAITFORPIDTOEXIT_NOT_ALLOWED_IN_PURE_JAVA_MODE = new StringId(2997, "waitForPidToExit not allowed in pure java mode");
  public static final StringId ObjIdMap_ILLEGAL_INITIAL_CAPACITY_0 = new StringId(2998, "Illegal Initial Capacity:  {0}");
  public static final StringId ObjIdMap_ILLEGAL_LOAD_FACTOR_0 = new StringId(2999, "Illegal Load factor:  {0}");

  public static final StringId Oplog_CANNOT_FIND_RECORD_0_WHEN_READING_FROM_1 = new StringId(3008, "Cannot find record  {0}  when reading from \"{1}\"");
  public static final StringId Oplog_COULD_NOT_LOCK_0 = new StringId(3009, "Could not lock \"{0}\". Other JVMs might have created diskstore with same name using the same directory.");
  public static final StringId Oplog_DIRECTORIES_ARE_FULL_NOT_ABLE_TO_ACCOMODATE_THIS_OPERATIONSWITCHING_PROBLEM_FOR_ENTRY_HAVING_DISKID_0 = new StringId(3010, "Directories are full, not able to accommodate this operation.Switching problem for entry having DiskID= {0}");

  public static final StringId Oplog_FAILED_CREATING_OPERATION_LOG_BECAUSE_0 = new StringId(3012, "Failed creating operation log because:  {0}");
  public static final StringId Oplog_FAILED_READING_FROM_0_OPLOGID_1_OFFSET_BEING_READ_2_CURRENT_OPLOG_SIZE_3_ACTUAL_FILE_SIZE_4_IS_ASYNCH_MODE_5_IS_ASYNCH_WRITER_ALIVE_6 = new StringId(3013, "Failed reading from \"{0}\". \n oplogID = {1} \n Offset being read= {2} Current Oplog Size= {3} Actual File Size = {4} IS ASYNCH MODE = {5} IS ASYNCH WRITER ALIVE= {6}");
  public static final StringId DiskInitFile_FAILED_SAVING_DATA_SERIALIZER_TO_DISK_BECAUSE_0 = new StringId(3014, "Failed saving data serializer to disk because:  {0}");
  public static final StringId DiskInitFile_FAILED_SAVING_INSTANTIATOR_TO_DISK_BECAUSE_0 = new StringId(3015, "Failed saving instantiator to disk because:  {0}");

  public static final StringId Oplog_FAILED_WRITING_KEY_TO_0 = new StringId(3017, "Failed writing key to \"{0}\"");
  public static final StringId Oplog_FAILED_WRITING_KEY_TO_0_DUE_TO_FAILURE_IN_ACQUIRING_READ_LOCK_FOR_ASYNCH_WRITING = new StringId(3018, "Failed writing key to \"{0}\" due to failure in acquiring read lock for asynch writing");
  public static final StringId Oplog_OPERATION_SIZE_CANNOT_EXCEED_THE_MAXIMUM_DIRECTORY_SIZE_SWITCHING_PROBLEM_FOR_ENTRY_HAVING_DISKID_0 = new StringId(3019, "Operation size cannot exceed the maximum directory size. Switching problem for entry having DiskID={0}");
  public static final StringId Oplog_THE_FILE_0_IS_BEING_USED_BY_ANOTHER_PROCESS = new StringId(3020, "The file \"{0}\" is being used by another process.");

  public static final StringId Oplog_UNKNOWN_OPCODE_0_FOUND_IN_DISK_OPERATION_LOG = new StringId(3024, "Unknown opCode  {0}  found in disk operation log.");
  public static final StringId DiskInitFile_UNKNOWN_OPCODE_0_FOUND = new StringId(3025, "Unknown opCode  {0}  found in disk initialization file.");
  public static final StringId Oplog_FAILED_CONFLICT_VERSION_TAG_0 = new StringId(3026, "Failed writing conflict version tag to \"{0}\"");
  public static final StringId PRQueryProcessor_GOT_UNEXPECTED_EXCEPTION_WHILE_EXECUTING_QUERY_ON_PARTITIONED_REGION_BUCKET = new StringId(3027, "Got unexpected exception while executing query on partitioned region bucket");
  public static final StringId PRQueryProcessor_TIMED_OUT_WHILE_EXECUTING_QUERY_TIME_EXCEEDED_0 = new StringId(3028, "Timed out while executing query, time exceeded  {0}");

  public static final StringId DiskInitFile_UNKNOWN_COMPRESSOR_0_FOUND = new StringId(3030, "Unknown Compressor  {0}  found in disk initialization file.");

  public static final StringId DistributionMessage_DISTRIBUTIONRESPONSE_GOT_MEMBERDEPARTED_EVENT_FOR_0_CRASHED_1 = new StringId(3032, "DistributionResponse got memberDeparted event for < {0} > crashed =  {1}");
  public static final StringId MemberMessage_MEMBERRESPONSE_GOT_MEMBERDEPARTED_EVENT_FOR_0_CRASHED_1 = new StringId(3033, "MemberResponse got memberDeparted event for < {0} > crashed =  {1}");
  public static final StringId PartitionMessage_0_COULD_NOT_FIND_PARTITIONED_REGION_WITH_ID_1 = new StringId(3034, "{0} : could not find partitioned region with Id  {1}");
  public static final StringId PartitionMessage_ATTEMPT_FAILED = new StringId(3035, "Attempt failed");
  public static final StringId PartitionMessage_DISTRIBUTED_SYSTEM_IS_DISCONNECTING = new StringId(3036, "Distributed system is disconnecting");
  public static final StringId PartitionMessage_MEMBERDEPARTED_GOT_NULL_MEMBERID = new StringId(3037, "memberDeparted got null memberId");
  public static final StringId PartitionMessage_PARTITIONRESPONSE_GOT_MEMBERDEPARTED_EVENT_FOR_0_CRASHED_1 = new StringId(3038, "memberDeparted event for < {0} > crashed =  {1}");
  public static final StringId PartitionMessage_PEER_FAILED_PRIMARY_TEST = new StringId(3039, "Peer failed primary test");
  public static final StringId PartitionMessage_PEER_REQUESTS_REATTEMPT = new StringId(3040, "Peer requests reattempt");
  public static final StringId PartitionMessage_REGION_IS_DESTROYED_IN_0 = new StringId(3041, "Region is destroyed in  {0}");

  public static final StringId PartitionMessage_REMOTE_CACHE_IS_CLOSED_0 = new StringId(3043, "Remote cache is closed:  {0}");
  public static final StringId PartitionMessage_UNKNOWN_EXCEPTION = new StringId(3044, "Unknown exception");
  public static final StringId PartitionedIndex_NOT_SUPPORTED_ON_PARTITIONED_INDEX = new StringId(3045, "Not supported on partitioned index");

  public static final StringId PartitionedRegionDataStore_BUCKET_ID_0_NOT_FOUND_ON_VM_1 = new StringId(3049, "Bucket id  {0}  not found on VM  {1}");
  public static final StringId PartitionedRegionDataStore_ENTRY_NOT_FOUND = new StringId(3050, "entry not found");

  public static final StringId PartitionedRegionDataStore_REGION_HAS_BEEN_DESTROYED = new StringId(3053, "Region has been destroyed");

  public static final StringId PartitionedRegionDataStore_UNABLE_TO_FETCH_KEYS_ON_0 = new StringId(3055, "Unable to fetch keys on {0}");
  public static final StringId PartitionedRegionDataStore_UNABLE_TO_GET_ENTRY = new StringId(3056, "Unable to get Entry.");
  public static final StringId Oplog_UNEXPECTED_PRODUCT_VERSION_0 = new StringId(3057, "Unknown version ordinal {0} found when recovering Oplogs");
  public static final StringId PartitionedRegionDataStore_UNABLE_TO_GET_VALUE_FOR_KEY = new StringId(3058, "Unable to get value for key.");
  public static final StringId PartitionedRegionDataStore_UNABLE_TO_SERIALIZE_VALUE = new StringId(3059, "Unable to serialize value");
  public static final StringId PartitionedRegionHelper_NEW_PARTITIONEDREGIONCONFIG_0_DOES_NOT_HAVE_NEWER_VERSION_THAN_PREVIOUS_1 = new StringId(3060, "New PartitionedRegionConfig  {0}  does not have newer version than previous  {1}");

  public static final StringId ClientTXStateStub_COMMIT_FAILED_ON_SERVER = new StringId(3064, "Commit failed on cache server");
  public static final StringId PartitionedRegion_CAN_NOT_CREATE_PARTITIONEDREGION_FAILED_TO_ACQUIRE_REGIONLOCK = new StringId(3065, "Can not create PartitionedRegion (failed to acquire RegionLock).");
  public static final StringId PartitionedRegion_CAN_NOT_REUSE_OLD_PARTITIONED_REGION_ID_0 = new StringId(3066, "Can NOT reuse old Partitioned Region Id= {0}");
  public static final StringId PartitionedRegion_CREATE_OF_ENTRY_ON_0_FAILED = new StringId(3067, "Create of entry on  {0}  failed");
  public static final StringId PartitionedRegion_DATA_STORE_ON_THIS_VM_IS_NULL_AND_THE_LOCAL_MAX_MEMORY_IS_NOT_ZERO_0 = new StringId(3068, "Data Store on this vm is null and the local max Memory is not zero {0}");
  public static final StringId PartitionedRegion_DATA_STORE_ON_THIS_VM_IS_NULL_AND_THE_LOCAL_MAX_MEMORY_IS_NOT_ZERO_THE_DATA_POLICY_IS_0_AND_THE_LOCALMAXMEMEORY_IS_1 = new StringId(3069, "Data Store on this vm is null and the local max Memory is not zero, the data policy is  {0}  and the localMaxMemeory is :  {1}");

  public static final StringId PartitionedRegion_DESTROY_OF_ENTRY_ON_0_FAILED = new StringId(3071, "Destroy of entry on  {0}  failed");
  public static final StringId PartitionedRegion_ENTRY_DESTROYED = new StringId(3072, "entry destroyed");

  public static final StringId PartitionedRegion_ENTRY_NOT_FOUND_FOR_KEY_0 = new StringId(3074, "Entry not found for key  {0}");
  public static final StringId PartitionedRegion_FALSE_RESULT_WHEN_IFNEW_AND_IFOLD_IS_UNACCEPTABLE_RETRYING = new StringId(3075, "false result when !ifNew and !ifOld is unacceptable - retrying");

  public static final StringId PartitionedRegion_INVALIDATION_OF_ENTRY_ON_0_FAILED = new StringId(3077, "Invalidation of entry on  {0}  failed");
  public static final StringId PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY = new StringId(3078, "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  public static final StringId PartitionedRegion_FUNCTION_NOT_EXECUTED_AS_REGION_IS_EMPTY = new StringId(3079, "Region is empty and the function cannot be executed");

  public static final StringId PartitionedRegion_NULL_KEY_NOT_ALLOWED_FOR_PRIDTOPR_MAP = new StringId(3080, "null key not allowed for prIdToPR Map");
  public static final StringId PartitionedRegion_NULL_VALUE_NOT_ALLOWED_FOR_PRIDTOPR_MAP = new StringId(3081, "null value not allowed for prIdToPR Map");
  public static final StringId PartitionedRegion_PRIDMAPGET_NOT_SUPPORTED_USE_GETREGION_INSTEAD = new StringId(3082, "PRIdMap.get not supported - use getRegion instead");
  public static final StringId PartitionedRegion_PR_0_IS_LOCALLY_CLOSED = new StringId(3083, "PR  {0}  is locally closed");
  public static final StringId PartitionedRegion_PUTTING_ENTRY_ON_0_FAILED = new StringId(3084, "Putting entry on  {0}  failed");
  public static final StringId PartitionedRegion_QUERY_MUST_BE_A_SELECT_EXPRESSION_ONLY = new StringId(3085, "query must be a select expression only");
  public static final StringId PartitionedRegion_REGION_FOR_PRID_0_IS_DESTROYED = new StringId(3086, "Region for prId= {0}  is destroyed");
  public static final StringId PartitionedRegion_REGION_WITH_PRID_0_IS_LOCALLY_DESTROYED_ON_THIS_NODE = new StringId(3087, "Region with prId= {0}  is locally destroyed on this node");
  public static final StringId PartitionedRegion_REQUESTED_REDUNDANCY_0_IS_INCOMPATIBLE_WITH_EXISTING_REDUNDANCY_1 = new StringId(3088, "Requested redundancy  {0}  is incompatible with existing redundancy  {1}");
  public static final StringId PartitionedRegion_SCOPE_IN_PARTITIONATTRIBUTES_IS_INCOMPATIBLE_WITH_ALREADY_SET_SCOPESET_THE_SCOPE_TO_0 = new StringId(3089, "Scope in PartitionAttributes is incompatible with already set scope.Set the scope to  {0} .");
  public static final StringId PartitionedRegion_STATISTICS_DISABLED_FOR_REGION_0 = new StringId(3090, "Statistics disabled for region '' {0} ''");
  public static final StringId PartitionedRegion_THE_TOTAL_NUMBER_OF_BUCKETS_FOUND_IN_PARTITIONATTRIBUTES_0_IS_INCOMPATIBLE_WITH_THE_TOTAL_NUMBER_OF_BUCKETS_USED_BY_OTHER_DISTRIBUTED_MEMBERS_SET_THE_NUMBER_OF_BUCKETS_TO_1 = new StringId(3091, "The total number of buckets found in PartitionAttributes ( {0} ) is incompatible with the total number of buckets used by other distributed members. Set the number of buckets to  {1}");

  public static final StringId PartitionedRegion_TIME_OUT_LOOKING_FOR_TARGET_NODE_FOR_DESTROY_WAITED_0_MS = new StringId(3093, "Time out looking for target node for destroy; waited  {0}  ms");
  public static final StringId PartitionedRegion_UNEXPECTED_QUERY_EXCEPTION_OCCURED_DURING_QUERY_EXECUTION_0 = new StringId(3094, "Unexpected query exception occurred during query execution  {0}");
  public static final StringId PooledExecutorWithDMStats_EXECUTOR_HAS_BEEN_SHUTDOWN = new StringId(3095, "executor has been shutdown");
  public static final StringId PooledExecutorWithDMStats_INTERRUPTED = new StringId(3096, "interrupted");
  public static final StringId PreferBytesCachedDeserializable_VALUE_MUST_NOT_BE_NULL = new StringId(3097, "value must not be null");

  public static final StringId PrimaryKeyIndexCreationHelper_INVALID_INDEXED_EXPRESSOION_0 = new StringId(3099, "Invalid indexed expressoion : '' {0} ''");
  public static final StringId PrimaryKeyIndexCreationHelper_INVALID_INDEXED_EXPRESSOION_0_N_1 = new StringId(3100, "Invalid indexed expressoion : '' {0} ''\n {1}");
  public static final StringId PrimaryKeyIndexCreationHelper_INVALID_PROJECTION_ATTRIBUTES_0 = new StringId(3101, "Invalid projection attributes : '' {0} ''");
  public static final StringId PrimaryKeyIndexCreationHelper_THE_FROMCLAUSE_FOR_A_PRIMARY_KEY_INDEX_SHOULD_BE_A_REGION_PATH_ONLY = new StringId(3102, "The fromClause for a Primary Key index should be a Region Path only");
  public static final StringId PrimaryKeyIndexCreationHelper_THE_FROMCLAUSE_FOR_A_PRIMARY_KEY_INDEX_SHOULD_ONLY_HAVE_ONE_ITERATOR_AND_THE_COLLECTION_MUST_BE_A_REGION_PATH_ONLY = new StringId(3103, "The fromClause for a Primary Key index should only have one iterator and the collection must be a Region Path only");
  public static final StringId PrimaryKeyIndex_INVALID_OPERATOR = new StringId(3104, "Invalid Operator");
  public static final StringId PrimaryRequestMessage_FAILED_SENDING_0 = new StringId(3105, "Failed sending < {0} >");

  public static final StringId PureStatSampler_COULD_NOT_FIND_STATISTICS_INSTANCE = new StringId(3111, "Could not find statistics instance");
  public static final StringId PutMessage_DID_NOT_RECEIVE_A_VALID_REPLY = new StringId(3112, "did not receive a valid reply");
  public static final StringId PutMessage_FAILED_SENDING_0 = new StringId(3113, "Failed sending < {0} >");

  public static final StringId QCompiler_ONLY_ONE_INDEX_EXPRESSION_SUPPORTED = new StringId(3115, "Only one index expression supported");
  public static final StringId QCompiler_RANGES_NOT_SUPPORTED_IN_INDEX_OPERATORS = new StringId(3116, "Ranges not supported in index operators");

  public static final StringId QCompiler_SYNTAX_ERROR_IN_QUERY_0 = new StringId(3118, "Syntax error in query:  {0}");

  public static final StringId QCompiler_TYPE_NOT_FOUND_0 = new StringId(3120, "Type not found:  {0}");
  public static final StringId QRegion_REGION_CAN_NOT_BE_NULL = new StringId(3121, "Region can not be NULL");
  public static final StringId QRegion_REGION_VALUES_IS_NOT_MODIFIABLE = new StringId(3122, "Region values is not modifiable");
  public static final StringId QueryUtils_ANDOR_OPERANDS_MUST_BE_OF_TYPE_BOOLEAN_NOT_TYPE_0 = new StringId(3123, "AND/OR operands must be of type boolean, not type '' {0} ''");
  public static final StringId QueuedOperation_THE_0_SHOULD_NOT_HAVE_BEEN_QUEUED = new StringId(3124, "The  {0}  should not have been queued.");
  public static final StringId RangeIndex_COULD_NOT_ADD_OBJECT_OF_TYPE_0 = new StringId(3125, "Could not add object of type  {0}");
  public static final StringId RangeIndex_INVALID_OPERATOR = new StringId(3126, "Invalid Operator");
  public static final StringId RangeIndex_NOT_YET_IMPLEMENTED = new StringId(3127, "Not yet implemented");
  public static final StringId RangeIndex_OPERATOR_0 = new StringId(3128, "Operator =  {0}");
  public static final StringId RangeIndex_PROBLEM_IN_INDEX_QUERY = new StringId(3129, "Problem in index query");

  public static final StringId RegionAdminRequest_REGION_0_NOT_FOUND_IN_REMOTE_CACHE_1 = new StringId(3134, "Region  {0}  not found in remote cache {1}.");
  public static final StringId RegionAttributesCreation_CACHELISTENERS_ARE_NOT_THE_SAME = new StringId(3135, "CacheListeners are not the same");

  public static final StringId RegionAttributesCreation_CANNOT_REFERENCE_NONEXISTING_REGION_ATTRIBUTES_NAMED_0 = new StringId(3138, "Cannot reference non-existing region attributes named \"{0}\"");
  public static final StringId RegionAttributesCreation_CONCURRENCYLEVEL_IS_NOT_THE_SAME = new StringId(3139, "ConcurrencyLevel is not the same");
  public static final StringId RegionAttributesCreation_DATA_POLICIES_ARE_NOT_THE_SAME_THIS_0_OTHER_1 = new StringId(3140, "Data Policies are not the same: this:  {0}  other:  {1}");
  public static final StringId RegionAttributesCreation_DIR_SIZE_CANNOT_BE_NEGATIVE_0 = new StringId(3141, "Dir size cannot be negative :  {0}");
  public static final StringId RegionAttributesCreation_DISK_DIRS_ARE_NOT_THE_SAME = new StringId(3142, "Disk Dirs are not the same");
  public static final StringId RegionAttributesCreation_DISK_DIR_SIZES_ARE_NOT_THE_SAME = new StringId(3143, "Disk Dir Sizes are not the same");
  public static final StringId RegionAttributesCreation_DISTWRITEATTRIBUTES_ARE_NOT_THE_SAME = new StringId(3144, "DistWriteAttributes are not the same");
  public static final StringId RegionAttributesCreation_EARLY_ACK_IS_NOT_THE_SAME = new StringId(3145, "Early Ack is not the same");
  public static final StringId RegionAttributesCreation_ENABLE_ASYNC_CONFLATION_IS_NOT_THE_SAME = new StringId(3146, "Enable Async Conflation is not the same");
  public static final StringId RegionAttributesCreation_ENABLE_SUBSCRIPTION_CONFLATION_IS_NOT_THE_SAME = new StringId(3147, "Enable Subscription Conflation is not the same");

  public static final StringId RegionAttributesCreation_ENTRYIDLETIMEOUT_IS_NOT_THE_SAME = new StringId(3149, "EntryIdleTimeout is not the same");
  public static final StringId RegionAttributesCreation_ENTRYTIMETOLIVE_IS_NOT_THE_SAME = new StringId(3150, "EntryTimeToLive is not the same");
  public static final StringId RegionAttributesCreation_EVICTION_ATTRIBUTES_ARE_NOT_THE_SAME_THIS_0_OTHER_1 = new StringId(3151, "Eviction Attributes are not the same: this: {0} other: {1}");

  public static final StringId RegionAttributesCreation_IGNORE_JTA_IS_NOT_THE_SAME = new StringId(3153, "Ignore JTA is not the same");
  public static final StringId RegionAttributesCreation_INDEX_MAINTENANCE_SYNCHRONOUS_IS_NOT_THE_SAME = new StringId(3154, "Index Maintenance Synchronous is not the same");
  public static final StringId RegionAttributesCreation_INITIAL_CAPACITY_IS_NOT_THE_SAME = new StringId(3155, "initial Capacity is not the same");
  public static final StringId RegionAttributesCreation_KEY_CONSTRAINTS_ARE_NOT_THE_SAME = new StringId(3156, "Key Constraints are not the same");
  public static final StringId RegionAttributesCreation_LOAD_FACTORS_ARE_NOT_THE_SAME = new StringId(3157, "Load Factors are not the same");
  public static final StringId RegionAttributesCreation_MEMBERSHIP_ATTRIBUTES_ARE_NOT_THE_SAME = new StringId(3158, "Membership Attributes are not the same");
  public static final StringId RegionAttributesCreation_MORE_THAN_ONE_CACHE_LISTENER_EXISTS = new StringId(3159, "more than one cache listener exists");
  public static final StringId RegionAttributesCreation_NO_MIRROR_TYPE_CORRESPONDS_TO_DATA_POLICY_0 = new StringId(3160, "No mirror type corresponds to data policy \"{0}\"");
  public static final StringId RegionAttributesCreation_NUMBER_OF_DISKSIZES_IS_0_WHICH_IS_NOT_EQUAL_TO_NUMBER_OF_DISK_DIRS_WHICH_IS_1 = new StringId(3161, "Number of diskSizes is {0} which is not equal to number of disk Dirs which is {1}");
  public static final StringId RegionAttributesCreation_PARTITIONATTRIBUTES_ARE_NOT_THE_SAME_0_1 = new StringId(3162, "PartitionAttributes are not the same. this: {0}, other: {1}");
  public static final StringId RegionAttributesCreation_REGION_IDLE_TIMEOUT_IS_NOT_THE_SAME = new StringId(3163, "Region Idle Timeout is not the same");
  public static final StringId RegionAttributesCreation_SCOPE_IS_NOT_THE_SAME = new StringId(3164, "Scope is not the same");
  public static final StringId RegionAttributesCreation_STATISTICS_ENABLED_IS_NOT_THE_SAME_THIS_0_OTHER_1 = new StringId(3165, "Statistics enabled is not the same: this: {0} other: {1}");
  public static final StringId RegionAttributesCreation_SUBSCRIPTION_ATTRIBUTES_ARE_NOT_THE_SAME = new StringId(3166, "Subscription Attributes are not the same");
  public static final StringId RegionAttributesCreation_VALUE_CONSTRAINTS_ARE_NOT_THE_SAME = new StringId(3167, "Value Constraints are not the same");
  public static final StringId RegionAttributesCreation__0_WAS_NOT_AN_EXISTING_DIRECTORY = new StringId(3168, "\"{0}\" was not an existing directory.");
  public static final StringId RegionAttributesCreation_DISKSTORE_IS_NOT_THE_SAME_THIS_0_OTHER_1 = new StringId(3169, "DiskStore is not the same: this: {0} other: {1}");
  public static final StringId RegionAttributesCreation_DISKSYNCHRONOUS_IS_NOT_THE_SAME = new StringId(3327, "Disk Synchronous write is not the same.");
  public static final StringId RegionCreation_GETTING_SUBREGIONS_RECURSIVELY_IS_NOT_SUPPORTED = new StringId(3170, "Getting subregions recursively is not supported.");
  public static final StringId RegionCreation_REGION_ATTRIBUTES_DIFFER_THIS_0_OTHER_1 = new StringId(3171, "region attributes differ this:  {0}  other:  {1}");
  public static final StringId RegionCreation_REGION_NAMES_DIFFER_THIS_0_OTHER_1 = new StringId(3172, "region names differ: this:  {0}  other:  {1}");
  public static final StringId RegionCreation_WRITING_A_REGIONCREATION_TO_DISK_IS_NOT_SUPPORTED = new StringId(3173, "Writing a RegionCreation to disk is not supported.");
  public static final StringId RegionFactory_NO_ATTRIBUTES_ASSOCIATED_WITH_0 = new StringId(3174, "No attributes associated with  {0}");
  public static final StringId RegionResponse_UNKNOWN_REGIONREQUEST_OPERATION_0 = new StringId(3175, "Unknown RegionRequest operation:  {0}");
  public static final StringId RegisterInterest_CACHECLIENTPROXY_FOR_THIS_CLIENT_IS_NO_LONGER_ON_THE_SERVER_SO_REGISTERINTEREST_OPERATION_IS_UNSUCCESSFUL = new StringId(3176, "CacheClientProxy for this client is no longer on the server , so registerInterest operation is unsuccessful");
  public static final StringId ReliableMessageQueueFactoryImpl_REGIONS_WITH_MESSAGE_QUEUING_ALREADY_EXIST = new StringId(3177, "Regions with message queuing already exist");
  public static final StringId ReliableMessageQueueFactoryImpl_RELIABLE_MESSAGE_QUEUE_IS_CLOSED = new StringId(3178, "reliable message queue is closed");
  public static final StringId ReliableMessageQueueFactoryImpl_UNEXPECTED_QUEUEDREGIONDATA_0_FOR_REGION_1 = new StringId(3179, "unexpected QueuedRegionData  {0}  for region  {1}");
  public static final StringId RemoteBridgeServer_A_REMOTE_BRIDGESERVER_CANNOT_BE_STARTED = new StringId(3180, "A remote BridgeServer cannot be started.");
  public static final StringId RemoteBridgeServer_A_REMOTE_BRIDGESERVER_CANNOT_BE_STOPPED = new StringId(3181, "A remote BridgeServer cannot be stopped.");
  public static final StringId RemoteBridgeServer_CANNOT_GET_THE_CACHE_OF_A_REMOTE_BRIDGESERVER = new StringId(3182, "Cannot get the Cache of a remote BridgeServer.");
  public static final StringId RemoteGemFireVM_0_IS_UNREACHABLE_IT_HAS_EITHER_LEFT_OR_CRASHED = new StringId(3183, "{0}  is unreachable. It has either left or crashed.");
  public static final StringId RemoteGemFireVM_CANNOT_CREATE_A_REMOTEGEMFIREVM_WITH_A_NULL_ID = new StringId(3184, "Cannot create a RemoteGemFireVM with a null id.");
  public static final StringId RemoteGemFireVM_THE_ID_IF_THIS_REMOTEGEMFIREVM_IS_NULL = new StringId(3185, "The id of this RemoteGemFireVM is null!");
  public static final StringId RemoteGfManagerAgent_0_IS_NOT_CURRENTLY_CONNECTED = new StringId(3186, "{0}  is not currently connected.");
  public static final StringId RemoteGfManagerAgent_EXPECTED_0_TO_BE_A_REMOTETRANSPORTCONFIG = new StringId(3187, "Expected  {0}  to be a RemoteTransportConfig");
  public static final StringId RemoteGfManagerAgent_RECURSION_DETECTED_WHILE_SENDING_0 = new StringId(3188, "Recursion detected while sending  {0}");
  public static final StringId RemoteGfManagerAgent_UNKNOWN_VM_KIND = new StringId(3189, "Unknown VM Kind");
  public static final StringId RemoteGfManagerAgent_UNKNOWN_VM_KIND_0 = new StringId(3190, "Unknown VM Kind:  {0}");
  public static final StringId RemoteRegionAttributes_MORE_THAN_ONE_CACHE_LISTENER_EXISTS = new StringId(3191, "More than one cache listener exists.");
  public static final StringId RemoteRegionAttributes_NO_MIRROR_TYPE_CORRESPONDS_TO_DATA_POLICY_0 = new StringId(3192, "No mirror type corresponds to data policy \"{0}\"");
  public static final StringId RemoteTransportConfig_EXPECTED_AT_LEAST_ONE_HOSTPORT_ID = new StringId(3193, "expected at least one host/port id");
  public static final StringId RemoveIndexesMessage_COULD_NOT_GET_PARTITIONED_REGION_FROM_ID_0_FOR_MESSAGE_1_RECEIVED_ON_MEMBER_2_MAP_3 = new StringId(3194, "Could not get Partitioned Region from Id  {0}  for message  {1}  received on member= {2}  map= {3}");
  public static final StringId RemoveIndexesMessage_REGION_IS_LOCALLY_DESTROYED_ON_0 = new StringId(3195, "Region is locally destroyed on  {0}");
  public static final StringId ReplyException_UNEXPECTED_EXCEPTION_ON_MEMBER_0 = new StringId(3196, "unexpected exception on member {0}");
  public static final StringId ReplyProcessor21_ABORTED_DUE_TO_SHUTDOWN = new StringId(3197, "aborted due to shutdown");
  public static final StringId ReplyProcessor21_THIS_REPLY_PROCESSOR_HAS_ALREADY_BEEN_REMOVED_FROM_THE_PROCESSOR_KEEPER = new StringId(3198, "This reply processor has already been removed from the processor keeper");
  public static final StringId RequiredRoles_REGION_HAS_NOT_BEEN_CONFIGURED_WITH_REQUIRED_ROLES = new StringId(3199, "Region has not been configured with required roles.");
  public static final StringId RequiredRoles_REGION_MUST_BE_SPECIFIED = new StringId(3200, "Region must be specified");
  public static final StringId ResultsBag_NEXT_MUST_BE_CALLED_BEFORE_REMOVE = new StringId(3201, "next() must be called before remove()");
  public static final StringId ResultsBag_THIS_COLLECTION_DOES_NOT_SUPPORT_STRUCT_ELEMENTS = new StringId(3202, "This collection does not support struct elements");
  public static final StringId ResultsCollectionWrapper_CONSTRAINT_CANNOT_BE_NULL = new StringId(3203, "constraint cannot be null");
  public static final StringId ResultsCollectionWrapper_CONSTRAINT_CLASS_MUST_BE_PUBLIC = new StringId(3204, "constraint class must be public");
  public static final StringId ResultsSet_THIS_COLLECTION_DOES_NOT_SUPPORT_STRUCT_ELEMENTS = new StringId(3205, "This collection does not support struct elements");
  public static final StringId ResumptionAction_INVALID_RESUMPTIONACTION_NAME_0 = new StringId(3206, "Invalid ResumptionAction name:  {0}");
  public static final StringId RuntimeIterator_ELEMENTTYPE_ANDOR_CMPITERATORDEFN_SHOULD_NOT_BE_NULL = new StringId(3207, "elementType and/or cmpIteratorDefn should not be null");
  public static final StringId SearchLoadAndWriteProcessor_ERROR_PROCESSING_REQUEST = new StringId(3208, "Error processing request");
  public static final StringId SearchLoadAndWriteProcessor_MESSAGE_NOT_SERIALIZABLE = new StringId(3209, "Message not serializable");
  public static final StringId SearchLoadAndWriteProcessor_NO_CACHEWRITER_DEFINED_0 = new StringId(3210, "No cachewriter defined {0}");
  public static final StringId SearchLoadAndWriteProcessor_NO_LOADER_DEFINED_0 = new StringId(3211, "No loader defined {0}");

  public static final StringId SearchLoadAndWriteProcessor_TIMED_OUT_LOCKING_0_BEFORE_LOAD = new StringId(3213, "Timed out locking  {0}  before load");
  public static final StringId SearchLoadAndWriteProcessor_TIMEOUT_EXPIRED_OR_REGION_NOT_READY_0 = new StringId(3214, "Timeout expired or region not ready {0}");
  public static final StringId ServerConnection_THE_ID_PASSED_IS_0_WHICH_DOES_NOT_CORRESPOND_WITH_ANY_TRANSIENT_DATA = new StringId(3215, "The ID passed is  {0}  which does not correspond with any transient data");

  public static final StringId SizeMessage_0_1_NO_DATASTORE_HERE_2 = new StringId(3221, "{0}  {1} no datastore here {2}");
  public static final StringId SmHelper_IS_PRIMITIVE = new StringId(3222, "Is primitive");
  public static final StringId SmHelper_POINTERSIZEBYTES_UNAVAILABLE_IN_PURE_MODE = new StringId(3223, "pointerSizeBytes unavailable in pure mode");
  public static final StringId HandShake_MISMATCH_IN_CHALLENGE_BYTES_MALICIOUS_CLIENT = new StringId(3224, "Mismatch in challenge bytes. Malicious client?");
  public static final StringId SocketCreator_FAILED_TO_CREATE_SERVER_SOCKET_ON_0_1 = new StringId(3225, "Failed to create server socket on  {0}[{1}]");
  public static final StringId HandShake_SERVER_PRIVATE_KEY_NOT_AVAILABLE_FOR_CREATING_SIGNATURE = new StringId(3226, "Server private key not available for creating signature.");
  public static final StringId SortedResultSet_THIS_COLLECTION_DOES_NOT_SUPPORT_STRUCT_ELEMENTS = new StringId(3227, "This collection does not support struct elements");
  public static final StringId SortedStructSet_ELEMENT_TYPE_MUST_BE_STRUCT = new StringId(3228, "element type must be struct");
  public static final StringId SortedStructSet_OBJ_DOES_NOT_HAVE_THE_SAME_STRUCTTYPE = new StringId(3229, "obj does not have the same StructType");
  public static final StringId SortedStructSet_STRUCTTYPE_MUST_NOT_BE_NULL = new StringId(3230, "structType must not be null");
  public static final StringId SortedStructSet_THIS_SET_ONLY_ACCEPTS_STRUCTIMPL = new StringId(3231, "This set only accepts StructImpl");
  public static final StringId SortedStructSet_TYPES_DONT_MATCH = new StringId(3232, "types do not match");
  public static final StringId StatArchiveReader_ARCHIVE_VERSION_0_IS_NO_LONGER_SUPPORTED = new StringId(3233, "Archive version:  {0}  is no longer supported.");
  public static final StringId StatArchiveReader_CANT_COMBINE_DIFFERENT_STATS = new StringId(3234, "Cannot combine different stats.");
  public static final StringId StatArchiveReader_CANT_COMBINE_VALUES_WITH_DIFFERENT_FILTERS = new StringId(3235, "Cannot combine values with different filters.");
  public static final StringId StatArchiveReader_CANT_COMBINE_VALUES_WITH_DIFFERENT_TYPES = new StringId(3236, "Cannot combine values with different types.");
  public static final StringId StatArchiveReader_FILTER_VALUE_0_MUST_BE_1_2_OR_3 = new StringId(3237, "Filter value \"{0}\" must be {1}, {2}, or {3}.");
  public static final StringId StatArchiveReader_GETVALUESEX_DIDNT_FILL_THE_LAST_0_ENTRIES_OF_ITS_RESULT = new StringId(3238, "getValuesEx did not fill the last  {0}  entries of its result.");
  public static final StringId StatArchiveReader_UNEXPECTED_TOKEN_BYTE_VALUE_0 = new StringId(3239, "Unexpected token byte value:  {0}");
  public static final StringId StatArchiveReader_UNEXPECTED_TYPECODE_0 = new StringId(3240, "Unexpected typecode  {0}");
  public static final StringId StatArchiveReader_UNEXPECTED_TYPECODE_VALUE_0 = new StringId(3241, "unexpected typeCode value  {0}");
  public static final StringId StatArchiveReader_UNSUPPORTED_ARCHIVE_VERSION_0_THE_SUPPORTED_VERSION_IS_1 = new StringId(3242, "Unsupported archive version:  {0} .  The supported version is:  {1} .");
  public static final StringId StatArchiveReader_UPDATE_OF_THIS_TYPE_OF_FILE_IS_NOT_SUPPORTED = new StringId(3243, "update of this type of file is not supported.");
  public static final StringId StatArchiveWriter_COULD_NOT_ARCHIVE_TYPE_0_BECAUSE_IT_HAD_MORE_THAN_1_STATISTICS = new StringId(3244, "Could not archive type \"{0}\" because it had more than {1} statistics.");
  public static final StringId StatArchiveWriter_COULD_NOT_CLOSE_STATARCHIVER_FILE = new StringId(3245, "Could not close statArchiver file");
  public static final StringId StatArchiveWriter_COULD_NOT_OPEN_0 = new StringId(3246, "Could not open \"{0}\"");
  public static final StringId StatArchiveWriter_EXPECTED_IDX_TO_BE_GREATER_THAN_2_IT_WAS_0_FOR_THE_VALUE_1 = new StringId(3247, "Expected idx to be greater than 2. It was  {0}  for the value  {1}");
  public static final StringId StatArchiveWriter_FAILED_WRITING_DELETE_RESOURCE_INSTANCE_TO_STATISTIC_ARCHIVE = new StringId(3248, "Failed writing delete resource instance to statistic archive");
  public static final StringId StatArchiveWriter_FAILED_WRITING_HEADER_TO_STATISTIC_ARCHIVE = new StringId(3249, "Failed writing header to statistic archive");
  public static final StringId StatArchiveWriter_FAILED_WRITING_NEW_RESOURCE_INSTANCE_TO_STATISTIC_ARCHIVE = new StringId(3250, "Failed writing new resource instance to statistic archive");
  public static final StringId StatArchiveWriter_FAILED_WRITING_NEW_RESOURCE_TYPE_TO_STATISTIC_ARCHIVE = new StringId(3251, "Failed writing new resource type to statistic archive");
  public static final StringId StatArchiveWriter_FAILED_WRITING_SAMPLE_TO_STATISTIC_ARCHIVE = new StringId(3252, "Failed writing sample to statistic archive");
  public static final StringId StatArchiveWriter_METHOD_UNIMPLEMENTED = new StringId(3253, "method unimplemented");
  public static final StringId StatArchiveWriter_TIMESTAMP_DELTA_0_WAS_GREATER_THAN_1 = new StringId(3254, "timeStamp delta  {0}  was greater than  {1}");
  public static final StringId StatArchiveWriter_UNEXPECTED_TYPE_CODE_0 = new StringId(3255, "Unexpected type code  {0}");

  public static final StringId SizeMessage_0_COULD_NOT_FIND_PARTITIONED_REGION_WITH_ID_1 = new StringId(3257, "{0} : could not find partitioned region with Id  {1}");
  public static final StringId StatisticDescriptorImpl_UNKNOWN_TYPE_CODE_0 = new StringId(3258, "Unknown type code:  {0}");
  public static final StringId StatisticResourceImpl_FAILED_TO_REFRESH_STATISTICS_0_FOR_1 = new StringId(3259, "Failed to refresh statistics {0} for {1}");
  public static final StringId StatisticResourceJmxImpl_MANAGEDBEAN_IS_NULL = new StringId(3260, "ManagedBean is null");
  public static final StringId StatisticsImpl_UNEXPECTED_STAT_DESCRIPTOR_TYPE_CODE_0 = new StringId(3261, "unexpected stat descriptor type code:  {0}");
  public static final StringId StatisticsTypeFactoryImpl_STATISTICS_TYPE_NAMED_0_ALREADY_EXISTS = new StringId(3262, "Statistics type named  {0}  already exists.");
  public static final StringId StatisticsTypeImpl_CANNOT_HAVE_A_NULL_STATISTICS_TYPE_NAME = new StringId(3263, "Cannot have a null statistics type name.");
  public static final StringId StatisticsTypeImpl_CANNOT_HAVE_A_NULL_STATISTIC_DESCRIPTORS = new StringId(3264, "Cannot have a null statistic descriptors.");
  public static final StringId StatisticsTypeImpl_DUPLICATE_STATISTICDESCRIPTOR_NAMED_0 = new StringId(3265, "Duplicate StatisticDescriptor named  {0}");
  public static final StringId StatisticsTypeImpl_THERE_IS_NO_STATISTIC_NAMED_0 = new StringId(3266, "There is no statistic named \"{0}\"");
  public static final StringId StatisticsTypeImpl_THE_REQUESTED_DESCRIPTOR_COUNT_0_EXCEEDS_THE_MAXIMUM_WHICH_IS_1 = new StringId(3267, "The requested descriptor count  {0}  exceeds the maximum which is   {1} .");
  public static final StringId StatisticsTypeXml_DTD_NOT_FOUND_0 = new StringId(3268, "DTD not found:  {0}");
  public static final StringId StatisticsTypeXml_FAILED_PARSING_XML = new StringId(3269, "Failed parsing XML");
  public static final StringId StatisticsTypeXml_FAILED_READING_XML_DATA = new StringId(3270, "Failed reading XML data");
  public static final StringId StatisticsTypeXml_FAILED_READING_XML_DATA_NO_DOCUMENT = new StringId(3271, "Failed reading XML data; no document");
  public static final StringId StatisticsTypeXml_FAILED_READING_XML_DATA_NO_ROOT_ELEMENT = new StringId(3272, "Failed reading XML data; no root element");
  public static final StringId StatisticsTypeXml_INVALID_PUBLIC_ID_0 = new StringId(3273, "Invalid public ID: '' {0} ''");
  public static final StringId StatisticsTypeXml_UNEXPECTED_STORAGE_TYPE_0 = new StringId(3274, "unexpected storage type  {0}");
  public static final StringId StopWatch_ATTEMPTED_TO_STOP_NONRUNNING_STOPWATCH = new StringId(3275, "Attempted to stop non-running StopWatch");
  public static final StringId StoppableCountDownOrUpLatch_COUNT_0 = new StringId(3276, "count < 0");
  public static final StringId StoppableNonReentrantLock_LOCK_REENTRY_IS_NOT_ALLOWED = new StringId(3277, "Lock reentry is not allowed");

  public static final StringId StreamingPartitionOperation_CALL_GETPARTITIONEDDATAFROM_INSTEAD = new StringId(3279, "call getPartitionedDataFrom instead");
  public static final StringId StreamingPartitionOperation_USE_GETNEXTREPLYOBJECTPARTITIONEDREGION_INSTEAD = new StringId(3280, "use getNextReplyObject(PartitionedRegion) instead");

  public static final StringId StructBag_ELEMENT_TYPE_MUST_BE_STRUCT = new StringId(3284, "element type must be struct");
  public static final StringId StructBag_EXPECTED_AN_OBJECT_BUT_ACTUAL_IS_0 = new StringId(3285, "Expected an Object[], but actual is  {0}");
  public static final StringId StructBag_OBJ_DOES_NOT_HAVE_THE_SAME_STRUCTTYPE = new StringId(3286, "obj does not have the same StructType.; collection structype ={0}; added obj type={1}");
  public static final StringId StructBag_STRUCTTYPE_MUST_NOT_BE_NULL = new StringId(3287, "structType must not be null");
  public static final StringId StructBag_THIS_SET_ONLY_ACCEPTS_STRUCTIMPL = new StringId(3288, "This set only accepts StructImpl");
  public static final StringId StructBag_TYPES_DONT_MATCH = new StringId(3289, "types do not match");
  public static final StringId StructImpl_TYPE_MUST_NOT_BE_NULL = new StringId(3290, "type must not be null");
  public static final StringId StructSet_ELEMENT_TYPE_MUST_BE_STRUCT = new StringId(3291, "element type must be struct");
  public static final StringId StructSet_OBJ_DOES_NOT_HAVE_THE_SAME_STRUCTTYPE_REQUIRED_0_ACTUAL_1 = new StringId(3292, "obj does not have the same StructType: required:  {0} , actual:  {1}");
  public static final StringId StructSet_STRUCTTYPE_MUST_NOT_BE_NULL = new StringId(3293, "structType must not be null");
  public static final StringId StructSet_THIS_SET_ONLY_ACCEPTS_STRUCTIMPL = new StringId(3294, "This set only accepts StructImpl");
  public static final StringId StructSet_TYPES_DONT_MATCH = new StringId(3295, "types do not match");
  public static final StringId StructTypeImpl_FIELDNAMES_MUST_NOT_BE_NULL = new StringId(3296, "fieldNames must not be null");
  public static final StringId StructTypeImpl_FIELDNAME_0_NOT_FOUND = new StringId(3297, "fieldName {0}  not found");
  public static final StringId SystemAdmin_ADDRESS_VALUE_WAS_NOT_A_KNOWN_IP_ADDRESS_0 = new StringId(3298, "-address value was not a known IP address:  {0}");
  public static final StringId SystemAdmin_COULD_NOT_CREATE_FILE_0_FOR_OUTPUT_BECAUSE_1 = new StringId(3299, "Could not create file \"{0}\" for output because  {1}");
  public static final StringId SystemAdmin_COULD_NOT_EXEC_0 = new StringId(3300, "Could not exec \"{0}\".");
  public static final StringId SystemAdmin_COULD_NOT_OPEN_TO_0_FOR_READING_BECAUSE_1 = new StringId(3301, "Could not open to \"{0}\" for reading because  {1}");
  public static final StringId SystemAdmin_FAILED_READING_0 = new StringId(3302, "Failed reading \"{0}\"");

  public static final StringId SystemAdmin_LOCATOR_IN_DIRECTORY_0_IS_NOT_RUNNING = new StringId(3304, "Locator in directory \"{0}\" is not running.");
  public static final StringId SystemAdmin_LOGFILE_0_COULD_NOT_BE_OPENED_FOR_WRITING_VERIFY_FILE_PERMISSIONS_AND_THAT_ANOTHER_LOCATOR_IS_NOT_ALREADY_RUNNING = new StringId(3305, "Logfile \"{0}\" could not be opened for writing. Verify file permissions are correct and that another locator is not already running in the same directory.");
  public static final StringId SystemAdmin_START_OF_LOCATOR_FAILED_CHECK_END_OF_0_FOR_REASON = new StringId(3306, "Start of locator failed. Check end of \"{0}\" for reason.");
  public static final StringId SystemAdmin_START_OF_LOCATOR_FAILED_THE_END_OF_0_CONTAINED_THIS_MESSAGE_1 = new StringId(3307, "Start of locator failed. The end of \"{0}\" contained this message: \"{1}\".");
  public static final StringId SystemAdmin_THE_ARCHIVE_AND_DIR_OPTIONS_ARE_MUTUALLY_EXCLUSIVE = new StringId(3308, "The \"-archive=\" and \"-dir=\" options are mutually exclusive.");
  public static final StringId SystemAdmin_THE_NOFILTER_AND_PERSEC_OPTIONS_ARE_MUTUALLY_EXCLUSIVE = new StringId(3309, "The \"-nofilter\" and \"-persec\" options are mutually exclusive.");
  public static final StringId SystemAdmin_THE_PERSAMPLE_AND_NOFILTER_OPTIONS_ARE_MUTUALLY_EXCLUSIVE = new StringId(3310, "The \"-persample\" and \"-nofilter\" options are mutually exclusive.");
  public static final StringId SystemAdmin_THE_PERSAMPLE_AND_PERSEC_OPTIONS_ARE_MUTUALLY_EXCLUSIVE = new StringId(3311, "The \"-persample\" and \"-persec\" options are mutually exclusive.");
  public static final StringId SystemAdmin_TIME_WAS_NOT_IN_THIS_FORMAT_0_1 = new StringId(3312, "Time was not in this format \"{0}\".  {1}");
  public static final StringId SystemAdmin_TROUBLE_MERGING_LOG_FILES = new StringId(3313, "trouble merging log files.");
  public static final StringId SystemAdmin_UNEXPECTED_VALID_OPTION_0 = new StringId(3314, "unexpected valid option  {0}");
  public static final StringId SystemAdmin_UNHANDLED_ALIAS_0 = new StringId(3315, "Unhandled alias  {0}");
  public static final StringId SystemAdmin__0_IS_NOT_A_VALID_IP_ADDRESS_FOR_THIS_MACHINE = new StringId(3316, "'' {0} '' is not a valid IP address for this machine");
  public static final StringId SystemFailure_YOU_ARE_NOT_PERMITTED_TO_UNSET_A_SYSTEM_FAILURE = new StringId(3317, "You are not permitted to un-set a system failure.");
  public static final StringId SystemMemberBridgeServerImpl_CANNOT_CHANGE_THE_CONFIGURATION_OF_A_RUNNING_BRIDGE_SERVER = new StringId(3318, "Cannot change the configuration of a running bridge server.");
  public static final StringId SystemMemberCacheImpl_THE_VM_0_DOES_NOT_CURRENTLY_HAVE_A_CACHE = new StringId(3319, "The VM  {0}  does not currently have a cache.");
  public static final StringId SystemMemberCacheJmxImpl_MANAGEDBEAN_IS_NULL = new StringId(3320, "ManagedBean is null");
  public static final StringId SystemMemberCacheJmxImpl_THIS_CACHE_DOES_NOT_CONTAIN_REGION_0 = new StringId(3321, "This cache does not contain region \"{0}\"");
  public static final StringId SystemMemberImpl_FAILED_TO_REFRESH_CONFIGURATION_PARAMETERS_FOR_0 = new StringId(3322, "Failed to refresh configuration parameters for: {0}");
  public static final StringId SystemMemberJmx_MANAGEDBEAN_IS_NULL = new StringId(3323, "ManagedBean is null");
  public static final StringId SystemMemberJmx_THIS_SYSTEM_MEMBER_DOES_NOT_HAVE_A_CACHE = new StringId(3324, "This System Member does not have a Cache.");
  public static final StringId TCPConduit_INTERRUPTED = new StringId(3326, "interrupted");

  public static final StringId TCPConduit_TCPIP_CONNECTION_LOST_AND_MEMBER_IS_NOT_IN_VIEW = new StringId(3328, "TCP/IP connection lost and member is not in view");
  public static final StringId TCPConduit_TCP_LAYER_HAS_BEEN_SHUTDOWN = new StringId(3329, "tcp layer has been shutdown");
  public static final StringId TCPConduit_THE_CONDUIT_IS_STOPPED = new StringId(3330, "The conduit is stopped");
  public static final StringId TCPConduit_UNABLE_TO_INITIALIZE_CONNECTION_TABLE = new StringId(3331, "Unable to initialize connection table");
  public static final StringId TCPConduit_UNABLE_TO_RECONNECT_TO_SERVER_POSSIBLE_SHUTDOWN_0 = new StringId(3332, "Unable to reconnect to server; possible shutdown:  {0}");
  public static final StringId TCPConduit_WHILE_CREATING_HANDSHAKE_POOL = new StringId(3333, "while creating handshake pool");
  public static final StringId TXCommitMessage_REGION_NOT_FOUND = new StringId(3334, "Region not found");
  public static final StringId TXEntryState_CONFLICT_CAUSED_BY_CACHE_EXCEPTION = new StringId(3335, "Conflict caused by cache exception");
  public static final StringId TXEntryState_ENTRY_FOR_KEY_0_ON_REGION_1_HAD_ALREADY_BEEN_CHANGED_FROM_2_TO_3 = new StringId(3336, "Entry for key  {0}  on region  {1}  had already been changed from  {2}  to  {3}");
  public static final StringId TXEntryState_ENTRY_FOR_KEY_0_ON_REGION_1_HAD_A_STATE_CHANGE = new StringId(3337, "Entry for key  {0}  on region  {1}  had a state change");
  public static final StringId TXEntryState_OPCODE_0_SHOULD_NEVER_BE_REQUESTED = new StringId(3338, "OpCode  {0}  should never be requested");
  public static final StringId TXEntryState_PREVIOUS_OP_0_UNEXPECTED_FOR_REQUESTED_OP_1 = new StringId(3339, "Previous op  {0}  unexpected for requested op  {1}");
  public static final StringId TXEntryState_UNEXPECTED_CURRENT_OP_0_FOR_REQUESTED_OP_1 = new StringId(3340, "Unexpected current op  {0}  for requested op  {1}");
  public static final StringId TXEntryState_UNHANDLED_0 = new StringId(3341, "Unhandled  {0}");
  public static final StringId TXEntryState_UNHANDLED_OP_0 = new StringId(3342, "<unhandled op  {0}  >");
  public static final StringId TXEntryUserAttrState_ENTRY_USER_ATTRIBUTE_FOR_KEY_0_ON_REGION_1_HAD_ALREADY_BEEN_CHANGED_TO_2 = new StringId(3343, "Entry user attribute for key  {0}  on region  {1}  had already been changed to  {2}");
  public static final StringId TXLockServiceImpl_CONCURRENT_TRANSACTION_COMMIT_DETECTED_0 = new StringId(3344, "Concurrent transaction commit detected {0}");
  public static final StringId TXLockServiceImpl_CONCURRENT_TRANSACTION_COMMIT_DETECTED_BECAUSE_REQUEST_WAS_INTERRUPTED = new StringId(3345, "Concurrent transaction commit detected because request was interrupted.");
  public static final StringId TXLockServiceImpl_FAILED_TO_REQUEST_TRY_LOCKS_FROM_GRANTOR_0 = new StringId(3346, "Failed to request try locks from grantor: {0}");
  public static final StringId TXLockServiceImpl_INVALID_TXLOCKID_NOT_FOUND_0 = new StringId(3347, "Invalid txLockId not found:  {0}");
  public static final StringId TXLockServiceImpl_INVALID_UPDATEDPARTICIPANTS_NULL = new StringId(3348, "Invalid updatedParticipants, null");
  public static final StringId TXLockServiceImpl_REGIONLOCKREQS_MUST_NOT_BE_NULL = new StringId(3349, "regionLockReqs must not be null");
  public static final StringId TXLockServiceImpl_TXLOCKSERVICE_CANNOT_BE_CREATED_UNTIL_CONNECTED_TO_DISTRIBUTED_SYSTEM = new StringId(3350, "TXLockService cannot be created until connected to distributed system.");
  public static final StringId TXManagerImpl_ADDLISTENER_PARAMETER_WAS_NULL = new StringId(3351, "addListener parameter was null");
  public static final StringId TXManagerImpl_INITLISTENERS_PARAMETER_HAD_A_NULL_ELEMENT = new StringId(3352, "initListeners parameter had a null element");
  public static final StringId TXManagerImpl_MORE_THAN_ONE_TRANSACTION_LISTENER_EXISTS = new StringId(3353, "More than one transaction listener exists.");
  public static final StringId TXManagerImpl_REMOVELISTENER_PARAMETER_WAS_NULL = new StringId(3354, "removeListener parameter was null");
  public static final StringId TXManagerImpl_THREAD_DOES_NOT_HAVE_AN_ACTIVE_TRANSACTION = new StringId(3355, "Thread does not have an active transaction");
  public static final StringId TXManagerImpl_TRANSACTION_0_ALREADY_IN_PROGRESS = new StringId(3356, "Transaction  {0}  already in progress");

  public static final StringId TXRegionState_OPERATIONS_ON_GLOBAL_REGIONS_ARE_NOT_ALLOWED_BECAUSE_THIS_THREAD_HAS_AN_ACTIVE_TRANSACTION = new StringId(3357, "Operations on global regions are not allowed because this thread has an active transaction");
  public static final StringId TXManagerImpl_CANNOT_CHANGE_TRANSACTION_MODE_WHILE_TRANSACTIONS_ARE_IN_PROGRESS = new StringId(3358, "Transaction mode cannot be changed when the thread has an active transaction");
  public static final StringId TXRegionState_OPERATIONS_ON_PERSISTBACKUP_REGIONS_ARE_NOT_ALLOWED_BECAUSE_THIS_THREAD_HAS_AN_ACTIVE_TRANSACTION = new StringId(3359, "Operations on persist-backup regions are not allowed because this thread has an active transaction");
  public static final StringId TXReservationMgr_THE_KEY_0_IN_REGION_1_WAS_BEING_MODIFIED_BY_ANOTHER_TRANSACTION_LOCALLY = new StringId(3360, "The key  {0}  in region  {1}  was being modified by another transaction locally.");
  public static final StringId TXState_CONFLICT_DETECTED_IN_GEMFIRE_TRANSACTION_0 = new StringId(3361, "Conflict detected in GemFire transaction  {0}");
  public static final StringId TransactionImpl_TRANSACTIONIMPLREGISTERSYNCHRONIZATIONSYNCHRONIZATION_IS_NULL = new StringId(3362, "TransactionImpl::registerSynchronization:Synchronization is null");
  public static final StringId TransactionManagerImpl_NO_TRANSACTION_PRESENT = new StringId(3363, "no transaction present");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGER_INVALID = new StringId(3364, "TransactionManager invalid");

  public static final StringId TypeUtils_BOOLEANS_CAN_ONLY_BE_COMPARED_WITH_BOOLEANS = new StringId(3374, "Booleans can only be compared with booleans");
  public static final StringId TypeUtils_BOOLEAN_VALUES_CAN_ONLY_BE_COMPARED_WITH_OR = new StringId(3375, "Boolean values can only be compared with = or <>");
  public static final StringId TypeUtils_INDEXES_ARE_NOT_SUPPORTED_FOR_TYPE_0 = new StringId(3376, "Indexes are not supported for type '' {0} ''");
  public static final StringId TypeUtils_INDEXES_ARE_NOT_SUPPORTED_ON_PATHS_OF_TYPE_0 = new StringId(3377, "Indexes are not supported on paths of type '' {0} ''");
  public static final StringId TypeUtils_UNABLE_TO_COMPARE_OBJECT_OF_TYPE_0_WITH_OBJECT_OF_TYPE_1 = new StringId(3378, "Unable to compare object of type '' {0} '' with object of type '' {1} ''");

  public static final StringId TypeUtils_UNABLE_TO_USE_A_RELATIONAL_COMPARISON_OPERATOR_TO_COMPARE_AN_INSTANCE_OF_CLASS_0_WITH_AN_INSTANCE_OF_1 = new StringId(3380, "Unable to use a relational comparison operator to compare an instance of class '' {0} '' with an instance of '' {1} ''");
  public static final StringId TypeUtils_UNKNOWN_OPERATOR_0 = new StringId(3381, "Unknown operator:  {0}");
  public static final StringId UniqueIdGenerator_ID_MAX_ID_0 = new StringId(3382, "id > MAX_ID:  {0}");
  public static final StringId UniqueIdGenerator_NEGATIVE_ID_0 = new StringId(3383, "negative id:  {0}");
  public static final StringId UniqueIdGenerator_NUMIDS_0 = new StringId(3384, "numIds < 0");
  public static final StringId UniqueIdGenerator_RAN_OUT_OF_MESSAGE_IDS = new StringId(3385, "Ran out of message ids");
  public static final StringId UniversalMembershipListenerAdapter_ARGUMENT_HISTORYSIZE_MUST_BE_BETWEEN_10_AND_INTEGERMAX_INT_0 = new StringId(3386, "Argument historySize must be between 10 and Integer.MAX_INT:  {0} .");
  public static final StringId VMCachedDeserializable_VALUE_MUST_NOT_BE_NULL = new StringId(3387, "value must not be null");

  public static final StringId AdminDistributedSystem_UNEXPECTED_ADMINEXCEPTION = new StringId(3389, "Unexpected AdminException");
  public static final StringId InternalDistributedSystem_A_CONNECTION_TO_A_DISTRIBUTED_SYSTEM_ALREADY_EXISTS_IN_THIS_VM_IT_HAS_THE_FOLLOWING_CONFIGURATION_0 = new StringId(3390, "A connection to a distributed system already exists in this VM.  It has the following configuration:\n{0}");
  public static final StringId AbstractConfig_THE_0_CONFIGURATION_ATTRIBUTE_CAN_NOT_BE_MODIFIED = new StringId(3391, "The \"{0}\" configuration attribute can not be modified.");

  public static final StringId LoaderHelperImpl_CANNOT_NETSEARCH_FOR_A_SCOPELOCAL_OBJECT = new StringId(3394, "Cannot netSearch for a Scope.LOCAL object");
  public static final StringId DistributedRegion_OPERATION_IS_DISALLOWED_BY_LOSSACTION_0_BECAUSE_THESE_REQUIRED_ROLES_ARE_MISSING_1 = new StringId(3395, "Operation is disallowed by LossAction {0} because these required roles are missing: {1}.");
  public static final StringId DistributedRegion_OPERATION_DISTRIBUTION_MAY_HAVE_FAILED_TO_NOTIFY_THESE_REQUIRED_ROLES_0 = new StringId(3396, "Operation distribution may have failed to notify these required roles: {0}");
  public static final StringId DistributedRegion_OPERATION_DISTRIBUTION_WAS_NOT_DONE_TO_THESE_REQUIRED_ROLES_0 = new StringId(3397, "Operation distribution was not done to these required roles: {0}");
  public static final StringId DistributedCacheOperation_THE_CACHE_HAS_BEEN_CLOSED = new StringId(3398, "The cache has been closed");

  public static final StringId DataSerializer_NO_INSTANTIATOR_HAS_BEEN_REGISTERED_FOR_CLASS_WITH_ID_0 = new StringId(3413, "No Instantiator has been registered for class with id  {0}");
  public static final StringId DataSerializer_COULD_NOT_INSTANTIATE_AN_INSTANCE_OF_0 = new StringId(3414, "Could not instantiate an instance of  {0}");
  public static final StringId DataSerializer_WHILE_INSTANTIATING_AN_INSTANCE_OF_0 = new StringId(3415, "While instantiating an instance of  {0}");
  public static final StringId Instantiator_CANNOT_REGISTER_A_NULL_CLASS = new StringId(3416, "Cannot register a null class.");
  public static final StringId AdminDistributedSystemImpl_WHILE_WAITING_FOR_FUTURE = new StringId(3417, "While waiting for Future");
  public static final StringId AdminDistributedSystemImpl_ONLY_ONE_ADMINDISTRIBUTEDSYSTEM_CONNECTION_CAN_BE_MADE_AT_ONCE = new StringId(3419, "Only one AdminDistributedSystem connection can be made at once.");
  public static final StringId AdminDistributedSystemImpl_AN_ADMINEXCEPTION_WAS_THROWN_WHILE_GETTING_THE_GEMFIRE_HEALTH = new StringId(3420, "An AdminException was thrown while getting the GemFire health.");
  public static final StringId DistributedSystemConfigImpl_ENTITY_CONFIGURATION_XML_FILE_0_DOES_NOT_EXIST = new StringId(3421, "Entity configuration XML file \"{0}\" does not exist");
  public static final StringId ManagedEntityConfigXml_PUBLIC_ID_0_SYSTEM_ID_1 = new StringId(3422, "Public Id: \"{0}\" System Id: \"{1}\"");
  public static final StringId ManagedEntityConfigXmlGenerator_AN_ADMINEXCEPTION_WAS_THROWN_WHILE_GENERATING_XML = new StringId(3423, "An AdminException was thrown while generating XML.");
  public static final StringId ManagedEntityConfigXmlParser_WHILE_PARSING_XML = new StringId(3424, "While parsing XML");
  public static final StringId ManagedEntityConfigXmlParser_MALFORMED_INTEGER_0 = new StringId(3425, "Malformed integer \"{0}\"");
  public static final StringId SystemMemberBridgeServerImpl_UNEXPECTED_EXCEPTION_WHILE_REFRESHING = new StringId(3426, "Unexpected exception while refreshing");
  public static final StringId AgentImpl_WHILE_CREATING_OBJECTNAME_0 = new StringId(3427, "While creating ObjectName:  {0}");
  public static final StringId GemFireHealthJmxImpl_WHILE_GETTING_THE_DISTRIBUTEDSYSTEMHEALTHCONFIG = new StringId(3429, "While getting the DistributedSystemHealthConfig");
  public static final StringId GemFireHealthJmxImpl_WHILE_GETTING_THE_GEMFIREHEALTHCONFIG = new StringId(3430, "While getting the GemFireHealthConfig");
  public static final StringId AbstractDistributionConfig_INVALID_LOCATOR_0_THE_PORT_1_WAS_NOT_GREATER_THAN_ZERO_AND_LESS_THAN_65536 = new StringId(3431, "Invalid locator \"{0}\". The port {1} was not greater than zero and less than 65,536.");
  public static final StringId DistributionManager_INTERRUPTED_WHILE_WAITING_FOR_FIRST_STARTUPRESPONSEMESSAGE = new StringId(3432, "Interrupted while waiting for first StartupResponseMessage");
  public static final StringId DistributionManager_RECEIVED_NO_CONNECTION_ACKNOWLEDGMENTS_FROM_ANY_OF_THE_0_SENIOR_CACHE_MEMBERS_1 = new StringId(3433, "Received no connection acknowledgments from any of the  {0}  senior cache members:  {1}");
  public static final StringId MessageFactory_AN_INSTANTIATIONEXCEPTION_WAS_THROWN_WHILE_INSTANTIATING_A_0 = new StringId(3434, "An InstantiationException was thrown while instantiating a  {0}");
  public static final StringId MessageFactory_COULD_NOT_ACCESS_ZEROARG_CONSTRUCTOR_OF_0 = new StringId(3435, "Could not access zero-arg constructor of  {0}");
  public static final StringId GroupMembershipService_AN_EXCEPTION_WAS_THROWN_WHILE_READING_JGROUPS_CONFIG = new StringId(3437, "An Exception was thrown while reading JGroups config.");
  public static final StringId GroupMembershipService_AN_EXCEPTION_WAS_THROWN_WHILE_JOINING = new StringId(3438, "An Exception was thrown while attempting to join the distributed system.");
  public static final StringId InternalInstantiator_CANNOT_UNREGISTER_A_NULL_CLASS = new StringId(3439, "Cannot unregister a null class");
  public static final StringId StatisticDescriptorImpl_THE_STATISTIC_0_WITH_ID_1_IS_OF_TYPE_2_AND_IT_WAS_EXPECTED_TO_BE_AN_INT = new StringId(3440, "The statistic  {0}  with id  {1}  is of type  {2}  and it was expected to be an int.");
  public static final StringId StatisticDescriptorImpl_THE_STATISTIC_0_WITH_ID_1_IS_OF_TYPE_2_AND_IT_WAS_EXPECTED_TO_BE_A_DOUBLE = new StringId(3441, "The statistic  {0}  with id  {1}  is of type  {2}  and it was expected to be a double.");
  public static final StringId StatisticsTypeXml_SAX_ERROR_WHILE_WORKING_WITH_XML = new StringId(3442, "SAX error while working with XML");
  public static final StringId StatisticsTypeXml_SAX_FATAL_ERROR_WHILE_WORKING_WITH_XML = new StringId(3443, "SAX fatal error while working with XML");
  public static final StringId AdminRequest_A_REPLYEXCEPTION_WAS_THROWN_WHILE_WAITING_FOR_A_REPLY = new StringId(3444, "A ReplyException was thrown while waiting for a reply.");
  public static final StringId RemoteAlert_INVALIDATE_TIMESTAMP_0 = new StringId(3446, "Invalidate timestamp:  {0}");
  public static final StringId RemoteGemFireVM_AN_EXCEPTION_WAS_THROWN_WHILE_CREATING_VM_ROOT_REGION_0 = new StringId(3447, "An Exception was thrown while creating VM root region \"{0}\"");
  public static final StringId RemoteGemFireVM_WHILE_CREATING_SUBREGION_0_OF_1 = new StringId(3448, "While creating subregion \"{0}\" of \"{1}\"");
  public static final StringId RemoteGfManagerAgent_AN_EXCEPUTIONEXCEPTION_WAS_THROWN_WHILE_WAITING_FOR_FUTURE = new StringId(3449, "An ExceputionException was thrown while waiting for Future.");
  public static final StringId BucketAdvisor_CANNOT_CHANGE_FROM_0_TO_1_FOR_BUCKET_2 = new StringId(3451, "Cannot change from  {0}  to  {1}  for bucket  {2}");
  public static final StringId GatewayEventRemoteDispatcher_0_EXCEPTION_DURING_PROCESSING_BATCH_1_ON_CONNECTION_2 = new StringId(3452, "{0} : Exception during processing batch  {1}  on connection  {2}");
  public static final StringId GemFireCache_COULD_NOT_CONVERT_XML_FILE_0_TO_AN_URL = new StringId(3453, "Could not convert XML file \"{0}\" to an URL.");
  public static final StringId GemFireCache_WHILE_OPENING_CACHE_XML_0_THE_FOLLOWING_ERROR_OCCURRED_1 = new StringId(3454, "While opening Cache XML \"{0}\" the following error occurred \"{1}\"");
  public static final StringId Oplog_TRIED_TO_SEEK_TO_0_BUT_THE_FILE_LENGTH_IS_1_OPLOG_FILE_OBJECT_USED_FOR_READING_2 = new StringId(3455, "Tried to seek to {0}, but the file length is {1}. Oplog File object used for reading={2}");
  public static final StringId PartitionedRegion_ATTEMPT_TO_ACQUIRE_PRIMARY_NODE_FOR_WRITE_ON_BUCKET_0_TIMED_OUT_IN_1_MS_CURRENT_REDUNDANCY_2_DOES_NOT_SATISFY_MINIMUM_3 = new StringId(3456, "Attempt to acquire primary node for write on bucket  {0}  timed out in  {1}  ms. Current redundancy [ {2} ] does not satisfy minimum [ {3} ]");
  public static final StringId PartitionedRegion_ATTEMPT_TO_ACQUIRE_PRIMARY_NODE_FOR_READ_ON_BUCKET_0_TIMED_OUT_IN_1_MS = new StringId(3458, "Attempt to acquire primary node for read on bucket  {0}  timed out in  {1}  ms");

  public static final StringId RegionAttributesCreation_CONCURRENCYCHECKSENABLED_IS_NOT_THE_SAME = new StringId(3459, "ConcurrencyChecksEnabled is not the same");
  public static final StringId RegionAttributesCreation_ENABLE_OFF_HEAP_MEMORY_IS_NOT_THE_SAME = new StringId(3460, "EnableOffHeapMemory is not the same");

  public static final StringId ProxyRegionMap_NO_ENTRY_SUPPORT_ON_REGIONS_WITH_DATAPOLICY_0 = new StringId(3461, "No entry support on regions with DataPolicy  {0}");
  public static final StringId SearchLoadAndWriteProcessor_WHILE_INVOKING_A_REMOTE_NETLOAD_0 = new StringId(3462, "While invoking a remote netLoad:  {0}");
  public static final StringId SearchLoadAndWriteProcessor_WHILE_INVOKING_A_REMOTE_NETWRITE_0 = new StringId(3463, "While invoking a remote netWrite:  {0}");
  public static final StringId SearchLoadAndWriteProcessor_TIMED_OUT_WHILE_DOING_NETSEARCHNETLOADNETWRITE_PROCESSORID_0_KEY_IS_1 = new StringId(3464, "Timed out while doing netsearch/netload/netwrite processorId= {0}  Key is  {1}");
  public static final StringId SearchLoadAndWriteProcessor_TIMEOUT_DURING_NETSEARCHNETLOADNETWRITE_DETAILS_0 = new StringId(3465, "Timeout during netsearch/netload/netwrite. Details:  {0}");
  public static final StringId TXCommitMessage_THESE_REGIONS_EXPERIENCED_RELIABILITY_FAILURE_DURING_DISTRIBUTION_OF_THE_OPERATION_0 = new StringId(3466, "These regions experienced reliability failure during distribution of the operation:  {0}");
  public static final StringId TXCommitMessage_COMMIT_OPERATION_GENERATED_ONE_OR_MORE_EXCEPTIONS_FROM_0 = new StringId(3467, "Commit operation generated one or more exceptions from  {0}");
  public static final StringId BucketSizeMessage_BUCKETSIZERESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION_FORCING_REATTEMPT = new StringId(3468, "BucketSizeResponse got remote CacheClosedException; forcing reattempt.");
  public static final StringId BucketSizeMessage_BUCKETSIZERESPONSE_GOT_REMOTE_REGION_DESTROYED_FORCING_REATTEMPT = new StringId(3469, "BucketSizeResponse got remote Region destroyed; forcing reattempt.");
  public static final StringId ContainsKeyValueMessage_CONTAINSKEYVALUERESPONSE_GOT_REMOTE_CACHEEXCEPTION_FORCING_REATTEMPT = new StringId(3470, "ContainsKeyValueResponse got remote CacheException; forcing reattempt.");
  public static final StringId DumpB2NRegion_B2NRESPONSE_GOT_REMOTE_CACHEEXCEPTION_THROWING_FORCEREATTEMPTEXCEPTION = new StringId(3471, "B2NResponse got remote CacheException, throwing ForceReattemptException.");
  public static final StringId FetchEntriesMessage_FETCHKEYSRESPONSE_GOT_REMOTE_CANCELLATION_FORCING_REATTEMPT = new StringId(3472, "FetchKeysResponse got remote cancellation; forcing reattempt.");
  public static final StringId FetchEntryMessage_FETCHENTRYRESPONSE_GOT_REMOTE_CACHEEXCEPTION_FORCING_REATTEMPT = new StringId(3473, "FetchEntryResponse got remote CacheException; forcing reattempt.");
  public static final StringId FetchKeysMessage_FETCHKEYSRESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION_FORCING_REATTEMPT = new StringId(3474, "FetchKeysResponse got remote CacheClosedException; forcing reattempt.");
  public static final StringId GetMessage_UNABLE_TO_DESERIALIZE_VALUE_IOEXCEPTION = new StringId(3475, "Unable to deserialize value (IOException)");
  public static final StringId GetMessage_UNABLE_TO_DESERIALIZE_VALUE_CLASSNOTFOUNDEXCEPTION = new StringId(3476, "Unable to deserialize value (ClassNotFoundException)");
  public static final StringId RemoteOperationMessage_0_COULD_NOT_FIND_REGION_1 = new StringId(3477, "{0} : could not find region {1}");
  public static final StringId ManageBucketMessage_NODERESPONSE_GOT_REMOTE_CANCELLATION_THROWING_PARTITIONEDREGIONCOMMUNICATION_EXCEPTION = new StringId(3478, "NodeResponse got remote cancellation, throwing PartitionedRegionCommunication Exception.");
  public static final StringId ManageBucketMessage_NODERESPONSE_GOT_LOCAL_DESTROY_ON_THE_PARTITIONREGION_THROWING_FORCEREATTEMPTEXCEPTION = new StringId(3479, "NodeResponse got local destroy on the PartitionRegion , throwing ForceReattemptException.");
  public static final StringId PartitionMessage_PARTITIONRESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION = new StringId(3480, "PartitionResponse got remote CacheClosedException");
  public static final StringId PrimaryRequestMessage_NODERESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION_THROWING_PARTITIONEDREGIONCOMMUNICATION_EXCEPTION = new StringId(3481, "NodeResponse got remote CacheClosedException, throwing PartitionedRegionCommunication Exception.");
  public static final StringId TombstoneService_UNEXPECTED_EXCEPTION = new StringId(3498, "GemFire garbage collection service encountered an unexpected exception");
  public static final StringId Oplog_FAILED_READING_FROM_0_OPLOG_DETAILS_1_2_3_4_5_6 = new StringId(3503, "Failed reading from \"{0}\". \n oplogID = {1}\n Offset being read={2} Current Oplog Size={3}  Actual File Size ={4} IS ASYNCH MODE ={5} IS ASYNCH WRITER ALIVE={6}");
  public static final StringId CacheCreation_WHILE_STARTING_CACHE_SERVER_0 = new StringId(3508, "While starting cache server  {0}");
  public static final StringId CacheXml_ERROR_WHILE_PARSING_XML = new StringId(3510, "Error while parsing XML");
  public static final StringId CacheXml_FATAL_ERROR_WHILE_PARSING_XML = new StringId(3511, "Fatal error while parsing XML");
  public static final StringId CacheXmlParser_WHILE_PARSING_XML = new StringId(3512, "While parsing XML");
  public static final StringId CacheXmlParser_MALFORMED_INTEGER_0 = new StringId(3513, "Malformed integer \"{0}\"");
  public static final StringId CacheXmlParser_MALFORMED_FLOAT_0 = new StringId(3514, "Malformed float \"{0}\"");
  public static final StringId CacheXmlParser_COULD_NOT_LOAD_KEYCONSTRAINT_CLASS_0 = new StringId(3517, "Could not load key-constraint class:  {0}");
  public static final StringId CacheXmlParser_COULD_NOT_LOAD_VALUECONSTRAINT_CLASS_0 = new StringId(3518, "Could not load value-constraint class:  {0}");
  public static final StringId CacheXmlParser_WHILE_INSTANTIATING_A_0 = new StringId(3519, "While instantiating a  {0}");
  public static final StringId RegionCreation_GETTING_ENTRIES_RECURSIVELY_IS_NOT_SUPPORTED = new StringId(3520, "Getting entries recursively is not supported.");
  public static final StringId AbstractPoolCache_ABSTRACTPOOLEDCACHEGETPOOLEDCONNECTIONFROMPOOLINTERRUPTEDEXCEPTION_IN_WAITING_THREAD = new StringId(3521, "AbstractPooledCache::getPooledConnectionFromPool:InterruptedException in waiting thread");
  public static final StringId ConnectionPoolCacheImpl_CONNECTIONPOOLCACHEIMPLGENEWCONNECTION_EXCEPTION_IN_CREATING_NEW_POOLEDCONNECTION = new StringId(3522, "ConnectionPoolCacheImpl::getNewConnection: Exception in creating new PooledConnection");
  public static final StringId ConnectionPoolCacheImpl_CONNECTIONPOOLCACHEIMPLGENEWCONNECTION_CONNECTIONPOOLCACHE_NOT_INTIALIZED_WITH_CONNECTIONPOOLDATASOURCE = new StringId(3523, "ConnectionPoolCacheImpl::getNewConnection: ConnectionPoolCache not initialized with ConnectionPoolDatasource");
  public static final StringId FacetsJCAConnectionManagerImpl_FACETSJCACONNECTIONMANAGERIMPL_ALLOCATECONNECTION_IN_GETTING_CONNECTION_FROM_POOL_DUE_TO_0 = new StringId(3524, "FacetsJCAConnectionManagerImpl:: allocateConnection : in getting connection from pool due to  {0}");
  public static final StringId FacetsJCAConnectionManagerImpl_FACETSJCACONNECTIONMANAGERIMPL_ALLOCATECONNECTION_SYSTEM_EXCEPTION_DUE_TO_0 = new StringId(3525, "FacetsJCAConnectionManagerImpl:: allocateConnection :system exception due to  {0}");
  public static final StringId JCAConnectionManagerImpl_JCACONNECTIONMANAGERIMPL_ALLOCATECONNECTION_IN_GETTING_CONNECTION_FROM_POOL_DUE_TO_0 = new StringId(3527, "JCAConnectionManagerImpl:: allocateConnection : in getting connection from pool due to  {0}");
  public static final StringId JCAConnectionManagerImpl_JCACONNECTIONMANAGERIMPL_ALLOCATECONNECTION_IN_TRANSACTION_DUE_TO_0 = new StringId(3528, "JCAConnectionManagerImpl:: allocateConnection : in transaction due to  {0}");
  public static final StringId JCAConnectionManagerImpl_JCACONNECTIONMANAGERIMPL_ALLOCATECONNECTION_SYSTEM_EXCEPTION_DUE_TO_0 = new StringId(3529, "JCAConnectionManagerImpl:: allocateConnection :system exception due to  {0}");
  public static final StringId ManagedPoolCacheImpl_MANAGEDPOOLCACHEIMPLGETNEWCONNECTION_EXCEPTION_IN_CREATING_NEW_MANAGED_POOLEDCONNECTION = new StringId(3530, "ManagedPoolCacheImpl::getNewConnection: Exception in creating new Managed PooledConnection");
  public static final StringId TranxPoolCacheImpl_TRANXPOOLCACHEIMPLGETNEWCONNECTION_EXCEPTION_IN_CREATING_NEW_TRANSACTION_POOLEDCONNECTION = new StringId(3531, "TranxPoolCacheImpl::getNewConnection: Exception in creating new transaction PooledConnection");
  public static final StringId TranxPoolCacheImpl_TRANXPOOLCACHEIMPLGETNEWCONNECTION_CONNECTIONPOOLCACHE_NOT_INTIALIZED_WITH_XADATASOURCE = new StringId(3532, "TranxPoolCacheImpl::getNewConnection: ConnectionPoolCache not intialized with XADatasource");
  public static final StringId SystemFailure_0_MEMORY_HAS_REMAINED_CHRONICALLY_BELOW_1_BYTES_OUT_OF_A_MAXIMUM_OF_2_FOR_3_SEC = new StringId(3545, "{0} : memory has remained chronically below  {1}  bytes (out of a maximum of  {2} ) for  {3}  sec.");
  public static final StringId SystemFailure_JVM_CORRUPTION_HAS_BEEN_DETECTED = new StringId(3546, "JVM corruption has been detected");
  public static final StringId LonerDistributionManager_CANNOT_RESOLVE_LOCAL_HOST_NAME_TO_AN_IP_ADDRESS = new StringId(3547, "Cannot resolve local host name to an IP address");
  public static final StringId AbstractDistributionConfig_UNEXPECTED_PROBLEM_GETTING_INETADDRESS_0 = new StringId(3548, "Unexpected problem getting inetAddress: {0}");
  public static final StringId DistributionManager_UNKNOWN_MEMBER_TYPE_0 = new StringId(3549, "Unknown  member type:  {0}");
  public static final StringId DistributionManager_UNKNOWN_PROCESSOR_TYPE = new StringId(3550, "unknown processor type {0}");
  public static final StringId DLockRequestProcessor_UNKNOWN_RESPONSE_CODE_0 = new StringId(3551, "Unknown response code {0}");
  public static final StringId StreamingOperation_THIS_SHOULDNT_HAPPEN = new StringId(3552, "this should not happen");
  public static final StringId GroupMembershipService_UNKNOWN_STARTUP_EVENT_0 = new StringId(3555, "unknown startup event:  {0}");
  public static final StringId UpdateOperation_UNKNOWN_DESERIALIZATION_POLICY_0 = new StringId(3558, "unknown deserialization policy:  {0}");
  public static final StringId QueuedOperation_CACHEWRITER_SHOULD_NOT_BE_CALLED = new StringId(3559, "CacheWriter should not be called");
  public static final StringId QueuedOperation_DISTRIBUTEDLOCK_SHOULD_NOT_BE_ACQUIRED = new StringId(3560, "DistributedLock should not be acquired");
  public static final StringId InitialImageOperation_ALREADY_PROCESSED_LAST_CHUNK = new StringId(3561, "Already processed last chunk");
  public static final StringId ExpiryTask_UNRECOGNIZED_EXPIRATION_ACTION_0 = new StringId(3565, "unrecognized expiration action:  {0}");
  public static final StringId LocalRegion_UNEXPECTED_EXCEPTION = new StringId(3566, "unexpected exception");
  public static final StringId LocalRegion_CACHE_WRITER_SHOULD_NOT_HAVE_BEEN_CALLED_FOR_LOCALDESTROY = new StringId(3567, "Cache Writer should not have been called for localDestroy");
  public static final StringId LocalRegion_NO_DISTRIBUTED_LOCK_SHOULD_HAVE_BEEN_ATTEMPTED_FOR_LOCALDESTROY = new StringId(3568, "No distributed lock should have been attempted for localDestroy");
  public static final StringId LocalRegion_CACHEWRITEREXCEPTION_SHOULD_NOT_BE_THROWN_IN_LOCALDESTROYREGION = new StringId(3569, "CacheWriterException should not be thrown in localDestroyRegion");
  public static final StringId LocalRegion_TIMEOUTEXCEPTION_SHOULD_NOT_BE_THROWN_IN_LOCALDESTROYREGION = new StringId(3570, "TimeoutException should not be thrown in localDestroyRegion");
  public static final StringId LocalRegion_UNKNOWN_INTEREST_TYPE = new StringId(3571, "unknown interest type");
  public static final StringId LocalRegion_NOT_YET_SUPPORTED = new StringId(3572, "not yet supported");
  public static final StringId LocalRegion_GOT_REGIONEXISTSEXCEPTION_IN_REINITIALIZE_WHEN_HOLDING_DESTROY_LOCK = new StringId(3573, "Got RegionExistsException in reinitialize when holding destroy lock");
  public static final StringId LocalRegion_UNEXPECTED_THREADINITLEVELREQUIREMENT = new StringId(3574, "Unexpected threadInitLevelRequirement");
  public static final StringId LocalRegion_ENTRY_ALREADY_EXISTED_0 = new StringId(3575, "Entry already existed:  {0}");
  public static final StringId LocalRegion_CACHE_WRITER_SHOULD_NOT_HAVE_BEEN_CALLED_FOR_EVICTDESTROY = new StringId(3576, "Cache Writer should not have been called for evictDestroy");
  public static final StringId LocalRegion_NO_DISTRIBUTED_LOCK_SHOULD_HAVE_BEEN_ATTEMPTED_FOR_EVICTDESTROY = new StringId(3577, "No distributed lock should have been attempted for evictDestroy");
  public static final StringId LocalRegion_ENTRYNOTFOUNDEXCEPTION_SHOULD_BE_MASKED_FOR_EVICTDESTROY = new StringId(3578, "EntryNotFoundException should be masked for evictDestroy");
  public static final StringId LocalRegion_CACHEWRITEREXCEPTION_SHOULD_NOT_BE_THROWN_HERE = new StringId(3579, "CacheWriterException should not be thrown here");
  public static final StringId LocalRegion_TIMEOUTEXCEPTION_SHOULD_NOT_BE_THROWN_HERE = new StringId(3580, "TimeoutException should not be thrown here");
  public static final StringId GemFireCache_UNEXPECTED_EXCEPTION = new StringId(3582, "unexpected exception");
  public static final StringId EntryEvents_MUST_NOT_SERIALIZE_0_IN_THIS_CONTEXT = new StringId(3583, "Must not serialize {0} in this context.");
  public static final StringId DistributedRegion_IF_LOADING_A_SNAPSHOT_THEN_SHOULD_NOT_BE_RECOVERING_ISRECOVERING_0_SNAPSHOTSTREAM_1 = new StringId(3584, "if loading a snapshot, then should not be recovering; isRecovering= {0} ,snapshotStream= {1}");
  public static final StringId AbstractUpdateOperation_CACHEWRITER_SHOULD_NOT_BE_CALLED = new StringId(3585, "CacheWriter should not be called");
  public static final StringId AbstractUpdateOperation_DISTRIBUTEDLOCK_SHOULD_NOT_BE_ACQUIRED = new StringId(3586, "DistributedLock should not be acquired");
  public static final StringId RegionEventImpl_CLONE_IS_SUPPORTED = new StringId(3587, "clone IS supported");
  public static final StringId DestroyOperation_CACHEWRITER_SHOULD_NOT_BE_CALLED = new StringId(3588, "CacheWriter should not be called");
  public static final StringId DestroyOperation_DISTRIBUTEDLOCK_SHOULD_NOT_BE_ACQUIRED = new StringId(3589, "DistributedLock should not be acquired");
  public static final StringId StreamingPartitionOperation_UNEXPECTED_CONDITION = new StringId(3590, "unexpected condition");
  public static final StringId PartitionMessage_SORRY_USE_OPERATEONPARTITIONEDREGION_FOR_PR_MESSAGES = new StringId(3591, "Sorry, use operateOnPartitionedRegion for PR messages");
  public static final StringId FetchEntryMessage_FETCHENTRYMESSAGE_MESSAGE_SENT_TO_WRONG_MEMBER = new StringId(3592, "FetchEntryMessage message sent to wrong member");
  public static final StringId GetMessage_GET_MESSAGE_SENT_TO_WRONG_MEMBER = new StringId(3593, "Get message sent to wrong member");
  public static final StringId CacheClientProxy_UNKNOWN_INTEREST_TYPE = new StringId(3596, "Unknown interest type");
  public static final StringId CacheClientProxy_BAD_INTEREST_TYPE = new StringId(3597, "bad interest type");
  public static final StringId InternalDistributedSystem_PROBLEM_IN_INITIALIZING_KEYS_FOR_CLIENT_AUTHENTICATION = new StringId(3598, "Problem in initializing keys for client authentication");
  public static final StringId BaseCommand_NOT_YET_SUPPORTED = new StringId(3602, "not yet supported");
  public static final StringId BaseCommand_UNKNOWN_INTEREST_TYPE = new StringId(3603, "unknown interest type");
  public static final StringId SystemAdmin_REGION_OPTION_HELP = new StringId(3604, "Used to specify what region an operation is to be done on.");
  public static final StringId EntryNotFoundInRegion_THIS_CLASS_IS_DEPRECATED = new StringId(3605, "this class is deprecated");
  public static final StringId DynamicRegionFactory_UNEXPECTED_EXCEPTION = new StringId(3606, "unexpected exception");
  public static final StringId CqAttributesFactory_CLONENOTSUPPORTEDEXCEPTION_THROWN_IN_CLASS_THAT_IMPLEMENTS_CLONEABLE = new StringId(3607, "CloneNotSupportedException thrown in class that implements cloneable");
  public static final StringId Support_ERROR_ASSERTION_FAILED_0 = new StringId(3608, "ERROR: Assertion failed: '' {0} ''");
  public static final StringId TypeUtils_EXPECTED_INSTANCE_OF_0_BUT_WAS_1 = new StringId(3609, "expected instance of  {0}  but was  {1}");
  public static final StringId SystemFailure_SINCE_THIS_IS_A_DEDICATED_CACHE_SERVER_AND_THE_JVM_HAS_BEEN_CORRUPTED_THIS_PROCESS_WILL_NOW_TERMINATE_PERMISSION_TO_CALL_SYSTEM_EXIT_INT_WAS_GIVEN_IN_THE_FOLLOWING_CONTEXT = new StringId(3610, "Since this is a dedicated cache server and the JVM has been corrupted, this process will now terminate. Permission to call System#exit(int) was given in the following context.");
  public static final StringId GatewayImpl_0_DID_NOT_WAIT_FOR_FAILOVER_COMPLETION_DUE_TO_INTERRUPTION = new StringId(3611, "{0}: did not wait for failover completion due to interruption.");
  public static final StringId GatewayImpl_0_AN_EXCEPTION_OCCURRED_WHILE_QUEUEING_1_TO_PERFORM_OPERATION_2_FOR_3 = new StringId(3612, "{0}: An Exception occurred while queueing {1} to perform operation {2} for {3}");
  public static final StringId GatewayImpl_THE_EVENT_QUEUE_SIZE_HAS_DROPPED_BELOW_THE_THRESHOLD_0 = new StringId(3613, "The event queue size has dropped below {0} events.");
  public static final StringId GatewayImpl_GATEWAY_FAILOVER_INITIATED_PROCESSING_0_UNPROCESSED_EVENTS = new StringId(3615, "Gateway Failover Initiated: Processing {0} unprocessed events.");
  public static final StringId GatewayImpl_AN_EXCEPTION_OCCURRED_THE_DISPATCHER_WILL_CONTINUE = new StringId(3618, "An Exception occurred. The dispatcher will continue.");
  public static final StringId GatewayImpl_0_THE_UNPROCESSED_EVENTS_MAP_ALREADY_CONTAINED_AN_EVENT_FROM_THE_HUB_1_SO_IGNORING_NEW_EVENT_2 = new StringId(3619, "{0}: The secondary map already contained an event from hub {1} so ignoring new event {2}.");
  public static final StringId GatewayImpl_EVENT_FAILED_TO_BE_INITIALIZED_0 = new StringId(3620, "Event failed to be initialized: {0}");
  public static final StringId GatewayImpl_0_THE_EVENT_QUEUE_SIZE_HAS_REACHED_THE_THRESHOLD_1 = new StringId(3622, "{0}: The event queue has reached {1} events. Processing will continue.");
  public static final StringId GatewayImpl_AN_INTERRUPTEDEXCEPTION_OCCURRED_THE_THREAD_WILL_EXIT = new StringId(3624, "An InterruptedException occurred. The thread will exit.");
  public static final StringId GatewayImpl_EVENT_DROPPED_DURING_FAILOVER_0 = new StringId(3628, "Event dropped during failover: {0}");
  public static final StringId GatewayImpl_0_DISPATCHER_STILL_ALIVE_EVEN_AFTER_JOIN_OF_5_SECONDS = new StringId(3629, "{0}:Dispatcher still alive even after join of 5 seconds.");
  public static final StringId GatewayImpl_0_INTERRUPTEDEXCEPTION_IN_JOINING_WITH_DISPATCHER_THREAD = new StringId(3630, "{0}:InterruptedException in joining with dispatcher thread.");
  public static final StringId AdminConfig_THIS_FILE_IS_GENERATED_BY_ADMINCONSOLE_EDIT_AS_YOU_WISH_BUT_IT_WILL_BE_OVERWRITTEN_IF_IT_IS_MODIFIED_IN_ADMINCONSOLE = new StringId(3632, "This file is generated by AdminConsole. Edit as you wish but it will be overwritten if it is modified in AdminConsole.");
  public static final StringId AdminConfig_MODIFIED_0 = new StringId(3633, "Modified {0}");
  public static final StringId DLockService_DISCONNECT_LISTENER_FOR_DISTRIBUTEDLOCKSERVICE = new StringId(3634, "Disconnect listener for DistributedLockService");
  public static final StringId DLockService_DISTRIBUTED_LOCKING_THREADS = new StringId(3635, "Distributed Locking Threads");
  public static final StringId DiskRegion_DISK_WRITERS = new StringId(3638, "Disk Writers");
  public static final StringId DiskRegion_ASYNCHRONOUS_DISK_WRITER_0 = new StringId(3639, "Asynchronous disk writer for region {0}");
  public static final StringId PartitionedRegion_SHUTDOWN_LISTENER_FOR_PARTITIONEDREGION = new StringId(3640, "Shutdown listener for PartitionedRegion");
  public static final StringId ExecuteFunction_CANNOT_SPECIFY_0_FOR_ONREGIONS_FUNCTION = new StringId(3641, "Cannot specify {0} for multi region function");
  public static final StringId PartitionedRegion_AN_EXCEPTION_WAS_CAUGHT_WHILE_REGISTERING_PARTITIONEDREGION_0_DUMPPRID_1 = new StringId(3642, "An exception was caught while registering PartitionedRegion \"{0}\". dumpPRId: {1}");
  public static final StringId Connection_COULD_NOT_START_READER_THREAD = new StringId(3643, "could not start reader thread");
  public static final StringId Connection_IDLE_CONNECTION_TIMED_OUT = new StringId(3644, "idle connection timed out");
  public static final StringId Connection_UNKNOWN = new StringId(3645, "unknown");
  public static final StringId Connection_HANDSHAKE_TIMED_OUT = new StringId(3646, "handshake timed out");
  public static final StringId Connection_INTERRUPTED = new StringId(3647, "interrupted");
  public static final StringId Connection_FAILED_HANDSHAKE = new StringId(3648, "failed handshake");
  public static final StringId Connection_FAILED_CONSTRUCTION = new StringId(3649, "failed construction");
  public static final StringId Connection_RUNNIOREADER_CAUGHT_CLOSED_CHANNEL = new StringId(3650, "runNioReader caught closed channel");
  public static final StringId Connection_RUNNIOREADER_CAUGHT_SHUTDOWN = new StringId(3651, "runNioReader caught shutdown");
  public static final StringId Connection_SOCKETCHANNEL_READ_RETURNED_EOF = new StringId(3652, "SocketChannel.read returned EOF");
  public static final StringId Connection_CACHECLOSED_IN_CHANNEL_READ_0 = new StringId(3654, "CacheClosed in channel read: {0}");
  public static final StringId Connection_IOEXCEPTION_IN_CHANNEL_READ_0 = new StringId(3655, "IOException in channel read: {0}");
  public static final StringId Connection_P2P_MESSAGE_READER_FOR_0 = new StringId(3656, "P2P message reader for {0} on port {1}");
  public static final StringId Connection_CLOSEDCHANNELEXCEPTION_IN_CHANNEL_READ_0 = new StringId(3657, "ClosedChannelException in channel read: {0}");
  public static final StringId Connection_IOEXCEPTION_RECEIVED_0 = new StringId(3658, "IOException received: {0}");
  public static final StringId Connection_STREAM_READ_RETURNED_NONPOSITIVE_LENGTH = new StringId(3659, "Stream read returned non-positive length");
  public static final StringId Connection_CURRENT_THREAD_INTERRUPTED = new StringId(3660, "Current thread interrupted");
  public static final StringId ExecuteFunction_RESULTS_NOT_COLLECTED_IN_TIME_PROVIDED = new StringId(3661, "All results not received in time provided");
  public static final StringId Connection_NO_DISTRIBUTION_MANAGER = new StringId(3662, "no distribution manager");
  public static final StringId Connection_FORCE_DISCONNECT_TIMED_OUT = new StringId(3664, "Force disconnect timed out");
  public static final StringId Connection_P2P_PUSHER_IO_EXCEPTION_FOR_0 = new StringId(3665, "P2P pusher io exception for {0}");
  public static final StringId Connection_P2P_PUSHER_0_CAUGHT_CACHECLOSEDEXCEPTION_1 = new StringId(3666, "P2P pusher {0} caught CacheClosedException: {1}");
  public static final StringId Connection_ACK_READ_IO_EXCEPTION_FOR_0 = new StringId(3669, "ack read io exception for {0}");
  public static final StringId Connection_CLASSNOTFOUND_DESERIALIZING_MESSAGE = new StringId(3670, "ClassNotFound deserializing message");
  public static final StringId Connection_FAILED_SENDING_HANDSHAKE_REPLY = new StringId(3671, "Failed sending handshake reply");
  public static final StringId GroupMembershipService_THE_MEMBER_WITH_ID_0_IS_NO_LONGER_IN_MY_OWN_VIEW_1 = new StringId(3679, "The Member with id {0}, is no longer in my own view, {1}");
  public static final StringId GemFireVersion_COULD_NOT_FIND_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_0 = new StringId(3701, "<Could not find resource org/apache/geode/internal/{0}>");
  public static final StringId GemFireVersion_COULD_NOT_READ_PROPERTIES_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_0_BECAUSE_1 = new StringId(3702, "<Could not read properties from resource org/apache/geode/internal/{0} because: {1}>");
  public static final StringId GemFireVersion_MISSING_PROPERTY_0_FROM_RESOURCE_COM_GEMSTONE_GEMFIRE_INTERNAL_1 = new StringId(3703, "<Missing property {0} from resource org/apache/geode/internal/{1}>");
  public static final StringId GemFireVersion_RUNNING_ON_0 = new StringId(3704, "Running on: {0}");
  public static final StringId GemFireVersion_WARNING_EXPECTED_JAVA_VERSION_0 = new StringId(3705, "Warning expected java version: {0}");
  public static final StringId GemFireVersion_WARNING_FAILED_TO_READ_0_BECAUSE_1 = new StringId(3707, "Warning failed to read \"{0}\" because {1}.");
  public static final StringId LocalRegion_THE_CACHE_IS_NOT_AVAILABLE = new StringId(3710, "The cache is not available");
  public static final StringId GlobalTransaction_GLOBALTRANSACTION_ADDTRANSACTION_CANNOT_ADD_A_NULL_TRANSACTION = new StringId(3713, "GlobalTransaction::addTransaction::Cannot add a null Transaction");
  public static final StringId GlobalTransaction_GLOBALTRANSACTION_COMMIT_ERROR_IN_COMMITTING_BUT_TRANSACTION_COULD_NOT_BE_ROLLED_BACK_DUE_TO_EXCEPTION_0 = new StringId(3714, "GlobalTransaction::commit::Error in committing, but transaction could not be rolled back due to exception: {0}");
  public static final StringId GlobalTransaction_GLOBALTRANSACTION_COMMIT_ERROR_IN_COMMITTING_THE_TRANSACTION_TRANSACTION_ROLLED_BACK_EXCEPTION_0_1 = new StringId(3715, "GlobalTransaction::commit:Error in committing the transaction. Transaction rolled back.Exception = {0} {1}");
  public static final StringId GlobalTransaction_GLOBALTRANSACTION_ROLLBACK_ROLLBACK_NOT_SUCCESSFUL_DUE_TO_EXCEPTION_0_1 = new StringId(3716, "GlobalTransaction::rollback:Rollback not successful due to exception {0} {1}");
  public static final StringId GlobalTransaction_GLOBALTRANSACTION_ENLISTRESOURCE_CANNOT_ENLIST_A_RESOURCE_TO_A_TRANSACTION_WHICH_IS_NOT_ACTIVE = new StringId(3717, "GlobalTransaction::enlistResource::Cannot enlist a resource to a transaction which is not active");
  public static final StringId GlobalTransaction_GLOBALTRANSACTION_ENLISTRESOURCE_EXCEPTION_OCCURED_IN_TRYING_TO_SET_XARESOURCE_TIMEOUT_DUE_TO_0_ERROR_CODE_1 = new StringId(3718, "GlobalTransaction::enlistResource:Exception occurred in trying to set XAResource timeout due to {0} Error Code = {1}");
  public static final StringId GlobalTransaction_ERROR_WHILE_DELISTING_XARESOURCE_0_1 = new StringId(3719, "error while delisting XAResource {0} {1}");
  public static final StringId GlobalTransaction_GLOBATRANSACTION_RESUME_RESUME_NOT_SUCCESFUL_DUE_TO_0 = new StringId(3720, "GlobaTransaction::resume:Resume not succesful due to {0}");
  public static final StringId GlobalTransaction_EXCEPTION_OCCURED_WHILE_TRYING_TO_SET_THE_XARESOURCE_TIMEOUT_DUE_TO_0_ERROR_CODE_1 = new StringId(3721, "Exception occurred while trying to set the XAResource TimeOut due to {0} Error code = {1}");
  public static final StringId SmHelper_NATIVE_CODE_UNAVAILABLE = new StringId(3722, "native code unavailable");
  public static final StringId SystemAdmin_WAITING_5_SECONDS_FOR_LOCATOR_PROCESS_TO_TERMINATE = new StringId(3723, "Waiting 5 seconds for locator process to terminate...");
  public static final StringId SystemAdmin_WAITING_FOR_LOCATOR_PROCESS_WITH_PID_0_TO_TERMINATE = new StringId(3724, "Waiting for locator process, with pid {0,number,#} to terminate...");
  public static final StringId SystemAdmin_LOCATOR_PROCESS_HAS_TERMINATED = new StringId(3725, "Locator process has terminated.");
  public static final StringId SystemAdmin_LOCATOR_IN_0_WAS_KILLED_WHILE_IT_WAS_1_LOCATOR_PROCESS_ID_WAS_2 = new StringId(3726, "Locator in \"{0}\" was killed while it was {1}. Locator process id was {2}.");
  public static final StringId SystemAdmin_LOCATOR_IN_0_IS_1_LOCATOR_PROCESS_ID_IS_2 = new StringId(3727, "Locator in \"{0}\" is {1}. Locator process id is {2}.");
  public static final StringId SystemAdmin_LOCATOR_IN_0_IS_STOPPED = new StringId(3728, "Locator in \"{0}\" is stopped.");
  public static final StringId SystemAdmin_LOCATOR_IN_0_IS_STARTING = new StringId(3729, "Locator in \"{0}\" is starting.");
  public static final StringId SystemAdmin_CLEANED_UP_ARTIFACTS_LEFT_BY_THE_PREVIOUS_KILLED_LOCATOR = new StringId(3730, "Cleaned up artifacts left by the previous killed locator.");
  public static final StringId SystemAdmin_LOG_FILE_0_DOES_NOT_EXIST = new StringId(3731, "Log file \"{0}\" does not exist.");
  public static final StringId SystemAdmin_AN_IOEXCEPTION_WAS_THROWN_WHILE_TAILING_0 = new StringId(3732, "An IOException was thrown while tailing \"{0}\"\n");
  public static final StringId SystemAdmin_MERGING_THE_FOLLOWING_LOG_FILES = new StringId(3733, "Merging the following log files:");
  public static final StringId SystemAdmin_COMPLETED_MERGE_OF_0_LOGS_TO_1 = new StringId(3734, "Completed merge of {0} logs to \"{1}\".");
  public static final StringId SystemAdmin_WARNING_NO_STATS_MATCHED_0 = new StringId(3735, "[warning] No stats matched \"{0}\".");
  public static final StringId SystemAdmin_ERROR_OPERATION_0_FAILED_BECAUSE_1 = new StringId(3736, "ERROR: Operation \"{0}\" failed because: {1}.");
  public static final StringId SystemAdmin_ERROR_UNKNOWN_COMMAND_0 = new StringId(3737, "ERROR: Unknown command \"{0}\".");
  public static final StringId SystemAdmin_INFO_FOUND_0_MATCHES_FOR_1 = new StringId(3738, "[info] Found {0} instances matching \"{1}\":");
  public static final StringId SystemAdmin_THIS_PROGRAM_ALLOWS_GEMFIRE_TO_BE_MANAGED_FROM_THE_COMMAND_LINE_IT_EXPECTS_A_COMMAND_TO_EXECUTE_SEE_THE_HELP_TOPIC_0_FOR_A_SUMMARY_OF_SUPPORTED_OPTIONS_SEE_THE_HELP_TOPIC_1_FOR_A_CONCISE_DESCRIPTION_OF_COMMAND_LINE_SYNTAX_SEE_THE_HELP_TOPIC_2_FOR_A_DESCRIPTION_OF_SYSTEM_CONFIGURATION_SEE_THE_HELP_TOPIC_3_FOR_HELP_ON_A_SPECIFIC_COMMAND_USE_THE_4_OPTION_WITH_THE_COMMAND_NAME = new StringId(3739, "This program allows GemFire to be managed from the command line. It expects a command to execute.\nSee the help topic \"{0}\". For a summary of supported options see the help topic \"{1}\".\nFor a concise description of command line syntax see the help topic \"{2}\".\nFor a description of system configuration see the help topic \"{3}\".\nFor help on a specific command use the \"{4}\" option with the command name.");
  public static final StringId SystemAdmin_ALL_COMMAND_LINE_OPTIONS_START_WITH_A_AND_ARE_NOT_REQUIRED_EACH_OPTION_HAS_A_DEFAULT_THAT_WILL_BE_USED_WHEN_ITS_NOT_SPECIFIED_OPTIONS_THAT_TAKE_AN_ARGUMENT_ALWAYS_USE_A_SINGLE_CHARACTER_WITH_NO_SPACES_TO_DELIMIT_WHERE_THE_OPTION_NAME_ENDS_AND_THE_ARGUMENT_BEGINS_OPTIONS_THAT_PRECEDE_THE_COMMAND_WORD_CAN_BE_USED_WITH_ANY_COMMAND_AND_ARE_ALSO_PERMITTED_TO_FOLLOW_THE_COMMAND_WORD = new StringId(3740, "All command line options start with a \"-\" and are not required.\nEach option has a default that will be used when its not specified.\nOptions that take an argument always use a single \"=\" character, with no spaces, to delimit where the option name ends and the argument begins.\nOptions that precede the command word can be used with any command and are also permitted to follow the command word.");
  public static final StringId SystemAdmin_NO_HELP_FOR_OPTION_0 = new StringId(3741, "no help for option \"{0}]\"");
  public static final StringId SystemAdmin_EXPLAINATION_OF_COMMAND_OPTIONS = new StringId(3742, "The following synax is used in the usage strings:\n\"[]\" designate an optional item\n\"()\" are used to group items\n\"<>\" designate non-literal text. Used to designate logical items\n\"*\" suffix means zero or more of the previous item\n\"|\" means the item to the left or right is required");
  public static final StringId SystemAdmin_ERROR_INVALID_HELP_TOPIC_0 = new StringId(3743, "ERROR: Invalid help topic \"{0}\".");
  public static final StringId SystemAdmin_USAGE = new StringId(3744, "Usage");
  public static final StringId SystemAdmin_ERROR_INVALID_COMMAND_0 = new StringId(3745, "ERROR: Invalid command \"{0}\".");
  public static final StringId SystemAdmin_GEMFIRE_HELP = new StringId(3746, "gemfire requires one of the following command strings:\n{0}\nFor additional help on a command specify it along with the \"{1}\" option.\nThe \"{2}\" option causes gemfire to print out extra information when it fails.\nThe \"{1}\" and \"{3}\" are synonyms that cause gemfire to print out help information instead of performing a task.\nThe \"{4}\" option quiets gemfire down by suppressing extra messages.\nThe \"{5}\" option passes <vmOpt> to the java vm''s command line.");
  public static final StringId SystemAdmin_VERSION_HELP = new StringId(3747, "Prints GemFire product version information.");
  public static final StringId SystemAdmin_HELP_HELP = new StringId(3749, "Prints information on how to use this executable.\nIf an optional help topic is specified then more detailed help is printed.");
  public static final StringId SystemAdmin_STATS_HELP_PART_A = new StringId(3750, "Prints statistic values from a statistic archive\nBy default all statistics are printed.\nThe statSpec arguments can be used to print individual resources or a specific statistic.\nThe format of a statSpec is: an optional combine operator, followed by an optional instanceId, followed by an optional typeId, followed by an optional statId.\nA combine operator can be \"{0}\" to combine all matches in the same file, or \"{1}\" to combine all matches across all files.\nAn instanceId must be the name or id of a resource.\nA typeId is a \"{2}\" followed by the name of a resource type.\nA statId is a \"{3}\" followed by the name of a statistic.\nA typeId or instanceId with no statId prints out all the matching resources and all their statistics.\nA typeId or instanceId with a statId prints out just the named statistic on the matching resources.\nA statId with no typeId or instanceId matches all statistics with that name.\nThe \"{4}\" option causes statistic descriptions to also be printed.\nThe \"{5}\" option, in conjunction with \"{6}\", causes the printed statistics to all be raw, unfiltered, values.\nThe \"{7}\" option, in conjunction with \"{6}\", causes the printed statistics to be the rate of change, per second, of the raw values.\nThe \"{8}\" option, in conjunction with \"{6}\", causes the printed statistics to be the rate of change, per sample, of the raw values.\nThe \"{9}\" option, in conjunction with \"{6}\", causes statistics whose values are all zero to not be printed.");
  public static final StringId SystemAdmin_STATS_HELP_PART_B = new StringId(3751, "The \"{0}\" option, in conjunction with \"{1}\", causes statistics samples taken before this time to be ignored. The argument format must match \"{2}\".\nThe \"{3}\" option, in conjunction with \"{1}\", causes statistics samples taken after this time to be ignored. The argument format must match \"{2}\".\nThe \"{1}\" option causes the data to come from an archive file.");
  public static final StringId SystemAdmin_ENCRYPTS_A_PASSWORD_FOR_USE_IN_CACHE_XML_DATA_SOURCE_CONFIGURATION = new StringId(3752, "Encrypts a password for use in cache.xml data source configuration.");
  public static final StringId SystemAdmin_START_LOCATOR_HELP = new StringId(3753, "Starts a locator.\nThe \"{0}\" option specifies the port the locator will listen on. It defaults to \"{1}\"\nThe \"{2}\" option specifies the address the locator will listen on. It defaults to listening on all local addresses.\nThe \"{3}\" option can be used to specify the directory the locator will run in.\nThe \"{4}\" option can be used to specify the gemfire.properties file for configuring the locator''s distributed system.  The file''s path should be absolute, or relative to the locator''s directory ({3})\nThe \"{5}\" option can be used to specify whether peer locator service should be enabled. True (the default) will enable the service.\nThe \"{6}\" option can be used to specify whether server locator service should be enabled. True (the default) will enable the service.\nThe \"{7}\" option can be used to specify a host name or ip address that will be sent to clients so they can connect to this locator. The default is to use the address the locator is listening on.\nThe \"{8}\" option can be used to set system properties for the locator VM\nThe \"{9}\" option can be used to set vendor-specific VM options and is usually used to increase the size of the locator VM when using multicast.\n");
  public static final StringId SystemAdmin_STOP_LOCATOR_HELP = new StringId(3754, "Stops a locator.\nThe \"{0}\" option specifies the port the locator is listening on. It defaults to \"{1}\"\nThe \"{2}\" option specifies the address the locator is listening on. It defaults to the local host''s address.\nThe \"{3}\" option can be used to specify the directory the locator is running in.");
  public static final StringId SystemAdmin_STATUS_LOCATOR_HELP = new StringId(3755, "Prints the status of a locator. The status string will one of the following:\n{0}\nThe \"{1}\" option can be used to specify the directory of the locator whose status is desired.");
  public static final StringId SystemAdmin_INFO_LOCATOR_HELP = new StringId(3756, "Prints information on a locator.\nThe information includes the process id of the locator, if the product is not running in PureJava mode.\nThe \"{0}\" option can be used to specify the directory of the locator whose information is desired.");
  public static final StringId SystemAdmin_TAIL_LOCATOR_HELP = new StringId(3757, "Prints the last 64K bytes of the locator''s log file.\nThe \"{0}\" option can be used to specify the directory of the locator whose information is desired.");
  public static final StringId SystemAdmin_MERGE_LOGS = new StringId(3758, "Merges multiple logs files into a single log.\nThe \"{0}\" option can be used to specify the file to write the merged log to. The default is stdout.");
  public static final StringId SystemAdmin_CAUSES_GEMFIRE_TO_WRITE_OUTPUT_TO_THE_SPECIFIED_FILE_THE_FILE_IS_OVERWRITTEN_IF_IT_ALREADY_EXISTS = new StringId(3759, "Causes gemfire to write output to the specified file. The file is overwritten if it already exists.");
  public static final StringId SystemAdmin_CAUSES_GEMFIRE_TO_PRINT_OUT_EXTRA_INFORMATION_WHEN_IT_FAILS_THIS_OPTION_IS_SUPPORTED_BY_ALL_COMMANDS = new StringId(3760, "Causes gemfire to print out extra information when it fails. This option is supported by all commands.");
  public static final StringId SystemAdmin_CAUSES_GEMFIRE_TO_PRINT_DETAILED_INFORMATION_WITH_THE_0_COMMAND_IT_MEANS_STATISTIC_DESCRIPTIONS = new StringId(3761, "Causes gemfire to print detailed information.  With the \"{0}\" command it means statistic descriptions.");
  public static final StringId SystemAdmin_CAUSES_GEMFIRE_0_COMMAND_TO_PRINT_UNFILTERED_RAW_STATISTIC_VALUES_THIS_IS_THE_DEFAULT_FOR_NONCOUNTER_STATISTICS = new StringId(3762, "Causes gemfire \"{0}\" command to print unfiltered, raw, statistic values. This is the default for non-counter statistics.");
  public static final StringId SystemAdmin_CAUSES_GEMFIRE_0_COMMAND_TO_PRINT_THE_RATE_OF_CHANGE_PER_SECOND_FOR_STATISTIC_VALUES_THIS_IS_THE_DEFAULT_FOR_COUNTER_STATISTICS = new StringId(3763, "Causes gemfire \"{0}\" command to print the rate of change, per second, for statistic values. This is the default for counter statistics.");
  public static final StringId SystemAdmin_CAUSES_GEMFIRE_0_COMMAND_TO_PRINT_THE_RATE_OF_CHANGE_PER_SAMPLE_FOR_STATISTIC_VALUES = new StringId(3764, "Causes gemfire \"{0}\" command to print the rate of change, per sample, for statistic values.");
  public static final StringId SystemAdmin_CAUSES_GEMFIRE_0_COMMAND_TO_NOT_PRINT_STATISTICS_WHOSE_VALUES_ARE_ALL_ZERO = new StringId(3765, "Causes gemfire \"{0}\" command to not print statistics whose values are all zero.");
  public static final StringId SystemAdmin_USED_TO_SPECIFY_A_NONDEFAULT_PORT_WHEN_STARTING_OR_STOPPING_A_LOCATOR = new StringId(3766, "Used to specify a non-default port when starting or stopping a locator.");
  public static final StringId SystemAdmin_USED_TO_SPECIFY_A_SPECIFIC_IP_ADDRESS_TO_LISTEN_ON_WHEN_STARTING_OR_STOPPING_A_LOCATOR = new StringId(3767, "Used to specify a specific IP address to listen on when starting or stopping a locator.");
  public static final StringId SystemAdmin_THE_ARGUMENT_IS_THE_STATISTIC_ARCHIVE_FILE_THE_0_COMMAND_SHOULD_READ = new StringId(3769, "The argument is the statistic archive file the \"{0}\" command should read.");
  public static final StringId SystemAdmin_CAUSES_GEMFIRE_TO_PRINT_OUT_INFORMATION_INSTEAD_OF_PERFORMING_THE_COMMAND_THIS_OPTION_IS_SUPPORTED_BY_ALL_COMMANDS = new StringId(3770, "Causes GemFire to print out information instead of performing the command. This option is supported by all commands.");
  public static final StringId SystemAdmin_TURNS_ON_QUIET_MODE_THIS_OPTION_IS_SUPPORTED_BY_ALL_COMMANDS = new StringId(3771, "Turns on quiet mode. This option is supported by all commands.");
  public static final StringId SystemAdmin_CAUSES_THE_0_COMMAND_TO_IGNORE_STATISTICS_SAMPLES_TAKEN_BEFORE_THIS_TIME_THE_ARGUMENT_FORMAT_MUST_MATCH_1 = new StringId(3772, "Causes the \"{0}\" command to ignore statistics samples taken before this time. The argument format must match \"{1}\".");
  public static final StringId SystemAdmin_CAUSES_THE_0_COMMAND_TO_IGNORE_STATISTICS_SAMPLES_TAKEN_AFTER_THIS_TIME_THE_ARGUMENT_FORMAT_MUST_MATCH_1 = new StringId(3773, "Causes the \"{0}\" command to ignore statistics samples taken after this time. The argument format must match \"{1}\".");
  public static final StringId SystemAdmin_DIR_ARGUMENT_HELP = new StringId(3774, "The argument is the system directory the command should operate on.\nIf the argument is empty then a default system directory will be search for.\nHowever the search will not include the \"{0}\" file.\nBy default if a command needs a system directory, and one is not specified, then a search is done. If a \"{0}\" file can be located then \"{1}\" property from that file is used. Otherwise if the \"{2}\" environment variable is set to a directory that contains a subdirectory named \"{3}\" then that directory is used.\nThe property file is search for in the following locations:\n1. The current working directory.\n2. The user''s home directory.\n3. The class path.\nAll commands except \"{4}\", and \"{5}\" use the system directory.");
  public static final StringId SystemAdmin_SETS_A_JAVA_SYSTEM_PROPERTY_IN_THE_LOCATOR_VM_USED_MOST_OFTEN_FOR_CONFIGURING_SSL_COMMUNICATION = new StringId(3775, "Sets a Java system property in the locator VM.  Used most often for configuring SSL communication.");
  public static final StringId SystemAdmin_SETS_A_JAVA_VM_X_SETTING_IN_THE_LOCATOR_VM_USED_MOST_OFTEN_FOR_INCREASING_THE_SIZE_OF_THE_VIRTUAL_MACHINE = new StringId(3776, "Sets a Java VM X setting in the locator VM.  Used most often for increasing the size of the virtual machine.");
  public static final StringId SystemAdmin_ERROR_UNKNOWN_OPTION_0 = new StringId(3778, "ERROR: Unknown option \"{0}\".");
  public static final StringId SystemAdmin_ERROR_WRONG_NUMBER_OF_COMMAND_LINE_ARGS = new StringId(3779, "ERROR: Wrong number of command line args.");
  public static final StringId SystemAdmin_ERROR_UNEXPECTED_COMMAND_LINE_ARGUMENTS_0 = new StringId(3780, "ERROR: unexpected command line arguments: \"{0}\".");
  public static final StringId SystemAdmin_GEMFIRE_PRODUCT_DIRECTORY_0 = new StringId(3781, "GemFire product directory: {0}");
  public static final StringId SystemAdmin_LOCATOR_START_COMPLETE = new StringId(3782, "Locator start complete.");
  public static final StringId SystemAdmin_LOCATOR_STOP_COMPLETE = new StringId(3783, "Locator stop complete.");
  public static final StringId SystemAdmin_ERROR_EXPECTED_AT_LEAST_ONE_LOG_FILE_TO_MERGE = new StringId(3784, "ERROR: expected at least one log file to merge.");
  public static final StringId AgentConfigImpl_USING_DEFAULT_CONFIGURATION_BECAUSE_PROPERTY_FILE_WAS_FOUND = new StringId(3785, "Using default configuration because property file was not found.");
  public static final StringId AgentConfigImpl_CONFIGURATION_LOADED_FROM_0 = new StringId(3786, "Configuration loaded from: {0}.");
  public static final StringId AgentConfigImpl_NAME_OF_THE_AGENTS_LOG_FILE = new StringId(3787, "Name of the agent''s log file");
  public static final StringId AgentConfigImpl_MINIMUM_LEVEL_OF_LOGGING_PERFORMED_BY_AGENT = new StringId(3788, "Minimum level of logging performed by agent. Valid values are: all, finest, finer, fine, config, info, warning, error, severe and none.");
  public static final StringId AgentConfigImpl_WHETHER_THE_AGENT_SHOULD_PRINT_DEBUGGING_INFORMATION = new StringId(3789, "Whether the agent should print debugging information");
  public static final StringId AgentConfigImpl_LIMIT_IN_MEGABYTES_OF_HOW_MUCH_DISK_SPACE_CAN_BE_CONSUMED_BY_OLD_INACTIVE_LOG_FILES = new StringId(3790, "Limit, in megabytes, of how much disk space can be consumed by old inactive log files. This value (in megabytes) should be in the range: 0-1000000.");
  public static final StringId AgentConfigImpl_LIMIT_IN_MEGABYTES_OF_HOW_LARGE_THE_CURRENT_STATISTIC_ARCHIVE_FILE_CAN_GROW_BEFORE_IT_IS_CLOSED_AND_ARCHIVAL_ROLLS_ON_TO_A_NEW_FILE = new StringId(3791, "Limit, in megabytes, of how large the current log file can grow before it is closed and log rolls on to a new file. This value (in megabytes) should be in the range: 0-1000000.");
  public static final StringId AgentConfigImpl_MULTICAST_PORT_USED_TO_CONNECT_TO_DISTRIBUTED_SYSTEM = new StringId(3792, "Multicast port used to connect to distributed system. To use IP multicast, you must also define mcast-address. The value must be in the range: 0-65535.");
  public static final StringId AgentConfigImpl_MULTICAST_ADDRESS_USED_TO_CONNECT_TO_DISTRIBUTED_SYSTEM = new StringId(3793, "Multicast address used to connect to distributed system. To use multicast, you must also define mcast-port, the IP port.");
  public static final StringId AgentConfigImpl_ADDRESSES_OF_THE_LOCATORS_OF_THE_DISTRIBUTED_SYSTEM = new StringId(3794, "A comma-separated list of address(es) of the locator(s) in the distributed system in host[port] form. E.g. locators=host1[port1],host2[port2],...,hostn[portn]");
  public static final StringId AgentConfigImpl_XML_CONFIGURATION_FILE_FOR_MANAGED_ENTITIES = new StringId(3795, "The name of an XML file that specifies the configuration for the managed entity administered by the Distributed System. The XML file must conform to the dtd - doc-files/ds5_0.dtd.");
  public static final StringId AgentConfigImpl_WILL_THE_AGENT_AUTOMATICALLY_CONNECT_TO_THE_DISTRIBUTED_SYSTEM = new StringId(3796, "Whether the JMX agent will connect ''automatically'' to the distributed system that it is configured to monitor.");
  public static final StringId AgentConfigImpl_COMMAND_PREFIX_USED_FOR_LAUNCHING_MEMBERS_OF_THE_DISTRIBUTED_SYSTEM = new StringId(3797, "Command prefix used for launching members of the distributed system");
  public static final StringId AgentConfigImpl_WILL_THE_AGENT_START_THE_HTTP_JMX_ADAPTER = new StringId(3798, "Whether the HTTP adapter is enabled in the JMX agent.");
  public static final StringId AgentConfigImpl_BIND_ADDRESS_OF_HTTP_ADAPTERS_SOCKETS = new StringId(3799, "Bind address of HTTP adapter''s sockets");
  public static final StringId AgentConfigImpl_THE_PORT_ON_WHICH_THE_HTTP_ADAPTER_WILL_BE_STARTED = new StringId(3800, "The port on which the HTTP adapter will be started. This value should be in the range: 0-65535.");
  public static final StringId AgentConfigImpl_WILL_THE_AGENT_START_THE_RMI_JMX_ADAPTER = new StringId(3801, "Whether the RMI JMX adapter is enabled.");
  public static final StringId AgentConfigImpl_WILL_THE_AGENT_HOST_AN_RMI_REGISTRY = new StringId(3802, "Whether the JMX agent should start RMI registry. Alternatively, a registry outside of the JMX agent VM can be used.");
  public static final StringId AgentConfigImpl_BIND_ADDRESS_OF_RMI_ADAPTERS_SOCKETS = new StringId(3803, "Bind address of RMI adapter''s sockets");
  public static final StringId AgentConfigImpl_THE_PORT_ON_WHICH_TO_CONTACT_THE_RMI_REGISTER = new StringId(3804, "The port on which to contact the RMI registry. The value must be in the range: 0-65535.");
  public static final StringId AgentConfigImpl_WILL_THE_AGENT_START_THE_SNMP_JMX_ADAPTER = new StringId(3805, "Whether the SNMP JMX adapter will be enabled.");
  public static final StringId AgentConfigImpl_BIND_ADDRESS_OF_SNMP_ADAPTERS_SOCKETS = new StringId(3806, "Bind address of SNMP adapter''s sockets");
  public static final StringId AgentConfigImpl_THE_DIRECTORY_IN_WHICH_SNMP_CONFIGURATION_RESIDES = new StringId(3807, "The directory in which SNMP configuration resides");
  public static final StringId AgentConfigImpl_WILL_THE_AGENT_COMMUNICATE_USING_SSL = new StringId(3808, "Whether the JMX Agent will use the SSL protocol for communication.");
  public static final StringId AgentConfigImpl_THE_SSL_PROTOCOLS_USED_BY_THE_AGENT = new StringId(3809, "The space-separated list of the SSL protocols to be used when connecting to the JMX agent.");
  public static final StringId AgentConfigImpl_THE_SSL_CIPHERS_USED_BY_THE_AGENT = new StringId(3810, "The space-separated list of the SSL ciphers to be used when connecting to the JMX Agent.");
  public static final StringId AgentConfigImpl_WILL_THE_AGENT_REQUIRE_SSL_AUTHENTICATION = new StringId(3811, "Whether or not SSL connections to the RMI adapter require authentication. If true, needs client authentication for RMI and other non-HTTP connectors/adaptors.");
  public static final StringId AgentConfigImpl_WILL_THE_HTTP_ADAPTER_REQUIRE_SSL_AUTHENTICATION = new StringId(3812, "Whether SSL connections to the HTTP adapter will need authentication.");
  public static final StringId AgentConfigImpl_WILL_THE_HTTP_JMX_ADAPTER_USE_HTTP_AUTHENTICATION = new StringId(3813, "Whether the HTTP adapter will use HTTP authentication.");
  public static final StringId AgentConfigImpl_THE_USER_NAME_FOR_AUTHENTICATION_IN_THE_HTTP_JMX_ADAPTER = new StringId(3814, "The user name for authentication in the HTTP JMX adapter");
  public static final StringId AgentConfigImpl_THE_PASSWORD_FOR_AUTHENTICATION_IN_THE_HTTP_JMX_ADAPTER = new StringId(3815, "The password for authentication in the HTTP JMX adapter");
  public static final StringId AgentConfigImpl_DOES_THE_DISTRIBUTED_SYSTEM_COMMUNICATE_USING_SSL = new StringId(3816, "Whether to use the SSL protocol for communication between members of the admin distributed system. If set to true, locators should be used.");
  public static final StringId AgentConfigImpl_SSL_PROTOCOLS_USED_TO_COMMUNICATE_WITH_DISTRIBUTED_SYSTEM = new StringId(3817, "A space-separated list of the SSL protocols used to communicate with distributed system.");
  public static final StringId AgentConfigImpl_SSL_CIPHERS_USED_TO_COMMUNICATE_WITH_DISTRIBUTED_SYSTEM = new StringId(3818, "A space-separated list of the SSL ciphers to be used to communicate with distributed system.");
  public static final StringId AgentConfigImpl_DOES_CONNECTING_TO_THE_DISTRIBUTED_SYSTEM_REQUIRE_SSL_AUTHENTICATION = new StringId(3819, "Whether connection to the distributed system needs SSL authentication.");
  public static final StringId AgentConfigImpl_PROPERTY_FILE_FROM_WHICH_AGENT_READS_CONFIGURATION = new StringId(3820, "Name and path of the Agent''s properties file from which agent reads configuration");
  public static final StringId AgentConfigImpl_HOST_ON_WHICH_THE_DISTRIBUTED_SYSTEMS_LOCATOR_RUNS = new StringId(3821, "Host on which the distributed system''s locator runs");
  public static final StringId SystemAdmin_USED_TO_SPECIFY_THE_0_FILE_TO_BE_USED_IN_CONFIGURING_THE_LOCATORS_DISTRIBUTEDSYSTEM = new StringId(3822, "Used to specify the {0} file to be used in configuring the locator''s DistributedSystem.");
  public static final StringId AgentConfigImpl_GEMFIRE_PRODUCT_DIRECTORY_USED_TO_LAUNCH_A_LOCATOR = new StringId(3823, "GemFire product directory used to launch a locator");
  public static final StringId AgentConfigImpl_DIRECTORY_IN_WHICH_A_LOCATOR_WILL_BE_LAUNCHED = new StringId(3824, "Directory in which a locator will be launched");
  public static final StringId AgentConfigImpl_COMMAND_PREFIX_USED_WHEN_LAUNCHING_A_LOCATOR = new StringId(3825, "Command prefix used when launching a locator");
  public static final StringId AgentConfigImpl_IP_ADDRESS_TO_USE_WHEN_CONTACTING_LOCATOR = new StringId(3826, "IP address to use when contacting locator");
  public static final StringId AgentConfigImpl_PROPERTIES_FOR_CONFIGURING_A_LOCATORS_DISTRIBUTED_SYSTEM = new StringId(3827, "Properties for configuring a locator''s distributed system");
  public static final StringId AgentLauncher_STARTS_THE_GEMFIRE_JMX_AGENT = new StringId(3828, "Starts the GemFire JMX Agent");
  public static final StringId AgentLauncher_VMARG = new StringId(3829, "<vmarg> a VM-option passed to the agent''s VM, example -J-Xmx1024M for a 1 Gb heap");
  public static final StringId AgentLauncher_DIR = new StringId(3830, "<dir> Directory in which agent runs, default is the current directory");
  public static final StringId AgentLauncher_PROP = new StringId(3831, "<prop> A configuration property/value passed to the agent");
  public static final StringId AgentLauncher_SEE_HELP_CONFIG = new StringId(3832, "(see \"help config\" for more details)");
  public static final StringId AgentLauncher_STOPS_A_GEMFIRE_JMX_AGENT = new StringId(3833, "Stops a GemFire JMX Agent");
  public static final StringId AgentLauncher_REPORTS_THE_STATUS_AND_THE_PROCESS_ID_OF_A_GEMFIRE_JMX_AGENT = new StringId(3834, "Reports the status and the process id of a GemFire JMX Agent");
  public static final StringId AgentLauncher_AGENT_CONFIGURATION_PROPERTIES = new StringId(3835, "Agent configuration properties");
  public static final StringId AgentLauncher_DEFAULT = new StringId(3836, "Default");
  public static final StringId AgentLauncher_STARTING_JMX_AGENT_WITH_PID_0 = new StringId(3837, "Starting JMX Agent with pid: {0,number,#}");
  public static final StringId AgentLauncher_STARTING_AGENT = new StringId(3838, "Starting agent");
  public static final StringId AgentLauncher_UNCAUGHT_EXCEPTION_IN_THREAD_0 = new StringId(3839, "Uncaught exception in thread {0}");
  public static final StringId AgentLauncher_0_HAS_STOPPED = new StringId(3840, "The {0} has shut down.");
  public static final StringId AgentLauncher_TIMEOUT_WAITING_FOR_0_TO_SHUTDOWN_STATUS_IS_1 = new StringId(3841, "Timeout waiting for {0} to shutdown, status is: {1}");
  public static final StringId AgentLauncher_NO_HELP_AVAILABLE_FOR_0 = new StringId(3842, "No help available for \"{0}\"");
  public static final StringId AgentLauncher_AGENT_HELP = new StringId(3843, "agent help");
  public static final StringId AgentLauncher_UNKNOWN_COMMAND_0 = new StringId(3844, "Unknown command: {0}");
  public static final StringId AgentLauncher_ERROR_0 = new StringId(3845, "Error : {0}");
  public static final StringId AgentLauncher_0_PID_1_STATUS = new StringId(3846, "{0} pid: {1,number,#} status: ");
  public static final StringId AgentLauncher_SHUTDOWN = new StringId(3847, "shutdown");
  public static final StringId AgentLauncher_STARTING = new StringId(3848, "starting");
  public static final StringId AgentLauncher_RUNNING = new StringId(3849, "running");
  public static final StringId AgentLauncher_SHUTDOWN_PENDING = new StringId(3850, "shutdown pending");
  public static final StringId AgentLauncher_UNKNOWN = new StringId(3851, "unknown");
  public static final StringId MsgStreamer_CLOSING_DUE_TO_0 = new StringId(3852, "closing due to {0}");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_BEGIN_NESTED_TRANSACTION_IS_NOT_SUPPORTED = new StringId(3853, "Nested transaction is not supported");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_TRANSACTION_IS_NULL_CANNOT_COMMIT_A_NULL_TRANSACTION = new StringId(3854, "Transaction is null, cannot commit a null transaction");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_GLOBAL_TRANSACTION_IS_NULL_CANNOT_COMMIT_A_NULL_GLOBAL_TRANSACTION = new StringId(3855, "Global Transaction is null, cannot commit a null global transaction");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_TRANSACTION_NOT_ACTIVE_CANNOT_BE_COMMITTED_TRANSACTION_STATUS_0 = new StringId(3856, "transaction not active, cannot be committed. Transaction Status= {0}");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_TRANSACTION_IS_NOT_ACTIVE_AND_CANNOT_BE_COMMITTED = new StringId(3857, "transaction is not active and cannot be committed");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_TRANSACTION_ROLLED_BACK_BECAUSE_OF_EXCEPTION_IN_NOTIFYBEFORECOMPLETION_FUNCTION_CALL_ACTUAL_EXCEPTION_0 = new StringId(3858, "Transaction rolled back because of Exception in notifyBeforeCompletion processing");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_COMMIT_TRANSACTION_ROLLED_BACK_BECAUSE_A_USER_MARKED_IT_FOR_ROLLBACK = new StringId(3859, "Transaction rolled back because a user marked it for Rollback");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_ROLLBACK_NO_TRANSACTION_EXISTS = new StringId(3860, "no transaction exists");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_ROLLBACK_NO_GLOBAL_TRANSACTION_EXISTS = new StringId(3861, "no global transaction exists");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_ROLLBACK_TRANSACTION_STATUS_DOES_NOT_ALLOW_ROLLBACK_TRANSACTIONAL_STATUS_0 = new StringId(3862, "Transaction status does not allow Rollback .Transactional status = {0}");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_ROLLBACK_TRANSACTION_ALREADY_IN_A_ROLLING_BACK_STATE_TRANSACTIONAL_STATUS_0 = new StringId(3863, "Transaction already in a Rolling Back state.Transactional status = {0}");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_ROLLBACK_TRANSACTION_STATUS_DOES_NOT_ALLOW_ROLLBACK = new StringId(3864, "Transaction status does not allow Rollback");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_SETROLLBACKONLY_NO_GLOBAL_TRANSACTION_EXISTS = new StringId(3865, "no global transaction exists");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_SETROLLBACKONLY_TRANSACTION_CANNOT_BE_MARKED_FOR_ROLLBACK_TRANSCATION_STATUS_0 = new StringId(3866, "Transaction cannot be marked for rollback. Transcation status = {0}");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_SETTRANSACTIONTIMEOUT_NO_GLOBAL_TRANSACTION_EXISTS = new StringId(3867, "no global transaction exists");
  public static final StringId GemFireCacheImpl_STARTING_GEMFIRE_MEMCACHED_SERVER_ON_PORT_0_FOR_1_PROTOCOL = new StringId(3868, "Starting GemFireMemcachedServer on port {0} for {1} protocol");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_SETTRANSACTIONTIMEOUT_TRANSACTION_HAS_EITHER_EXPIRED_OR_ROLLEDBACK_OR_COMITTED = new StringId(3869, "Transaction has either expired or rolledback or comitted");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_RESUME_CANNOT_RESUME_A_NULL_TRANSACTION = new StringId(3870, "cannot resume a null transaction");
  public static final StringId AbstractDistributionConfig_SECURITY_CLIENT_DHALGO_NAME_0 = new StringId(3871, "User defined name for the symmetric encryption algorithm to use in Diffie-Hellman key exchange for encryption of credentials.  Defaults to \"{0}\". Legal values can be any of the available symmetric algorithm names in JDK like \"DES\", \"DESede\", \"AES\", \"Blowfish\". It may be required to install Unlimited Strength Jurisdiction Policy Files from Sun for some symmetric algorithms to work (like \"AES\")");  
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_RESUME_ERROR_IN_LISTING_THREAD_TO_TRANSACTION_MAP_DUE_TO_0 = new StringId(3872, "Error in listing thread to transaction map due to {0}");
  public static final StringId TransactionManagerImpl_TRANSACTIONMANAGERIMPL_GETGLOBALTRANSACTION_NO_TRANSACTION_EXISTS = new StringId(3873, "no transaction exists");
  public static final StringId UserTransactionImpl_USERTRANSACTIONIMPL_SETTRANSACTIONTIMEOUT_CANNOT_SET_A_NEGATIVE_TIME_OUT_FOR_TRANSACTIONS = new StringId(3874, "Cannot set a negative Time Out for transactions");
  public static final StringId MergeLogFiles_USAGE = new StringId(3875, "Usage");
  public static final StringId MergeLogFiles_NUMBER_OF_PARENT_DIRS_TO_PRINT = new StringId(3876, "Number of parent dirs to print");
  public static final StringId MergeLogFiles_FILE_IN_WHICH_TO_PUT_MERGED_LOGS = new StringId(3877, "File in which to put merged logs");
  public static final StringId MergeLogFiles_SEARCH_FOR_PIDS_IN_FILE_NAMES_AND_USE_THEM_TO_IDENTIFY_FILES = new StringId(3878, "Search for PIDs in file names and use them to identify files");
  public static final StringId MergeLogFiles_ALIGN_NONTIMESTAMPED_LINES_WITH_OTHERS = new StringId(3879, "Align non-timestamped lines with others");
  public static final StringId MergeLogFiles_SUPPRESS_OUTPUT_OF_BLANK_LINES = new StringId(3880, "Suppress output of blank lines");
  public static final StringId MergeLogFiles_USE_MULTITHREADING_TO_TAKE_ADVANTAGE_OF_MULTIPLE_CPUS = new StringId(3881, "Use multithreading to take advantage of multiple CPUs");
  public static final StringId MergeLogFiles_MERGES_MULTIPLE_GEMFIRE_LOG_FILES_AND_SORTS_THEM_BY_TIMESTAMP = new StringId(3882, "Merges multiple GemFire log files and sorts them by timestamp.");
  public static final StringId MergeLogFiles_THE_MERGED_LOG_FILE_IS_WRITTEN_TO_SYSTEM_OUT_OR_A_FILE = new StringId(3883, "The merged log file is written to System.out (or a file).");
  public static final StringId MergeLogFiles_MISSING_NUMBER_OF_PARENT_DIRECTORIES = new StringId(3884, "Missing number of parent directories");
  public static final StringId MergeLogFiles_NOT_A_NUMBER_0 = new StringId(3885, "Not a number: {0}");
  public static final StringId MergeLogFiles_MISSING_MERGE_FILE_NAME = new StringId(3886, "Missing merge file name");
  public static final StringId MergeLogFiles_FILE_0_DOES_NOT_EXIST = new StringId(3887, "File \"{0}\" does not exist");
  public static final StringId MergeLogFiles_MISSING_FILENAME = new StringId(3888, "Missing filename");
  public static final StringId MergeLogFiles_LOG_FILE_READER = new StringId(3889, "Log File Reader");
  public static final StringId MergeLogFiles_EXCEPTION_IN_0 = new StringId(3890, "Exception in {0}");
  public static final StringId MergeLogFiles_READER_THREADS = new StringId(3891, "Reader threads");
  public static final StringId OSProcess_WARNING_0_IS_NOT_A_DIRECTORY_DEFAULTING_TO_CURRENT_DIRECTORY_1 = new StringId(3892, "WARNING: \"{0}\" is not a directory. Defaulting to current directory \"{1}\".");
  public static final StringId OSProcess_REAPER_THREAD = new StringId(3893, "Reaper Thread");
  public static final StringId ConnectionTable_CANCEL_AFTER_ACCEPT = new StringId(3894, "cancel after accept");
  public static final StringId ConnectionTable_CONNECTION_TABLE_NO_LONGER_IN_USE = new StringId(3895, "Connection table no longer in use");
  public static final StringId ConnectionTable_PENDING_CONNECTION_CANCELLED = new StringId(3896, "pending connection cancelled");
  public static final StringId ConnectionTable_PENDING_CONNECTION_CLOSED = new StringId(3897, "pending connection closed");
  public static final StringId ConnectionTable_SOMEONE_ELSE_CREATED_THE_CONNECTION = new StringId(3898, "someone else created the connection");
  public static final StringId ConnectionTable_CONNECTION_TABLE_BEING_DESTROYED = new StringId(3899, "Connection table being destroyed");
  public static final StringId ConnectionTable_THREAD_FINALIZATION = new StringId(3900, "thread finalization");
  public static final StringId ConnectException_COULD_NOT_CONNECT_TO_0 = new StringId(3902, "Could not connect to: {0}");
  public static final StringId ConnectException_CAUSES = new StringId(3903, "Causes:");
  public static final StringId ConnectException_COULD_NOT_CONNECT = new StringId(3904, "Could not connect");
  public static final StringId SortLogFile_USAGE = new StringId(3905, "Usage");
  public static final StringId SortLogFile_FILE_IN_WHICH_TO_PUT_SORTED_LOG = new StringId(3906, "File in which to put sorted log");
  public static final StringId SortLogFile_SORTS_A_GEMFIRE_LOG_FILE_BY_TIMESTAMP_THE_MERGED_LOG_FILE_IS_WRITTEN_TO_SYSTEM_OUT_OR_A_FILE = new StringId(3907, "Sorts a GemFire log file by timestamp. The merged log file is written to System.out (or a file).");
  public static final StringId SortLogFile_FILE_0_DOES_NOT_EXIST = new StringId(3908, "File \"{0}\" does not exist");
  public static final StringId SortLogFile_EXTRANEOUS_COMMAND_LINE_0 = new StringId(3909, "Extraneous command line: {0}");
  public static final StringId SortLogFile_MISSING_FILENAME = new StringId(3910, "Missing filename");
  public static final StringId PureLogWriter_IGNORING_EXCEPTION = new StringId(3911, "Ignoring exception: ");
  public static final StringId CacheHealthEvaluator_THE_AVERAGE_DURATION_OF_A_CACHE_NETSEARCH_0_MS_EXCEEDS_THE_THRESHOLD_1_MS = new StringId(3913, "The average duration of a Cache netSearch ({0} ms) exceeds the threshold ({1} ms)");
  public static final StringId CacheHealthEvaluator_THE_SIZE_OF_THE_CACHE_EVENT_QUEUE_0_MS_EXCEEDS_THE_THRESHOLD_1_MS = new StringId(3915, "The size of the cache event queue ({0} ms) exceeds the threshold ({1} ms)");
  public static final StringId GemFireHealth_GOOD = new StringId(3916, "Good");
  public static final StringId GemFireHealth_OKAY = new StringId(3917, "Okay");
  public static final StringId GemFireHealth_POOR = new StringId(3918, "Poor");
  public static final StringId DistributedSystemHealth_THE_NUMBER_OF_APPLICATIONS_THAT_HAVE_LEFT_THE_DISTRIBUTED_SYSTEM_0_EXCEEDS_THE_THRESHOLD_1 = new StringId(3919, "The number of applications that have left the distributed system ({0}) exceeds the threshold ({1})");
  public static final StringId DistributedSystemHealthMonitor_HEALTH_MONITORS = new StringId(3920, "Health Monitors");
  public static final StringId DistributedSystemHealthMonitor_HEALTH_MONITOR_FOR_0 = new StringId(3921, "Health monitor for {0}");
  public static final StringId DistributedSystemHealthMonitor_INTERRUPTED_WHILE_STOPPING_HEALTH_MONITOR_THREAD = new StringId(3922, "Interrupted while stopping health monitor thread");
  public static final StringId GemFireHealthConfigImpl_DEFAULT_GEMFIRE_HEALTH_CONFIGURATION = new StringId(3933, "Default GemFire health configuration");
  public static final StringId GemFireHealthConfigImpl_GEMFIRE_HEALTH_CONFIGURATION_FOR_HOST_0 = new StringId(3934, "GemFire health configuration for host \"{0}\"");
  public static final StringId MemberHealthEvaluator_THE_SIZE_OF_THIS_VM_0_MEGABYTES_EXCEEDS_THE_THRESHOLD_1_MEGABYTES = new StringId(3936, "The size of this VM ({0} megabytes) exceeds the threshold ({1} megabytes)");
  public static final StringId MemberHealthEvaluator_THE_SIZE_OF_THE_OVERFLOW_QUEUE_0_EXCEEDS_THE_THRESHOLD_1 = new StringId(3937, "The size of the overflow queue ({0}) exceeds the threshold ({1}).");
  public static final StringId MemberHealthEvaluator_THE_NUMBER_OF_MESSAGE_REPLY_TIMEOUTS_0_EXCEEDS_THE_THRESHOLD_1 = new StringId(3938, "The number of message reply timeouts ({0}) exceeds the threshold ({1})");
  public static final StringId MemberHealthEvaluator_THERE_ARE_0_REGIONS_MISSING_REQUIRED_ROLES_BUT_ARE_CONFIGURED_FOR_FULL_ACCESS = new StringId(3939, "There are {0} regions missing required roles; however, they are configured for full access.");
  public static final StringId MemberHealthEvaluator_THERE_ARE_0_REGIONS_MISSING_REQUIRED_ROLES_AND_CONFIGURED_WITH_LIMITED_ACCESS = new StringId(3940, "There are {0} regions missing required roles and configured with limited access.");
  public static final StringId MemberHealthEvaluator_THERE_ARE_0_REGIONS_MISSING_REQUIRED_ROLES_AND_CONFIGURED_WITHOUT_ACCESS = new StringId(3941, "There are {0} regions missing required roles and configured without access.");
  public static final StringId SystemMemberImpl_NO_LOG_FILE_CONFIGURED_LOG_MESSAGES_WILL_BE_DIRECTED_TO_STDOUT = new StringId(3942, "No log file configured, log messages will be directed to stdout.");
  public static final StringId SystemMemberImpl_TAIL_OF_CHILD_LOG = new StringId(3943, "-------------------- tail of child log --------------------");
  public static final StringId AgentImpl_NO_LOG_FILE_CONFIGURED_LOG_MESSAGES_WILL_BE_DIRECTED_TO_STDOUT = new StringId(3944, "No log file configured, log messages will be directed to stdout.");
  public static final StringId AgentImpl_TAIL_OF_CHILD_LOG = new StringId(3945, "-------------------- tail of child log --------------------");
  public static final StringId AgentImpl_AGENT_CONFIG_PROPERTY_FILE_NAME_0 = new StringId(3946, "Agent config property file name: {0}");
  public static final StringId SystemAdmin_ERROR = new StringId(3947, "ERROR");
  public static final StringId AgentImpl_FAILED_READING_CONFIGURATION_0 = new StringId(3948, "Failed reading configuration: {0}");
  public static final StringId MX4JModelMBean_CANNOT_RESTORE_PREVIOUSLY_SAVED_STATUS = new StringId(3950, "Cannot restore previously saved status");
  public static final StringId CacheClientUpdater_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_PUT_ENTRY_REGION_0_KEY_1_VALUE_2 = new StringId(3951, "The following exception occurred while attempting to put entry (region: {0} key: {1} value: {2})");
  public static final StringId CacheClientUpdater_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_INVALIDATE_ENTRY_REGION_0_KEY_1 = new StringId(3952, "The following exception occurred while attempting to invalidate entry (region: {0} key: {1})");
  public static final StringId CacheClientUpdater_CAUGHT_AN_EXCEPTION_WHILE_ATTEMPTING_TO_DESTROY_REGION_0 = new StringId(3953, "Caught an exception while attempting to destroy region {0}");
  public static final StringId CacheClientUpdater_CAUGHT_THE_FOLLOWING_EXCEPTION_WHILE_ATTEMPTING_TO_CLEAR_REGION_0 = new StringId(3954, "Caught the following exception while attempting to clear region {0}");
  public static final StringId Put_REGION_WAS_NOT_FOUND_DURING_PUT_REQUEST = new StringId(3956, ": Region was not found during put request");
  public static final StringId CacheServerLauncher_STARTS_A_GEMFIRE_CACHESERVER_VM = new StringId(3957, "Starts a GemFire CacheServer VM");
  public static final StringId CacheServerLauncher_VMARG = new StringId(3958, "<vmarg> a VM-option passed to the spawned CacheServer VM, example -J-Xmx1024M for a 1 Gb heap");
  public static final StringId CacheServerLauncher_DIR = new StringId(3959, "<workingdir> Directory in which cacheserver runs, default is the current directory");
  public static final StringId CacheServerLauncher_CLASSPATH = new StringId(3960, "<classpath> Location of user classes required by the cache server.  This path is appended to the current classpath.");
  public static final StringId CacheServerLauncher_ATTNAME = new StringId(3961, "<attName> Distributed system attribute such as \"mcast-port\" or \"cache-xml-file\".");
  public static final StringId CacheServerLauncher_STOPS_A_GEMFIRE_CACHESERVER_VM = new StringId(3962, "Stops a GemFire CacheServer VM");
  public static final StringId CacheServerLauncher_STATUS = new StringId(3963, "Reports the status and process id of a GemFire CacheServer VM");
  public static final StringId CacheServerLauncher_ERROR_STARTING_SERVER_PROCESS = new StringId(3964, "Error starting server process. ");
  public static final StringId CacheServerLauncher_ERROR_0 = new StringId(3965, "Error: {0}");
  public static final StringId CacheServerLauncher_STARTING_0_WITH_PID_1 = new StringId(3966, "Starting {0} with pid: {1,number,#}");
  public static final StringId ExecuteFunction_RESULTS_ALREADY_COLLECTED = new StringId(3967, "Function results already collected");
  public static final StringId CacheServerLauncher_THE_SPECIFIED_WORKING_DIRECTORY_0_CONTAINS_NO_STATUS_FILE = new StringId(3968, "The specified working directory ({0}) contains no status file");
  public static final StringId CacheServerLauncher_0_STOPPED = new StringId(3969, "The {0} has stopped.");
  public static final StringId CacheServerLauncher_TIMEOUT_WAITING_FOR_0_TO_SHUTDOWN_STATUS_IS_1 = new StringId(3970, "Timeout waiting for {0} to shutdown, status is: {1}");
  public static final StringId CacheServerLauncher_SEE_LOG_FILE_FOR_DETAILS = new StringId(3971, "See log file for details.");
  public static final StringId DistributionManager__0_MESSAGE_DISTRIBUTION_HAS_TERMINATED = new StringId(3972, "{0}: Message distribution has terminated");
  public static final StringId SystemFailure_DISTRIBUTION_HALTED_DUE_TO_JVM_CORRUPTION = new StringId(3973, "Distribution halted due to JVM corruption");
  public static final StringId DistributionManager_SERIAL_MESSAGE_PROCESSOR = new StringId(3976, "Serial Message Processor");
  public static final StringId DistributionManager_VIEW_MESSAGE_PROCESSOR = new StringId(3977, "View Message Processor");
  public static final StringId DistributionManager_POOLED_MESSAGE_PROCESSOR = new StringId(3978, "Pooled Message Processor ");
  public static final StringId DistributionManager_POOLED_HIGH_PRIORITY_MESSAGE_PROCESSOR = new StringId(3979, "Pooled High Priority Message Processor ");
  public static final StringId DistributionManager_POOLED_WAITING_MESSAGE_PROCESSOR = new StringId(3980, "Pooled Waiting Message Processor ");
  public static final StringId PasswordUtil_ENCRYPTED_TO_0 = new StringId(3981, "Encrypted to {0}");
  public static final StringId DistributionManager_SHUTDOWN_MESSAGE_THREAD_FOR_0 = new StringId(3982, "Shutdown Message Thread for {0}");
  public static final StringId HealthMonitorImpl_HEALTH_MONITOR_OWNED_BY_0 = new StringId(3983, "Health Monitor owned by {0}");
  public static final StringId AbstractDistributionConfig_SECURITY_UDP_DHALGO_NAME_0 = new StringId(3984, "User defined name for the symmetric encryption algorithm to use in Diffie-Hellman key exchange for encryption of udp messages.  Defaults to \"{0}\". Legal values can be any of the available symmetric algorithm names in JDK like \"DES\", \"DESede\", \"AES\", \"Blowfish\". It may be required to install Unlimited Strength Jurisdiction Policy Files from Sun for some symmetric algorithms to work (like \"AES\")");
  // ok to reuse 3984
  public static final StringId InternalDistributedSystem_COULD_NOT_RENAME_0_TO_1 = new StringId(3985, "Could not rename \"{0}\" to \"{1}\".");
  public static final StringId InternalDistributedSystem_RENAMED_OLD_LOG_FILE_TO_0 = new StringId(3986, "Renamed old log file to \"{0}\".");
  public static final StringId InternalDistributedSystem_COULD_NOT_OPEN_LOG_FILE_0 = new StringId(3987, "Could not open log file \"{0}\".");
  public static final StringId SystemFailure_DISTRIBUTED_SYSTEM_DISCONNECTED_DUE_TO_JVM_CORRUPTION = new StringId(3988, "Distributed system disconnected due to JVM corruption");
  public static final StringId TransactionManagerImpl_CLEAN_UP_THREADS = new StringId(3990, "Clean up threads");
  public static final StringId InternalDistributedSystem_NORMAL_DISCONNECT = new StringId(3991, "normal disconnect");
  public static final StringId InternalDistributedSystem_NO_DISTRIBUTION_MANAGER = new StringId(3993, "no distribution manager");
  public static final StringId DestroyRegion__THE_INPUT_REGION_NAME_FOR_THE_DESTROY_REGION_REQUEST_IS_NULL = new StringId(4001, " The input region name for the destroy region request is null.");
  public static final StringId DestroyRegion_REGION_WAS_NOT_FOUND_DURING_DESTROY_REGION_REQUEST = new StringId(4002, "Region was not found during destroy region request");
  //ok to reuse 4003
  public static final StringId CreateRegionProcessor_CANNOT_CREATE_PARTITIONEDREGION_0_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_REGION_DEFINED_AS_A_NON_PARTITIONEDREGION = new StringId(4004, "Cannot create PartitionedRegion {0} because another cache ({1}) has the same region defined as a non PartitionedRegion.");
  public static final StringId CreateRegionProcessor_CANNOT_CREATE_THE_NON_PARTITIONEDREGION_0_BECAUSE_ANOTHER_CACHE_HAS_A_PARTITIONED_REGION_DEFINED_WITH_THE_SAME_NAME = new StringId(4005, "Cannot create the non PartitionedRegion {0} because another cache ({1}) has a Partitioned Region defined with the same name.");
  public static final StringId CreateRegionProcessor_CANNOT_CREATE_REGION_0_WITH_1_SCOPE_BECAUSE_ANOTHER_CACHE_HAS_SAME_REGION_WITH_2_SCOPE = new StringId(4006, "Cannot create region {0} with {1} scope because another cache ({2}) has same region with {3} scope.");
  public static final StringId AvailablePort_THIS_PROGRAM_EITHER_PRINTS_WHETHER_OR_NOT_A_PORT_IS_AVAILABLE_FOR_A_GIVEN_PROTOCOL_OR_IT_PRINTS_OUT_AN_AVAILABLE_PORT_FOR_A_GIVEN_PROTOCOL = new StringId(4007, "This program either prints whether or not a port is available for a given protocol, or it prints out an available port for a given protocol.");
  public static final StringId CacheInfoRequest_FETCH_CACHE_UP_TIME = new StringId(4008, "Fetch cache up time");
  public static final StringId ResultsCollectionWrapper_CONSTRAINT_VIOLATION_0_IS_NOT_A_1 = new StringId(4009, "Constraint Violation: {0} is not a {1}");
  public static final StringId DistributionConfigSnapshot_THE_0_CONFIGURATION_ATTRIBUTE_CAN_NOT_BE_MODIFIED_WHILE_THE_SYSTEM_IS_RUNNING = new StringId(4011, "The \"{0}\" configuration attribute can not be modified while the system is running.");
  public static final StringId BaseCommand_INVALID_DATA_RECEIVED_PLEASE_SEE_THE_CACHE_SERVER_LOG_FILE_FOR_ADDITIONAL_DETAILS = new StringId(4012, "Invalid data received. Please see the cache server log file for additional details.");
  public static final StringId LRUCapacityController_LRUCAPACITYCONTROLLER_WITH_A_CAPACITY_OF_0_ENTRIES_AND_EVICTION_ACTION_1 = new StringId(4013, "LRUCapacityController with a capacity of {0} entries and eviction action {1}");
  public static final StringId HeapLRUCapacityController_HEAPLRUCAPACITYCONTROLLER_WITH_A_CAPACITY_OF_0_OF_HEAP_AND_AN_THREAD_INTERVAL_OF_1_AND_EVICTION_ACTION_2 = new StringId(4014, "HeapLRUCapacityController with a capacity of {0}% of memory and eviction action {1}.");
  public static final StringId ShutdownMessage_SHUTDOWN_MESSAGE_RECEIVED = new StringId(4016, "shutdown message received");
  public static final StringId ShutdownMessage_SHUTDOWNMESSAGE_DM_0_HAS_SHUTDOWN = new StringId(4017, "ShutdownMessage DM {0} has shutdown");
  public static final StringId StartupOperation_LEFT_THE_MEMBERSHIP_VIEW = new StringId(4018, "left the membership view");
  public static final StringId StartupOperation_DISAPPEARED_DURING_STARTUP_HANDSHAKE = new StringId(4019, "disappeared during startup handshake");
  public static final StringId DistributionLocator__0_IS_NOT_A_VALID_IP_ADDRESS_FOR_THIS_MACHINE = new StringId(4020, "''{0}'' is not a valid IP address for this machine");
  public static final StringId DistributionLocator_DISTRIBUTION_LOCATOR_ON_0 = new StringId(4022, "Distribution Locator on {0}");
  public static final StringId AddHealthListenerRequest_ADD_HEALTH_LISTENER = new StringId(4024, "Add health listener");
  public static final StringId AddStatListenerRequest_ADD_STATISTIC_RESOURCE_LISTENER = new StringId(4025, "Add statistic resource listener");
  public static final StringId AdminConsoleDisconnectMessage_AUTOMATIC_ADMIN_DISCONNECT_0 = new StringId(4026, "Reason for automatic admin disconnect : {0}");
  public static final StringId AdminRequest_COULD_NOT_RETURN_ONE_RECIPIENT_BECAUSE_THIS_MESSAGE_HAS_0_RECIPIENTS = new StringId(4027, "Could not return one recipient because this message has {0} recipients");
  public static final StringId AdminResponse_COULD_NOT_RETURN_ONE_RECIPIENT_BECAUSE_THIS_MESSAGE_HAS_0_RECIPIENTS = new StringId(4028, "Could not return one recipient because this message has {0} recipients");
  public static final StringId AdminWaiters_REQUEST_WAIT_WAS_INTERRUPTED = new StringId(4029, "Request wait was interrupted.");
  public static final StringId AlertLevelChangeMessage_CHANGING_ALERT_LEVEL_TO_0 = new StringId(4030, "Changing alert level to {0}");
  public static final StringId BridgeServerRequest_ADD_BRIDGE_SERVER = new StringId(4031, "Add bridge server");
  public static final StringId BridgeServerRequest_GET_INFO_ABOUT_BRIDGE_SERVER_0 = new StringId(4032, "Get info about cache server {0}");
  public static final StringId BridgeServerRequest_START_BRIDGE_SERVER_0 = new StringId(4033, "Start cache server {0}");
  public static final StringId BridgeServerRequest_STOP_BRIDGE_SERVER_0 = new StringId(4034, "Stop cache server {0}");
  public static final StringId BridgeServerRequest_UNKNOWN_OPERATION_0 = new StringId(4035, "Unknown operation {0}");
  public static final StringId CacheConfigRequest_SET_A_SINGLE_CACHE_CONFIGURATION_ATTRIBUTE = new StringId(4036, "Set a single cache configuration attribute");
  public static final StringId CancellationMessage_CANCELLATIONMESSAGE_FROM_0_FOR_MESSAGE_ID_1 = new StringId(4037, "CancellationMessage from {0} for message id {1}");
  public static final StringId CancelStatListenerRequest_REMOVE_STATISTIC_RESOURCE_LISTENER = new StringId(4038, "Remove statistic resource listener");
  public static final StringId CancelStatListenerRequest_CANCELSTATLISTENERREQUEST_FROM_0_FOR_1 = new StringId(4039, "CancelStatListenerRequest from {0} for {1}");
  public static final StringId CancelStatListenerResponse_CANCELSTATLISTENERRESPONSE_FROM_0 = new StringId(4040, "CancelStatListenerResponse from {0}");
  public static final StringId DestroyEntryMessage_DESTROYENTRYMESSAGE_FROM_0 = new StringId(4041, "DestroyEntryMessage from {0}");
  public static final StringId DestroyRegionMessage_DESTROYREGIONMESSAGE_FROM_0 = new StringId(4042, "DestroyRegionMessage from {0}");
  public static final StringId FetchDistLockInfoRequest_LIST_DISTRIBUTED_LOCKS = new StringId(4043, "List distributed locks");
  public static final StringId FetchDistLockInfoRequest_FETCHDISTLOCKINFOREQUEST_FROM_0 = new StringId(4044, "FetchDistLockInfoRequest from {0}");
  public static final StringId FetchDistLockInfoResponse_FETCHDISTLOCKINFORESPONSE_FROM_0 = new StringId(4045, "FetchDistLockInfoResponse from {0}");
  public static final StringId FetchHealthDiagnosisRequest_FETCH_HEALTH_DIAGNOSIS_FOR_HEALTH_CODE_0 = new StringId(4046, "fetch health diagnosis for health code {0}");
  public static final StringId FetchHealthDiagnosisRequest_FETCHHEALTHDIAGNOSISREQUEST_FROM_ID_1_HEALTHCODE_2 = new StringId(4047, "FetchHealthDiagnosisRequest from id={1} healthCode={2}");
  public static final StringId FetchHostRequest_FETCH_REMOTE_HOST = new StringId(4048, "Fetch remote host");
  public static final StringId FetchHostRequest_FETCHHOSTREQUEST_FOR_0 = new StringId(4049, "FetchHostRequest for {0}");
  public static final StringId FetchHostResponse_FETCHHOSTRESPONSE_FOR_0_HOST_1 = new StringId(4050, "FetchHostResponse for {0} host={1}");
  public static final StringId FetchResourceAttributesRequest_FETCH_STATISTICS_FOR_RESOURCE = new StringId(4051, "Fetch statistics for resource");
  public static final StringId FetchResourceAttributesRequest_FETCHRESOURCEATTRIBUTESREQUEST_FOR_0 = new StringId(4052, "Fetch statistics for {0}");
  public static final StringId FetchSysCfgRequest_FETCH_CONFIGURATION_PARAMETERS = new StringId(4053, "Fetch configuration parameters");
  public static final StringId HealthListenerMessage_THE_STATUS_OF_LISTENER_0_IS_1 = new StringId(4054, "The status of listener {0} is {1}");
  public static final StringId LicenseInfoRequest_FETCH_CURRENT_LICENSE_INFORMATION = new StringId(4055, "Fetch current license information");
  public static final StringId ObjectDetailsRequest_INSPECT_CACHED_OBJECT = new StringId(4056, "Inspect cached object");
  public static final StringId ObjectNamesRequest_LIST_CACHED_OBJECTS = new StringId(4057, "List cached objects");
  public static final StringId RegionAttributesRequest_FETCH_REGION_ATTRIBUTES = new StringId(4058, "Fetch region attributes");
  public static final StringId RegionRequest_GET_A_SPECIFIC_REGION_FROM_THE_ROOT = new StringId(4059, "Get a specific region from the root");
  public static final StringId RegionRequest_CREATE_A_NEW_ROOT_VM_REGION = new StringId(4060, "Create a new root VM region");
  public static final StringId RegionRequest_CREATE_A_NEW_VM_REGION = new StringId(4061, "Create a new VM region");
  public static final StringId RegionRequest_UNKNOWN_OPERATION_0 = new StringId(4062, "Unknown operation {0}");
  public static final StringId RegionSizeRequest_FETCH_REGION_SIZE = new StringId(4063, "Fetch region size");
  public static final StringId RegionStatisticsRequest_FETCH_REGION_STATISTICS = new StringId(4064, "Fetch region statistics");
  public static final StringId RemoteCacheInfo_INFORMATION_ABOUT_THE_CACHE_0_WITH_1_BRIDGE_SERVERS = new StringId(4065, "Information about the cache \"{0}\" with {1} bridge servers");
  public static final StringId RemoteGfManagerAgent_DISCONNECT_LISTENER_FOR_0 = new StringId(4066, "Disconnect listener for {0}");
  public static final StringId RemoveHealthListenerRequest_REMOVE_HEALTH_LISTENER = new StringId(4067, "Remove health listener");
  public static final StringId ResetHealthStatusRequest_RESET_HEALTH_STATUS = new StringId(4068, "reset health status");
  public static final StringId RootRegionRequest_INSPECT_ROOT_CACHE_REGIONS = new StringId(4069, "Inspect root cache regions");
  public static final StringId StoreSysCfgRequest_APPLY_CONFIGURATION_PARAMETERS = new StringId(4070, "Apply configuration parameters");
  public static final StringId SubRegionRequest_LIST_SUBREGIONS = new StringId(4071, "List subregions");
  public static final StringId TailLogRequest_TAIL_SYSTEM_LOG = new StringId(4072, "Tail system log");
  public static final StringId TailLogResponse_NO_LOG_FILE_WAS_SPECIFIED_IN_THE_CONFIGURATION_MESSAGES_WILL_BE_DIRECTED_TO_STDOUT = new StringId(4073, "No log file was specified in the configuration, messages will be directed to stdout.");
  public static final StringId VersionInfoRequest_FETCH_CURRENT_VERSION_INFORMATION = new StringId(4074, "Fetch current version information");
  public static final StringId ArchiveSplitter_USAGE = new StringId(4075, "Usage");
  public static final StringId DistributedRegion_RECONNECT_DISTRIBUTED_SYSTEM = new StringId(4076, "Reconnect Distributed System");
  public static final StringId CloseCQ_THE_CQNAME_FOR_THE_CQ_CLOSE_REQUEST_IS_NULL = new StringId(4078, "The cqName for the cq close request is null");
  public static final StringId CloseCQ_EXCEPTION_WHILE_CLOSING_CQ_CQNAME_0 = new StringId(4079, "Exception while closing CQ CqName :{0}");
  public static final StringId CloseCQ_CQ_CLOSED_SUCCESSFULLY = new StringId(4080, "cq closed successfully.");
  public static final StringId CreateRegion__0_WAS_NOT_FOUND_DURING_SUBREGION_CREATION_REQUEST = new StringId(4081, "{0} was not found during subregion creation request");
  public static final StringId Destroy__THE_INPUT_KEY_FOR_THE_DESTROY_REQUEST_IS_NULL = new StringId(4082, " The input key for the destroy request is null");
  public static final StringId Destroy__THE_INPUT_REGION_NAME_FOR_THE_DESTROY_REQUEST_IS_NULL = new StringId(4083, " The input region name for the destroy request is null");
  public static final StringId Destroy__0_WAS_NOT_FOUND_DURING_DESTROY_REQUEST = new StringId(4084, "{0} was not found during destroy request");
  public static final StringId ExecuteCQ_SERVER_NOTIFYBYSUBSCRIPTION_MODE_IS_SET_TO_FALSE_CQ_EXECUTION_IS_NOT_SUPPORTED_IN_THIS_MODE = new StringId(4085, "Server notifyBySubscription mode is set to false. CQ execution is not supported in this mode.");
  public static final StringId HandShake_HANDSHAKE_FAILED_TO_FIND_PUBLIC_KEY_FOR_SERVER_WITH_SUBJECT_0 = new StringId(4086, "HandShake failed to find public key for server with subject {0}");
  public static final StringId ExecuteCQ_CQ_CREATED_SUCCESSFULLY = new StringId(4088, "cq created successfully.");
  public static final StringId KeySet__0_WAS_NOT_FOUND_DURING_KEY_SET_REQUEST = new StringId(4089, "{0} was not found during key set request");
  public static final StringId MonitorCQ__0_THE_MONITORCQ_OPERATION_IS_INVALID = new StringId(4090, "{0}: The MonitorCq operation is invalid.");
  public static final StringId MonitorCQ__0_A_NULL_REGION_NAME_WAS_PASSED_FOR_MONITORCQ_OPERATION = new StringId(4091, "{0}: A null Region name was passed for MonitorCq operation.");
  public static final StringId MonitorCQ_EXCEPTION_WHILE_HANDLING_THE_MONITOR_REQUEST_OP_IS_0 = new StringId(4092, "Exception while handling the monitor request, the operation is {0}");
  public static final StringId Request__0_WAS_NOT_FOUND_DURING_GET_REQUEST = new StringId(4094, "{0} was not found during get request");
  public static final StringId StopCQ_THE_CQNAME_FOR_THE_CQ_STOP_REQUEST_IS_NULL = new StringId(4095, "The cqName for the cq stop request is null");
  public static final StringId StopCQ_CQ_STOPPED_SUCCESSFULLY = new StringId(4096, "cq stopped successfully.");
  public static final StringId ServerConnection_ERROR_IN_GETSOCKETSTRING_0 = new StringId(4097, "Error in getSocketString: {0}");
  public static final StringId TXManagerImpl_CAN_NOT_COMMIT_THIS_TRANSACTION_BECAUSE_IT_IS_ENLISTED_WITH_A_JTA_TRANSACTION_USE_THE_JTA_MANAGER_TO_PERFORM_THE_COMMIT = new StringId(4099, "Can not commit this transaction because it is enlisted with a JTA transaction, use the JTA manager to perform the commit.");
  public static final StringId TXManagerImpl_CAN_NOT_ROLLBACK_THIS_TRANSACTION_IS_ENLISTED_WITH_A_JTA_TRANSACTION_USE_THE_JTA_MANAGER_TO_PERFORM_THE_ROLLBACK = new StringId(4100, "Can not rollback this transaction is enlisted with a JTA transaction, use the JTA manager to perform the rollback.");
  public static final StringId CacheXmlParser_EXCEPTION_IN_PARSING_ELEMENT_0_THIS_IS_A_REQUIRED_FIELD = new StringId(4101, "Exception in parsing element {0}. This is a required field.");
  public static final StringId AbstractDataSource_CANNOT_CREATE_A_CONNECTION_WITH_THE_USER_0_AS_IT_DOESNT_MATCH_THE_EXISTING_USER_NAMED_1_OR_THE_PASSWORD_WAS_INCORRECT = new StringId(4103, "Cannot create a connection with the user, {0}, as it doesnt match the existing user named {1}, or the password was incorrect.");
  public static final StringId DataSourceFactory_DATASOURCEFACTORY_INVOKEALLMETHODS_EXCEPTION_IN_CREATING_CLASS_WITH_THE_GIVEN_CONFIGPROPERTYTYPE_CLASSNAME_EXCEPTION_STRING_0 = new StringId(4104, "DataSourceFactory::invokeAllMethods: Exception in creating Class with the given config-property-type classname. Exception string={0}");
  public static final StringId DataSourceFactory_DATASOURCEFACTORY_INVOKEALLMETHODS_EXCEPTION_IN_CREATING_METHOD_USING_CONFIGPROPERTYNAME_PROPERTY_EXCEPTION_STRING_0 = new StringId(4105, "DataSourceFactory::invokeAllMethods: Exception in creating method using config-property-name property. Exception string={0}");
  public static final StringId DataSourceFactory_DATASOURCEFACTORY_INVOKEALLMETHODS_EXCEPTION_IN_CREATING_INSTANCE_OF_THE_CLASS_USING_THE_CONSTRUCTOR_WITH_A_STRING_PARAMETER_EXCEPTION_STRING_0 = new StringId(4106, "DataSourceFactory::invokeAllMethods: Exception in creating instance of the class using the constructor with a String parameter. Exception string={0}");
  public static final StringId FacetsJCAConnectionManagerImpl_FACETSJCACONNECTIONMANAGERIMPL_CONSTRUCTOR_AN_EXCEPTION_WAS_CAUGHT_WHILE_INITIALIZING_DUE_TO_0 = new StringId(4107, "FacetsJCAConnectionManagerImpl::Constructor: An Exception was caught while initializing due to {0}");
  public static final StringId FacetsJCAConnectionManagerImpl_FACETSJCACONNECTIONMANAGERIMPL_AN_EXCEPTION_WAS_CAUGHT_WHILE_ALLOCATING_A_CONNECTION_DUE_TO_0 = new StringId(4108, "FacetsJCAConnectionManagerImpl:: An Exception was caught while allocating a connection due to {0}");
  public static final StringId ElderInitProcessor_0_DISREGARDING_REQUEST_FROM_DEPARTED_MEMBER = new StringId(4109, "{0}: disregarding request from departed member.");
  public static final StringId DistributionLocator_USAGE = new StringId(4110, "Usage");
  public static final StringId DistributionLocator_A_ZEROLENGTH_ADDRESS_WILL_BIND_TO_LOCALHOST = new StringId(4111, "A zero-length address will bind to localhost");
  public static final StringId GemFireFormatter_IGNORING_THE_FOLLOWING_EXCEPTION = new StringId(4112, "Ignoring the following exception:");
  public static final StringId InternalInstantiator_REGISTER_INSTANTIATOR_0_OF_CLASS_1_THAT_INSTANTIATES_A_2 = new StringId(4113, "Register Instantiator {0} of class {1} that instantiates a {2}");
  public static final StringId TransactionImpl_TRANSACTIONIMPL_SETROLLBACKONLY_NO_GLOBAL_TRANSACTION_EXISTS = new StringId(4114, "TransactionImpl::setRollbackOnly: No global transaction exists.");
  public static final StringId GemFireCacheImpl_MEMCACHED_SERVER_ON_PORT_0_IS_SHUTTING_DOWN = new StringId(4115, "GemFireMemcachedServer on port {0} is shutting down");
  public static final StringId TransactionImpl_SETTRANSACTIONTIMEOUT_IS_NOT_SUPPORTED = new StringId(4116, "setTransactionTimeout is not supported.");
  public static final StringId TransactionImpl_TRANSACTIONIMPL_ENLISTRESOURCE_NO_GLOBAL_TRANSACTION_EXISTS = new StringId(4117, "TransactionImpl::enlistResource: No global transaction exists");
  public static final StringId TransactionImpl_TRANSACTIONIMPL_DELISTRESOURCE_NO_GLOBAL_TRANSACTION_EXISTS = new StringId(4118, "TransactionImpl::delistResource: No global transaction exists");
  public static final StringId TransactionImpl_TRANSACTIONIMPL_REGISTERSYNCHRONIZATION_SYNCHRONIZATION_CANNOT_BE_REGISTERED_BECAUSE_THE_TRANSACTION_HAS_BEEN_MARKED_FOR_ROLLBACK = new StringId(4119, "TransactionImpl::registerSynchronization: Synchronization cannot be registered because the transaction has been marked for rollback");
  public static final StringId TransactionImpl_TRANSACTIONIMPL_REGISTERSYNCHRONIZATION_SYNCHRONIZATION_CANNOT_BE_REGISTERED_ON_A_TRANSACTION_WHICH_IS_NOT_ACTIVE = new StringId(4120, "TransactionImpl::registerSynchronization: Synchronization cannot be registered on a transaction which is not active");
  public static final StringId LogFileParser_MISSING_LOG_FILE_NAME = new StringId(4121, "** Missing log file name");
  public static final StringId ManagerInfo_STOPPED = new StringId(4122, "stopped");
  public static final StringId ManagerInfo_STOPPING = new StringId(4123, "stopping");
  public static final StringId ManagerInfo_KILLED = new StringId(4124, "killed");
  public static final StringId ManagerInfo_STARTING = new StringId(4125, "starting");
  public static final StringId ManagerInfo_RUNNING = new StringId(4126, "running");
  public static final StringId ProcessOutputReader_FAILED_TO_GET_EXIT_STATUS_AND_IT_WROTE_TO_STDERR_SO_SETTING_EXIT_STATUS_TO_1 = new StringId(4127, "Failed to get exit status and it wrote to stderr so setting exit status to 1.");
  public static final StringId CacheClientNotifier_CACHECLIENTNOTIFIER_POST_PROCESS_AUTHORIZATION_CALLBACK_ENABLED_BUT_AUTHENTICATION_CALLBACK_0_RETURNED_WITH_NULL_CREDENTIALS_FOR_PROXYID_1 = new StringId(4128, "CacheClientNotifier: Post process authorization callback enabled but authentication callback ({0}) returned with null credentials for proxyID: {1}");
  public static final StringId CacheClientNotifier_AN_EXCEPTION_WAS_THROWN_FOR_CLIENT_0_1 = new StringId(4129, "An exception was thrown for client [{0}]. {1}");
  public static final StringId HandShake_AN_EXCEPTION_WAS_THROWN_WHILE_SENDING_WAN_CREDENTIALS_0 = new StringId(4130, "An exception was thrown while sending wan credentials: {0}");
  public static final StringId SystemTimer_TIMER_TASK_0_ENCOUNTERED_EXCEPTION = new StringId(4132, "Timer task <{0}> encountered exception");
  public static final StringId CacheClientUpdater_0_SSL_NEGOTIATION_FAILED_1 = new StringId(4135, "{0} SSL negotiation failed. {1}");
  public static final StringId CacheClientUpdater_0_SECURITY_EXCEPTION_WHEN_CREATING_SERVERTOCLIENT_COMMUNICATION_SOCKET_1 = new StringId(4137, "{0}: Security exception when creating server-to-client communication socket. {1}");
  public static final StringId ServerConnection_0_AN_EXCEPTION_WAS_THROWN_WHILE_CLOSING_CLIENT_AUTHORIZATION_CALLBACK_1 = new StringId(4144, "{0}: An exception was thrown while closing client authorization callback. {1}");
  public static final StringId ServerConnection_0_AN_EXCEPTION_WAS_THROWN_WHILE_CLOSING_CLIENT_POSTPROCESS_AUTHORIZATION_CALLBACK_1 = new StringId(4145, "{0}: An exception was thrown while closing client post-process authorization callback. {1}");
  public static final StringId Connection_0_TIMED_OUT_DURING_A_MEMBERSHIP_CHECK = new StringId(4148, "{0} timed out during a membership check.");
  public static final StringId GroupMembershipService_THE_MEMBERSHIP_CHECK_WAS_TERMINATED_WITH_AN_EXCEPTION = new StringId(4149, "The membership check was terminated with an exception.");
  public static final StringId DynamicRegionFactory_EXCEPTION_WHEN_REGISTERING_INTEREST_FOR_ALL_KEYS_IN_DYNAMIC_REGION_0_1 = new StringId(4150, "Exception when registering interest for all keys in dynamic region [{0}]. {1}");
  public static final StringId ElderInitProcessor__0_RETURNING_EMPTY_LISTS_BECAUSE_I_KNOW_OF_NO_OTHER_MEMBERS = new StringId(4152, "{0}: returning empty lists because I know of no other members.");
  public static final StringId FilterPostAuthorization_FILTERPOSTAUTHORIZATION_AN_EXCEPTION_WAS_THROWN_WHILE_TRYING_TO_DESERIALIZE = new StringId(4155, "FilterPostAuthorization: An exception was thrown while trying to de-serialize.");
  public static final StringId FilterPostAuthorization_FILTERPOSTAUTHORIZATION_AN_EXCEPTION_WAS_THROWN_WHILE_TRYING_TO_SERIALIZE = new StringId(4156, "FilterPostAuthorization: An exception was thrown while trying to serialize.");
  public static final StringId FilterPostAuthorization_FILTERPOSTAUTHORIZATION_THE_USER_0_IS_NOT_AUTHORIZED_FOR_THE_OBJECT_1 = new StringId(4157, "FilterPostAuthorization: The user [{0}] is not authorized for the object {1}.");
  public static final StringId FilterPostAuthorization_FILTERPOSTAUTHORIZATION_THE_OBJECT_OF_TYPE_0_IS_NOT_AN_INSTANCE_OF_1 = new StringId(4158, "FilterPostAuthorization: The object of type {0}, is not an instance of {1}.");
  public static final StringId AuthorizeRequest_AUTHORIZEREQUEST_CLIENT_0_IS_SETTING_AUTHORIZATION_CALLBACK_TO_1 = new StringId(4159, "AuthorizeRequest: Client[{0}] is setting authorization callback to {1}.");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_GET_OPERATION_ON_REGION_0 = new StringId(4160, "Not authorized to perform GET operation on region [{0}]");
  public static final StringId CqEventImpl_EXCEPTION_OCCURED_WHILE_APPLYING_QUERY_ON_A_CACHE_EVENT = new StringId(4161, "Exception occurred while applying query on a cache event.");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_PUT_OPERATION_ON_REGION_0 = new StringId(4162, "Not authorized to perform PUT operation on region {0}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_DESTROY_OPERATION_ON_REGION_0 = new StringId(4163, "Not authorized to perform DESTROY operation on region {0}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFOM_QUERY_OPERATION_0_ON_THE_CACHE = new StringId(4164, "Not authorized to perfom QUERY operation [{0}] on the cache");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFOM_EXECUTE_CQ_OPERATION_0_ON_THE_CACHE = new StringId(4165, "Not authorized to perfom EXECUTE_CQ operation [{0}] on the cache");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFOM_STOP_CQ_OPERATION_0_ON_THE_CACHE = new StringId(4166, "Not authorized to perfom STOP_CQ operation [{0}] on the cache");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFOM_CLOSE_CQ_OPERATION_0_ON_THE_CACHE = new StringId(4167, "Not authorized to perfom CLOSE_CQ operation [{0}] on the cache");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_REGION_CLEAR_OPERATION_ON_REGION_0 = new StringId(4168, "Not authorized to perform REGION_CLEAR operation on region {0}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_REGISTER_INTEREST_OPERATION_FOR_REGION_0 = new StringId(4169, "Not authorized to perform REGISTER_INTEREST operation for region {0}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_REGISTER_INTEREST_LIST_OPERATION_FOR_REGION_0 = new StringId(4170, "Not authorized to perform REGISTER_INTEREST_LIST operation for region {0}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_UNREGISTER_INTEREST_OPERATION_FOR_REGION_0 = new StringId(4171, "Not authorized to perform UNREGISTER_INTEREST operation for region {0}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_UNREGISTER_INTEREST_LIST_OPERATION_FOR_REGION_0 = new StringId(4172, "Not authorized to perform UNREGISTER_INTEREST_LIST operation for region {0}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_KEY_SET_OPERATION_ON_REGION_0 = new StringId(4173, "Not authorized to perform KEY_SET operation on region {0}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_CONTAINS_KEY_OPERATION_ON_REGION_0 = new StringId(4174, "Not authorized to perform CONTAINS_KEY operation on region {0}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_CREATE_REGION_OPERATION_FOR_THE_REGION_0 = new StringId(4175, "Not authorized to perform CREATE_REGION operation for the region {0}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_REGION_DESTROY_OPERATION_FOR_THE_REGION_0 = new StringId(4176, "Not authorized to perform REGION_DESTROY operation for the region {0}");
  public static final StringId CqService_ERROR_WHILE_PROCESSING_CQ_ON_THE_EVENT_KEY_0_CQNAME_1_ERROR_2 = new StringId(4177, "Error while processing CQ on the event, key : {0} CqName: {1}, Error: {2}");
  public static final StringId CacheClientProxy__0_NOT_ADDING_MESSAGE_TO_QUEUE_1_BECAUSE_THE_OPERATION_CONTEXT_OBJECT_COULD_NOT_BE_OBTAINED_FOR_THIS_CLIENT_MESSAGE = new StringId(4178, "{0}: Not Adding message to queue: {1} because the operation context object could not be obtained for this client message.");
  public static final StringId CacheClientProxy__0_NOT_ADDING_MESSAGE_TO_QUEUE_1_BECAUSE_AUTHORIZATION_FAILED = new StringId(4179, "{0}: Not Adding message to queue {1} because authorization failed.");
  public static final StringId TCPConduit_STOPPING_P2P_LISTENER_DUE_TO_SSL_CONFIGURATION_PROBLEM = new StringId(4180, "Stopping P2P listener due to SSL configuration problem.");
  public static final StringId AuthorizeRequestPP_AUTHORIZEREQUESTPP_SETTING_POST_PROCESS_AUTHORIZATION_CALLBACK_TO_1_FOR_CLIENT_0 = new StringId(4181, "AuthorizeRequestPP: Setting post process authorization callback to {1} for client[{0}].");
  public static final StringId AuthorizeRequestPP_IN_POSTPROCESS_NOT_AUTHORIZED_TO_PERFORM_GET_OPERATION_ON_REGION_0 = new StringId(4182, "In post-process: not authorized to perform GET operation on region {0}");
  public static final StringId AuthorizeRequestPP_IN_POSTPROCESS_NOT_AUTHORIZED_TO_PERFORM_QUERY_OPERATION_0_ON_THE_CACHE = new StringId(4183, "In post-process: not authorized to perform QUERY operation {0} on the cache");
  public static final StringId AuthorizeRequestPP_IN_POSTPROCESS_NOT_AUTHORIZED_TO_PERFORM_EXECUTE_CQ_OPERATION_0_ON_THE_CACHE = new StringId(4184, "In post-process: not authorized to perform EXECUTE_CQ operation {0} on the cache");
  public static final StringId AuthorizeRequestPP_IN_POSTPROCESS_NOT_AUTHORIZED_TO_PERFORM_KEY_SET_OPERATION_ON_REGION_0 = new StringId(4185, "In post-process: not authorized to perform KEY_SET operation on region {0}");
  public static final StringId ServerConnection_DUPLICATE_DURABLE_CLIENTID_0 = new StringId(4187, "Duplicate durable clientId ({0})");
  public static final StringId PrimaryKeyIndex_FOR_A_PRIMARYKEY_INDEX_A_RANGE_HAS_NO_MEANING = new StringId(4189, "For a PrimaryKey Index , a range has no meaning");
  public static final StringId BaseCommand_REGION_DESTROYED_DURING_THE_EXECUTION_OF_THE_QUERY = new StringId(4190, "Region destroyed during the execution of the query");
  public static final StringId HandShake_SERVER_EXPECTING_SSL_CONNECTION = new StringId(4192, "Server expecting SSL connection");
  public static final StringId HandShake_FAILED_TO_ACQUIRE_AUTHINITIALIZE_METHOD_0 = new StringId(4194, "Failed to acquire AuthInitialize method {0}");
  public static final StringId HandShake_NO_SECURITY_PROPERTIES_ARE_PROVIDED = new StringId(4195, "No security-* properties are provided");
  public static final StringId HandShake_FAILURE_IN_READING_CREDENTIALS = new StringId(4196, "Failure in reading credentials");
  public static final StringId HandShake_FAILED_TO_ACQUIRE_AUTHENTICATOR_OBJECT = new StringId(4197, "Failed to acquire Authenticator object");
  public static final StringId HandShake_AUTHENTICATOR_INSTANCE_COULD_NOT_BE_OBTAINED = new StringId(4198, "Authenticator instance could not be obtained");
  public static final StringId HandShake_UNABLE_TO_DESERIALIZE_MEMBER = new StringId(4200, "Unable to deserialize member");
  public static final StringId CacheClientUpdater__0_IS_WAITING_FOR_1_TO_COMPLETE = new StringId(4201, "{0} is waiting for {1} to complete.");
  public static final StringId CacheClientUpdater_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_ATTEMPTING_TO_HANDLE_A_MARKER = new StringId(4202, "The following exception occurred while attempting to handle a marker.");
  public static final StringId CacheClientUpdater_CAUGHT_THE_FOLLOWING_EXCEPTION_WHILE_ATTEMPTING_TO_INVALIDATE_REGION_0 = new StringId(4203, "Caught the following exception while attempting to invalidate region {0}.");
  public static final StringId LocalRegion_REGION_0_MUST_BE_DESTROYED_BEFORE_CALLING_GETDESTROYEDSUBREGIONSERIALNUMBERS = new StringId(4206, "Region {0} must be destroyed before calling getDestroyedSubregionSerialNumbers");
  public static final StringId PRHARedundancyProvider_IF_YOUR_SYSTEM_HAS_SUFFICIENT_SPACE_PERHAPS_IT_IS_UNDER_MEMBERSHIP_OR_REGION_CREATION_STRESS = new StringId(4207, "If your system has sufficient space, perhaps it is under membership or region creation stress?");
  public static final StringId PRHARedundancyProvider_CONSIDER_STARTING_ANOTHER_MEMBER = new StringId(4208, "Advise you to start enough data store nodes");
  public static final StringId CacheClientUpdater_CLOSING_SOCKET_IN_0_FAILED = new StringId(4210, "Closing socket in {0} failed");
  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2 = new StringId(4215, "Could not set \"{0}\" to \"{1}\" because its value can not be less than \"{2}\".");
  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2 = new StringId(4216, "Could not set \"{0}\" to \"{1}\" because its value can not be greater than \"{2}\".");
  public static final StringId AbstractDistributionConfig_DEFAULT_ACK_WAIT_THRESHOLD_0_1_2 = new StringId(4217, "The number of seconds a distributed message can wait for acknowledgment before it sends an alert to signal that something might be wrong with the system node that is unresponsive. After sending this alert the waiter continues to wait. The alerts are logged in the system log as warnings and if a gfc is running will cause a console alert to be signalled.  Defaults to \"{0}\".  Legal values are in the range [{1}..{2}].");
  public static final StringId AbstractDistributionConfig_ARCHIVE_FILE_SIZE_LIMIT_NAME = new StringId(4218, "The maximum size in megabytes of a statistic archive file. Once this limit is exceeded, a new statistic archive file is created, and the current archive file becomes inactive. If set to zero, file size is unlimited.");
  public static final StringId AbstractDistributionConfig_ARCHIVE_DISK_SPACE_LIMIT_NAME = new StringId(4219, "The maximum size in megabytes of all inactive statistic archive files combined. If this limit is exceeded, inactive archive files will be deleted, oldest first, until the total size is within the limit. If set to zero, disk space usage is unlimited.");
  public static final StringId AbstractDistributionConfig_CACHE_XML_FILE_NAME_0 = new StringId(4220, "The file whose contents is used, by default, to initialize a cache if one is created.  Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_DISABLE_TCP_NAME_0 = new StringId(4221, "Determines whether TCP/IP communications will be disabled, forcing use of datagrams between members of the distributed system. Defaults to {0}");
  public static final StringId AbstractDistributionConfig_ENABLE_TIME_STATISTICS_NAME = new StringId(4222, "Turns on timings in distribution and cache statistics.  These are normally turned off to avoid expensive clock probes.");
  public static final StringId AbstractDistributionConfig_LOG_FILE_NAME_0 = new StringId(4225, "The file a running system will write log messages to.  Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_LOG_LEVEL_NAME_0_1 = new StringId(4226, "Controls the type of messages that will actually be written to the system log.  Defaults to \"{0}\".  Allowed values \"{1}\".");
  public static final StringId AbstractDistributionConfig_LOG_FILE_SIZE_LIMIT_NAME = new StringId(4227, "The maximum size in megabytes of a child log file. Once this limit is exceeded, a new child log is created, and the current child log becomes inactive. If set to zero, child logging is disabled.");
  public static final StringId AbstractDistributionConfig_LOG_DISK_SPACE_LIMIT_NAME = new StringId(4228, "The maximum size in megabytes of all inactive log files combined. If this limit is exceeded, inactive log files will be deleted, oldest first, until the total size is within the limit. If set to zero, disk space usage is unlimited.");
  public static final StringId AbstractDistributionConfig_LOCATORS_NAME_0 = new StringId(4229, "A possibly empty list of locators used to find other system nodes. Each element of the list must be a host name followed by bracketed, \"[]\", port number. Host names may be followed by a colon and a bind address used by the locator on that host.  Multiple elements must be comma seperated. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_TCP_PORT_NAME_0_1_2 = new StringId(4230, "The port used for tcp/ip communcations in the distributed system. If zero then a random available port is selected by the operating system.   Defaults to \"{0}\".  Legal values are in the range [{1}..{2}].");
  public static final StringId AbstractDistributionConfig_MCAST_PORT_NAME_0_1_2 = new StringId(4231, "The port used for multicast communcations in the distributed system. If zero then locators are used, and multicast is disabled.   Defaults to \"{0}\".  Legal values are in the range [{1}..{2}].");
  public static final StringId AbstractDistributionConfig_MCAST_ADDRESS_NAME_0_1 = new StringId(4232, "The address used for multicast communications. Only used if {0} is non-zero.  Defaults to \"{1}\".");
  public static final StringId AbstractDistributionConfig_MCAST_TTL_NAME_0_1_2 = new StringId(4233, "Determines how far through your network mulicast packets will propogate. Defaults to \"{0}\".  Legal values are in the range [{1}..{2}].");
  public static final StringId AbstractDistributionConfig_MCAST_SEND_BUFFER_SIZE_NAME_0 = new StringId(4234, "Sets the size of multicast socket transmission buffers, in bytes.  Defaults to {0} but this may be limited by operating system settings");
  public static final StringId AbstractDistributionConfig_MCAST_RECV_BUFFER_SIZE_NAME_0 = new StringId(4235, "Sets the size of multicast socket receive buffers, in bytes.  Defaults to {0} but this may be limited by operating system settings");
  public static final StringId AbstractDistributionConfig_MCAST_FLOW_CONTROL_NAME_0 = new StringId(4236, "Sets the flow-of-control parameters for multicast messaging.  Defaults to {0}.");
  public static final StringId AbstractDistributionConfig_MEMBER_TIMEOUT_NAME_0 = new StringId(4237, "Sets the number of milliseconds to wait for ping responses when determining whether another member is still alive. Defaults to {0}.");
  public static final StringId AbstractDistributionConfig_UDP_SEND_BUFFER_SIZE_NAME_0 = new StringId(4238, "Sets the size of datagram socket transmission buffers, in bytes.  Defaults to {0} but this may be limited by operating system settings");
  public static final StringId AbstractDistributionConfig_UDP_RECV_BUFFER_SIZE_NAME_0 = new StringId(4239, "Sets the size of datagram socket receive buffers, in bytes. Defaults to {0} but this may be limited by operating system settings");
  public static final StringId AbstractDistributionConfig_UDP_FRAGMENT_SIZE_NAME_0 = new StringId(4240, "Sets the maximum size of a datagram for UDP and multicast transmission.  Defaults to {0}.");
  public static final StringId AbstractDistributionConfig_SOCKET_LEASE_TIME_NAME_0_1_2 = new StringId(4241, "The number of milliseconds a thread can keep exclusive access to a socket that it is not actively using. Once a thread loses its lease to a socket it will need to re-acquire a socket the next time it sends a message. A value of zero causes socket leases to never expire. Defaults to \"{0}\" .  Legal values are in the range [{1}..{2}].");
  public static final StringId AbstractDistributionConfig_SOCKET_BUFFER_SIZE_NAME_0_1_2 = new StringId(4242, "The size of each socket buffer, in bytes. Smaller buffers conserve memory. Larger buffers can improve performance; in particular if large messages are being sent. Defaults to \"{0}\".  Legal values are in the range [{1}..{2}].");
  public static final StringId AbstractDistributionConfig_CONSERVE_SOCKETS_NAME_0 = new StringId(4243, "If true then a minimal number of sockets will be used when connecting to the distributed system. This conserves resource usage but can cause performance to suffer. If false, the default, then every application thread that sends distribution messages to other members of the distributed system will own its own sockets and have exclusive access to them. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_ROLES_NAME_0 = new StringId(4244, "The application roles that this member performs in the distributed system. This is a comma delimited list of user-defined strings. Any number of members can be configured to perform the same role, and a member can be configured to perform any number of roles. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_BIND_ADDRESS_NAME_0 = new StringId(4245, "The address server sockets will listen on. An empty string causes the server socket to listen on all local addresses. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_SERVER_BIND_ADDRESS_NAME_0 = new StringId(4246, "The address server sockets in a client-server topology will listen on. An empty string causes the server socket to listen on all local addresses. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_STATISTIC_ARCHIVE_FILE_NAME_0 = new StringId(4248, "The file a running system will write statistic samples to.  Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_STATISTIC_SAMPLE_RATE_NAME_0_1_2 = new StringId(4249, "The rate, in milliseconds, that a running system will sample statistics.  Defaults to \"{0}\".  Legal values are in the range [{1}..{2}].");
  public static final StringId AbstractDistributionConfig_STATISTIC_SAMPLING_ENABLED_NAME_0 = new StringId(4250, "If false then archiving is disabled and operating system statistics are no longer updated.  Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_SSL_ENABLED_NAME_0 = new StringId(4251, "Communication is performed through SSL when this property is set to true. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_SSL_PROTOCOLS_NAME_0 = new StringId(4252, "List of available SSL protocols that are to be enabled. Defaults to \"{0}\" meaning your provider''s defaults.");
  public static final StringId AbstractDistributionConfig_SSL_CIPHERS_NAME_0 = new StringId(4253, "List of available SSL cipher suites that are to be enabled. Defaults to \"{0}\" meaning your provider''s defaults.");
  public static final StringId AbstractDistributionConfig_SSL_REQUIRE_AUTHENTICATION_NAME = new StringId(4254, "if set to false, ciphers and protocols that permit anonymous peers are allowed. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_MAX_WAIT_TIME_FOR_RECONNECT = new StringId(4255, "Specifies the maximum time to wait before trying to reconnect to distributed system in the case of required role loss.");
  public static final StringId AbstractDistributionConfig_MAX_NUM_RECONNECT_TRIES = new StringId(4256, "Maximum number of tries before shutting the member down in the case of required role loss.");
  public static final StringId AbstractDistributionConfig_DURABLE_CLIENT_TIMEOUT_NAME_0 = new StringId(4257, "The value (in seconds) used by the server to keep disconnected durable clients alive. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_SECURITY_CLIENT_AUTH_INIT_NAME_0 = new StringId(4258, "User defined fully qualified method name implementing AuthInitialize interface for client. Defaults to \"{0}\". Legal values can be any \"method name\" of a static method that is present in the classpath.");
  public static final StringId AbstractDistributionConfig_SECURITY_CLIENT_AUTHENTICATOR_NAME_0 = new StringId(4259, "User defined fully qualified method name implementing Authenticator interface for client verification. Defaults to \"{0}\". Legal values can be any \"method name\" of a static method that is present in the classpath.");
  public static final StringId AbstractDistributionConfig_SECURITY_PEER_AUTH_INIT_NAME_0 = new StringId(4260, "User defined fully qualified method name implementing AuthInitialize interface for peer. Defaults to \"{0}\". Legal values can be any \"method name\" of a static method that is present in the classpath.");
  public static final StringId AbstractDistributionConfig_SECURITY_PEER_AUTHENTICATOR_NAME_0 = new StringId(4261, "User defined fully qualified method name implementing Authenticator interface for peer verificaiton. Defaults to \"{0}\". Legal values can be any \"method name\" of a static method that is present in the classpath.");
  public static final StringId AbstractDistributionConfig_SECURITY_CLIENT_ACCESSOR_NAME_0 = new StringId(4262, "User defined fully qualified method name implementing AccessControl interface for client authorization. Defaults to \"{0}\". Legal values can be any \"method name\" of a static method that is present in the classpath.");
  public static final StringId AbstractDistributionConfig_SECURITY_CLIENT_ACCESSOR_PP_NAME_0 = new StringId(4263, "User defined fully qualified method name implementing AccessControl interface for client authorization in post-processing phase. Defaults to \"{0}\". Legal values can be any \"method name\" of a static method that is present in the classpath.");
  public static final StringId AbstractDistributionConfig_SECURITY_LOG_LEVEL_NAME_0_1 = new StringId(4264, "Controls the type of messages that will actually be written to the system security log. Defaults to \"{0}\".  Allowed values \"{1}\".");
  public static final StringId AbstractDistributionConfig_SECURITY_LOG_FILE_NAME_0 = new StringId(4265, "The file a running system will write security log messages to. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME_0 = new StringId(4266, "The timeout value (in milliseconds) used by a peer to verify membership of an unknown authenticated peer requesting a secure connection. Defaults to \"{0}\" milliseconds. The timeout value should not exceed peer handshake timeout.");
  public static final StringId AbstractDistributionConfig_SECURITY_PREFIX_NAME = new StringId(4267, "Prefix for \"security\" related properties which are packed together and invoked as authentication parameter. Neither key nor value can be NULL. Legal tags can be [security-username, security-digitalid] and Legal values can be any string data.");
  public static final StringId AbstractDistributionConfig_DURABLE_CLIENT_ID_NAME_0 = new StringId(4268, "An id used by durable clients to identify themselves as durable to servers. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_ASYNC_DISTRIBUTION_TIMEOUT_NAME_0_1_2 = new StringId(4269, "The number of milliseconds before a publishing process should attempt to distribute a cache operation before switching over to asynchronous messaging for this process. Defaults to \"{0}\". Legal values are in the range [{1}..{2}].");
  public static final StringId AbstractDistributionConfig_ASYNC_QUEUE_TIMEOUT_NAME_0_1_2 = new StringId(4270, "The number of milliseconds a queuing may enqueue asynchronous messages without any distribution to this process before that publisher requests this process to depart. Defaults to \"{0}\" Legal values are in the range [{1}..{2}].");
  public static final StringId AbstractDistributionConfig_ASYNC_MAX_QUEUE_SIZE_NAME_0_1_2 = new StringId(4271, "The maximum size in megabytes that a publishing process should be allowed to asynchronously enqueue for this process before asking this process to depart from the distributed system. Defaults to \"{0}\". Legal values are in the range [{1}..{2}].");
  public static final StringId AbstractDistributionConfig_START_LOCATOR_NAME = new StringId(4274, "The host|bindAddress[port] of a Locator to start in this VM along with the DistributedSystem. The default is to not start a Locator.");
  public static final StringId InternalDistributedSystem_DISTRIBUTED_SYSTEM_HAS_DISCONNECTED = new StringId(4275, "Distributed system has disconnected during startup.");
  public static final StringId InternalDistributedSystem_EXCEPTION_TRYING_TO_CLOSE_CACHE = new StringId(4276, "Exception trying to close cache");
  public static final StringId InternalDistributedSystem_PROBLEM_STARTING_A_LOCATOR_SERVICE = new StringId(4277, "Problem starting a locator service");
  public static final StringId AbstractGroupOrRangeJunction_INTERMEDIATERESULTS_CAN_NOT_BE_NULL = new StringId(4278, "intermediateResults can not be null");
  public static final StringId RangeJunction_IN_THE_CASE_OF_AN_OR_JUNCTION_A_RANGEJUNCTION_SHOULD_NOT_BE_FORMED_FOR_NOW = new StringId(4279, "In the case of an OR junction a RangeJunction should not be formed for now");
  public static final StringId CqService_A_CQ_WITH_THE_GIVEN_NAME_0_ALREADY_EXISTS = new StringId(4290, "A CQ with the given name \"{0}\" already exists.");
  public static final StringId Connection_THREAD_OWNED_RECEIVER_FORCING_ITSELF_TO_SEND_ON_THREAD_OWNED_SOCKETS = new StringId(4305, "thread owned receiver forcing itself to send on thread owned sockets");
  public static final StringId RemoteGFManagerAgent_AN_AUTHENTICATIONFAILEDEXCEPTION_WAS_CAUGHT_WHILE_CONNECTING_TO_DS = new StringId(4307, "[RemoteGfManagerAgent]: An AuthenticationFailedException was caught while connecting to DS");
  public static final StringId StoreAllCachedDeserializable_VALUE_MUST_NOT_BE_NULL = new StringId(4310, "value must not be null");
  public static final StringId PartitionAttributesImpl_CLONENOTSUPPORTEDEXCEPTION_THROWN_IN_CLASS_THAT_IMPLEMENTS_CLONEABLE = new StringId(4312, "CloneNotSupportedException thrown in class that implements cloneable");
  public static final StringId PartitionAttributesImpl_REDUNDANTCOPIES_0_IS_AN_ILLEGAL_VALUE_PLEASE_CHOOSE_A_VALUE_BETWEEN_0_AND_3 = new StringId(4313, "RedundantCopies {0} is an illegal value, please choose a value between 0 and 3");
  public static final StringId PartitionAttributesImpl_TOTALNUMBICKETS_0_IS_AN_ILLEGAL_VALUE_PLEASE_CHOOSE_A_VALUE_GREATER_THAN_0 = new StringId(1902, "TotalNumBuckets {0} is an illegal value, please choose a value greater than 0");
  public static final StringId PartitionAttributesImpl_UNKNOWN_LOCAL_PROPERTY_0 = new StringId(4314, "Unknown local property: ''{0}''");
  public static final StringId PartitionAttributesImpl_UNKNOWN_GLOBAL_PROPERTY_0 = new StringId(4315, "Unknown global property: ''{0}''");
  public static final StringId Scope_0_IS_NOT_A_VALID_STRING_REPRESENTATION_OF_1 = new StringId(4322, "{0} is not a valid string representation of {1}.");
  public static final StringId CacheXmlParser_A_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_PARTITIONATTRIBUTES = new StringId(4323, "A {0} must be defined in the context of partition-attributes");
  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_1 = new StringId(4324, "A {0} is not an instance of a {1}");
  public static final StringId ExecuteFunction_THE_FUNCTION_GET_ID_RETURNED_NULL = new StringId(4326, "The Function#getID() returned null");
  public static final StringId PartitionedRegion_NO_TARGET_NODE_FOUND_FOR_KEY_0 = new StringId(4327, "No target node found for KEY = {0}");
  public static final StringId Connection_CONNECTION_FAILED_CONSTRUCTION_FOR_PEER_0 = new StringId(4328, "Connection: failed construction for peer {0}");
  public static final StringId AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_2_MUST_BE_0_WHEN_SECURITY_IS_ENABLED = new StringId(4329, "Could not set \"{0}\" to \"{1}\" because \"{2}\" must be 0 when security is enabled.");
  public static final StringId CqService_NOT_SENDING_CQ_CLOSE_TO_THE_SERVER_AS_IT_IS_A_DURABLE_CQ = new StringId(4330, "Not sending CQ close to the server as it is a durable CQ ");
  public static final StringId CacheClientProxy_CQEXCEPTION_WHILE_CLOSING_NON_DURABLE_CQS_0 = new StringId(4334, "CqException while closing non durable Cqs. {0}");
  public static final StringId InitialImageOperation_REGION_0_REQUESTING_INITIAL_IMAGE_FROM_1 = new StringId(4336, "Region {0} requesting initial image from {1}");
  public static final StringId CacheClientUpdater_ERROR_WHILE_PROCESSING_THE_CQ_MESSAGE_PROBLEM_WITH_READING_MESSAGE_FOR_CQ_0 = new StringId(4337, "Error while processing the CQ Message. Problem with reading message for CQ# : {0}");
  public static final StringId StopCQ_EXCEPTION_WHILE_STOPPING_CQ_NAMED_0 = new StringId(4338, "Exception while stopping CQ named {0} :");
  public static final StringId CacheClientUpdater_0_READY_TO_PROCESS_MESSAGES = new StringId(4340, "{0} : ready to process messages.");
  public static final StringId ClientHealthMonitoringRegion_ERROR_WHILE_CREATING_AN_ADMIN_REGION = new StringId(4341, "Error while creating an admin region");
  public static final StringId TCPConduit_UNABLE_TO_CLOSE_AND_RECREATE_SERVER_SOCKET = new StringId(4346, "Unable to close and recreate server socket");
  public static final StringId TCPConduit_INTERRUPTED_AND_EXITING_WHILE_TRYING_TO_RECREATE_LISTENER_SOCKETS = new StringId(4347, "Interrupted and exiting while trying to recreate listener sockets");
  public static final StringId TCPConduit_SERVERSOCKET_THREW_SOCKET_CLOSED_EXCEPTION_BUT_SAYS_IT_IS_NOT_CLOSED = new StringId(4348, "ServerSocket threw ''socket closed'' exception but says it is not closed");
  public static final StringId TCPConduit_SERVERSOCKET_CLOSED_REOPENING = new StringId(4349, "ServerSocket closed - reopening");
  public static final StringId StatAlertsManager_STATALERTSMANAGER_CREATED = new StringId(4350, "StatAlertsManager created");
  public static final StringId StatAlertsManager_STATALERTSMANAGER_CREATEMEMBERSTATALERTDEFINITION_STATISTICS_WITH_GIVEN_TEXTID_0_NOT_FOUND = new StringId(4353, "StatAlertsManager.createMemberStatAlertDefinition :: statistics with given textId={0}, NOT found.");
  public static final StringId HARegionQueue_TASK_TO_DECREMENT_THE_REF_COUNT_MAY_NOT_HAVE_BEEN_STARTED = new StringId(4361, "Exception in HARegionQueue.updateHAContainer(). The task to decrement the ref count by one for all the HAEventWrapper instances of this queue present in the haContainer may not have been started");
  public static final StringId CacheCreation_0_IS_NOT_BEING_STARTED_SINCE_IT_IS_CONFIGURED_FOR_MANUAL_START = new StringId(4364, "{0} is not being started since it is configured for manual start");
  public static final StringId CqService_ERROR_PROCESSING_CQLISTENER_FOR_CQ_0 = new StringId(4366, "Error processing CqListener for cq: {0}");
  public static final StringId CqService_VIRTUALMACHINEERROR_PROCESSING_CQLISTENER_FOR_CQ_0 = new StringId(4367, "VirtualMachineError processing CqListener for cq: {0}");
  public static final StringId RangeJunction_THE_OBJECT_IS_NOT_OF_TYPE_NOTEQUALCONDITIONEVALUATOR = new StringId(4368, "The Object is not of type NotEqualConditionEvaluator");
  public static final StringId CqQueryImpl_GOT_SECURITY_EXCEPTION_WHILE_EXECUTING_CQ_ON_SERVER = new StringId(4370, "Got security exception while executing cq on server");
  public static final StringId Instantiator_INSTANTIATOR_ID_CANNOT_BE_ZERO = new StringId(4371, "Instantiator id cannot be zero");
  public static final StringId InternalDataSerializer_THE_DATASERIALIZER_0_HAS_NO_SUPPORTED_CLASSES_ITS_GETSUPPORTEDCLASSES_METHOD_MUST_RETURN_AT_LEAST_ONE_CLASS = new StringId(4372, "The DataSerializer {0} has no supported classes. It''s getSupportedClasses method must return at least one class");
  public static final StringId InternalDataSerializer_THE_DATASERIALIZER_GETSUPPORTEDCLASSES_METHOD_FOR_0_RETURNED_AN_ARRAY_THAT_CONTAINED_A_NULL_ELEMENT = new StringId(4373, "The DataSerializer getSupportedClasses method for {0} returned an array that contained a null element.");
  public static final StringId InternalDataSerializer_THE_DATASERIALIZER_GETSUPPORTEDCLASSES_METHOD_FOR_0_RETURNED_AN_ARRAY_THAT_CONTAINED_AN_ARRAY_CLASS_WHICH_IS_NOT_ALLOWED_SINCE_ARRAYS_HAVE_BUILTIN_SUPPORT = new StringId(4374, "The DataSerializer getSupportedClasses method for {0} returned an array that contained an array class which is not allowed since arrays have built-in support.");
  public static final StringId InternalDataSerializer_ATTEMPTED_TO_SERIALIZE_ILLEGAL_DSFID = new StringId(4375, "attempted to serialize ILLEGAL dsfid");
  public static final StringId InternalDataSerializer_UNKNOWN_PRIMITIVE_TYPE_0 = new StringId(4376, "unknown primitive type: {0}");
  public static final StringId InternalDataSerializer_UNEXPECTED_TYPECODE_0 = new StringId(4377, "unexpected typeCode: {0}");
  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_CUSTOMEXPIRY = new StringId(4379, "A {0} is not an instance of CustomExpiry");
  public static final StringId RegionAttributesCreation_CUSTOMENTRYIDLETIMEOUT_IS_NOT_THE_SAME = new StringId(4380, "CustomEntryIdleTimeout is not the same");
  public static final StringId RegionAttributesCreation_CUSTOMENTRYTIMETOLIVE_IS_NOT_THE_SAME = new StringId(4381, "CustomEntryTimeToLive is not the same");
  public static final StringId AbstractRegion_CANNOT_SET_CUSTOM_TIME_TO_LIVE_WHEN_STATISTICS_ARE_DISABLED = new StringId(4382, "Cannot set custom time to live when statistics are disabled");
  public static final StringId CacheServerImpl__0_INVALID_EVICTION_POLICY = new StringId(4384, "{0} Invalid eviction policy");
  public static final StringId MergeLogFiles_IF_A_DIRECTORY_IS_SPECIFIED_ALL_LOG_FILES_IN_THAT_DIRECTORY_ARE_MERGED = new StringId(4385, "If a directory is specified, all .log files in that directory are merged.");
  public static final StringId MergeLogFiles_FILE_0_IS_NEITHER_A_FILE_NOR_A_DIRECTORY = new StringId(4386, "File ''{0}'' is neither a file nor a directory.");
  public static final StringId AvailablePort_NETWORK_IS_UNREACHABLE = new StringId(4387, "Network is unreachable");
  public static final StringId InetAddressUtil_UNABLE_TO_QUERY_NETWORK_INTERFACE = new StringId(4388, "Unable to query network interface");
  public static final StringId DistributionLocator_A_ZEROLENGTH_GEMFIREPROPERTIESFILE_WILL_MEAN_USE_THE_DEFAULT_SEARCH_PATH = new StringId(4389, "A zero-length gemfire-properties-file will mean use the default search path");
  public static final StringId DistributionLocator_PEERLOCATOR_AND_SERVERLOCATOR_BOTH_DEFAULT_TO_TRUE = new StringId(4390, "peerLocator and serverLocator both default to true");
  public static final StringId DistributionLocator_A_ZEROLENGTH_HOSTNAMEFORCLIENTS_WILL_DEFAULT_TO_BINDADDRESS = new StringId(4391, "A zero-length hostname-for-clients will default to bind-address");
  public static final StringId PartitionedRegion_0_SECONDS_HAVE_ELAPSED_WAITING_FOR_THE_PARTITIONED_REGION_LOCK_HELD_BY_1 = new StringId(4392, "{0} seconds have elapsed waiting for the partitioned region lock held by {1}");
  public static final StringId PartitionedRegion_0_SECONDS_HAVE_ELAPSED_WAITING_FOR_GLOBAL_REGION_ENTRY_LOCK_HELD_BY_1 = new StringId(4393, "{0} seconds have elapsed waiting for global region entry lock held by {1}");
  public static final StringId InternalLocator_EXPECTED_ONE_OF_THESE_0_BUT_RECEIVED_1 = new StringId(4394, "Expected one of these: {0} but received {1}");
  public static final StringId InternalLocator_STARTING_0 = new StringId(4395, "Starting {0}");
  public static final StringId LocalRegion_RECURSIVEDESTROYREGION_RECURSION_FAILED_DUE_TO_CACHE_CLOSURE_REGION_0 = new StringId(4398, "recursiveDestroyRegion: recursion failed due to cache closure. region = {0}");
  public static final StringId LocalRegion_BASICDESTROYREGION_PARENT_REMOVAL_FAILED_DUE_TO_CACHE_CLOSURE_REGION_0 = new StringId(4399, "basicDestroyRegion: parent removal failed due to cache closure. region = {0}");
  public static final StringId LocalRegion_BASICDESTROYREGION_INDEX_REMOVAL_FAILED_DUE_TO_CACHE_CLOSURE_REGION_0 = new StringId(4400, "basicDestroyRegion: index removal failed due to cache closure. region = {0}");
  public static final StringId LocalRegion_RECURSIVEDESTROYREGION_POSTDESTROYREGION_FAILED_DUE_TO_CACHE_CLOSURE_REGION_0 = new StringId(4401, "recursiveDestroyRegion: postDestroyRegion failed due to cache closure. region = {0}");
  public static final StringId LocalRegion_RECURSIVEDESTROYREGION_PROBLEM_IN_CACHEWRITEBEFOREREGIONDESTROY = new StringId(4402, "recursiveDestroyRegion: problem in cacheWriteBeforeRegionDestroy");
  public static final StringId LocalRegion_A_DISKACCESSEXCEPTION_HAS_OCCURED_WHILE_WRITING_TO_THE_DISK_FOR_REGION_0_THE_REGION_WILL_BE_CLOSED = new StringId(4403, "A DiskAccessException has occurred while writing to the disk for region {0}. The region will be closed.");
  public static final StringId PoolImpl_DESTROYING_CONNECTION_POOL_0 = new StringId(4410, "Destroying connection pool {0}");
  public static final StringId PoolImpl_TIMEOUT_WAITING_FOR_BACKGROUND_TASKS_TO_COMPLETE = new StringId(4411, "Timeout waiting for background tasks to complete.");
  public static final StringId PoolImpl_ERROR_ENCOUNTERED_WHILE_STOPPING_BACKGROUNDPROCESSOR = new StringId(4412, "Error encountered while stopping backgroundProcessor.");
  public static final StringId PoolImpl_INTERRUPTED_WHILE_STOPPING_BACKGROUNDPROCESSOR = new StringId(4413, "Interrupted while stopping backgroundProcessor");
  public static final StringId PoolImpl_ERROR_ENCOUNTERED_WHILE_STOPPING_CONNECTION_SOURCE = new StringId(4414, "Error encountered while stopping connection source.");
  public static final StringId PoolImpl_ERROR_ENCOUNTERED_WHILE_STOPPING_CONNECTION_MANAGER = new StringId(4415, "Error encountered while stopping connection manager.");
  public static final StringId PoolImpl_ERROR_ENCOUNTERED_WHILE_STOPPING_SUBSCRIPTION_MANAGER = new StringId(4416, "Error encountered while stopping subscription manager");
  public static final StringId PoolImpl_ERROR_ENCOUNTERED_WHILE_STOPPING_ENDPOINT_MANAGER = new StringId(4417, "Error encountered while stopping endpoint manager");
  public static final StringId PoolImpl_ERROR_WHILE_CLOSING_STATISTICS = new StringId(4418, "Error while closing statistics");
  public static final StringId PoolImpl_UNEXPECTED_ERROR_IN_POOL_TASK_0 = new StringId(4419, "Unexpected error in pool task <{0}>");
  public static final StringId PoolImpl_DISTRIBUTED_SYSTEM_MUST_BE_CREATED_BEFORE_CREATING_POOL = new StringId(4420, "Distributed System must be created before creating pool");
  public static final StringId PoolImpl_POOL_COULD_NOT_BE_DESTROYED_BECAUSE_IT_IS_STILL_IN_USE_BY_0_REGIONS = new StringId(4421, "Pool could not be destroyed because it is still in use by {0} regions");
  public static final StringId PoolImpl__0_IS_NOT_THE_SAME_AS_1_BECAUSE_IT_SHOULD_HAVE_BEEN_A_POOLIMPL = new StringId(4422, "{0} is not the same as {1} because it should have been a PoolImpl");
  public static final StringId PoolImpl_0_ARE_DIFFERENT = new StringId(4423, "Pool {0} are different");
  public static final StringId PoolImpl_0_IS_DIFFERENT = new StringId(4424, "Pool {0} is different");
  public static final StringId TXCommitMessage_TRANSACTION_MESSAGE_0_FROM_SENDER_1_FAILED_PROCESSING_UNKNOWN_TRANSACTION_STATE_2 = new StringId(4425, "Transaction message {0} from sender {1} failed processing, unknown transaction state: {2}");
  public static final StringId StatAlertsManager_EVALUATEALERTDEFNSTASK_FAILED_WITH_AN_EXCEPTION = new StringId(4426, "EvaluateAlertDefnsTask failed with an exception. ");
  public static final StringId ConnectionManagerImpl_ERROR_CLOSING_CONNECTION_0 = new StringId(4427, "Error closing connection {0}");
  public static final StringId ConnectionManagerImpl_ERROR_STOPPING_LOADCONDITIONINGPROCESSOR = new StringId(4428, "Error stopping loadConditioningProcessor");
  public static final StringId ConnectionManagerImpl_INTERRUPTED_STOPPING_LOADCONDITIONINGPROCESSOR = new StringId(4429, "Interrupted stopping loadConditioningProcessor");
  public static final StringId ConnectionManagerImpl_ERROR_PREFILLING_CONNECTIONS = new StringId(4430, "Error prefilling connections");
  public static final StringId ConnectionManagerImpl_TIMEOUT_WAITING_FOR_LOAD_CONDITIONING_TASKS_TO_COMPLETE = new StringId(4431, "Timeout waiting for load conditioning tasks to complete");
  public static final StringId ConnectionManagerImpl_UNABLE_TO_PREFILL_POOL_TO_MINIMUM_BECAUSE_0 = new StringId(4432, "Unable to prefill pool to minimum because: {0}");
  public static final StringId ConnectionManagerImpl_LOADCONDITIONINGTASK_0_ENCOUNTERED_EXCEPTION = new StringId(4433, "LoadConditioningTask <{0}> encountered exception");
  public static final StringId ConnectionManagerImpl_IDLEEXPIRECONNECTIONSTASK_0_ENCOUNTERED_EXCEPTION = new StringId(4434, "IdleExpireConnectionsTask <{0}> encountered exception");
  public static final StringId ConnectionManagerImpl_SECURITY_EXCEPTION_CONNECTING_TO_SERVER_0_1 = new StringId(4435, "Security exception connecting to server ''{0}'': {1}");
  public static final StringId ConnectionManagerImpl_SERVER_0_REFUSED_NEW_CONNECTION_1 = new StringId(4436, "Server ''{0}'' refused new connection: {1}");
  public static final StringId ConnectionManagerImpl_ERROR_CLOSING_CONNECTION_TO_SERVER_0 = new StringId(4437, "Error closing connection to server {0}");
  public static final StringId ConnectionManagerImpl_ERROR_EXPIRING_CONNECTION_0 = new StringId(4438, "Error expiring connection {0}");
  public static final StringId ServerHandShakeProcessor_0_SERVERS_CURRENT_VERSION_IS_1 = new StringId(4439, "{0} Server''s current version is {1}.");
  public static final StringId ServerHandShakeProcessor_0_AUTHORIZATION_ENABLED_BUT_AUTHENTICATION_CALLBACK_1_RETURNED_WITH_NULL_CREDENTIALS_FOR_PROXYID_2 = new StringId(4442, "{0}: Authorization enabled but authentication callback ({1})  returned with null credentials for proxyID: {2}");
  public static final StringId ServerHandShakeProcessor_0_POSTPROCESS_AUTHORIZATION_ENABLED_BUT_NO_AUTHENTICATION_CALLBACK_2_IS_CONFIGURED = new StringId(4443, "{0}: Post-process authorization enabled, but no authentication callback ({2}) is configured");
  public static final StringId ServerHandShakeProcessor_0_HANDSHAKE_REPLY_CODE_TIMEOUT_NOT_RECEIVED_WITH_IN_1_MS = new StringId(4444, "{0}: Handshake reply code timeout, not received with in {1} ms");
  public static final StringId ServerHandShakeProcessor_0_RECEIVED_NO_HANDSHAKE_REPLY_CODE = new StringId(4445, "{0}: Received no handshake reply code");
  public static final StringId ServerHandShakeProcessor_HANDSHAKEREADER_EOF_REACHED_BEFORE_CLIENT_VERSION_COULD_BE_READ = new StringId(4446, "HandShakeReader: EOF reached before client version could be read");
  public static final StringId QueueManagerImpl_TIMEOUT_WAITING_FOR_RECOVERY_THREAD_TO_COMPLETE = new StringId(4447, "Timeout waiting for recovery thread to complete");
  public static final StringId QueueManagerImpl_ERROR_CLOSING_PRIMARY_CONNECTION_TO_0 = new StringId(4448, "Error closing primary connection to {0}");
  public static final StringId QueueManagerImpl_ERROR_CLOSING_BACKUP_CONNECTION_TO_0 = new StringId(4449, "Error closing backup connection to {0}");
  public static final StringId QueueManagerImpl_SENDING_READY_FOR_EVENTS_TO_PRIMARY_0 = new StringId(4450, "Sending ready for events to primary: {0}");
  public static final StringId QueueManagerImpl_COULD_NOT_CREATE_A_QUEUE_NO_QUEUE_SERVERS_AVAILABLE = new StringId(4451, "Could not create a queue. No queue servers available.");
  public static final StringId QueueManagerImpl_COULD_NOT_INITIALIZE_A_PRIMARY_QUEUE_ON_STARTUP_NO_QUEUE_SERVERS_AVAILABLE = new StringId(4452, "Could not initialize a primary queue on startup. No queue servers available.");
  public static final StringId QueueManagerImpl_UNABLE_TO_INITIALIZE_ENOUGH_REDUNDANT_QUEUES_ON_STARTUP_THE_REDUNDANCY_COUNT_IS_CURRENTLY_0 = new StringId(4453, "Unable to initialize enough redundant queues on startup. The redundancy count is currently {0}.");
  public static final StringId QueueManagerImpl_UNABLE_TO_CREATE_A_SUBSCRIPTION_CONNECTION_TO_SERVER_0 = new StringId(4454, "unable to create a subscription connection to server {0}");
  public static final StringId QueueManagerImpl_REDUNDANCY_LEVEL_0_IS_NOT_SATISFIED_BUT_THERE_ARE_NO_MORE_SERVERS_AVAILABLE_REDUNDANCY_IS_CURRENTLY_1 = new StringId(4455, "Redundancy level {0} is not satisfied, but there are no more servers available. Redundancy is currently {1}.");
  public static final StringId QueueManagerImpl_QUEUEMANAGERIMPL_FAILED_TO_RECOVER_INTEREST_TO_SERVER_0 = new StringId(4456, "QueueManagerImpl failed to recover interest to server {0}.");
  public static final StringId QueueManagerImpl_ERROR_IN_REDUNDANCY_SATISFIER = new StringId(4457, "Error in redundancy satisfier");
  public static final StringId GroupMembershipService_MEMBERSHIP_EXPIRING_MEMBERSHIP_OF_SURPRISE_MEMBER_0 = new StringId(4458, "Membership: expiring membership of surprise member <{0}>");
  public static final StringId GemFireCacheImpl_STARTING_GEMFIRE_REDIS_SERVER_ON_PORT_0 = new StringId(4459, "Starting GeodeRedisServer on port {0}");
  public static final StringId GroupMembershipService_EXCEPTION_DESERIALIZING_MESSAGE_PAYLOAD_0 = new StringId(4460, "Exception deserializing message payload: {0}");
  public static final StringId GroupMembershipService_MEMBERSHIP_SERVICE_FAILURE_0 = new StringId(4461, "Membership service failure: {0}");
  public static final StringId GroupMembershipService_EXCEPTION_CAUGHT_WHILE_SHUTTING_DOWN = new StringId(4462, "Exception caught while shutting down");
  public static final StringId GroupMembershipService_MEMBERSHIP_REQUESTING_REMOVAL_OF_0_REASON_1 = new StringId(4464, "Membership: requesting removal of {0}. Reason={1}");
  public static final StringId DLockGrantor_INITIALIZATION_OF_HELD_LOCKS_IS_SKIPPING_0_BECAUSE_LOCK_IS_ALREADY_HELD_1 = new StringId(4465, "Initialization of held locks is skipping {0} because lock is already held: {1}");
  public static final StringId DLockGrantor_IGNORING_LOCK_REQUEST_FROM_NONMEMBER_0 = new StringId(4466, "Ignoring lock request from non-member: {0}");
  public static final StringId HARegionQueue_DURABLE_CLIENT_QUEUE_INITIALIZATION_TOOK_0_MS = new StringId(4470, "Durable client queue initialization took {0} ms.");
  public static final StringId ClientStatsManager_FAILED_TO_PUBLISH_CLIENT_STATISTICS = new StringId(4472, "Failed to publish client statistics");
  public static final StringId ClientStatsManager_FAILED_TO_SEND_CLIENT_HEALTH_STATS_TO_CACHESERVER = new StringId(4473, "Failed to send client health stats to cacheserver.");
  public static final StringId ClientStatsManager_CLIENTSTATSMANAGER_INTIALIZING_THE_STATISTICS = new StringId(4474, "ClientStatsManager, intializing the statistics...");
  public static final StringId ClientStatsManager_CLIENTSTATSMANAGER_0_ARE_NOT_AVAILABLE = new StringId(4475, "ClientStatsManager, {0} are not available.");
  public static final StringId AutoConnectionSourceImpl_AUTOCONNECTIONSOURCE_DISCOVERED_NEW_LOCATORS_0 = new StringId(4476, "AutoConnectionSource discovered new locators {0}");
  public static final StringId AutoConnectionSourceImpl_AUTOCONNECTIONSOURCE_DROPPING_PREVIOUSLY_DISCOVERED_LOCATORS_0 = new StringId(4477, "AutoConnectionSource dropping previously discovered locators {0}");
  public static final StringId AutoConnectionSourceImpl_RECEIVED_EXCEPTION_FROM_LOCATOR_0 = new StringId(4478, "Received exception from locator {0}");
  public static final StringId AutoConnectionSourceImpl_COMMUNICATION_HAS_BEEN_RESTORED_WITH_LOCATOR_0 = new StringId(4479, "Communication has been restored with locator {0}.");
  public static final StringId AutoConnectionSourceImpl_COMMUNICATION_WITH_LOCATOR_0_FAILED_WITH_1 = new StringId(4480, "Communication with locator {0} failed with {1}.");
  public static final StringId AutoConnectionSourceImpl_LOCATOR_0_IS_NOT_RUNNING = new StringId(4481, "locator {0} is not running.");
  public static final StringId AutoConnectionSourceImpl_COULD_NOT_CREATE_A_NEW_CONNECTION_TO_SERVER_0 = new StringId(4484, "Could not create a new connection to server: {0}");
  public static final StringId DistributionAdvisor_0_SEC_HAVE_ELAPSED_WHILE_WAITING_FOR_CURRENT_OPERATIONS_TO_DISTRIBUTE = new StringId(4485, "{0} seconds have elapsed while waiting for current operations to distribute");
  public static final StringId DistributionManager_I_0_AM_THE_ELDER = new StringId(4486, "I, {0}, am the elder.");
  public static final StringId InternalLocator_STARTING_PEER_LOCATION_FOR_0 = new StringId(4488, "Starting peer location for {0}");
  public static final StringId InternalLocator_EITHER_PEER_LOCATOR_OR_SERVER_LOCATOR_MUST_BE_ENABLED = new StringId(4489, "Either peer locator or server locator must be enabled");
  public static final StringId InternalLocator_PEER_LOCATION_IS_ALREADY_RUNNING_FOR_0 = new StringId(4490, "Peer location is already running for {0}");
  public static final StringId InternalLocator_SERVER_LOCATION_IS_ALREADY_RUNNING_FOR_0 = new StringId(4491, "Server location is already running for {0}");
  public static final StringId InternalLocator_STARTING_SERVER_LOCATION_FOR_0 = new StringId(4492, "Starting server location for {0}");
  public static final StringId InternalLocator_SINCE_SERVER_LOCATION_IS_ENABLED_THE_DISTRIBUTED_SYSTEM_MUST_BE_CONNECTED = new StringId(4493, "Since server location is enabled the distributed system must be connected.");
  public static final StringId CacheClientUpdater_0_HAS_COMPLETED_WAITING_FOR_1 = new StringId(4496, "{0} has completed waiting for {1}");
  public static final StringId Connection_UNABLE_TO_FORM_A_TCPIP_CONNECTION_TO_0_IN_OVER_1_SECONDS = new StringId(4498, "Unable to form a TCP/IP connection to {0} in over {1} seconds");
  public static final StringId Connection_UNABLE_TO_FORM_A_TCPIP_CONNECTION_IN_A_REASONABLE_AMOUNT_OF_TIME = new StringId(4499, "Unable to form a TCP/IP connection in a reasonable amount of time");
  public static final StringId DLOCKRECOVERGRANTORPROCESSOR_DLOCKRECOVERGRANTORMESSAGE_PROCESS_THROWABLE = new StringId(4500, "[DLockRecoverGrantorMessage.process] throwable: ");
  public static final StringId StartupMessage_UNABLE_TO_EXAMINE_NETWORK_INTERFACES = new StringId(4501, "Unable to examine network interfaces");
  public static final StringId StartupMessage_REJECTED_NEW_SYSTEM_NODE_0_BECAUSE_PEER_HAS_NO_NETWORK_INTERFACES = new StringId(4502, "Rejected new system node {0} because peer has no network interfaces");
  public static final StringId DisKRegion_OUTSTANDING_OPS_REMAIN_AFTER_0_SECONDS_FOR_DISK_REGION_1 = new StringId(4503, "Outstanding ops remain after {0} seconds for disk region {1}");
  public static final StringId DisKRegion_OUTSTANDING_OPS_CLEARED_FOR_DISK_REGION_0 = new StringId(4504, "Outstanding ops cleared for disk region {0}");
  public static final StringId CacheClientProxy_0_SLEEP_INTERRUPTED = new StringId(4505, "{0}: sleep interrupted.");
  public static final StringId Connection_0_SECONDS_HAVE_ELAPSED_WAITING_FOR_A_RESPONSE_FROM_1_FOR_THREAD_2 = new StringId(4506, "{0} seconds have elapsed waiting for a response from {1} for thread {2}");
  public static final StringId Connection_TRANSMIT_ACKWAITTHRESHOLD = new StringId(4507, "Sender has been unable to transmit a message within ack-wait-threshold seconds");
  public static final StringId Connection_RECEIVE_ACKWAITTHRESHOLD = new StringId(4508, "Sender has been unable to receive a response to a message within ack-wait-threshold seconds");
  public static final StringId Connection_MESSAGE_DESERIALIZATION_OF_0_DID_NOT_READ_1_BYTES = new StringId(4509, "Message deserialization of {0} did not read {1} bytes.");
  public static final StringId Connection_UNCAUGHT_EXCEPTION_FROM_LISTENER = new StringId(4510, "Uncaught exception from listener");
  public static final StringId DistributedRegion_0_SECONDS_HAVE_ELAPSED_WAITING_FOR_GLOBAL_REGION_ENTRY_LOCK_HELD_BY_1 = new StringId(4511, "{0} seconds have elapsed waiting for global region entry lock held by {1}");
  public static final StringId DistributedRegion_REGION_0_1_SPLITBRAIN_CONFIG_WARNING = new StringId(4512, "Region {0} is being created with scope {1} but enable-network-partition-detection is enabled in the distributed system.  This can lead to cache inconsistencies if there is a network failure.");
  public static final StringId GemFireCache_0_ERROR_IN_LAST_STAGE_OF_PARTITIONEDREGION_CACHE_CLOSE = new StringId(4513, "{0}: error in last stage of PartitionedRegion cache close");
  public static final StringId GemFireCache_0_ERROR_CLOSING_REGION_1 = new StringId(4514, "{0}: error closing region {1}");
  public static final StringId TCPConduit_PROBLEM_CONNECTING_TO_0 = new StringId(4516, "Problem connecting to {0}");
  public static final StringId TCPConduit_THROWING_IOEXCEPTION_AFTER_FINDING_BREAKLOOP_TRUE = new StringId(4517, "Throwing IOException after finding breakLoop=true");
  public static final StringId InstantiatorRecoveryListener_INSTANTIATORRECOVERYTASK_ERROR_RECOVERING_INSTANTIATORS = new StringId(4518, "InstantiatorRecoveryTask - Error recovering instantiators: ");
  public static final StringId Generic_0_UNEXPECTED_EXCEPTION = new StringId(4519, "{0}: Unexpected Exception");
  public static final StringId PutAll_THE_INPUT_REGION_NAME_FOR_THE_PUTALL_REQUEST_IS_NULL = new StringId(4520, " The input region name for the putAll request is null");
  public static final StringId PutAll_ONE_OF_THE_INPUT_KEYS_FOR_THE_PUTALL_REQUEST_IS_NULL = new StringId(4521, " One of the input keys for the putAll request is null");
  public static final StringId PutAll_ONE_OF_THE_INPUT_VALUES_FOR_THE_PUTALL_REQUEST_IS_NULL = new StringId(4522, " One of the input values for the putAll request is null");
  public static final StringId AbstractDistributionConfig_REDIS_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1 = new StringId(4526, "The redis-bind-address \"{0}\" is not a valid address for this machine.  These are the valid addresses for this machine: {1}");
  public static final StringId CacheServerImpl_CACHESERVER_ERROR_CLOSING_LOAD_MONITOR = new StringId(4527, "CacheServer - Error closing load monitor");
  public static final StringId CacheServerImpl_CACHESERVER_ERROR_CLOSING_ADVISOR = new StringId(4528, "CacheServer - Error closing advisor");
  public static final StringId CacheServerImpl_CACHESERVER_ERROR_CLOSING_ACCEPTOR_MONITOR = new StringId(4529, "CacheServer - Error closing acceptor monitor");
  public static final StringId DiskRegion_COMPLEXDISKREGION_CLOSE_EXCEPTION_IN_STOPPING_COMPACTOR = new StringId(4530, "DiskRegion::close: Exception in stopping compactor");
  public static final StringId CacheClientNotifier_CACHECLIENTNOTIFIER_CAUGHT_EXCEPTION_ATTEMPTING_TO_CLIENT = new StringId(4531, "CacheClientNotifier: Caught exception attempting to client: ");
  public static final StringId CqQueryImpl_EXCEPTION_WHILE_EXECUTING_CQ_EXCEPTION_0 = new StringId(4532, "Exception while executing cq Exception: {0}");
  public static final StringId CqQueryImpl_FAILED_TO_EXECUTE_THE_CQ_CQNAME_0_QUERY_STRING_IS_1_ERROR_FROM_LAST_SERVER_2 = new StringId(4533, "Failed to execute the CQ. CqName: {0}, Query String is: {1}, Error from last server: {2}");
  public static final StringId RegisterInterestTracker_REMOVESINGLEINTEREST_KEY_0_NOT_REGISTERED_IN_THE_CLIENT = new StringId(4534, "removeSingleInterest: key {0} not registered in the client");
  public static final StringId RegisterInterestTracker_PROBLEM_REMOVING_ALL_INTEREST_ON_REGION_0_INTERESTTYPE_1_2 = new StringId(4535, "Problem removing all interest on region={0} interestType={1} :{2}");
  public static final StringId RegisterInterestTracker_REMOVEINTERESTLIST_KEY_0_NOT_REGISTERED_IN_THE_CLIENT = new StringId(4536, "removeInterestList: key {0} not registered in the client");
  public static final StringId PoolmanagerImpl_ERROR_REGISTERING_INSTANTIATOR_ON_POOL = new StringId(4537, "Error registering instantiator on pool: ");
  public static final StringId ServerLocator_SERVERLOCATOR_UNEXPECTED_PROFILE_UPDATE = new StringId(4538, "ServerLocator - unexpected profile update.");
  public static final StringId LoadMonitor_CACHESERVER_LOAD_MONITOR_ERROR_IN_POLLING_THREAD = new StringId(4539, "CacheServer Load Monitor Error in polling thread");
  public static final StringId LoadMonitor_INTERRUPTED_WAITING_FOR_POLLING_THREAD_TO_FINISH = new StringId(4540, "Interrupted waiting for polling thread to finish");
  public static final StringId BucketAdvisor_REDUNDANCY_HAS_DROPPED_BELOW_0_CONFIGURED_COPIES_TO_1_ACTUAL_COPIES_FOR_2 = new StringId(4543, "Redundancy has dropped below {0} configured copies to {1} actual copies for {2}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_PUTALL_OPERATION_ON_REGION_0 = new StringId(4544, "Not authorized to perform PUTALL operation on region [{0}]");
  public static final StringId LocalRegion_CAUGHT_THE_FOLLOWING_EXCEPTION_FOR_KEY_0_WHILE_PERFORMING_A_REMOTE_GETALL = new StringId(4545, "Caught the following exception for key {0} while performing a remote getAll");
  public static final StringId GetAll_THE_INPUT_REGION_NAME_FOR_THE_GETALL_REQUEST_IS_NULL = new StringId(4546, "The input region name for the getAll request is null");
  public static final StringId GetAll_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1 = new StringId(4547, "{0}: Caught the following exception attempting to get value for key={1}");
  public static final StringId AcceptorImpl_COULD_NOT_CHECK_FOR_STUCK_KEYS = new StringId(4548, "Could not check for stuck keys.");
  public static final StringId AcceptorImpl_STUCK_SELECTION_KEY_DETECTED_ON_0 = new StringId(4549, "stuck selection key detected on {0}");
  public static final StringId AcceptorImpl_UNEXPECTED_EXCEPTION = new StringId(4550, "Unexpected Exception:");
  public static final StringId GroupMembershipService_MEMBERSHIP_DISREGARDING_SHUNNED_MEMBER_0 = new StringId(4551, "Membership: disregarding shunned member <{0}>");
  public static final StringId GlobalTransaction_COULD_NOT_COMPARE_0_TO_1 = new StringId(4552, "Could not compare {0} to {1}");
  public static final StringId DistributedSystem_THIS_VM_ALREADY_HAS_ONE_OR_MORE_DISTRIBUTED_SYSTEM_CONNECTIONS_0 = new StringId(4553, "This VM already has one or more Distributed System connections {0}");
  public static final StringId Op_UNKNOWN_MESSAGE_TYPE_0 = new StringId(4555, "Unknown message type {0}");
  public static final StringId AdminDistributedSystem_ENCOUNTERED_AN_IOEXCEPTION_0 = new StringId(4556, "Encountered an IOException while serializing notifications, objects were not sent to the jmx clients as a result. {0}");
  public static final StringId AdminDistributedSystem_ENCOUNTERED_A_0_WHILE_LOADING_STATALERTDEFINITIONS_1 = new StringId(4557, "Encountered a {0} while loading StatAlertDefinitions [from {1}]. This could be due to GemFire version mismatch. Loading of statAlertDefinitions has been aborted.");
  public static final StringId AdminDistributedSystem_ENCOUNTERED_A_0_WHILE_SAVING_STATALERTDEFINITIONS_1 = new StringId(4558, "Encountered a {0} while saving StatAlertDefinitions [from {1}]. This could be due to GemFire version mismatch. Saving of statAlertDefinitions has been aborted.");
  public static final StringId ExecuteRegionFunction_THE_REGION_NAMED_0_WAS_NOT_FOUND_DURING_EXECUTE_FUNCTION_REQUEST = new StringId(4560, "The region named {0} was not found during execute Function request.");
  public static final StringId ExecuteRegionFunction_CAN_NOT_EXECUTE_ON_NORMAL_REGION = new StringId(4565, "Function execution on region with DataPolicy.NORMAL is not supported");
  public static final StringId ExecuteFunction_THE_INPUT_FUNCTION_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL = new StringId(4567, "The input function for the execute function request is null");
  public static final StringId ExecuteFunction_FUNCTION_NAMED_0_IS_NOT_REGISTERED = new StringId(4568, "Function named {0} is not registered to FunctionService");
  public static final StringId ExecuteFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0 = new StringId(4569, "Exception on server while executing function: {0}");
  public static final StringId ExecuteFunction_SERVER_COULD_NOT_SEND_THE_REPLY = new StringId(4570, "Server could not send the reply");
  public static final StringId ExecuteRegionFunction_BUCKET_FILTER_ON_NON_PR = new StringId(4572, "Buckets as filter cannot be applied to a non partitioned region: {0}");
  public static final StringId ExecuteRegionFunction_SERVER_COULD_NOT_SEND_THE_REPLY = new StringId(4573, "Server could not send the reply");
  public static final StringId ExecuteRegionFunction_EXCEPTION_ON_SERVER_WHILE_EXECUTIONG_FUNCTION_0 = new StringId(4574, "Exception on server while executing function : {0}");
  public static final StringId ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL = new StringId(4575, "The input {0} for the execute function request is null");
  public static final StringId ExecuteRegionFunction_THE_FUNCTION_0_HAS_NOT_BEEN_REGISTERED = new StringId(4576, "The function, {0}, has not been registered");
  public static final StringId ExecuteFunction_THE_FUNCTION_0_DID_NOT_SENT_LAST_RESULT = new StringId(4577, "The function, {0}, did not send last result");
  public static final StringId CacheClientProxy_REGION_NOTIFICATION_OF_INTEREST_FAILED = new StringId(4579, "Region notification of interest failed");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_EXECUTE_REGION_FUNCTION_OPERATION = new StringId(4580, "Not authorized to perform EXECUTE_REGION_FUNCTION operation");
  public static final StringId RemoteBridgeServer_INTERESTREGISTRATIONLISTENERS_CANNOT_BE_REGISTERED_ON_A_REMOTE_BRIDGESERVER = new StringId(4582, "InterestRegistrationListeners cannot be registered on a remote BridgeServer");
  public static final StringId RemoteBridgeServer_INTERESTREGISTRATIONLISTENERS_CANNOT_BE_UNREGISTERED_FROM_A_REMOTE_BRIDGESERVER = new StringId(4583, "InterestRegistrationListeners cannot be unregistered from a remote BridgeServer");
  public static final StringId RemoteBridgeServer_INTERESTREGISTRATIONLISTENERS_CANNOT_BE_RETRIEVED_FROM_A_REMOTE_BRIDGESERVER = new StringId(4584, "InterestRegistrationListeners cannot be retrieved from a remote BridgeServer");
  public static final StringId PartitionAttributesImpl_REGION_SPECIFIED_IN_COLOCATEDWITH_IS_NOT_PRESENT_IT_SHOULD_BE_CREATED_BEFORE_SETTING_COLOCATED_WITH_THIS_REGION = new StringId(4585, "Region specified in ''colocated-with'' is not present. It should be created before setting ''colocated-with'' to this region.");
  public static final StringId PartitionAttributesImpl_SETTING_THE_ATTRIBUTE_COLOCATEDWITH_IS_SUPPORTED_ONLY_FOR_PARTITIONEDREGIONS = new StringId(4586, "Setting the attribute ''colocated-with'' is supported only for PartitionedRegions");
  public static final StringId PartitionAttributesImpl_CURRENT_PARTITIONEDREGIONS_TOTALNUMBUCKETS_SHOULD_BE_SAME_AS_TOTALNUMBUCKETS_OF_COLOCATED_PARTITIONEDREGION = new StringId(4587, "Current PartitionedRegion''s TotalNumBuckets should be same as TotalNumBuckets of colocated PartitionedRegion");
  public static final StringId PartitionAttributesImpl_CURRENT_PARTITIONEDREGIONS_REDUNDANCY_SHOULD_BE_SAME_AS_THE_REDUNDANCY_OF_COLOCATED_PARTITIONEDREGION = new StringId(4588, "Current PartitionedRegion''s redundancy should be same as the redundancy of colocated PartitionedRegion");
  public static final StringId LocalRegion_REGION_INTEREST_REGISTRATION_IS_ONLY_SUPPORTED_FOR_PARTITIONEDREGIONS = new StringId(4590, "Region interest registration is only supported for PartitionedRegions");
  public static final StringId PartitionRegionConfigValidator_INCOMPATIBLE_PARTITION_LISTENER = new StringId(4597, "The PartitionListeners={0} are incompatible with the PartitionListeners={1} used by other distributed members.");
  public static final StringId PartitionRegionConfigValidator_INCOMPATIBLE_PARTITION_RESOLVER = new StringId(4598, "The PartitionResolver={0} is incompatible with the PartitionResolver={1} used by other distributed members.");
  public static final StringId PartitionRegionConfigValidator_INCOMPATIBLE_COLOCATED_WITH = new StringId(4599, "The colocatedWith={0} found in PartitionAttributes is incompatible with the colocatedWith={1} used by other distributed members.");
  public static final StringId PartitionedRegionHelper_THE_ROUTINGOBJECT_RETURNED_BY_PARTITIONRESOLVER_IS_NULL = new StringId(4600, "The RoutingObject returned by PartitionResolver is null.");
  public static final StringId AgentConfigImpl_IDENTIFY_IF_EMAIL_NOTIFICATIONS_ARE_ENABLED_OR_NOT = new StringId(4602, "Whether the e-mail notifications are enabled.");
  public static final StringId AgentConfigImpl_IDENTIFY_THE_EMAIL_ADDRESS_USING_WHICH_EMAIL_NOTIFICATIONS_ARE_SENT = new StringId(4603, "E-mail address to be used to send e-mail notifications.");
  public static final StringId AgentConfigImpl_IDENTIFY_THE_EMAIL_SERVER_HOST_USING_WHICH_EMAIL_NOTIFICATIONS_ARE_SENT = new StringId(4604, "The host name of the e-mail server to be used for sending email notification.");
  public static final StringId AgentConfigImpl_IDENTIFY_THE_COMMA_SEPARATED_EMAIL_ADDRESSES_LIST_TO_WHICH_EMAIL_NOTIFICATIONS_ARE_SENT = new StringId(4605, "A comma-separated list of recipient e-mail addresses to send e-mail notifications to.");
  public static final StringId AgentConfigImpl_IDENTIFY_THE_NAME_OF_THE_FILE_TO_BE_USED_FOR_SAVING_AGENT_STATE = new StringId(4606, "The name(not the path) of the file to be used for saving agent state. The file is stored in the same directory in which the agent.properties file is located.");
  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_FUNCTION = new StringId(4607, "A {0} is not an instance of a Function");
  public static final StringId PartitionedRegion_EVICTIONATTRIBUTES_0_WILL_HAVE_NO_EFFECT_1_2 = new StringId(4608, "EvictionAttributes {0} will have no effect for Partitioned Region {1} on this VM because localMaxMemory is {2}.");
  public static final StringId GemFireCache_INITIALIZATION_FAILED_FOR_REGION_0 = new StringId(4609, "Initialization failed for Region {0}");
  public static final StringId LocalRegion_INITIALIZATION_FAILED_FOR_REGION_0 = new StringId(4610, "Initialization failed for Region {0}");
  public static final StringId PartitionedRegion_0_EVICTIONATTRIBUTES_1_DO_NOT_MATCH_WITH_OTHER_2 = new StringId(4611, "For Partitioned Region {0} the locally configured EvictionAttributes {1} do not match with other EvictionAttributes {2} and may cause misses during reads from VMs with smaller maximums.");
  public static final StringId AbstractRegionMap_THE_CURRENT_VALUE_WAS_NOT_EQUAL_TO_EXPECTED_VALUE = new StringId(4619, "The current value was not equal to expected value.");
  public static final StringId AbstractRegionEntry_THE_CURRENT_VALUE_WAS_NOT_EQUAL_TO_EXPECTED_VALUE = new StringId(4620, "The current value was not equal to expected value.");
  public static final StringId AbstractRegionMap_ENTRY_NOT_FOUND_WITH_EXPECTED_VALUE = new StringId(4621, "entry not found with expected value");
  public static final StringId PartitionedRegion_CANNOT_APPLY_A_DELTA_WITHOUT_EXISTING_ENTRY = new StringId(4622, "Cannot apply a delta without existing entry");
  public static final StringId CacheXmlParser_EXPECTED_A_FUNCTIONSERVICECREATION_INSTANCE = new StringId(4624, "Expected a FunctionServiceCreation instance");
  public static final StringId CacheXmlParser_A_0_IS_ONLY_ALLOWED_IN_THE_CONTEXT_OF_1_MJTDEBUG_E_2 = new StringId(4625, "A {0} is only allowed in the context of {1} MJTDEBUG e={2}");
  public static final StringId CacheHealthEvaluator_THE_AVERAGE_DURATION_OF_A_CACHE_LOAD_0_MS_EXCEEDS_THE_THRESHOLD_1_MS = new StringId(4627, "The average duration of a Cache load ({0} ms) exceeds the threshold ({1} ms)");
  public static final StringId DistributionManager_UNEXPECTED_CANCELLATION = new StringId(4628, "Unexpected cancellation");
  public static final StringId GatewayImpl_GATEWAY_0_IS_NOT_CLOSING_CLEANLY_FORCING_CANCELLATION = new StringId(4629, "Gateway <{0}> is not closing cleanly; forcing cancellation.");
  public static final StringId GatewayImpl_A_CANCELLATION_OCCURRED_STOPPING_THE_DISPATCHER = new StringId(4631, "A cancellation occurred. Stopping the dispatcher.");
  public static final StringId GatewayImpl_MESSAGE_DISPATCH_FAILED_DUE_TO_UNEXPECTED_EXCEPTION = new StringId(4632, "Message dispatch failed due to unexpected exception..");
  public static final StringId DistributionManager_INTERRUPTED_SENDING_SHUTDOWN_MESSAGE_TO_PEERS = new StringId(4634, "Interrupted sending shutdown message to peers");
  public static final StringId DistributionManager_INTERRUPTED_DURING_SHUTDOWN = new StringId(4635, "Interrupted during shutdown");
  public static final StringId GemFireCache_0_NOW_CLOSING = new StringId(4636, "{0}: Now closing.");
  public static final StringId HARegionQueue_QUEUEREMOVALTHREAD_IGNORED_CANCELLATION = new StringId(4637, "QueueRemovalThread ignored cancellation");
  public static final StringId ConnectionTable_UNABLE_TO_FORM_A_TCPIP_CONNECTION_TO_0_IN_OVER_1_SECONDS = new StringId(4638, "Unable to form a TCP/IP connection to {0} in over {1} seconds");
  public static final StringId InternalDistributedSystem_DISCONNECT_WAIT_INTERRUPTED = new StringId(4639, "Disconnect wait interrupted");
  public static final StringId InternalDistributedSystem_WAITING_THREAD_FOR_RECONNECT_GOT_INTERRUPTED = new StringId(4640, "Waiting thread for reconnect got interrupted.");
  public static final StringId DLockRequestProcess_INTERRUPTED_WHILE_RELEASING_GRANT = new StringId(4641, "Interrupted while releasing grant.");
  public static final StringId CacheClientUpdater_0_NO_CACHE_EXITING = new StringId(4643, "{0}: no cache (exiting)");
  public static final StringId CacheClientUpdater_0_WAIT_TIMED_OUT_MORE_THAN_1_SECONDS = new StringId(4644, "{0}: wait timed out (more than {1} seconds)");
  public static final StringId CacheClientUpdater_0_ABANDONED_WAIT_DUE_TO_CANCELLATION = new StringId(4645, "{0}: abandoned wait due to cancellation.");
  public static final StringId CacheClientUpdater_0_ABANDONED_WAIT_BECAUSE_IT_IS_NO_LONGER_CONNECTED = new StringId(4646, "{0}: abandoned wait because it is no longer connected");
  public static final StringId MailManager_ALERT_MAIL_SUBJECT = new StringId(4648, "Alert from GemFire Admin Agent");
  public static final StringId MailManager_EMAIL_ALERT_HAS_BEEN_SENT_0_1_2 = new StringId(4649, "Email sent to {0}. Subject: {1}, Content: {2}");
  public static final StringId CacheClientProxy_THE_FOLLOWING_EXCEPTION_OCCURRED_0 = new StringId(4655, "The following exception occurred while attempting to serialize {0}");
  public static final StringId RemoteBridgeServer_CANNOT_GET_CLIENT_SESSION = new StringId(4656, "Cannot get a client session for a remote BridgeServer");
  public static final StringId RemoteBridgeServer_CANNOT_GET_ALL_CLIENT_SESSIONS = new StringId(4657, "Cannot get all client sessions for a remote BridgeServer");
  public static final StringId CacheServerLauncher_REBALANCE = new StringId(4658, "-rebalance  Indicates that the Cache should immediately be rebalanced");
  public static final StringId PRHARedundancyProvider_UNEXPECTED_EXCEPTION_DURING_BUCKET_RECOVERY = new StringId(4659, "Unexpected exception during bucket recovery");
  public static final StringId PartitionedRegionRebalanceOp_UNABLE_TO_RELEASE_RECOVERY_LOCK = new StringId(4660, "Unable to release recovery lock");
  public static final StringId PartitionedRegionRebalanceOp_ERROR_IN_RESOURCE_OBSERVER = new StringId(4661, "Error in resource observer");
  public static final StringId AuthorizeRequestPP_0_NOT_AUTHORIZED_TO_PERFORM_EXECUTE_REGION_FUNCTION_1 = new StringId(4664, "{0}: In post-process: Not authorized to perform EXECUTE_REGION_FUNCTION operation on region [{1}]");
  public static final StringId PartitionedRegionLoadModel_INCOMPLETE_COLOCATION = new StringId(4665, "PartitionedRegionLoadModel - member {0} has incomplete colocation, but it has buckets for some regions. Should have colocated regions {1}  but had {2}  and contains buckets {3}");
  public static final StringId HeapMemoryMonitor_OVERRIDDING_MEMORYPOOLMXBEAN_HEAP_0_NAME_1 = new StringId(4666, "Overridding MemoryPoolMXBean heap threshold bytes {0} on pool {1}");
  public static final StringId MemoryMonitor_MEMBER_ABOVE_CRITICAL_THRESHOLD = new StringId(4669, "Member: {0} above {1} critical threshold");
  public static final StringId MemoryMonitor_MEMBER_ABOVE_HIGH_THRESHOLD = new StringId(4670, "Member: {0} above {1} eviction threshold");
  public static final StringId ResourceManager_REJECTED_EXECUTION_CAUSE_NOHEAP_EVENTS = new StringId(4671, "No memory events will be delivered because of RejectedExecutionException");
  public static final StringId ResourceManager_FAILED_TO_STOP_RESOURCE_MANAGER_THREADS = new StringId(4672, "Failed to stop resource manager threads in {0} seconds");
  public static final StringId GatewayImpl_EVENT_QUEUE_ALERT_OPERATION_0_REGION_1_KEY_2_VALUE_3_TIME_4 = new StringId(4673, "{0} event for region={1} key={2} value={3} was in the queue for {4} milliseconds");
  public static final StringId CacheClientProxy_NOT_PRIMARY = new StringId(4676, "This process is not the primary server for the given client");
  public static final StringId CacheServerImpl_MUST_BE_RUNNING = new StringId(4677, "The cache server must be running to use this operation");
  public static final StringId InstantiatorRecoveryListener_INSTANTIATORRECOVERYTASK_ERROR_CLASSNOTFOUNDEXCEPTION = new StringId(4679, "InstantiatorRecoveryTask - Error ClassNotFoundException: {0}");
  public static final StringId ResourceAdvisor_MEMBER_CAUGHT_EXCEPTION_PROCESSING_PROFILE = new StringId(4682, "This member caught exception processing profile {0} {1}");
  public static final StringId MemoryMonitor_EXCEPTION_OCCURED_WHEN_NOTIFYING_LISTENERS = new StringId(4683, "Exception occurred when notifying listeners ");
  public static final StringId CacheXmlParser_A_0_IS_NOT_DATA_SERIALIZABLE = new StringId(4684, "The class {0}, presented for instantiator registration is not an instance of DataSerializable and cannot be registered.");
  public static final StringId CacheXmlParser_NO_SERIALIZATION_ID = new StringId(4685, "The instantiator registration did not include an ID attribute.");
  public static final StringId CacheXmlParser_NO_CLASSNAME_FOUND = new StringId(4686, "A string class-name was expected, but not found while parsing.");
  public static final StringId CacheXmlParser_A_0_CLASS_NOT_FOUND = new StringId(4687, "Unable to load class {0}");
  public static final StringId CacheXmlParser_A_0_NOT_A_SERIALIZER = new StringId(4688, "The class {0} presented for serializer registration does not extend DataSerializer and cannot be registered.");
  public static final StringId SerializerCreation_A_0_INSTANTIATION_FAILED = new StringId(4689, "Failed to create a new instance of DataSerializable class {0}");
  public static final StringId HeapMemoryMonitor_NO_POOL_FOUND_POOLS_0 = new StringId(4690, "No tenured pools found.  Known pools are: {0}");
  public static final StringId MemoryThresholds_CRITICAL_PERCENTAGE_GT_ZERO_AND_LTE_100 = new StringId(4691, "Critical percentage must be greater than 0.0 and less than or equal to 100.0.");
  public static final StringId MemoryThresholds_CRITICAL_PERCENTAGE_GTE_EVICTION_PERCENTAGE = new StringId(4692, "Critical percentage must be greater than the eviction percentage.");
  public static final StringId MemoryThresholds_EVICTION_PERCENTAGE_GT_ZERO_AND_LTE_100 = new StringId(4693, "Eviction percentage must be greater than 0.0 and less than or equal to 100.0.");
  public static final StringId MemoryMonitor_EVICTION_PERCENTAGE_LTE_CRITICAL_PERCENTAGE = new StringId(4694, "Eviction percentage must be less than the critical percentage.");
  public static final StringId PartitionedRegionFunctionResultSender_UNEXPECTED_EXCEPTION_DURING_FUNCTION_EXECUTION_ON_LOCAL_NODE = new StringId(4695, "Unexpected exception during function execution on local node Partitioned Region");
  public static final StringId DistributedRegionFunctionResultSender_UNEXPECTED_EXCEPTION_DURING_FUNCTION_EXECUTION_ON_LOCAL_NODE = new StringId(4696, "Unexpected exception during function execution on local node Distributed Region");
  public static final StringId MemberResultSender_UNEXPECTED_EXCEPTION_DURING_FUNCTION_EXECUTION_ON_LOCAL_NODE = new StringId(4697, "Unexpected exception during function execution local member");
  public static final StringId ResourceManager_LOW_MEMORY_IN_0_FOR_PUT_1_MEMBER_2 = new StringId(4700, "Region: {0} cannot process operation on key: {1} because member {2} is running low on memory");
  public static final StringId ResourceManager_LOW_MEMORY_PR_0_KEY_1_MEMBERS_2 = new StringId(4701, "PartitionedRegion: {0} cannot process operation on key {1} because members {2} are running low on memory");
  public static final StringId ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE = new StringId(4702, "Cannot {0} result as the Function#hasResult() is false");
  public static final StringId ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1 = new StringId(4703, "Function: {0} cannot be executed because the members {1} are running low on memory");
  public static final StringId ResourceManager_LOW_MEMORY_FOR_INDEX = new StringId(4704, "Cannot create index on region {0} because the target member is running low on memory");
  public static final StringId DistributedSystemConfigImpl_0_IS_NOT_A_VALID_INTEGER_1 = new StringId(4706, "{0} is not a valid integer for {1}");
  public static final StringId AdminDistributedSystemJmxImpl_JMX_CLIENT_COUNT_HAS_DROPPED_TO_ZERO = new StringId(4707, "JMX Client count has dropped to zero.");
  public static final StringId AdminDistributedSystemJmxImpl_MEMBER_JOINED_THE_DISTRIBUTED_SYSTEM_MEMBER_ID_0 = new StringId(4708, "Member joined the Distributed System\n\tMember Id: {0}");
  public static final StringId AdminDistributedSystemJmxImpl_MEMBER_LEFT_THE_DISTRIBUTED_SYSTEM_MEMBER_ID_0 = new StringId(4709, "Member left the Distributed System\n\tMember Id: {0}");
  public static final StringId AdminDistributedSystemJmxImpl_MEMBER_CRASHED_IN_THE_DISTRIBUTED_SYSTEM_MEMBER_ID_0 = new StringId(4710, "Member crashed in the Distributed System\n\tMember Id: {0}");
  public static final StringId AdminDistributedSystemJmxImpl_MEMBER_JOINED = new StringId(4711, "Member Joined");
  public static final StringId AdminDistributedSystemJmxImpl_MEMBER_LEFT = new StringId(4712, "Member Left");
  public static final StringId AdminDistributedSystemJmxImpl_MEMBER_CRASHED = new StringId(4713, "Member Crashed");
  public static final StringId AdminDistributedSystemJmxImpl_SYSTEM_ALERT_FROM_DISTRIBUTED_SYSTEM_0 = new StringId(4714, "System Alert from Distributed System\n\t: {0}");
  public static final StringId AdminDistributedSystemJmxImpl_STATISTICS_ALERT_FOR_MEMBER = new StringId(4715, "Statistics Alert for member");
  public static final StringId AgentConfigImpl_IP_ADDRESS_OF_THE_AGENTS_DISTRIBUTED_SYSTEM = new StringId(4716, "An IP address of the JMX Agent''s distributed system on a multi-homed host. On a multi-homed host, you must explicitly specify the bind address.");
  public static final StringId AgentConfigImpl_REFRESH_INTERVAL_IN_SECONDS_FOR_AUTOREFRESH_OF_MEMBERS_AND_STATISTIC_RESOURCES = new StringId(4718, "Refresh Interval, in seconds, to be used for auto-refresh of SystemMember, StatisticResource and CacheServer refreshes");
  public static final StringId AgentConfigImpl_AGENT_CONFIGURATION = new StringId(4720, "Agent Configuration:");
  public static final StringId AgentImpl_FAILED_TO_START_RMI_SERVICE = new StringId(4721, "Failed to start RMI service, verify RMI configuration properties");
  public static final StringId AgentImpl_SETTING_0 = new StringId(4722, "Setting {0}");
  public static final StringId AgentLauncher_SERVER_FAILED_TO_START_0 = new StringId(4723, "Server failed to start: {0}");
  public static final StringId AgentLauncher_MISSING_COMMAND = new StringId(4724, "Missing command");
  public static final StringId AgentLauncher_THE_SPECIFIED_WORKING_DIRECTORY_0_CONTAINS_NO_STATUS_FILE = new StringId(4725, "The specified working directory ({0}) contains no status file");
  public static final StringId AgentLauncher_NO_AVAILABLE_STATUS = new StringId(4726, "No available status.");
  public static final StringId AgentLauncher_COULD_NOT_DELETE_FILE_0 = new StringId(4727, "Could not delete file: \"{0}\".");
  public static final StringId AgentLauncher_REQUEST_INTERRUPTED_BY_USER = new StringId(4728, "Request interrupted by user");
  public static final StringId MANAGED_RESOURCE_REFRESH_INTERVAL_CANT_BE_SET_DIRECTLY = new StringId(4729, "RefreshInterval can not be set directly. Use DistributedSystemConfig.refreshInterval.");
  public static final StringId MBeanUtil_0_IN_1 = new StringId(4730, "{0} in ''{1}''");
  public static final StringId MailManager_AN_EXCEPTION_OCCURRED_WHILE_SENDING_EMAIL = new StringId(4731, "An exception occurred while sending email.");
  public static final StringId MailManager_UNABLE_TO_SEND_EMAIL_PLEASE_CHECK_YOUR_EMAIL_SETTINGS_AND_LOG_FILE = new StringId(4732, "Unable to send email. Please check your mail settings and the log file.");
  public static final StringId MailManager_EXCEPTION_MESSAGE_0 = new StringId(4733, "Exception message: {0}");
  public static final StringId MailManager_FOLLOWING_EMAIL_WAS_NOT_DELIVERED = new StringId(4734, "Following email was not delivered:");
  public static final StringId MailManager_MAIL_HOST_0 = new StringId(4735, "Mail Host: {0}");
  public static final StringId MailManager_FROM_0 = new StringId(4736, "From: {0}");
  public static final StringId MailManager_TO_0 = new StringId(4737, "To: {0}");
  public static final StringId MailManager_SUBJECT_0 = new StringId(4738, "Subject: {0}");
  public static final StringId MailManager_CONTENT_0 = new StringId(4739, "Content: {0}");
  public static final StringId RegionAttributesCreation__CLONING_ENABLE_IS_NOT_THE_SAME_THIS_0_OTHER_1 = new StringId(4741, "Cloning enabled is not the same: this:  {0}  other:  {1}");
  public static final StringId UpdateOperation_ERROR_APPLYING_DELTA_FOR_KEY_0_OF_REGION_1 = new StringId(4742, "Error applying delta for key {0} of region {1}");
  public static final StringId RequestEventValue_0_THE_EVENT_ID_FOR_THE_GET_EVENT_VALUE_REQUEST_IS_NULL = new StringId(4744, "{0}: The event id for the get event value request is null.");
  public static final StringId RequestEventValue_UNABLE_TO_FIND_A_CLIENT_UPDATE_MESSAGE_FOR_0 = new StringId(4745, "Unable to find a client update message for {0}");
  public static final StringId PoolImpl_STATISTIC_SAMPLING_MUST_BE_ENABLED_FOR_SAMPLING_RATE_OF_0_TO_TAKE_AFFECT = new StringId(4746, "statistic-sampling must be enabled for sampling rate of {0} to take affect");
  public static final StringId AgentLauncher_EXCEPTION_IN_0_1 = new StringId(4747, "Exception in {0} : {1} ");
  public static final StringId MBeanUtil_FAILED_TO_CREATE_MBEAN_FOR_0 = new StringId(4748, "Failed to create MBean representation for resource {0}.");
  public static final StringId GemFireCache_LONG_RUNNING_QUERY_EXECUTION_CANCELED = new StringId(4749, "Query execution taking more than the max query-execution time is canceled, for query: {0} on thread thread id: {1} canceled");
  public static final StringId QueryMonitor_LONG_RUNNING_QUERY_CANCELED = new StringId(4750, "Query execution cancelled after exceeding max execution time {0}ms.");
  public static final StringId AdminDistributedSystemJmxImpl_STATISTICS_ALERT_FROM_DISTRIBUTED_SYSTEM_MEMBER_0_STATISTICS_1 = new StringId(4751, "Statistics Alert from Distributed System\n\tMember: {0}\n\tStatistics: {1}");
  public static final StringId SystemAdmin_VALIDATE_DISK_STORE = new StringId(4752, "Checks to make sure files of a disk store are valid.\n The name of the disk store and the directories its files are stored in are required arguments.");
  public static final StringId SystemAdmin_MODIFY_DISK_STORE = new StringId(4753, "Modifies the contents stored in a disk store.\n Note that this operation writes to the disk store files so use it with care. Requires that a region name by specified using -region=<regionName>\n Options:\n   -remove will remove the region from the disk store causing any data stored in the disk store for this region to no longer exist. Subregions of the removed region will not be removed.\n   -lru=<type> Sets region''s lru algorithm. Valid types are: none, lru-entry-count, lru-heap-percentage, or lru-memory-size\n   -lruAction=<action> Sets the region''s lru action. Valid actions are: none, overflow-to-disk, local-destroy\n   -lruLimit=<int> Sets the region''s lru limit. Valid values are >= 0\n   -concurrencyLevel=<int> Sets the region''s concurrency level. Valid values are >= 0\n   -initialCapacity=<int> Sets the region''s initial capacity. Valid values are >= 0.\n   -loadFactor=<float> Sets the region''s load factory. Valid values are >= 0.0\n   -statisticsEnabled=<boolean> Sets the region''s statistics enabled. Value values are true or false.\n The name of the disk store and the directories its files are stored in and the region to target are all required arguments.");
  public static final StringId DiskRegion_COMPACTION_SUMMARY = new StringId(4756, "compaction did {0} creates and updates in {1} ms");
  public static final StringId DiskRegion_COMPACTION_OPLOGIDS = new StringId(4757, "OplogCompactor for {0} compaction oplog id(s): {1}");
  public static final StringId DiskRegion_COMPACTION_FAILURE = new StringId(4759, "OplogCompactor for {0} did NOT complete compaction of oplog id(s): {1}");
  public static final StringId DiskRegion_OPLOG_LOAD_TIME = new StringId(4761, "recovery oplog load took {0} ms");
  public static final StringId DiskRegion_REGION_INIT_TIME = new StringId(4762, "recovery region initialization took {0} ms");
  public static final StringId Oplog_DUPLICATE_CREATE = new StringId(4764, "Oplog::readNewEntry: Create is present in more than one Oplog. This should not be possible. The Oplog Key ID for this entry is {0,number,#}.");
  public static final StringId StreamingPartitionOperation_GOT_MEMBERDEPARTED_EVENT_FOR_0_CRASHED_1 = new StringId(4765, "Streaming reply processor got memberDeparted event for < {0} > crashed =  {1}");
  public static final StringId Eviction_EVICTOR_TASK_EXCEPTION = new StringId(4766, "Exception: {0} occurred during eviction ");
  public static final StringId AdminDistributedSystemJmxImpl_READONLY_STAT_ALERT_DEF_FILE_0 = new StringId(4767, "stat-alert definitions could not be saved in the read-only file {0}.");
  public static final StringId Oplog_CLOSING_EMPTY_OPLOG_0_1 = new StringId(4768, "Closing {1} early since it is empty. It is for disk store {0}.");
  public static final StringId CacheServerHelper_UTF8_EXCEPTION = new StringId(4769, "UTF-8 Exception malformed input");
  public static final StringId Mem_LRU_Eviction_Attribute_Reset = new StringId(4770, "For region {0} with data policy PARTITION, memory LRU eviction attribute \"maximum\" has been reset from {1}MB to local-max-memory {2}MB");
  public static final StringId AdminDistributedSystemJmxImpl_FAILED_TO_CREATE_STAT_ALERT_DEF_FILE_0 = new StringId(4773, "Could not create file {0} to save stat-alert definitions. stat-alert definitions could not be saved");
  public static final StringId GemFireHealthImpl_COULD_NOT_FIND_A_HOST_WITH_NAME_0 = new StringId(4774, "Could not find a host with name \"{0}\".");
  public static final StringId AbstractDistributionConfig_REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME_0 = new StringId(4775, "Whether to remove unresponsive client or not. Defaults to {0}.");
  public static final StringId CacheClientNotifier_CLIENT_0_IS_A_SLOW_RECEIVER = new StringId(4776, "Client {0} is a slow receiver.");
  public static final StringId HARegionQueue_CLIENT_QUEUE_FOR_0_IS_FULL = new StringId(4777, "Client queue for {0} client is full.");

  public static final StringId Oplog_DELETE_0_1_2 = new StringId(4778, "Deleted {0} {1} for disk store {2}.");
  public static final StringId Oplog_CREATE_0_1_2 = new StringId(4779, "Created {0} {1} for disk store {2}.");
  public static final StringId DiskRegion_RECOVERING_OPLOG_0_1_2 = new StringId(4780, "Recovering {0} {1} for disk store {2}.");
  public static final StringId DiskRegion_COULD_NOT_OPEN_0 = new StringId(4781, "Could not open {0}.");

  public static final StringId MemoryMonitor_MEMBER_BELOW_CRITICAL_THRESHOLD = new StringId(4782, "Member: {0} below {1} critical threshold");
  public static final StringId MemoryMonitor_MEMBER_BELOW_HIGH_THRESHOLD = new StringId(4783, "Member: {0} below {1} eviction threshold");
  public static final StringId AttributesFactory_INVALIDATE_REGION_NOT_SUPPORTED_FOR_PR = new StringId(4784, "ExpirationAction INVALIDATE or LOCAL_INVALIDATE for region is not supported for Partitioned Region.");
  public static final StringId AttributesFactory_LOCAL_DESTROY_IS_NOT_SUPPORTED_FOR_PR = new StringId(4785, "ExpirationAction LOCAL_DESTROY is not supported for Partitioned Region.");
  public static final StringId AttributesFactory_LOCAL_INVALIDATE_IS_NOT_SUPPORTED_FOR_PR = new StringId(4786, "ExpirationAction LOCAL_INVALIDATE is not supported for Partitioned Region.");
  public static final StringId AttributesFactory_DESTROY_REGION_NOT_SUPPORTED_FOR_PR = new StringId(4794, "ExpirationAction DESTROY or LOCAL_DESTROY for region is not supported for Partitioned Region.");
  public static final StringId ExecuteFunction_DS_NOT_CREATED_OR_NOT_READY = new StringId(4795, "DistributedSystem is either not created or not ready");

  public static final StringId CacheClientUpdater_CLASS_NOT_FOUND = new StringId(4796, "Unable to load the class: {0}");
  public static final StringId DataSerializerRecoveryListener_ERROR_CLASSNOTFOUNDEXCEPTION = new StringId(4797, "DataSerializerRecoveryTask - Error ClassNotFoundException: {0}");
  public static final StringId DataSerializerRecoveryListener_ERROR_RECOVERING_DATASERIALIZERS = new StringId(4798, "DataSerializerRecoveryTask - Error recovering dataSerializers: ");
  public static final StringId GetClientPRMetadata_THE_INPUT_REGION_PATH_IS_NULL = new StringId(4799, " The input region path for the GetClientPRMetadata request is null");
  public static final StringId GetClientPartitionAttributes_THE_INPUT_REGION_PATH_IS_NULL = new StringId(4800, " The input region path for the GetClientPartitionAttributes request is null");
  public static final StringId GetClientPRMetadata_REGION_NOT_FOUND_FOR_SPECIFIED_REGION_PATH = new StringId(4801, "Region was not found during GetClientPRMetadata request for region path : {0}");
  public static final StringId GetClientPartitionAttributes_REGION_NOT_FOUND_FOR_SPECIFIED_REGION_PATH = new StringId(4802, "Region was not found during GetClientPartitionAttributes request for region path : {0}");
  public static final StringId GetClientPRMetadata_REGION_NOT_FOUND = new StringId(4803, "Region was not found during GetClientPRMetadata request for region path : ");
  public static final StringId GetClientPartitionAttributes_REGION_NOT_FOUND = new StringId(4804, "Region was not found during GetClientPartitionAttributes request for region path : ");
  public static final StringId ClientPartitionAdvisor_CANNOT_CREATE_AN_INSTANCE_OF_PARTITION_RESOLVER_0 = new StringId(4805, "Cannot create an instance of PartitionResolver : {0}");
  public static final StringId FunctionService_0_PASSED_IS_NULL = new StringId(4806, "{0} passed is null");
  public static final StringId FunctionService_FUNCTION_GET_ID_RETURNED_NULL = new StringId(4807, "function.getId() returned null, implement the Function.getId() method properly");
  public static final StringId DistributionManager_CAUGHT_EXCEPTION_WHILE_SENDING_DELTA = new StringId(4808, "Caught exception while sending delta. ");
  public static final StringId DirectChannel_FAILURE_SENDING_DIRECT_REPLY = new StringId(4809, "Failed sending a direct reply to {0}");

  public static final StringId GatewayImpl_EVENT_QUEUE_DISPATCH_FAILED = new StringId(4811, "During normal processing, unsuccessfully dispatched {0} events (batch #{1})");
  public static final StringId PartitionedRegion_KEY_0_NOT_COLOCATED_WITH_TRANSACTION = new StringId(4815, "Key {0} is not colocated with transaction");
  public static final StringId LocalRegion_NON_TRANSACTIONAL_REGION_COLLECTION_IS_BEING_USED_IN_A_TRANSACTION = new StringId(4816, "The Region collection is not transactional but is being used in a transaction {0}.");
  public static final StringId TXEntry_UA_NOT_SUPPORTED_FOR_PR = new StringId(4818, "Partitioned region does not support UserAttributes in transactional context");
  public static final StringId TXStateStub_ROLLBACK_ON_NODE_0_FAILED = new StringId(4819, "Rollback operation on node {0} failed");
  public static final StringId PartitionedRegion_INVALIDATING_REGION_CAUGHT_EXCEPTION = new StringId(4820, "Invalidating partitioned region caught exception {0}");
  public static final StringId PartitionedRegion_TRANSACTIONAL_DATA_MOVED_DUE_TO_REBALANCING = new StringId(4821, "Transactional data moved, due to rebalancing.");
  public static final StringId PartitionedRegion_TRANSACTION_DATA_NODE_0_HAS_DEPARTED_TO_PROCEED_ROLLBACK_THIS_TRANSACTION_AND_BEGIN_A_NEW_ONE = new StringId(4822, "Transaction data node {0} has departed. To proceed, rollback this transaction and begin a new one.");

  public static final StringId TXState_REGION_DESTROY_NOT_SUPPORTED_IN_A_TRANSACTION = new StringId(4825, "destroyRegion() is not supported while in a transaction");
  public static final StringId TXState_REGION_INVALIDATE_NOT_SUPPORTED_IN_A_TRANSACTION = new StringId(4826, "invalidateRegion() is not supported while in a transaction");
  public static final StringId PartitionedRegion_TX_FUNCTION_ON_MORE_THAN_ONE_NODE = new StringId(4827, "Function inside a transaction cannot execute on more than one node");
  public static final StringId PartitionedRegion_TX_ON_DATASTORE = new StringId(4828, "PartitionedRegion Transactions cannot execute on nodes with local max memory zero");
  public static final StringId PartitionedRegion_TX_FUNCTION_EXECUTION_NOT_COLOCATED = new StringId(4829, "Function execution is not colocated with transaction");
  public static final StringId FAILED_SENDING_0 = new StringId(4830, "Failed sending < {0} >");
  public static final StringId RemoteContainsKeyValueMessage_CONTAINSKEYVALUERESPONSE_GOT_REMOTE_CACHEEXCEPTION = new StringId(4831, "RemoteContainsKeyResponse got remote CacheException; triggering RemoteOperationException.");
  public static final StringId RemoteContainsKeyValueMessage_NO_RETURN_VALUE_RECEIVED = new StringId(4832, "no return value received");
  public static final StringId RemoteDestroyMessage_FAILED_SENDING_0 = new StringId(4833, "Failed sending < {0} >");

  public static final StringId ReliableReplyProcessor_FAILED_TO_DELIVER_MESSAGE_TO_MEMBERS_0 = new StringId(4835, "Failed to deliver message to members: {0}");
  public static final StringId RemotePutMessage_DID_NOT_RECEIVE_A_VALID_REPLY = new StringId(4836, "did not receive a valid reply");
  public static final StringId RemotePutMessage_FAILED_SENDING_0 = new StringId(4837, "Failed sending < {0} >");
  public static final StringId RemotePutMessage_UNABLE_TO_PERFORM_PUT_BUT_OPERATION_SHOULD_NOT_FAIL_0 = new StringId(4838, "unable to perform put, but operation should not fail {0}");
  public static final StringId TXState_REGION_CLEAR_NOT_SUPPORTED_IN_A_TRANSACTION = new StringId(4839, "clear() is not supported while in a transaction");
  public static final StringId PartitionedRegion_TX_FUNCTION_EXECUTION_NOT_COLOCATED_0_1 = new StringId(4840, "Function execution is not colocated with transaction. The transactional data is hosted on node {0}, but you are trying to target node {1}");
  public static final StringId GemFireCacheImpl_STARTING_GEMFIRE_REDIS_SERVER_ON_BIND_ADDRESS_0_PORT_1 = new StringId(4841, "Starting GeodeRedisServer on bind address {0} on port {1}");
  public static final StringId RemoteFetchEntryMessage_ENTRY_NOT_FOUND = new StringId(4842, "entry not found");
  public static final StringId RemoteFetchEntryMessage_FAILED_SENDING_0 = new StringId(4843, "Failed sending < {0} >");
  public static final StringId RemoteFetchEntryMessage_FETCHENTRYRESPONSE_GOT_REMOTE_CACHEEXCEPTION_FORCING_REATTEMPT = new StringId(4844, "FetchEntryResponse got remote CacheException; forcing reattempt.");
  public static final StringId Dist_TX_PRECOMMIT_NOT_SUPPORTED_IN_A_TRANSACTION = new StringId(4845, "precommit() operation {0} meant for Dist Tx is not supported");
  public static final StringId Dist_TX_ROLLBACK_NOT_SUPPORTED_IN_A_TRANSACTION = new StringId(4846, "rollback() operation {0} meant for Dist Tx is not supported");
  public static final StringId AdminDistributedSystemJmxImpl_PROCESSING_CLIENT_MEMBERSHIP_EVENT_0_FROM_1_FOR_2_RUNNING_ON_3 = new StringId(4847, "Processing client membership event \"{0}\" from {1} for client with id: {2} running on host: {3}");
  public static final StringId AdminDistributedSystemJmxImpl_FAILED_TO_PROCESS_CLIENT_MEMBERSHIP_FROM_0_FOR_1 = new StringId(4848, "Could not process client membership notification from {0} for client with id {1}.");
  public static final StringId SystemMemberJmx_FAILED_TO_SEND_0_NOTIFICATION_FOR_1 = new StringId(4849, "Failed to send {0} notification for {1}");

  public static final StringId DISTTX_TX_EXPECTED = new StringId(4852, "Expected {0} during a distributed transaction but got {1}");

  public static final StringId PartitionManager_REGION_0_IS_NOT_A_PARTITIONED_REGION = new StringId(4855, "Region {0} is not a Partitioned Region");

  public static final StringId PartitionAttributesFactory_PARTITION_LISTENER_PARAMETER_WAS_NULL = new StringId(4859, "PartitionListner parameter was null");
  public static final StringId PartitionRegionConfigValidator_INCOMPATIBLE_EXPIRATION_ATTRIBUETS = new StringId(4860, "The {0} set in RegionAttributes is incompatible with {0} used by other distributed members.");

  public static final StringId AdminDistributedSystem_ENCOUNTERED_A_0_WHILE_LOADING_STATALERTDEFINITIONS_1_LOADING_ABORTED = new StringId(4864, "Encountered a {0} while loading StatAlertDefinitions [from {1}]. Loading of statAlertDefinitions has been aborted.");
  public static final StringId AgentConfigImpl_THE_PORT_USED_TO_CONFIGURE_RMI_CONNECTOR_SERVER = new StringId(4865, "The port on which the RMI Connector Server should start. The value must be in the range: 0-65535.");
  public static final StringId AbstractDistributionConfig_MEMBERSHIP_PORT_RANGE_NAME_0 = new StringId(4866, "Sets the range of datagram socket ports that can be used for membership ID purposes and unicast datagram messaging. Defaults to {0}.");
  public static final StringId AgentConfigImpl_ALLOWED_RANGE_OF_UDP_PORTS_TO_FORM_UNIQUE_MEMBERSHIP_ID = new StringId(4867, "The allowed range of UDP ports for use in forming an unique membership identifier. This range is given as two numbers separated by a minus sign.");
  public static final StringId DistributedSystemConfigImpl_INVALID_VALUE_FOR_MEMBERSHIP_PORT_RANGE = new StringId(4868, "The value specified \"{0}\" is invalid for the property : \"{1}\". This range should be specified as min-max.");

  public static final StringId ServerConnection_SERVER_FAILED_TO_ENCRYPT_DATA_0 = new StringId(4869, "Server failed to encrpt data {0}");
  public static final StringId CacheClientProxy__0_NOT_ADDING_CQ_MESSAGE_TO_QUEUE_1_BECAUSE_AUTHORIZATION_FAILED = new StringId(4870, "{0}: Not Adding CQ message to queue {1} because authorization failed.");
  public static final StringId PoolImpl_POOL_0_STARTED_WITH_MULTIUSER_SECURE_MODE_ENABLED_1 = new StringId(4871, "Pool {0} started with multiuser-authentication={1}");
  public static final StringId HARegionQueue_ENYTRY_EXPIRY_TASKS_DISABLED_BECAUSE_QUEUE_BECAME_PRIMARY_OLD_MSG_TTL_0 = new StringId(4872, "Entry expiry tasks disabled because the queue became primary. Old messageTimeToLive was: {0}");
  public static final StringId HARegionQueue_RESUMING_WITH_PROCESSING_PUTS = new StringId(4873, "Resuming with processing puts ...");

  public static final StringId InternalDistributedSystem_MEMORY_OVERCOMMIT = new StringId(4875, "Insufficient free memory ({0}) when attempting to lock {1} bytes.  Either reduce the amount of heap or off-heap memory requested or free up additional system memory.  You may also specify -Dgemfire.Cache.ALLOW_MEMORY_OVERCOMMIT=true on the command-line to override the constraint check.");
  public static final StringId InternalDistributedSystem_MEMORY_OVERCOMMIT_WARN = new StringId(4876, "System memory appears to be over committed by {0} bytes.  You may experience instability, performance issues, or terminated processes due to the Linux OOM killer.");


  public static final StringId BaseCommand__THE_INPUT_KEY_FOR_THE_0_REQUEST_IS_NULL = new StringId(4884, " The input key for the {0} request is null");
  public static final StringId GroupMembershipService_PROBLEM_GENERATING_CACHE_XML = new StringId(4885, "Unable to generate XML description for reconnect of cache due to exception");
  public static final StringId BaseCommand__THE_INPUT_REGION_NAME_FOR_THE_0_REQUEST_IS_NULL = new StringId(4886, " The input region name for the {0} request is null");
  public static final StringId BaseCommand__0_WAS_NOT_FOUND_DURING_1_REQUEST = new StringId(4887, "{0} was not found during {0} request");
  public static final StringId BaseCommand_DURING_0_NO_ENTRY_WAS_FOUND_FOR_KEY_1 = new StringId(4888, "During {0} no entry was found for key {1}");

  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_INVALIDATE_OPERATION_ON_REGION_0 = new StringId(4891, "Not authorized to perform INVALIDATE operation on region {0}");

  public static final StringId GemFireCache_INDEX_CREATION_EXCEPTION_1 = new StringId(4893, "Failed to create index {0} on region {1}");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_GET_DURABLE_CQS_OPERATION_0_ON_THE_CACHE = new StringId(4894, "Not authorized to perform GET_DURABLE_CQS operation on cache");

  public static final StringId AbstractDistributionConfig_DEPLOY_WORKING_DIR_0 = new StringId(4895, "The working directory that can be used to persist JARs deployed during runtime. Defaults to \"{0}\".");

  public static final StringId DiskStoreAlreadyInVersion_0 = new StringId(4896, "This disk store is already at version {0}.");

  public static final StringId DiskStoreStillAtVersion_0 = new StringId(4897, "This disk store is still at version {0}.");

  public static final StringId AbstractDistributionConfig_ENABLE_SHARED_CONFIGURATION = new StringId(4898, "Enables cluster configuration support in dedicated locators.  This allows the locator to share configuration information amongst members and save configuration changes made using GFSH.");
  public static final StringId AbstractDistributionConfig_LOAD_SHARED_CONFIGURATION_FROM_DIR = new StringId(4899, "Loads cluster configuration from the \"{0}\" directory of a locator. This is property is only applicable to the locator(s)");

  public static final StringId DISTRIBUTED_SYSTEM_RECONNECTING = new StringId(4901, "Attempting to reconnect to the distributed system.  This is attempt #{0}.");
  public static final StringId AbstractDistributionConfig_USE_SHARED_CONFIGURATION = new StringId(4902, "Boolean flag that allows the cache to use the cluster configuration provided by the cluster config service");
  public static final StringId GemFireCache_RECEIVED_SHARED_CONFIGURATION_FROM_LOCATORS = new StringId(4903, "Received cluster configuration from the locator");
  public static final StringId GemFireCache_SHARED_CONFIGURATION_NOT_AVAILABLE = new StringId(4904, "cluster configuration service not available");
  public static final StringId GemFireCache_EXCEPTION_OCCURED_WHILE_DEPLOYING_JARS_FROM_SHARED_CONDFIGURATION = new StringId(4905, "Exception while deploying the jars received as a part of cluster Configuration");
  public static final StringId GemFireCache_NO_LOCATORS_FOUND_WITH_SHARED_CONFIGURATION = new StringId(4906, "No locator(s) found with cluster configuration service");
  public static final StringId GemFireCache_NOT_USING_SHARED_CONFIGURATION = new StringId(4907, "The cache has been created with \"use-cluster-configuration=false\". It will not receive any cluster configuration");
  public static final StringId AbstractDistributionConfig_CLUSTER_CONFIGURATION_DIR = new StringId(4908, "The directory to store the cluster configuration artifacts and disk-store. This property is only applicable to the locator(s)");
  public static final StringId GemFireCache_INDEX_RECOVERY_EXCEPTION_1 = new StringId(4909, "Failed to populate indexes after disk recovery on region {0}");
  public static final StringId GemFireCache_INDEX_LOADING = new StringId(4910, "Loading data into the indexes");
  public static final StringId PoolManagerImpl_GETPENDINGEVENTCOUNT_SHOULD_BE_CALLED_BEFORE_INVOKING_READYFOREVENTS = new StringId(4911, "getPendingEventCount() should be called before invoking readyForEvents().");
  public static final StringId RemoveAll_THE_INPUT_REGION_NAME_FOR_THE_REMOVEALL_REQUEST_IS_NULL = new StringId(4912, "The input region name for the removeAll request is null");
  public static final StringId RemoveAll_ONE_OF_THE_INPUT_KEYS_FOR_THE_REMOVEALL_REQUEST_IS_NULL = new StringId(4913, " One of the input keys for the removeAll request is null");
  public static final StringId AuthorizeRequest_NOT_AUTHORIZED_TO_PERFORM_REMOVEALL_OPERATION_ON_REGION_0 = new StringId(4914, "Not authorized to perform removeAll operation on region [{0}]");
  public static final StringId AbstractDistributionConfig_OFF_HEAP_MEMORY_SIZE_0 = new StringId(4915, "The amount of off-heap memory to be allocated for GemFire. Value is <n>[g|m], where <n> is the size and [g|m] specifies the units in gigabytes or megabytes. Defaults to \"{0}\".");
  public static final StringId CacheServerLauncher_CRITICAL_OFF_HEAP_PERCENTAGE = new StringId(4916, "<critical-Off-heap-percentage>  Sets the critical off-heap " + "threshold limit of the Resource Manager. This overrides the " + "critical-off-heap-percentage set in the <resource-manager> element " + "of the \"cache-xml-file\"");
  public static final StringId CacheServerLauncher_EVICTION_OFF_HEAP_PERCENTAGE = new StringId(4917, "<eviction-off-heap-percentage>  Sets the eviction heap " + "threshold limit of the Resource Manager above which the eviction " + "should begin on Regions configured for eviction by off-heap LRU. " + "This overrides the eviction-off-heap-percentage set in the " + "<resource-manager> element of the \"cache-xml-file\"");
  public static final StringId CacheServerLauncher_CRITICAL_HEAP_PERCENTAGE = new StringId(4918, "<critical-heap-percentage>  Sets the critical heap " + "threshold limit of the Resource Manager. This best works with " + "parallel young generation collector (UseParNewGC) and concurrent " + "low pause collector (UseConcMarkSweepGC) with appropriate " + "CMSInitiatingOccupancyFraction like 50%. This overrides the " + "critical-heap-percentage set in the <resource-manager> element " + "of the \"cache-xml-file\"");
  public static final StringId CacheServerLauncher_EVICTION_HEAP_PERCENTAGE = new StringId(4919, "<eviction-heap-percentage>  Sets the eviction heap " + "threshold limit of the Resource Manager above which the eviction " + "should begin on Regions configured for eviction by heap LRU. " + "This overrides the eviction-heap-percentage set in the " + "<resource-manager> element of the \"cache-xml-file\"");
  public static final StringId AbstractDistributionConfig_LOCK_MEMORY = new StringId(4920, "Locks heap and off-heap memory pages into RAM, thereby preventing the operating system from swapping them out to disk.");
  public static final StringId CacheServerLauncher_LOCK_MEMORY = new StringId(4921, "-lock-memory Locks heap and off-heap memory pages into RAM, thereby preventing the operating system from swapping them out to disk.");

  public static final StringId CacheXmlParser_NULL_DiskStoreName = new StringId(5000, "Disk Store name is configured to use null name.");
  public static final StringId CacheXmlParser_A_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_REGIONATTRIBUTES_OR_DISKSTORE = new StringId(5001, "A  {0}  must be defined in the context of region-attributes or disk-store.");
  public static final StringId DiskStoreAttributesCreation_NUMBER_OF_DISKSIZES_IS_0_WHICH_IS_NOT_EQUAL_TO_NUMBER_OF_DISK_DIRS_WHICH_IS_1 = new StringId(5002, "Number of diskSizes is  {0}  which is not equal to number of disk Dirs which is  {1}");
  public static final StringId DiskStoreAttributesCreation_AUTOCOMPACT_OF_0_IS_NOT_THE_SAME_THIS_1_OTHER_2 = new StringId(5003, "AutoCompact of disk store {0} is not the same: this:  {1}  other:  {2}");
  public static final StringId DiskStoreAttributesCreation_COMPACTIONTHRESHOLD_OF_0_IS_NOT_THE_SAME_THIS_1_OTHER_2 = new StringId(5004, "CompactionThreshold of disk store {0} is not the same: this:  {1}  other:  {2}");
  public static final StringId DiskStoreAttributesCreation_ALLOWFORCECOMPACTION_OF_0_IS_NOT_THE_SAME_THIS_1_OTHER_2 = new StringId(5005, "AllowForceCompaction of disk store {0} is not the same: this:  {1}  other:  {2}");
  public static final StringId SystemAdmin_REMOVE_OPTION_HELP = new StringId(5006, "Causes the region specified by the -region=<regionName> to be removed from a disk store. Any records in the disk store for this region become garbage and will be deleted from the disk store files if compact-disk-store is called. Note that this option writes to the disk store files so use it with care.");
  public static final StringId DiskStoreAttributesCreation_MAXOPLOGSIZE_OF_0_IS_NOT_THE_SAME_THIS_1_OTHER_2 = new StringId(5007, "MaxOpLogSize of disk store {0} is not the same: this:  {1}  other:  {2}");
  public static final StringId DiskStoreAttributesCreation_TIMEINTERVAL_OF_0_IS_NOT_THE_SAME_THIS_1_OTHER_2 = new StringId(5008, "TimeInterval of disk store {0} is not the same: this:  {1}  other:  {2}");
  public static final StringId DiskStoreAttributesCreation_WRITEBUFFERSIZE_OF_0_IS_NOT_THE_SAME_THIS_1_OTHER_2 = new StringId(5009, "WriteBufferSize of disk store {0} is not the same: this:  {1}  other:  {2}");
  public static final StringId DiskStoreAttributesCreation_QUEUESIZE_OF_0_IS_NOT_THE_SAME_THIS_1_OTHER_2 = new StringId(5010, "QueueSize of disk store {0} is not the same: this:  {1}  other:  {2}");
  public static final StringId DiskStoreAttributesCreation_DISK_DIRS_OF_0_ARE_NOT_THE_SAME = new StringId(5011, "Disk Dirs of disk store {0} are not the same");
  public static final StringId DiskStoreAttributesCreation_DISK_DIR_SIZES_OF_0_ARE_NOT_THE_SAME = new StringId(5012, "Disk Dir Sizes of disk store {0} are not the same");
  public static final StringId DiskStoreAttributesCreation_0_WAS_NOT_AN_EXISTING_DIRECTORY_FOR_DISKSTORE_1 = new StringId(5013, "\"{0}\" was not an existing directory for disk store {1}.");
  public static final StringId DiskStoreAttributesCreation_DIR_SIZE_CANNOT_BE_NEGATIVE_0_FOR_DISKSTORE_1 = new StringId(5014, "Dir size cannot be negative :  {0} for disk store {1}");
  public static final StringId DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1 = new StringId(5015, "Deprecated API {0} cannot be used with DiskStore {1}");
  public static final StringId PartitionedRegion_REGION_WITH_PRID_0_FAILED_INITIALIZATION_ON_THIS_NODE = new StringId(5016, "Region with prId= {0}  failed initialization on this node");
  public static final StringId PersistenceAdvisorImpl_MEMBER_REVOKED = new StringId(5017, "The following persistent member has been revoked:\n{0}");
  public static final StringId CreatePersistentRegionProcessor_DONE_WAITING_FOR_BUCKET_MEMBERS = new StringId(5018, "Region {0} has successfully completed waiting for other members to recover the latest data.\nMy persistent member information:{1}");
  public static final StringId CreatePersistentRegionProcessor_WAITING_FOR_OFFLINE_BUCKET_MEMBERS = new StringId(5019, "Region {0} (and any colocated sub-regions) has potentially stale data.  Buckets {1} are waiting for another offline member to recover the latest data.\nMy persistent id is:{2}\nOffline members with potentially new data:\n{3}\nUse the \"gfsh show missing-disk-stores\" command to see all disk stores that are being waited on by other members.");
  public static final StringId CreatePersistentRegionProcessor_SPLIT_DISTRIBUTED_SYSTEM = new StringId(5020, "Region {0} remote member {1} with persistent data {2} was not part of the same distributed system as the local data from {3}");
  public static final StringId CreatePersistentRegionProcessor_INITIALIZING_FROM_OLD_DATA = new StringId(5021, "Region {0} refusing to initialize from member {1} with persistent data {2} which was offline when the local data from {3} was last online");
  public static final StringId CreatePersistentRegionProcessor_WAITING_FOR_LATEST_MEMBER = new StringId(5022, "Region {0} has potentially stale data. It is waiting for another member to recover the latest data.\nMy persistent id:\n{1}\nMembers with potentially new data:\n{2}\nUse the \"gfsh show missing-disk-stores\" command to see all disk stores that are being waited on by other members.");
  public static final StringId PersistenceAdvisorImpl_UNABLE_TO_PERSIST_MEMBERSHIP_CHANGE = new StringId(5023, "Unable to persist membership change");
  public static final StringId DistributedRegion_ERROR_CLEANING_UP_FAILED_INITIALIZATION = new StringId(5024, "Error cleaning up after failed region initialization of region {0}");
  public static final StringId PartitionedRegionDataStore_DATA_OFFLINE_MESSAGE = new StringId(5025, "Region {0} bucket {1} has persistent data that is no longer online stored at these locations: {2}");
  public static final StringId PersistenceAdvisorImpl_FINISHING_INCOMPLETE_DESTROY = new StringId(5026, "Region {0} crashed during a region destroy. Finishing the destroy.");
  public static final StringId CacheCreation_DISKSTORE_NOTFOUND_0 = new StringId(5027, "Disk store {0} not found");
  public static final StringId FetchEntriesMessage_ERROR_DESERIALIZING_VALUES = new StringId(5028, "Error deserializing values");
  public static final StringId DistributedRegion_INITIALIZED_FROM_DISK = new StringId(5030, "Region {0} recovered from the local disk. Old persistent ID: {1}, new persistent ID {2}");
  public static final StringId BackupManager_README = new StringId(5031, "This directory contains a backup of the persistent data for a single gemfire VM. The layout is:\n\ndiskstores\n\tA backup of the persistent disk stores in the VM\nuser\n\tAny files specified by the backup element in the cache.xml file.\nconfig\n\tThe cache.xml and gemfire.properties for the backed up member.\nrestore.[sh|bat]\n\tA script to restore the backup.\n\nPlease note that the config is not restored, only the diskstores and user files.");
  public static final StringId PartitionedRegion_MULTIPLE_TARGET_NODE_FOUND_FOR = new StringId(5032, "Multiple target nodes found for single hop operation");
  public static final StringId CqQueryImpl_Null_CQ_Result_Key_Cache_0 = new StringId(5033, "The CQ Result key cache is Null. This should not happen as the call to isPartOfCqResult() is based on the condition cqResultsCacheInitialized.");
  public static final StringId TXState_DELTA_WITHOUT_CLONING_CANNOT_BE_USED_IN_TX = new StringId(5034, "Delta without cloning cannot be used in transaction");
  public static final StringId SearchLoadAndWriteProcessor_UNEXPECTED_EXCEPTION = new StringId(5035, "Unexpected exception creating net search reply");
  public static final StringId FunctionService_FUNCTION_ATTRIBUTE_MISMATCH = new StringId(5036, "For Functions with isHA true, hasResult must also be true.");
  public static final StringId FunctionService_FUNCTION_ATTRIBUTE_MISMATCH_CLIENT_SERVER = new StringId(5037, "Function attributes at client and server don''t match");
  public static final StringId Region_PutAll_Applied_PartialKeys_0_1 = new StringId(5038, "Region {0} putAll: {1}");
  public static final StringId Region_PutAll_Applied_PartialKeys_At_Server_0 = new StringId(5039, "Region {0} putAll at server applied partial keys due to exception.");
  public static final StringId BucketPersistenceAdvisor_WAITING_FOR_LATEST_MEMBER = new StringId(5040, "Region {0}, bucket {1} has potentially stale data.  It is waiting for another member to recover the latest data.\nMy persistent id:\n{2}\nMembers with potentially new data:\n{3}\nUse the \"gfsh show missing-disk-stores\" command to see all disk stores that are being waited on by other members.");

  public static final StringId AgentConfigImpl_TCP_PORT = new StringId(5041, "TCP/IP port number to use in the agent''s distributed system");

  public static final StringId PartitionRegionConfigValidator_CACHE_LOADER_IS_NULL_IN_PARTITIONED_REGION_0_ON_OTHER_DATASTORE = new StringId(5042, "Incompatible CacheLoader. CacheLoader is null in partitionedRegion {0} on another datastore.");
  public static final StringId PartitionRegionConfigValidator_CACHE_LOADER_IS_NOTNULL_IN_PARTITIONED_REGION_0_ON_OTHER_DATASTORE = new StringId(5043, "Incompatible CacheLoader. CacheLoader is not null in partitionedRegion {0} on another datastore.");
  public static final StringId PartitionRegionConfigValidator_CACHE_WRITER_IS_NULL_IN_PARTITIONED_REGION_0_ON_OTHER_DATASTORE = new StringId(5044, "Incompatible CacheWriter. CacheWriter is null in partitionedRegion {0} on another datastore.");
  public static final StringId PartitionRegionConfigValidator_CACHE_WRITER_IS_NOTNULL_IN_PARTITIONED_REGION_0_ON_OTHER_DATASTORE = new StringId(5045, "Incompatible CacheWriter. CacheWriter is not null in partitionedRegion {0} on another datastore.");

  public static final StringId PutAllOp_Retry_OtherException_0 = new StringId(5049, "PutAllOp : Retry failed with exception. Send back the saved succeeded keys: {0}");
  public static final StringId CacheServerLauncher_SERVER_PORT = new StringId(5050, "<server-port>  Port the server is to listen on for client connections. This overrides the port set in the <cache-server> element of the \"cache-xml-file\"");
  public static final StringId CacheServerLauncher_SERVER_BIND_ADDRESS = new StringId(5051, "<server-bind-address>  Address the server is to listen on for client connections. This overrides the bind-address set in the <cache-server> element of the \"cache-xml-file\"");
  public static final StringId CacheServerLauncher_SERVER_PORT_MORE_THAN_ONE_CACHE_SERVER = new StringId(5052, "When using \"-server-port\" or \"-server-bind-address\" arguments, the \"cache-xml-file\" can not have more than one cache-server defined.");
  public static final StringId MemberInfoWithStatsMBean_EXCEPTION_FOR_OPERATION_0 = new StringId(5053, "Exception occurred for operation: {0}");
  public static final StringId MemberInfoWithStatsMBean_EXCEPTION_FOR_OPERATION_0_FOR_MEMBER_1 = new StringId(5054, "Exception occurred for operation: {0} for member: {1}");
  public static final StringId MemberInfoWithStatsMBean_EXCEPTION_WHILE_INTIALIZING = new StringId(5055, "Exception occurred while intializing.");
  public static final StringId MemberInfoWithStatsMBean_EXCEPTION_WHILE_INTIALIZING_0_CONTINUING = new StringId(5056, "Exception occurred while intializing : {0}. Contiuning with next  ...");
  public static final StringId MemberInfoWithStatsMBean_EXCEPTION_WHILE_REGISTERING_NOTIFICATION_LISTENER_FOR_0 = new StringId(5057, "Exception while registering notification listener for: {0}");
  public static final StringId MemberInfoWithStatsMBean_EXCEPTION_WHILE_UNREGISTERING_NOTIFICATION_LISTENER_FOR_0 = new StringId(5058, "Exception while unregistering notification listener for: {0}");
  public static final StringId AgentImpl_FAILED_TO_INITIALIZE_MEMBERINFOWITHSTATSMBEAN = new StringId(5059, "Failed to initialize MemberInfoWithStatsMBean.");


  public static final StringId PoolManagerImpl_ONLY_DURABLE_CLIENTS_SHOULD_CALL_READYFOREVENTS = new StringId(5060, "Only durable clients should call readyForEvents()");
  public static final StringId LocalRegion_DURABLE_FLAG_ONLY_APPLICABLE_FOR_DURABLE_CLIENTS = new StringId(5061, "Durable flag only applicable for durable clients.");
  public static final StringId DistributedRegion_NEW_PERSISTENT_REGION_CREATED = new StringId(5062, "Region {0} was created on this member with the persistent id {1}.");
  public static final StringId InitialImageOperation_REGION_0_INITIALIZED_PERSISTENT_REGION_WITH_ID_1_FROM_2 = new StringId(5063, "Region {0} initialized persistent id: {1} with data from {2}.");
  public static final StringId MemberInfoWithStatsMBean_EXCEPTION_WHILE_INITIALIZING_STATISICS_FOR_0 = new StringId(5064, "Exception while initializing statistics for: {0}");
  public static final StringId MemberInfoWithStatsMBean_REINITIALIZING_STATS_FOR_0 = new StringId(5065, "Re-initializing statistics for: {0}");

  public static final StringId PartitionAttributesImpl_FIXED_PARTITION_NAME_CANNOT_BE_NULL = new StringId(5066, "Fixed partition name cannot be null");
  public static final StringId PartitionAttributesImpl_PARTITION_NAME_0_CAN_BE_ADDED_ONLY_ONCE_IN_FIXED_PARTITION_ATTRIBUTES = new StringId(5067, "Partition name \"{0}\" can be added only once in FixedPartitionAttributes");
  public static final StringId PartitionedRegionConfigValidator_FOR_REGION_0_SAME_PARTITION_NAME_1_CANNOT_BE_DEFINED_AS_PRIMARY_ON_MORE_THAN_ONE_NODE = new StringId(5068, "For region \"{0}\", same partition name \"{1}\" can not be defined as primary on more than one node.");
  public static final StringId PartitionAttributesImpl_FIXED_PARTITION_ATTRBUTES_0_CANNOT_BE_DEFINED_FOR_ACCESSOR = new StringId(5069, "FixedPartitionAttributes \"{0}\" can not be defined for accessor");
  public static final StringId PartitionedRegionConfigValidator_FOR_REGION_0_NUMBER_OF_SECONDARY_PARTITIONS_1_OF_A_PARTITION_2_SHOULD_NEVER_EXCEED_NUMBER_OF_REDUNDANT_COPIES_3 = new StringId(5070, "For region \"{0}\", number of secondary partitions {1} of a partition \"{2}\" should never exceed number of redundant copies {3}.");
  public static final StringId PartitionedRegionConfigValidator_FOR_REGION_0_SUM_OF_NUM_BUCKETS_1_FOR_DIFFERENT_PRIMARY_PARTITIONS_SHOULD_NOT_BE_GREATER_THAN_TOTAL_NUM_BUCKETS_2 = new StringId(5071, "For region \"{0}\",sum of num-buckets {1} for different primary partitions should not be greater than total-num-buckets {2}.");
  public static final StringId FOR_FIXED_PARTITION_REGION_0_PARTITION_1_IS_NOT_YET_INITIALIZED_ON_DATASTORE = new StringId(5072, "For FixedPartitionedRegion \"{0}\", Partition \"{1}\" is not yet initialized on datastore");
  public static final StringId AbstractDistributionConfig_MEMCACHED_PROTOCOL_MUST_BE_ASCII_OR_BINARY = new StringId(5073, "memcached-protocol must be \"ASCII\" or \"BINARY\" ");

  public static final StringId Disk_Store_Exception_During_Cache_Close = new StringId(5074, "Cache close caught an exception during disk store close");
  public static final StringId PoolManagerImpl_ONLY_DURABLE_CLIENTS_SHOULD_CALL_GETPENDINGEVENTCOUNT = new StringId(5075, "Only durable clients should call getPendingEventCount()");
  public static final StringId PartitionAttributesImpl_IF_COLOCATED_WITH_IS_SPECFIED_THEN_FIXED_PARTITION_ATTRIBUTES_CAN_NOT_BE_SPECIFIED = new StringId(5077, "FixedPartitionAttributes \"{0}\" can not be specified in PartitionAttributesFactory if colocated-with is specified. ");
  public static final StringId PartitionedRegionHelper_FOR_REGION_0_FOR_PARTITION_1_PARTITIION_NUM_BUCKETS_ARE_SET_TO_0_BUCKETS_CANNOT_BE_CREATED_ON_THIS_MEMBER = new StringId(5078, "For region \"{0}\", For partition \"{1}\" partition-num-buckets is set to 0. Buckets cann not be created on this partition.");
  public static final StringId PartitionedRegionHelper_FOR_REGION_0_PARTITION_NAME_1_IS_NOT_AVAILABLE_ON_ANY_DATASTORE = new StringId(5079, "For region \"{0}\", partition name \"{1}\" is not available on any datastore.");
  public static final StringId PartitionedRegionHelper_FOR_REGION_0_PARTITIONRESOLVER_1_RETURNED_PARTITION_NAME_NULL = new StringId(5080, "For region \"{0}\", partition resolver {1} returned partition name null");
  public static final StringId PartitionedRegionConfigValidator_FOR_REGION_0_FOR_PARTITION_1_NUM_BUCKETS_ARE_NOT_SAME_ACROSS_NODES = new StringId(5081, "For region \"{0}\",for partition \"{1}\", num-buckets are not same ({2}, {3})across nodes.");
  public static final StringId PartitionedRegionHelper_FOR_FIXED_PARTITIONED_REGION_0_FIXED_PARTITION_RESOLVER_IS_NOT_AVAILABLE = new StringId(5082, "For FixedPartitionedRegion \"{0}\", FixedPartitionResolver is not available (neither through the partition attribute partition-resolver nor key/callbackArg implementing FixedPartitionResolver)");
  public static final StringId PartitionedRegionHelper_FOR_FIXED_PARTITIONED_REGION_0_RESOLVER_DEFINED_1_IS_NOT_AN_INSTANCE_OF_FIXEDPARTITIONRESOLVER = new StringId(5083, "For FixedPartitionedRegion \"{0}\", Resolver defined {1} is not an instance of FixedPartitionResolver");
  public static final StringId PartitionedRegionHelper_FOR_FIXED_PARTITIONED_REGION_0_FIXED_PARTITION_IS_NOT_AVAILABLE_FOR_BUCKET_1_ON_ANY_DATASTORE = new StringId(5084, "For FixedPartitionedRegion \"{0}\", Fixed partition is not defined for bucket id {1} on any datastore");
  public static final StringId PartitionedRegionConfigValidator_FIXED_PARTITION_REGION_ONE_DATASTORE_IS_WITHOUTFPA = new StringId(5085, "Region \"{0}\" uses fixed partitioning but at least one datastore node (localMaxMemory > 0) has no fixed partitions. Please make sure that each datastore creating this region is configured with at least one fixed partition.");

  public static final StringId PartitionRegionHelper_ARGUMENT_REGION_IS_NULL = new StringId(5088, "Argument ''Region'' is null");
  public static final StringId PartitionedRegionHelper_FOR_FIXED_PARTITIONED_REGION_0_FIXED_PARTITION_1_IS_NOT_AVAILABLE_ON_ANY_DATASTORE = new StringId(5089, "For FixedPartitionedRegion \"{0}\", partition \"{1}\" is not available on any datastore.");
  public static final StringId CacheServerLauncher_DISABLE_DEFAULT_SERVER = new StringId(5090, "-disable-default-server  Do not add a default <cache-server>");
  public static final StringId Oplog_REMOVING_INCOMPLETE_KRF = new StringId(5091, "Removing incomplete krf {0} for oplog {1}, disk store \"{2}\"");
  public static final StringId PlaceHolderDiskRegion_A_DISKACCESSEXCEPTION_HAS_OCCURED_WHILE_RECOVERING_FROM_DISK = new StringId(5092, "A DiskAccessException has occurred while recovering values asynchronously from disk for region {0}.");
  public static final StringId PartitionedRegion_FOR_REGION_0_TotalBucketNum_1_SHOULD_NOT_BE_CHANGED_Previous_Configured_2 = new StringId(5093, "For partition region \"{0}\",total-num-buckets {1} should not be changed. Previous configured number is {2}.");

  public static final StringId PR_CONTAINSVALUE_WARNING = new StringId(5095, "PR containsValue warning. Got an exception while executing function");
  public static final StringId MultiUserSecurityEnabled_USE_POOL_API = new StringId(5096, "Use Pool APIs for doing operations when multiuser-secure-mode-enabled is set to true.");

  public static final StringId GetFunctionAttribute_THE_INPUT_0_FOR_GET_FUNCTION_ATTRIBUTE_REQUEST_IS_NULL = new StringId(5099, "The input {0} for GetFunctionAttributes request is null");
  public static final StringId GetFunctionAttribute_THE_FUNCTION_IS_NOT_REGISTERED_FOR_FUNCTION_ID_0 = new StringId(5100, "The function is not registered for function id {0}");
  public static final StringId CacheXmlPropertyResolver_UNSEROLVAVLE_STRING_FORMAT_ERROR__0 = new StringId(5101, "Format of the string \"{0}\" used for perameterization is unresolvable");
  public static final StringId CacheXmlPropertyResolverHelper_SOME_UNRESOLVED_STRING_REPLACED_CIRCULAR_ERROR__0 = new StringId(5102, "Some still unresolved string \"{0}\"was replaced by resolver, leading to circular references.");
  public static final StringId AgentLauncher_UNABLE_TO_DELETE_FILE_0 = new StringId(5103, "Unable to delete file {0}.");
  public static final StringId AgentLauncher_0_IS_NOT_RUNNING_IN_SPECIFIED_WORKING_DIRECTORY_1 = new StringId(5104, "{0} is not running in the specified working directory: ({1}).");
  public static final StringId AgentLauncher_SEE_LOG_FILE_FOR_DETAILS = new StringId(5105, "See log file for details.");
  public static final StringId AgentLauncher_SHUTDOWN_PENDING_AFTER_FAILED_STARTUP = new StringId(5106, "shutdown pending after failed startup");
  public static final StringId MBeanUtil_MBEAN_SERVER_NOT_INITIALIZED_YET = new StringId(5107, "MBean Server is not initialized yet.");
  public static final StringId MBeanUtil_FAILED_TO_FIND_0 = new StringId(5108, "Failed to find {0}");
  public static final StringId MBeanUtil_FAILED_TO_LOAD_0 = new StringId(5109, "Failed to load metadata from {0}");
  public static final StringId MBeanUtil_COULDNT_FIND_MBEAN_REGISTERED_WITH_OBJECTNAME_0 = new StringId(5110, "Could not find a MBean registered with ObjectName: {0}.");
  public static final StringId MBeanUtil_COULD_NOT_FIND_REGISTERED_REFRESHTIMER_INSTANCE = new StringId(5111, "Could not find registered RefreshTimer instance.");
  public static final StringId MBeanUtil_FAILED_TO_CREATE_REFRESH_TIMER = new StringId(5112, "Failed to create/register/start refresh timer.");
  public static final StringId MBeanUtil_FAILED_TO_REGISTER_SERVERNOTIFICATIONLISTENER = new StringId(5113, "Failed to register ServerNotificationListener.");
  public static final StringId AbstractDistributionConfig_USERDEFINED_PREFIX_NAME = new StringId(5114, "Prefix for \"user defined\" properties which are used for replacements in Cache.xml. Neither key nor value can be NULL. Legal tags can be [custom-any-string] and Legal values can be any string data.");

  public static final StringId DataSerializer_COULD_NOT_CREATE_AN_INSTANCE_OF_A_CLASS_0 = new StringId(5116, "Could not create an instance of a class {0}");
  public static final StringId StartupMessage_REJECTED_NEW_SYSTEM_NODE_0_BECAUSE_DISTRIBUTED_SYSTEM_ID_1_DOES_NOT_MATCH_THE_DISTRIBUTED_SYSTEM_2_IT_IS_ATTEMPTING_TO_JOIN = new StringId(5117, "Rejected new system node {0} because distributed-system-id={1} does not match the distributed system {2} it is attempting to join.");
  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_PDX_SERIALIZER = new StringId(5118, "A  {0}  is not an instance of a PdxSerializer.");

  public static final StringId MBeanUtil_FAILED_WHILE_UNREGISTERING_MBEAN_WITH_OBJECTNAME_0 = new StringId(5120, "Failed while unregistering MBean with ObjectName : {0}");
  public static final StringId MBeanUtil_WHILE_UNREGISTERING_COULDNT_FIND_MBEAN_WITH_OBJECTNAME_0 = new StringId(5121, "While unregistering, could not find MBean with ObjectName : {0}");
  public static final StringId MBeanUtil_COULD_NOT_UNREGISTER_MBEAN_WITH_OBJECTNAME_0 = new StringId(5122, "Could not un-register MBean with ObjectName : {0}");
  public static final StringId AcceptorImpl_REJECTED_CONNECTION_FROM_0_BECAUSE_REQUEST_REJECTED_BY_POOL = new StringId(5123, "Rejected connection from {0} because incoming request was rejected by pool possibly due to thread exhaustion");

  public static final StringId EXECUTE_FUNCTION_NO_HAS_RESULT_RECEIVED_EXCEPTION = new StringId(5125, "Function execution without result encountered an Exception on server.");

  public static final StringId CreateRegionProcessor_CANNOT_CREATE_REGION_0_WITH_OFF_HEAP_EQUALS_1_BECAUSE_ANOTHER_CACHE_2_HAS_SAME_THE_REGION_WITH_OFF_HEAP_EQUALS_3 = new StringId(5128, "Cannot create region {0} with off-heap={1} because another cache ({2}) has the same region with off-heap={3}.");

  public static final StringId ConnectionTable_OUT_OF_FILE_DESCRIPTORS_USING_SHARED_CONNECTION = new StringId(5129, "This process is out of file descriptors.\nThis will hamper communications and slow down the system.\nAny conserve-sockets setting is now being ignored.\nPlease consider raising the descriptor limit.\nThis alert is only issued once per process.");
  public static final StringId DistributedRegion_INITIALIZING_REGION_0 = new StringId(5130, "Initializing region {0}");
  public static final StringId CacheXmlParser_CACHEXMLPARSERENDINDEXINDEX_CREATION_ATTRIBUTE_NOT_CORRECTLY_SPECIFIED = new StringId(5131, "CacheXmlParser::endIndex:Index creation attribute not correctly specified.");

  public static final StringId GatewayParallel_0_CREATED_1_GATEWAYS_2 = new StringId(5133, "{0}: Created {1} parallel gateways named {2}");
  public static final StringId CacheXmlParser_UNKNOWN_GATEWAY_ORDER_POLICY_0_1 = new StringId(5134, "An invalid order-policy value ({1}) was configured for gateway {0}");
  public static final StringId CacheXmlParser_INVALID_GATEWAY_ORDER_POLICY_CONCURRENCY_0 = new StringId(5135, "The \"order-policy\" attribute configured for gateway {0} is only valid when the \"concurrency-level\" attribute is also configured.");
  public static final StringId DefaultQuery_FUNCTIONCONTEXT_CANNOT_BE_NULL = new StringId(5136, "''Function Context'' cannot be null");
  public static final StringId BucketPersistenceAdvisor_ERROR_RECOVERYING_SECONDARY_BUCKET_0 = new StringId(5137, "Unable to recover secondary bucket from disk for region {0} bucket {1}");
  public static final StringId FunctionService_EXCEPTION_ON_LOCAL_NODE = new StringId(5138, "Exception occured on local node while executing Function:");
  public static final StringId AbstractIndex_WRONG_COMPARETO_IMPLEMENTATION_IN_INDEXED_OBJECT_0 = new StringId(5139, "Indexed object''s class {0} compareTo function is errorneous.");
  public static final StringId DefaultQuery_API_ONLY_FOR_PR = new StringId(5140, "This query API can only be used for Partition Region Queries.");
  public static final StringId MailManager_REQUIRED_MAILSERVER_CONFIGURATION_NOT_SPECIFIED = new StringId(5141, "Required mail server configuration is not specfied.");
  public static final StringId DiskStoreImpl_CreatedDiskStore_0_With_Id_1 = new StringId(5142, "Created disk store {0} with unique id {1}");
  public static final StringId DiskStoreImpl_RecoveredDiskStore_0_With_Id_1 = new StringId(5143, "Recovered disk store {0} with unique id {1}");
  public static final StringId PersistentMemberManager_Member_0_is_already_revoked = new StringId(5144, "The persistent member id {0} has been revoked in this distributed system. You cannot recover from disk files which have been revoked.");
  public static final StringId RevokeFailedException_Member_0_is_already_running_1 = new StringId(5145, "Member {0} is already running with persistent files matching {1}. You cannot revoke the disk store of a running member.");


  public static final StringId CacheXmlParser_A_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_GATEWAY_SENDER = new StringId(5148, "A  {0}  must be defined in the context of gateway-sender.");
  public static final StringId GemFireCache_A_GATEWAYSENDER_WITH_ID_0_IS_ALREADY_DEFINED_IN_THIS_CACHE = new StringId(5149, "A GatewaySender with id  {0}  is already defined in this cache.");
  public static final StringId AttributesFactory_GATEWAY_SENDER_ID_IS_NULL = new StringId(5150, "GatewaySender id is null.");
  public static final StringId ParallelGatewaySenderImpl_STARTED__0 = new StringId(5165, "Started  {0}");
  public static final StringId RVV_LOCKING_CONFUSED = new StringId(5166, "Request from {0} to block operations found that operations are already blocked by member {1}.");
  public static final StringId GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_REMOTE_DS_ID_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_2_REMOTE_DS_ID = new StringId(5167, "Cannot create Gateway Sender \"{0}\" with remote ds id \"{1}\" because another cache has the same Gateway Sender defined with remote ds id \"{2}\".");
  public static final StringId GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_AS_PARALLEL_GATEWAY_SENDER_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_AS_SERIAL_GATEWAY_SENDER = new StringId(5168, "Cannot create Gateway Sender \"{0}\" as parallel gateway sender because another cache has the same sender as serial gateway sender");
  public static final StringId GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_AS_SERIAL_GATEWAY_SENDER_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_AS_PARALLEL_GATEWAY_SENDER = new StringId(5169, "Cannot create Gateway Sender \"{0}\" as serial gateway sender because another cache has the same sender as parallel gateway sender");
  public static final StringId GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_IS_BACTH_CONFLATION_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_IS_BATCH_CONFLATION_2 = new StringId(5170, "Cannot create Gateway Sender \"{0}\" with isBatchConflationEnabled \"{1}\" because another cache has the same Gateway Sender defined with isBatchConflationEnabled \"{2}\"");
  public static final StringId GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_IS_PERSISTENT_ENABLED_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_IS_PERSISTENT_ENABLED_2 = new StringId(5171, "Cannot create Gateway Sender \"{0}\" with isPersistentEnabled \"{1}\" because another cache has the same Gateway Sender defined with isPersistentEnabled \"{2}\"");
  public static final StringId GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_ALERT_THRESHOLD_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_ALERT_THRESHOLD_2 = new StringId(5172, "Cannot create Gateway Sender \"{0}\" with alertThreshold \"{1}\" because another cache has the same Gateway Sender defined with alertThreshold \"{2}\"");
  public static final StringId GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_MANUAL_START_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_MANUAL_START_2 = new StringId(5173, "Cannot create Gateway Sender \"{0}\" with manual start \"{1}\" because another cache has the same Gateway Sender defined with manual start \"{2}\"");
  public static final StringId GatewaySenderAdvisor_GATEWAY_EVENT_FILTERS_MISMATCH = new StringId(5174, "Cannot create Gateway Sender \"{0}\" with GatewayEventFilters \"{1}\" because another cache has the same Gateway Sender defined with GatewayEventFilters \"{2}\"");
  public static final StringId GatewaySenderAdvisor_GATEWAY_TRANSPORT_FILTERS_MISMATCH = new StringId(5175, "Cannot create Gateway Sender \"{0}\" with GatewayTransportFilters \"{1}\" because another cache has the same Gateway Sender defined with GatewayTransportFilters \"{2}\"");
  public static final StringId GatewaySenderAdvisor_GATEWAY_SENDER_LISTENER_MISMATCH = new StringId(5176, "Cannot create Gateway Sender \"{0}\" with AsyncEventListeners \"{1}\" because another cache has the same Gateway Sender defined with AsyncEventListener \"{2}\"");
  public static final StringId GatewaySenderAdvisor_GATEWAY_SENDER_IS_DISK_SYNCHRONOUS_MISMATCH = new StringId(5177, "Cannot create Gateway Sender \"{0}\" with isDiskSynchronous \"{1}\" because another cache has the same Gateway Sender defined with isDiskSynchronous \"{2}\"");
  public static final StringId PartitionRegion_NON_COLOCATED_REGIONS_1_2_CANNOT_HAVE_SAME_PARALLEL_GATEWAY_SENDER_ID_2 = new StringId(5178, "Non colocated regions \"{0}\", \"{1}\" cannot have the same parallel {2} id \"{3}\" configured.");

  public static final StringId GatewayReceiver_STARTED_ON_PORT = new StringId(5182, "The GatewayReceiver started on port : {0}");
  public static final StringId GatewayReceiver_Address_Already_In_Use = new StringId(5183, "The GatewayReceiver port \"{0}\" is already in use");
  public static final StringId GatewaySenderImpl_GATEWAY_0_CANNOT_BE_CREATED_WITH_REMOTE_SITE_ID_EQUAL_TO_THIS_SITE_ID = new StringId(5184, "GatewaySender {0} cannot be created with remote DS Id equal to this DS Id. ");

  public static final StringId AbstractDistributionConfig_REMOTE_DISTRIBUTED_SYSTEMS_NAME_0 = new StringId(5185, "A possibly empty list of locators used to find other distributed systems. Each element of the list must be a host name followed by bracketed, \"[]\", port number. Host names may be followed by a colon and a bind address used by the locator on that host.  Multiple elements must be comma seperated. Defaults to \"{0}\".");
  public static final StringId SerialGatewaySenderImpl_0__BECOMING_PRIMARY_GATEWAYSENDER = new StringId(5186, "{0} : Becoming primary gateway sender");
  public static final StringId AbstractGatewaySender_REMOTE_LOCATOR_FOR_REMOTE_SITE_0_IS_NOT_AVAILABLE_IN_LOCAL_LOCATOR_1 = new StringId(5187, "Remote locator host port information for remote site \"{0}\" is not available in local locator \"{1}\".");

  public static final StringId CreateRegionProcessor_CANNOT_CREATE_REGION_0_WITH_1_GATEWAY_SENDER_IDS_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_REGION_WITH_2_GATEWAY_SENDER_IDS = new StringId(5191, "Cannot create Region {0} with {1} gateway sender ids because another cache has the same region defined with {2} gateway sender ids");
  public static final StringId GatewaySender_SENDER_0_IS_ALREADY_RUNNING = new StringId(5192, "Gateway Sender {0} is already running");
  public static final StringId LOCATOR_DISCOVERY_TASK_COULD_NOT_EXCHANGE_LOCATOR_INFORMATION_0_WITH_1_AFTER_2 = new StringId(5193, "Locator discovery task could not exchange locator information {0} with {1} after {2} retry attempts.");
  public static final StringId LOCATOR_DISCOVERY_TASK_COULD_NOT_EXCHANGE_LOCATOR_INFORMATION_0_WITH_1_AFTER_2_RETRYING_IN_3_MS = new StringId(5194, "Locator discovery task could not exchange locator information {0} with {1} after {2} retry attempts. Retrying in {3} ms.");
  public static final StringId LOCATOR_DISCOVERY_TASK_ENCOUNTERED_UNEXPECTED_EXCEPTION = new StringId(5195, "Locator discovery task encountred unexpected exception");
  public static final StringId LOCATOR_MEMBERSHIP_LISTENER_COULD_NOT_EXCHANGE_LOCATOR_INFORMATION_0_1_WIHT_2_3 = new StringId(5196, "Locator Membership listener could not exchange locator information {0}:{1} with {2}:{3}");
  public static final StringId SerialGatewaySenderImpl_GATEWAY_0_CANNOT_DEFINE_A_REMOTE_SITE_BECAUSE_AT_LEAST_ONE_LISTENER_IS_ALREADY_ADDED = new StringId(5197, "SerialGatewaySener  {0}  cannot define a remote site because at least AsyncEventListener is already added. Both listeners and remote site cannot be defined for the same gateway sender.");
  public static final StringId SerialGatewayEventCallbackDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH = new StringId(5198, "Stopping the processor because the following exception occurred while processing a batch:");
  public static final StringId SerialGatewayEventCallbackDispatcher__0___EXCEPTION_DURING_PROCESSING_BATCH__1_ = new StringId(5199, "{0}: Exception during processing batch {1}");
  public static final StringId GatewaySenderImpl_GATEWAY_0_CANNOT_BE_CREATED_WITH_REMOTE_SITE_ID_LESS_THAN_ZERO = new StringId(5200, "GatewaySender {0} cannot be created with remote DS Id less than 0. ");
  public static final StringId CacheXmlParser_A_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_GATEWAYSENDER_OR_GATEWAYRECEIVER = new StringId(5201, "A  {0}  must be defined in the context of gateway-sender or gateway-receiver.");
  public static final StringId Sender_COULD_NOT_START_GATEWAYSENDER_0_BECAUSE_OF_EXCEPTION_1 = new StringId(5202, "Could not start a gateway sender {0} because of exception {1}");
  public static final StringId LOCATOR_DISCOVERY_TASK_EXCHANGED_LOCATOR_INFORMATION_0_WITH_1 = new StringId(5203, "Locator discovery task exchanged locator information {0} with {1}: {2}.");

  public static final StringId DistributionManager_RECEIVED_NO_STARTUP_RESPONSES_BUT_OTHER_MEMBERS_EXIST_MULTICAST_IS_NOT_RESPONSIVE = new StringId(5205, "Did not receive a startup response but other members exist.  Multicast does not seem to be working.");

  public static final StringId PartitionedRegion_FOR_REGION_0_ColocatedWith_1_SHOULD_NOT_BE_CHANGED_Previous_Configured_2 = new StringId(5151, "For partition region \"{0}\", Cannot change colocated-with to \"{1}\" because there is persistent data with different colocation. Previous configured value is \"{2}\".");
  public static final StringId Oplog_FAILED_RECORDING_RVV_BECAUSE_OF_0 = new StringId(5152, "Failed in persisting the garbage collection of entries because of: {0}");
  public static final StringId CacheServerLauncher_LAUNCH_IN_PROGRESS_0 = new StringId(5153, "The server is still starting. {0} seconds have elapsed since the last log message: \n {1}");

  public static final StringId Snapshot_EXPORT_BEGIN_0 = new StringId(5154, "Exporting region {0}");
  public static final StringId Snapshot_EXPORT_END_0_1_2_3 = new StringId(5155, "Snapshot export of {0} entries ({1} bytes) in region {2} to file {3} is complete");
  public static final StringId Snapshot_IMPORT_BEGIN_0 = new StringId(5156, "Importing region {0}");
  public static final StringId Snapshot_IMPORT_END_0_1_2_3 = new StringId(5157, "Snapshot import of {0} entries ({1} bytes) in region {2} from file {3} is complete");
  public static final StringId Snapshot_UNABLE_TO_CREATE_DIR_0 = new StringId(5158, "Unable to create snapshot directory {0}");
  public static final StringId Snapshot_NO_SNAPSHOT_FILES_FOUND_0 = new StringId(5159, "No snapshot files found in {0}");
  public static final StringId Snapshot_UNSUPPORTED_SNAPSHOT_VERSION_0 = new StringId(5160, "Unsupported snapshot version: {0}");
  public static final StringId Snapshot_COULD_NOT_FIND_REGION_0_1 = new StringId(5161, "Could not find region {0}. Ensure that the region is created prior to importing the snapshot file {1}.");
  public static final StringId Snapshot_UNRECOGNIZED_FILE_TYPE_0 = new StringId(5162, "Unrecognized snapshot file type: {0}");
  public static final StringId Snapshot_UNRECOGNIZED_FILE_VERSION_0 = new StringId(5163, "Unrecognized snapshot file version: {0}");
  public static final StringId Snapshot_PDX_CONFLICT_0_1 = new StringId(5164, "Detected conflicting PDX types during import:\n{0}\n{1}\nSnapshot data containing PDX types must be imported into an empty cache with no pre-existing type definitions. Allow the import to complete prior to inserting additional data into the cache.");

  public static final StringId CacheServerLauncher_CREATE_STATUS_EXCEPTION_0 = new StringId(5207, "The cacheserver status file could not be recreated due to the following exception: {0}");

  public static final StringId GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_ORDER_POLICY_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_ORDER_POLICY_2 = new StringId(5209, "Cannot create Gateway Sender \"{0}\" with orderPolicy \"{1}\" because another cache has the same Gateway Sender defined with orderPolicy \"{2}\"");

  public static final StringId SerialGatewaySender_UNKNOWN_GATEWAY_ORDER_POLICY_0_1 = new StringId(5211, "An invalid order-policy value ({1}) was configured for gateway sender {0}");
  public static final StringId CommandServiceManager_COULD_NOT_FIND__0__LIB_NEEDED_FOR_CLI_GFSH = new StringId(5212, "Could not find {0} library which is needed for CLI/gfsh in classpath. Internal support for CLI & gfsh is not enabled. Note: For convenience, absolute path of \"gfsh-dependencies.jar\" from \"lib\" directory of GemFire product distribution can be included in CLASSPATH of an application.");

  public static final StringId CacheXmlParser_A_0_IS_NOT_AN_INSTANCE_OF_A_ASYNCEVENTLISTENER = new StringId(5214, "A  {0}  is not an instance of a AsyncEventListener");
  public static final StringId CacheXmlParser_A_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_ASYNCEVENTQUEUE = new StringId(5215, "A  {0}  must be defined in the context of async-event-queue.");
  public static final StringId AttributesFactory_ASYNC_EVENT_QUEUE_ID_0_IS_ALREADY_ADDED = new StringId(5216, "async-event-queue-id {0} is already added");
  public static final StringId AsyncEventQueue_ASYNC_EVENT_LISTENER_CANNOT_BE_NULL = new StringId(5217, "AsyncEventListener cannot be null");

  public static final StringId CreateRegionProcessor_CANNOT_CREATE_REGION_0_WITH_1_ASYNC_EVENT_IDS_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_REGION_WITH_2_ASYNC_EVENT_IDS = new StringId(5218, "Cannot create Region {0} with {1} async event ids because another cache has the same region defined with {2} async event ids");

  public static final StringId GatewaySenderImpl_NULL_CANNNOT_BE_ADDED_TO_GATEWAY_EVENT_FILTER_LIST = new StringId(5220, "null value can not be added to gateway-event-filters list");

  public static final StringId AbstractGatewaySender_LOCATOR_SHOULD_BE_CONFIGURED_BEFORE_STARTING_GATEWAY_SENDER = new StringId(5222, "Locators must be configured before starting gateway-sender.");
  public static final StringId AttributesFactory_GATEWAY_SENDER_ID_0_IS_ALREADY_ADDED = new StringId(5223, "gateway-sender-id {0} is already added");

  public static final StringId AbstractGatewaySender_SENDER_0_IS_NOT_ABLE_TO_CONNECT_TO_LOCAL_LOCATOR_1 = new StringId(5227, "GatewaySender {0} is not able to connect to local locator {1}");
  public static final StringId AbstractGatewaySender_SENDER_0_COULD_NOT_GET_REMOTE_LOCATOR_INFORMATION_FOR_SITE_1 = new StringId(5228, "GatewaySender \"{0}\" could not get remote locator information for remote site \"{1}\".");

  public static final StringId GatewaySender_PAUSED__0 = new StringId(5230, "Paused {0}");
  public static final StringId GatewaySender_RESUMED__0 = new StringId(5231, "Resumed {0}");

  public static final StringId PersistenceAdvisorImpl_PERSISTENT_VIEW = new StringId(5233, "The following persistent member has gone offline for region {0}:\n{1}\nRemaining participating members for the region include:\n{2}");

  public static final StringId DefaultQuery_A_QUERY_ON_A_PARTITIONED_REGION_0_MAY_NOT_REFERENCE_ANY_OTHER_NON_COLOCATED_PARTITIONED_REGION_1 = new StringId(5234, "A query on a Partitioned Region ( {0} ) may not reference any other region except Co-located Partitioned Region. PR region (1) is not collocated with other PR region in the query.");
  public static final StringId CreatePersistentRegionProcessor_WAITING_FOR_ONLINE_BUCKET_MEMBERS = new StringId(5235, "Region {0} (and any colocated sub-regions) has potentially stale data.  Buckets {1} are waiting for another online member to recover the latest data.\nMy persistent id is:{2}\nOnline members with potentially new data:\n{3}\nUse the \"gfsh show missing-disk-stores\" command to see all disk stores that are being waited on by other members.");

  // Localized Strings for the AbstractLauncher, Locator and Server Launcher classes
  public static final StringId Launcher_ATTACH_API_NOT_FOUND_ERROR_MESSAGE = new StringId(5236, "The Attach API classes could not be found on the classpath.  Please include JDK tools.jar on the classpath or add the JDK tools.jar to the jre/lib/ext directory.");

  public static final StringId Launcher_Builder_INVALID_PORT_ERROR_MESSAGE = new StringId(5238, "The port on which the {0} will listen must be between 1 and 65535 inclusive.");
  public static final StringId Launcher_Builder_MEMBER_NAME_ERROR_MESSAGE = new StringId(5239, "The {0} member name must be specified.");
  public static final StringId Launcher_Builder_MEMBER_NAME_VALIDATION_ERROR_MESSAGE = new StringId(5240, "The member name of the {0} must be provided as an argument to the launcher, or a path to gemfire.properties must be specified, which assumes the {0} member name will be set using the \"name\" property.");
  public static final StringId Launcher_Builder_PARSE_COMMAND_LINE_ARGUMENT_ERROR_MESSAGE = new StringId(5241, "An error occurred while parsing command-line arguments for the {0}: {1}");
  public static final StringId Launcher_Builder_PID_ERROR_MESSAGE = new StringId(5242, "A process ID (PID) must be a non-negative integer value.");
  public static final StringId Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE = new StringId(5243, "The hostname/IP address to which the {0} will be bound is unknown.");
  public static final StringId Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE = new StringId(5244, "The working directory for the {0} could not be found.");
  public static final StringId Launcher_Builder_WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE = new StringId(5245, "Specifying the --dir option is not valid when starting a {0} with the {0}Launcher.");
  public static final StringId Launcher_Command_START_IO_ERROR_MESSAGE = new StringId(5246, "An IO error occurred while starting a {0} in {1} on {2}: {3}");
  public static final StringId Launcher_Command_START_SERVICE_ALREADY_RUNNING_ERROR_MESSAGE = new StringId(5247, "A {0} is already running in {1} on {2}.");
  public static final StringId Launcher_Command_START_PID_FILE_ALREADY_EXISTS_ERROR_MESSAGE = new StringId(5248, "A PID file already exists and a {0} may be running in {1} on {2}.");
  public static final StringId Launcher_Command_START_PID_UNAVAILABLE_ERROR_MESSAGE = new StringId(5249, "The process ID could not be determined while starting {0} {1} in {2}: {3}");
  public static final StringId Launcher_ServiceStatus_STARTING_MESSAGE = new StringId(5250, "Starting {0} in {1} on {2} as {3} at {4}\nProcess ID: {5}\nGemFire Version: {6}\nJava Version: {7}\nLog File: {8}\nJVM Arguments: {9}\nClass-Path: {10}");
  public static final StringId Launcher_ServiceStatus_RUNNING_MESSAGE = new StringId(5251, "{0} in {1} on {2} as {3} is currently {4}.\nProcess ID: {5}\nUptime: {6}\nGemFire Version: {7}\nJava Version: {8}\nLog File: {9}\nJVM Arguments: {10}\nClass-Path: {11}");
  public static final StringId Launcher_ServiceStatus_STOPPED_MESSAGE = new StringId(5252, "{0} in {1} on {2} has been requested to stop.");
  public static final StringId Launcher_ServiceStatus_MESSAGE = new StringId(5253, "{0} in {1} on {2} is currently {3}.");
  public static final StringId Launcher_Status_NOT_RESPONDING = new StringId(5254, "not responding");
  public static final StringId Launcher_Status_ONLINE = new StringId(5255, "online");
  public static final StringId Launcher_Status_STARTING = new StringId(5256, "starting");
  public static final StringId Launcher_Status_STOPPED = new StringId(5257, "stopped");
  public static final StringId Launcher_Command_FAILED_TO_GET_SHARED_CONFIGURATION = new StringId(5258, "Unable to retrieve cluster configuration from the locator.");

  public static final StringId LocatorLauncher_Builder_INVALID_HOSTNAME_FOR_CLIENTS_ERROR_MESSAGE = new StringId(5260, "The hostname used by clients to connect to the Locator must have an argument if the --hostname-for-clients command-line option is specified!");
  public static final StringId LocatorLauncher_LOCATOR_LAUNCHER_HELP = new StringId(5261, "A GemFire launcher used to start, stop and determine a Locator''s status.");
  public static final StringId LocatorLauncher_START_LOCATOR_HELP = new StringId(5262, "Starts a Locator running in the current working directory listening on the default port ({0}) bound to all IP addresses available to the localhost.  The Locator must be given a member name in the GemFire cluster.  The default bind-address and port may be overridden using the corresponding command-line options.");
  public static final StringId LocatorLauncher_STATUS_LOCATOR_HELP = new StringId(5263, "Displays the status of a Locator given any combination of the bind-address[port], member name/ID, PID, or the directory in which the Locator is running.");
  public static final StringId LocatorLauncher_STOP_LOCATOR_HELP = new StringId(5264, "Stops a running Locator given a member name/ID, PID, or the directory in which the Locator is running.");
  public static final StringId LocatorLauncher_VERSION_LOCATOR_HELP = new StringId(5265, "Displays GemFire product version information.");
  public static final StringId LocatorLauncher_LOCATOR_BIND_ADDRESS_HELP = new StringId(5266, "Specifies the IP address on which to bind, or on which the Locator is bound, listening for client requests.  Defaults to all IP addresses available to the localhost.");
  public static final StringId LocatorLauncher_LOCATOR_DEBUG_HELP = new StringId(5267, "Displays verbose information during the invocation of the launcher.");
  public static final StringId LocatorLauncher_LOCATOR_DIR_HELP = new StringId(5268, "Specifies the working directory where the Locator is running.  Defaults to the current working directory.");
  public static final StringId LocatorLauncher_LOCATOR_FORCE_HELP = new StringId(5269, "Enables any existing Locator PID file to be overwritten on start.  The default is to throw an error if a PID file already exists and --force is not specified.");
  public static final StringId LocatorLauncher_LOCATOR_HOSTNAME_FOR_CLIENTS_HELP = new StringId(5270, "An option to specify the hostname or IP address to send to clients so they can connect to this Locator. The default is to use the IP address to which the Locator is bound.");
  public static final StringId LocatorLauncher_LOCATOR_MEMBER_HELP = new StringId(5271, "Identifies the Locator by member name or ID in the GemFire cluster.");
  public static final StringId LocatorLauncher_LOCATOR_PID_HELP = new StringId(5272, "Indicates the OS process ID of the running Locator.");
  public static final StringId LocatorLauncher_LOCATOR_PORT_HELP = new StringId(5273, "Specifies the port on which the Locator is listening for client requests. Defaults to {0}.");
  public static final StringId LocatorLauncher_LOCATOR_REDIRECT_OUTPUT_HELP = new StringId(5274, "An option to cause the Locator to redirect standard out and standard error to the GemFire log file.");

  public static final StringId ServerLauncher_SERVER_LAUNCHER_HELP = new StringId(5280, "A GemFire launcher used to start, stop and determine a Server''s status.\n");
  public static final StringId ServerLauncher_START_SERVER_HELP = new StringId(5281, "Starts a Server running in the current working directory listening on the default port ({0}) bound to all IP addresses available to the localhost.  The Server must be given a member name in the GemFire cluster.  The default server-bind-address and server-port may be overridden using the corresponding command-line options.");
  public static final StringId ServerLauncher_STATUS_SERVER_HELP = new StringId(5282, "Displays the status of a Server given any combination of the member name/ID, PID, or the directory in which the Server is running.");
  public static final StringId ServerLauncher_STOP_SERVER_HELP = new StringId(5283, "Stops a running Server given given a member name/ID, PID, or the directory in which the Server is running.");
  public static final StringId ServerLauncher_VERSION_SERVER_HELP = new StringId(5284, "Displays GemFire product version information.");
  public static final StringId ServerLauncher_SERVER_ASSIGN_BUCKETS_HELP = new StringId(5285, "Causes buckets to be assigned to the partitioned regions in the GemFire cache on Server start.");
  public static final StringId ServerLauncher_SERVER_BIND_ADDRESS_HELP = new StringId(5286, "Specifies the IP address on which to bind, or on which the Server is bound, listening for client requests.  Defaults to all IP addresses available to the localhost.");
  public static final StringId ServerLauncher_SERVER_DEBUG_HELP = new StringId(5287, "Displays verbose information during the invocation of the launcher.");
  public static final StringId ServerLauncher_SERVER_DIR_HELP = new StringId(5288, "Specifies the working directory where the Server is running.  Defaults to the current working directory.");
  public static final StringId ServerLauncher_SERVER_DISABLE_DEFAULT_SERVER_HELP = new StringId(5289, "Disables the addition of a default GemFire cache server.");
  public static final StringId ServerLauncher_SERVER_FORCE_HELP = new StringId(5290, "Enables any existing Server PID file to be overwritten on start.  The default is to throw an error if a PID file already exists and --force is not specified.");
  public static final StringId ServerLauncher_SERVER_MEMBER_HELP = new StringId(5291, "Identifies the Server by member name or ID in the GemFire cluster.");
  public static final StringId ServerLauncher_SERVER_PID_HELP = new StringId(5292, "Indicates the OS process ID of the running Server.");
  public static final StringId ServerLauncher_SERVER_PORT_HELP = new StringId(5293, "Specifies the port on which the Server is listening for client requests. Defaults to {0}.");
  public static final StringId ServerLauncher_SERVER_REBALANCE_HELP = new StringId(5294, "An option to cause the GemFire cache''s partitioned regions to be rebalanced on start.");
  public static final StringId ServerLauncher_SERVER_REDIRECT_OUTPUT_HELP = new StringId(5295, "An option to cause the Server to redirect standard out and standard error to the GemFire log file.");
  public static final StringId ServerLauncher_SERVER_HOSTNAME_FOR_CLIENT_HELP = new StringId(5296, "An option to specify the hostname or IP address to send to clients so they can connect to this Server. The default is to use the IP address to which the Server is bound.");

  public static final StringId PoolFactoryImpl_HOSTNAME_UNKNOWN = new StringId(5300, "Hostname is unknown: {0}. Creating pool with unknown host in case the host becomes known later.");

  public static final StringId GatewaySenderEventRemoteDispatcher_GATEWAY_SENDER_0_RECEIVED_ACK_FOR_BATCH_ID_1_WITH_EXCEPTION = new StringId(5302, "Gateway Sender {0} : Received ack for batch id {1} with exception:");

  public static final StringId Region_REGION_0_HAS_1_GATEWAY_SENDER_IDS_ANOTHER_CACHE_HAS_THE_SAME_REGION_WITH_2_GATEWAY_SENDER_IDS_FOR_REGION_ACROSS_ALL_MEMBERS_IN_DS_GATEWAY_SENDER_IDS_SHOULD_BE_SAME = new StringId(5303, "Region {0} has {1} gateway sender IDs. Another cache has same region with {2} gateway sender IDs. For region across all members, gateway sender ids should be same.");

  public static final StringId Region_REGION_0_HAS_1_ASYNC_EVENT_QUEUE_IDS_ANOTHER_CACHE_HAS_THE_SAME_REGION_WITH_2_ASYNC_EVENT_QUEUE_IDS_FOR_REGION_ACROSS_ALL_MEMBERS_IN_DS_ASYNC_EVENT_QUEUE_IDS_SHOULD_BE_SAME = new StringId(5304, "Region {0} has {1} AsyncEvent queue IDs. Another cache has same region with {2} AsyncEvent queue IDs. For region across all members, AsyncEvent queue IDs should be same.");
  public static final StringId GatewayEventFilter_EXCEPTION_OCCURED_WHILE_HANDLING_CALL_TO_0_AFTER_ACKNOWLEDGEMENT_FOR_EVENT_1 = new StringId(5305, "Exception occured while handling call to {0}.afterAcknowledgement for event {1}:");
  public static final StringId GatewayReceiverImpl_USING_LOCAL_HOST = new StringId(5399, "No bind-address or hostname-for-sender is specified, Using local host ");
  public static final StringId GatewayReceiverImpl_COULD_NOT_GET_HOST_NAME = new StringId(5400, "Could not get host name");
  public static final StringId CqService_ERROR_SENDING_CQ_CONNECTION_STATUS = new StringId(5401, "Error while sending connection status to cq listeners");
  public static final StringId AbstractGatewaySender_SENDER_0_GOT_REMOTE_LOCATOR_INFORMATION_FOR_SITE_1 = new StringId(5402, "GatewaySender \"{0}\" got remote locator information for remote site \"{1}\" after {2} failures in connecting to remote site.");

  public static final StringId CreatePersistentRegionProcessor_WAITING_FOR_ONLINE_LATEST_MEMBER = new StringId(5403, "Region {0} has potentially stale data. It is waiting for another online member to recover the latest data.\nMy persistent id:\n{1}\nMembers with potentially new data:\n{2}\nUse the \"gfsh show missing-disk-stores\" command to see all disk stores that are being waited on by other members.");
  public static final StringId AttributesFactory_CONCURRENCY_CHECKS_MUST_BE_ENABLED = new StringId(5404, "Concurrency checks cannot be disabled for regions that use persistence");

  public static final StringId CqService_UNABLE_TO_RETRIEVE_DURABLE_CQS_FOR_CLIENT_PROXY_ID = new StringId(5406, "Unable to retrieve durable CQs for client proxy id {0}");
  public static final StringId DiskInitFile_THE_INIT_FILE_0_DOES_NOT_EXIST = new StringId(5407, "The init file \"{0}\" does not exist.");
  public static final StringId DiskInitFile_IF_IT_NO_LONGER_EXISTS_DELETE_FOLLOWING_FILES_TO_CREATE_THIS_DISK_STORE_EXISTING_OPLOGS_0 = new StringId(5408, " If it no longer exists then delete the following files to be able to create this disk store. Existing oplogs are: {0}");

  public static final StringId GatewaySender_ACKREADERTHREAD_IGNORED_CANCELLATION = new StringId(5409, "AckReaderThread ignored cancellation");
  public static final StringId QueryMonitor_LOW_MEMORY_CANCELED_QUERY = new StringId(5410, "Query execution canceled due to memory threshold crossed in system, memory used: {0} bytes.");
  public static final StringId QueryMonitor_LOW_MEMORY_WHILE_GATHERING_RESULTS_FROM_PARTITION_REGION = new StringId(5411, "Query execution canceled due to low memory while gathering results from partitioned regions");
  public static final StringId IndexCreationMsg_CANCELED_DUE_TO_LOW_MEMORY = new StringId(5412, "Index creation canceled due to low memory");

  public static final StringId GatewaySenderImpl_PARALLEL_GATEWAY_SENDER_0_CANNOT_BE_CREATED_WITH_ORDER_POLICY_1 = new StringId(5413, "Parallel Gateway Sender {0} can not be created with OrderPolicy {1}");
  public static final StringId GatewaySenderImpl_GATEWAY_SENDER_0_CANNOT_HAVE_DISPATCHER_THREADS_LESS_THAN_1 = new StringId(5414, "GatewaySender {0} can not be created with dispatcher threads less than 1");

  public static final StringId GroupMembershipService_POSSIBLE_LOSS_OF_QUORUM_DETECTED = new StringId(5415, "Possible loss of quorum due to the loss of {0} cache processes: {1}");

  public static final StringId AsyncEventQueue_UNKNOWN_ORDER_POLICY_0_1 = new StringId(5420, "An invalid order-policy value ({1}) was configured for AsyncEventQueue {0}");

  public static final StringId ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_VERSION_REQUEST_1_CONTAINING_2_EVENTS = new StringId(5421, "{0}: Caught exception processing batch update version request request {1} containing {2} events");
  public static final StringId PartitionedRegion_NO_VM_AVAILABLE_FOR_UPDATE_ENTRY_VERSION_IN_0_ATTEMPTS = new StringId(5422, "No VM available for update-version in {0} attempts.");
  public static final StringId PartitionedRegion_UPDATE_VERSION_OF_ENTRY_ON_0_FAILED = new StringId(5423, "Update version of entry on {0} failed.");

  public static final StringId UpdateEntryVersionMessage_FAILED_SENDING_0 = new StringId(5424, "Failed sending < {0} >.");
  public static final StringId UpdateVersionOperation_CACHEWRITER_SHOULD_NOT_BE_CALLED = new StringId(5425, "CacheWriter should not be called");
  public static final StringId UpdateVersionOperation_DISTRIBUTEDLOCK_SHOULD_NOT_BE_ACQUIRED = new StringId(5426, "DistributedLock should not be acquired");
  public static final StringId ProcessBatch_0_DURING_BATCH_UPDATE_VERSION_NO_ENTRY_WAS_FOUND_FOR_KEY_1 = new StringId(5427, "Entry for key {1} was not found in Region {0} during ProcessBatch for Update Entry Version");

  public static final StringId CacheClientNotifier_COULD_NOT_CONNECT_DUE_TO_CQ_BEING_DRAINED = new StringId(5429, "CacheClientNotifier: Connection refused due to cq queue being drained from admin command, please wait...");
  public static final StringId CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_RESTARTING_DURABLE_CLIENT = new StringId(5430, "CacheClientProxy: Could not drain cq {0} due to client proxy id {1} reconnecting.");
  public static final StringId CacheClientProxy_COULD_NOT_DRAIN_CQ_DUE_TO_ACTIVE_DURABLE_CLIENT = new StringId(5431, "CacheClientProxy: Could not drain cq {0} because client proxy id {1} is connected.");
  public static final StringId ParallelGatewaySenderQueue_COULD_NOT_TERMINATE_CONFLATION_THREADPOOL = new StringId(5432, "Conflation thread pool did not terminate for the GatewaySender : {0}");

  public static final StringId NOT_QUEUING_AS_USERPR_IS_NOT_YET_CONFIGURED = new StringId(5433, "GatewaySender: Not queuing the event {0}, as the region for which this event originated is not yet configured in the GatewaySender");

  public static final StringId TCPConduit_EXCEPTION_PARSING_TCPPORTRANGESTART = new StringId(5434, "Exception parsing membership-port-range start port.");
  public static final StringId TCPConduit_EXCEPTION_PARSING_TCPPORTRANGEEND = new StringId(5435, "Exception parsing membership-port-range end port.");
  public static final StringId TCPConduit_UNABLE_TO_FIND_FREE_PORT = new StringId(5436, "Unable to find a free port in the membership-port-range");

  public static final StringId Snapshot_INVALID_EXPORT_FILE = new StringId(5438, "File is invalid or is a directory: {0}");
  public static final StringId Snapshot_INVALID_IMPORT_FILE = new StringId(5439, "File does not exist or is a directory: {0}");

  public static final StringId LocalRegion_REGION_IS_BEING_DESTROYED_WAITING_FOR_PARALLEL_QUEUE_TO_DRAIN = new StringId(5440, "Region is being destroyed. Waiting for paralle queue to drain.");
  public static final StringId PartitionedRegion_GATEWAYSENDER_0_IS_PAUSED_RESUME_IT_BEFORE_DESTROYING_USER_REGION_1 = new StringId(5441, "GatewaySender {0} is paused. Resume it before destroying region {1}.");
  public static final StringId PartitionedRegion_GATEWAYSENDERS_0_ARE_PAUSED_RESUME_THEM_BEFORE_DESTROYING_USER_REGION_1 = new StringId(5442, "GatewaySenders {0} are paused. Resume them before destroying region {1}.");

  public static final StringId GatewaySender_COULD_NOT_DESTROY_SENDER_AS_IT_IS_STILL_IN_USE = new StringId(5443, "The GatewaySender {0} could not be destroyed as it is still used by region(s).");
  public static final StringId AbstractGatewaySender_REGION_0_UNDERLYING_GATEWAYSENDER_1_IS_ALREADY_DESTROYED = new StringId(5444, "Region {0} that underlies the GatewaySender {1} is already destroyed.");
  public static final StringId AsyncEventQueue_0_CANNOT_HAVE_DISPATCHER_THREADS_LESS_THAN_1 = new StringId(5445, "AsyncEventQueue {0} can not be created with dispatcher threads less than 1");

  public static final StringId AsyncEventQueue_0_CANNOT_BE_CREATED_WITH_ORDER_POLICY_1 = new StringId(5447, "AsyncEventQueue {0} can not be created with OrderPolicy {1} when it is set parallel");
  public static final StringId GatewaySender_0_CAUGHT_EXCEPTION_WHILE_STOPPING_1 = new StringId(5448, "GatewaySender {0} caught exception while stopping: {1}");
  public static final StringId Oplog_DELETE_FAIL_0_1_2 = new StringId(5449, "Could not delete the file {0} {1} for disk store {2}.");

  public static final StringId PERCENTAGE_MOVE_DIRECTORY_SOURCE_NOT_DATA_STORE = new StringId(5450, "Source member does not exist or is not a data store for the partitioned region {0}: {1}");
  public static final StringId PERCENTAGE_MOVE_DIRECTORY_TARGET_NOT_DATA_STORE = new StringId(5451, "Target member does not exist or is not a data store for the partitioned region {0}: {1}");
  public static final StringId PERCENTAGE_MOVE_TARGET_SAME_AS_SOURCE = new StringId(5452, "Target member is the same as source member for the partitioned region {0}: {1}");

  public static final StringId GatewaySender_SEQUENCENUMBER_GENERATED_FOR_EVENT_IS_INVALID = new StringId(5453, "ERROR! The sequence number {0} generated for the bucket {1} is incorrect.");

  public static final StringId CacheXmlParser_A_0_MUST_BE_DEFINED_IN_THE_CONTEXT_OF_GATEWAY_SENDER_OR_ASYNC_EVENT_QUEUE = new StringId(5456, "A  {0}  must be defined in the context of gateway-sender or async-event-queue.");

  public static final StringId BucketAdvisor_WAITING_FOR_PRIMARY = new StringId(5457, "{0} secs have elapsed waiting for a primary for bucket {1}. Current bucket owners {2}");
  public static final StringId BucketAdvisor_WAITING_FOR_PRIMARY_DONE = new StringId(5458, "Wait for primary completed");

  public static final StringId DistributedPutAllOperation_MISSING_VERSION = new StringId(5459, "memberID cannot be null for persistent regions: {0}");

  public static final StringId Server_Ping_Failure = new StringId(5460, "Could not ping one of the following servers: {0}");

  public static final StringId DistributionManager_PR_META_DATA_CLEANUP_MESSAGE_PROCESSOR = new StringId(5500, "PrMetaData cleanup Message Processor ");
  public static final StringId RegionCreation_REGION_DESTROYED_DURING_INITIALIZATION = new StringId(5501, "Region was globally destroyed during cache initialization: {0}");
  public static final StringId SnappyCompressor_UNABLE_TO_LOAD_NATIVE_SNAPPY_LIBRARY = new StringId(5502, "Unable to load native Snappy library.");

  public static final StringId PartitionAttributesImpl_REDUCED_LOCAL_MAX_MEMORY_FOR_PARTITION_ATTRIBUTES_WHEN_SETTING_FROM_AVAILABLE_OFF_HEAP_MEMORY_SIZE = new StringId(5602, "Reduced local max memory for partition attribute when setting from available off-heap memory size");
  public static final StringId ParallelQueueRemovalMessage_QUEUEREMOVALMESSAGEPROCESSEXCEPTION_IN_PROCESSING_THE_LAST_DISPTACHED_KEY_FOR_A_SHADOWPR_THE_PROBLEM_IS_WITH_KEY__0_FOR_SHADOWPR_WITH_NAME_1 = new StringId(5603, "ParallelQueueRemovalMessage::process:Exception in processing the last disptached key for a ParallelGatewaySenderQueue''s shadowPR. The problem is with key ={0} for shadowPR with name={1}");

  public static final StringId DistributionManager_DISTRIBUTIONMANAGER_MEMBER_0_IS_1_EQUIVALENT = new StringId(5604, "Member {0} is {1}equivalent or in the same redundancy zone.");

  public static final StringId GemFireCache_INIT_CLEANUP_FAILED_FOR_REGION_0 = new StringId(5605, "Initialization failed for Region {0}");
  public static final StringId GemFireCache_ENFORCE_UNIQUE_HOST_NOT_APPLICABLE_FOR_LONER = new StringId(5606, "enforce-unique-host and redundancy-zone properties have no effect for a LonerDistributedSystem.");
  public static final StringId AttributesFactory_UNABLE_TO_CREATE_DISK_STORE_DIRECTORY_0 = new StringId(5607, "Unable to create directory : {0}");
  public static final StringId LocalRegion_A_DISKACCESSEXCEPTION_HAS_OCCURED_WHILE_WRITING_TO_THE_DISK_FOR_REGION_0_THE_CACHE_WILL_BE_CLOSED = new StringId(5608, "A DiskAccessException has occurred while writing to the disk for region {0}. The cache will be closed.");
  public static final StringId LocalRegion_A_DISKACCESSEXCEPTION_HAS_OCCURED_WHILE_WRITING_TO_THE_DISK_FOR_DISKSTORE_0_THE_CACHE_WILL_BE_CLOSED = new StringId(5609, "A DiskAccessException has occurred while writing to the disk for disk store {0}. The cache will be closed.");
  public static final StringId LocalRegion_AN_EXCEPTION_OCCURED_WHILE_CLOSING_THE_CACHE = new StringId(5610, "An Exception occurred while closing the cache.");

  public static final StringId DiskWriteAttributesFactory_DISK_USAGE_WARNING_INVALID_0 = new StringId(5611, "Disk usage warning percentage must be set to a value between 0-100.  The value {0} is invalid.");
  public static final StringId DiskWriteAttributesFactory_DISK_USAGE_CRITICAL_INVALID_0 = new StringId(5612, "Disk usage critical percentage must be set to a value between 0-100.  The value {0} is invalid.");
  public static final StringId DistributedRegion_REGION_0_ENABLE_NETWORK_PARTITION_WARNING = new StringId(5832, "Creating persistent region {0}, but enable-network-partition-detection is set to false. Running with network partition detection disabled can lead to an unrecoverable system in the event of a network split.");

  public static final StringId DiskStoreMonitor_LOG_DISK_NORMAL = new StringId(5613, "The disk volume {0} for log files has returned to normal usage levels and is {1} full.");
  public static final StringId DiskStoreMonitor_LOG_DISK_WARNING = new StringId(5614, "The disk volume {0} for log files has exceeded the warning usage threshold and is {1} full.");

  public static final StringId DiskStoreMonitor_DISK_WARNING = new StringId(5616, "The disk volume {0} for disk store {1} has exceeded the warning usage threshold and is {2} full");
  public static final StringId DiskStoreMonitor_DISK_CRITICAL = new StringId(5617, "The disk volume {0} for disk store {1} has exceeded the critical usage threshold and is {2} full");
  public static final StringId DiskStoreMonitor_DISK_NORMAL = new StringId(5618, "The disk volume {0} for disk store {1} has returned to normal usage levels and is {2} full");

  public static final StringId DiskStoreMonitor_ERR = new StringId(5619, "The DiskStore Monitor has encountered an error");
  public static final StringId DiskStoreMonitor_ThreadGroup = new StringId(5620, "DiskStoreMonitorss");

  public static final StringId GatewayImpl_GATEWAY_0_HAS_BEEN_REBALANCED = new StringId(5621, "GatewaySender {0} has been rebalanced");

  public static final StringId OffHeapMemoryMonitor_NO_OFF_HEAP_MEMORY_HAS_BEEN_CONFIGURED = new StringId(5622, "No off-heap memory has been configured.");

  public static final StringId Oplog_Close_Failed = new StringId(5640, "Failed to close file {0}");
  public static final StringId Oplog_PreAllocate_Failure = new StringId(5641, "Could not pre-allocate file {0} with size={1}");
  public static final StringId Oplog_PreAllocate_Failure_Init = new StringId(5642, "Could not create and pre grow file in dir {0} with size={1}");

  public static final StringId InternalInstantiator_REGISTERED = new StringId(5650, "Instantiator registered with id {0} class {1}");
  public static final StringId InternalInstantiator_REGISTERED_HOLDER = new StringId(5651, "Instantiator registered with holder id {0} class {1}");
  public static final StringId RegisterInstantiators_BAD_CLIENT = new StringId(5652, "Client {0} failed to register instantiators: {1}");
  public static final StringId GatewayReceiver_EXCEPTION_WHILE_PROCESSING_BATCH = new StringId(5653, "Exception occurred while processing a batch on the receiver running on DistributedSystem with Id: {0}, DistributedMember on which the receiver is running: {1}");

  public static final StringId DiskStoreAttributesCreation_DISK_USAGE_WARN_ARE_NOT_THE_SAME = new StringId(5660, "Disk usage warning percentages of disk store {0} are not the same");
  public static final StringId DiskStoreAttributesCreation_DISK_USAGE_CRITICAL_ARE_NOT_THE_SAME = new StringId(5661, "Disk usage critical percentages of disk store {0} are not the same");

  public static final StringId MEMSCALE_JVM_INCOMPATIBLE_WITH_OFF_HEAP = new StringId(5662, "Your Java virtual machine is incompatible with off-heap memory.  Please refer to {0} documentation for suggested JVMs.");
  public static final StringId MEMSCALE_EVICTION_INIT_FAIL = new StringId(5663, "Cannot initialize the off-heap evictor.  There is no off-heap memory available for eviction.");


  // If the text is changed for this StringId, do the same in ConnectionPoolImplJUnitTest.java
  public static final StringId QueueManagerImpl_COULD_NOT_FIND_SERVER_TO_CREATE_PRIMARY_CLIENT_QUEUE = new StringId(5700, "Could not find any server to create primary client queue on. Number of excluded servers is {0} and exception is {1}.");
  // If the text is changed for this StringId, do the same in ConnectionPoolImplJUnitTest.java
  public static final StringId QueueManagerImpl_COULD_NOT_FIND_SERVER_TO_CREATE_REDUNDANT_CLIENT_QUEUE = new StringId(5701, "Could not find any server to create redundant client queue on. Number of excluded servers is {0} and exception is {1}.");
  public static final StringId QueueManagerImpl_SUBSCRIPTION_ENDPOINT_CRASHED_SCHEDULING_RECOVERY = new StringId(5702, "{0} subscription endpoint {1} crashed. Scheduling recovery.");
  public static final StringId QueueManagerImpl_CACHE_CLIENT_UPDATER_FOR_ON_ENDPOINT_EXITING_SCHEDULING_RECOVERY = new StringId(5703, "Cache client updater for {0} on endpoint {1} exiting. Scheduling recovery.");
  //SubscriptionManager redundancy satisfier - primary endpoint has been lost. Attempting to recover
  public static final StringId QueueManagerImpl_SUBSCRIPTION_MANAGER_REDUNDANCY_SATISFIER_PRIMARY_ENDPOINT_HAS_BEEN_LOST_ATTEMPTIMG_TO_RECOVER = new StringId(5704, "SubscriptionManager redundancy satisfier - primary endpoint has been lost. Attempting to recover.");
  public static final StringId QueueManagerImpl_SUBSCRIPTION_MANAGER_REDUNDANCY_SATISFIER_REDUNDANT_ENDPOINT_HAS_BEEN_LOST_ATTEMPTIMG_TO_RECOVER = new StringId(5705, "SubscriptionManager redundancy satisfier - redundant endpoint has been lost. Attempting to recover.");
  public static final StringId DiskStoreImpl_FATAL_ERROR_ON_FLUSH = new StringId(5706, "Fatal error from asynchronous flusher thread");
  public static final StringId AbstractGatewaySender_META_REGION_CREATION_EXCEPTION_0 = new StringId(5707, "{0}: Caught the following exception attempting to create gateway event id index metadata region:");
  public static final StringId AbstractGatewaySender_FAILED_TO_LOCK_META_REGION_0 = new StringId(5708, "{0}: Failed to lock gateway event id index metadata region");

  public static final StringId GatewayReceiver_PDX_CONFIGURATION = new StringId(5709, "This gateway receiver has received a PDX type from {0} that does match the existing PDX type. This gateway receiver will not process any more events, in order to prevent receiving objects which may not be deserializable.");
  public static final StringId Gateway_CONFIGURED_SOCKET_READ_TIMEOUT_TOO_LOW = new StringId(5710, "{0} cannot configure socket read timeout of {1} milliseconds because it is less than the minimum of {2} milliseconds. The default will be used instead.");
  public static final StringId Gateway_OBSOLETE_SYSTEM_POPERTY = new StringId(5711, "Obsolete java system property named {0} was set to control {1}. This property is no longer supported. Please use the GemFire API instead.");
  public static final StringId GatewayReceiver_EXCEPTION_WHILE_STARTING_GATEWAY_RECEIVER = new StringId(5712, "Exception occured while starting gateway receiver");
  public static final StringId GatewayReceiver_IS_NOT_RUNNING = new StringId(5713, "Gateway Receiver is not running");
  public static final StringId GatewayReceiver_IS_ALREADY_RUNNING = new StringId(5714, "Gateway Receiver is already running");
  public static final StringId ParallelGatewaySenderQueue_NON_PERSISTENT_GATEWAY_SENDER_0_CAN_NOT_BE_ATTACHED_TO_PERSISTENT_REGION_1 = new StringId(5715, "Non persistent gateway sender {0} can not be attached to persistent region {1}");
  public static final StringId ParallelAsyncEventQueue_0_CAN_NOT_BE_USED_WITH_REPLICATED_REGION_1 = new StringId(5716, "Parallel Async Event Queue {0} can not be used with replicated region {1}");
  public static final StringId ParallelGatewaySender_0_CAN_NOT_BE_USED_WITH_REPLICATED_REGION_1 = new StringId(5717, "Parallel gateway sender {0} can not be used with replicated region {1}");

  public static final StringId GemFireCacheImpl_REST_SERVER_ON_PORT_0_IS_SHUTTING_DOWN = new StringId(6500, "Rest Server on port {0} is shutting down");
  public static final StringId PartitionedRegion_QUERY_TRACE_LOCAL_NODE_LOG = new StringId(6501, "  Local {0} took {1}ms and returned {2} results; {3}");
  public static final StringId PartitionedRegion_QUERY_TRACE_REMOTE_NODE_LOG = new StringId(6502, "  Remote {0} took {1}ms and returned {2} results; {3}");
  public static final StringId PartitionedRegion_QUERY_TRACE_LOG = new StringId(6503, "Trace Info for Query: {0}");

  public static final StringId Region_RemoveAll_Applied_PartialKeys_At_Server_0 = new StringId(6504, "Region {0} removeAll at server applied partial keys due to exception.");
  public static final StringId Region_RemoveAll_Applied_PartialKeys_0_1 = new StringId(6505, "Region {0} removeAll: {1}");

  public static final StringId DefaultQuery_ORDER_BY_ATTRIBS_NOT_PRESENT_IN_PROJ = new StringId(6507, "Query contains atleast one order by field which is not present in projected fields.");
  public static final StringId DefaultQuery_PROJ_COL_ABSENT_IN_GROUP_BY = new StringId(6508, "Query contains projected column not present in group by clause");
  public static final StringId DefaultQuery_GROUP_BY_COL_ABSENT_IN_PROJ = new StringId(6509, "Query contains group by columns not present in projected fields");

  public static final StringId CqQueryImpl_CQ_NOT_SUPPORTED_FOR_REPLICATE_WITH_LOCAL_DESTROY = new StringId(6598, "CQ is not supported for replicated region: {0} with eviction action: {1}");

  public static final StringId AbstractDistributionConfig_LOCATOR_WAIT_TIME_NAME_0 = new StringId(6599, "The amount of time, in seconds, to wait for a locator to be available before throwing an exception during startup.  The default is {0}.");
  public static final StringId CliLegacyMessage_ERROR = new StringId(6600, "Error processing request {0}.");

  public static final StringId AbstractDistributionConfig_MEMCACHED_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1 = new StringId(6601, "The memcached-bind-address \"{0}\" is not a valid address for this machine.  These are the valid addresses for this machine: {1}");
  public static final StringId GemFireCacheImpl_STARTING_GEMFIRE_MEMCACHED_SERVER_ON_BIND_ADDRESS_0_PORT_1_FOR_2_PROTOCOL = new StringId(6602, "Starting GemFireMemcachedServer on bind address {0} on port {1} for {2} protocol");

  public static final StringId PersistenceAdvisorImpl_RETRYING_GII = new StringId(6603, "GII failed from all sources, but members are still online. Retrying the GII.");

  public static final StringId MinimumSystemRequirements_NOT_MET = new StringId(6604, "Minimum system requirements not met. Unexpected behavior may result in additional errors.");
  public static final StringId MinimumSystemRequirements_JAVA_VERSION = new StringId(6605, "Java version older than {0}.");

  public static final StringId LOCATOR_UNABLE_TO_RECOVER_VIEW = new StringId(6606, "Unable to recover previous membership view from {0}");

  public static final StringId Network_partition_detected = new StringId(6607, "Exiting due to possible network partition event due to loss of {0} cache processes: {1}");

  // GMSAuthenticator
  public static final StringId AUTH_PEER_AUTHENTICATION_FAILED_WITH_EXCEPTION = new StringId(6608, "Security check failed for [{0}]. {1}");
  public static final StringId AUTH_PEER_AUTHENTICATION_FAILED = new StringId(6609, "Security check failed. {0}");
  public static final StringId AUTH_PEER_AUTHENTICATION_MISSING_CREDENTIALS = new StringId(6610, "Failed to find credentials from [{0}]");
  public static final StringId AUTH_FAILED_TO_ACQUIRE_AUTHINITIALIZE_INSTANCE = new StringId(6611, "AuthInitialize instance could not be obtained");
  public static final StringId AUTH_FAILED_TO_OBTAIN_CREDENTIALS_IN_0_USING_AUTHINITIALIZE_1_2 = new StringId(6612, "Failed to obtain credentials using AuthInitialize [{1}]. {2}");
  public static final StringId DistributedSystem_BACKUP_ALREADY_IN_PROGRESS = new StringId(6613, "A backup is already in progress.");

  public static final StringId AbstractGatewaySenderEventProcessor_SET_BATCH_SIZE = new StringId(6614, "Set the batch size from {0} to {1} events");
  public static final StringId AbstractGatewaySenderEventProcessor_ATTEMPT_TO_SET_BATCH_SIZE_FAILED = new StringId(6615, "Attempting to set the batch size from {0} to {1} events failed. Instead it was set to 1.");
  public static final StringId GatewaySenderEventRemoteDispatcher_MESSAGE_TOO_LARGE_EXCEPTION = new StringId(6616, "The following exception occurred attempting to send a batch of {0} events. The batch will be tried again after reducing the batch size to {1} events.");

  // Developer REST interface
  public static final StringId SwaggerConfig_VENDOR_PRODUCT_LINE = new StringId(6617, "Apache Geode Developer REST API");
  public static final StringId SwaggerConfig_DESCRIPTOR = new StringId(6618, "Developer REST API and interface to Geode''s distributed, in-memory data grid and cache.");
  public static final StringId SwaggerConfig_EULA_LINK = new StringId(6619, "http://www.apache.org/licenses/");
  public static final StringId SwaggerConfig_SUPPORT_LINK = new StringId(6620, "user@geode.incubator.apache.org");
  public static final StringId SwaggerConfig_DOC_TITLE = new StringId(6621, "Apache Geode Documentation");
  public static final StringId SwaggerConfig_DOC_LINK = new StringId(6622, "http://geode.incubator.apache.org/docs/");

  public static final StringId LuceneXmlParser_CLASS_0_IS_NOT_AN_INSTANCE_OF_ANALYZER = new StringId(6623, "Class {0} is not an instance of Analyzer.");
  public static final StringId LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_FIELDS_2_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_FIELDS_3 = new StringId(6624, "Cannot create Lucene index {0} on region {1} with fields {2} because another member defines the same index with fields {3}.");
  public static final StringId LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_ANALYZER_2_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_ANALYZER_3 = new StringId(6625, "Cannot create Lucene index {0} on region {1} with analyzer {2} because another member defines the same index with analyzer {3}.");
  public static final StringId LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_FIELD_ANALYZERS_2_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_NO_FIELD_ANALYZERS = new StringId(6626, "Cannot create Lucene index {0} on region {1} with field analyzers {2} because another member defines the same index with no field analyzers.");
  public static final StringId LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_NO_FIELD_ANALYZERS_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_FIELD_ANALYZERS_2 = new StringId(6627, "Cannot create Lucene index {0} on region {1} with no field analyzers because another member defines the same index with field analyzers {2}.");
  public static final StringId LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_FIELD_ANALYZERS_2_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_FIELD_ANALYZERS_3 = new StringId(6628, "Cannot create Lucene index {0} on region {1} with field analyzers {2} because another member defines the same index with field analyzers {3}.");
  public static final StringId LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_NO_ANALYZER_ON_FIELD_2_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_ANALYZER_3_ON_THAT_FIELD = new StringId(6629, "Cannot create Lucene index {0} on region {1} with no analyzer on field {2} because another member defines the same index with analyzer {3} on that field.");
  public static final StringId LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_ANALYZER_2_ON_FIELD_3_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_NO_ANALYZER_ON_THAT_FIELD = new StringId(6630, "Cannot create Lucene index {0} on region {1} with analyzer {2} on field {3} because another member defines the same index with no analyzer on that field.");
  public static final StringId LuceneService_CANNOT_CREATE_INDEX_0_ON_REGION_1_WITH_ANALYZER_2_ON_FIELD_3_BECAUSE_ANOTHER_MEMBER_DEFINES_THE_SAME_INDEX_WITH_ANALYZER_4_ON_THAT_FIELD = new StringId(6631, "Cannot create Lucene index {0} on region {1} with analyzer {2} on field {3} because another member defines the same index with analyzer {4} on that field.");

  public static final StringId AbstractDistributionConfig_CLUSTER_SSL_ALIAS_0 = new StringId(6633, "SSL communication uses the this alias when determining the key to use from the keystore for SSL. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_GATEWAY_SSL_ALIAS_0 = new StringId(6634, "SSL gateway communication uses the this alias when determining the key to use from the keystore for SSL. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_SERVER_SSL_ALIAS_0 = new StringId(6635, "SSL inter-server communication (peer-to-peer) uses the this alias when determining the key to use from the keystore for SSL. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_HTTP_SERVICE_SSL_ALIAS_0 = new StringId(6636, "SSL http service communication uses the this alias when determining the key to use from the keystore for SSL. Defaults to \"{0}\".");
  public static final StringId AbstractDistributionConfig_JMX_MANAGER_SSL_ALIAS_0 = new StringId(6637, "SSL jmx communication uses the this alias when determining the key to use from the keystore for SSL. Defaults to \"{0}\".");

  public static final StringId AbstractDistributionConfig_SSL_ENABLED_COMPONENTS_0_INVALID_TRY_1 = new StringId(6638, "\"{0}\" is not in the valid set of options \"{1}\"");

  public static final StringId AbstractDistributionConfig_SSL_ENABLED_COMPONENTS_SET_INVALID_DEPRECATED_SSL_SET = new StringId(6639, "When using ssl-enabled-components one cannot use any other SSL properties other than cluster-ssl-* or the corresponding aliases");

  public static final StringId ColocationHelper_REGION_SPECIFIED_IN_COLOCATEDWITH_DOES_NOT_EXIST = new StringId(6640, "Region specified in ''colocated-with'' ({0}) for region {1} does not exist. It should be created before setting ''colocated-with'' attribute for this region.");
  public static final StringId ColocationLogger_PERSISTENT_DATA_RECOVERY_OF_REGION_PREVENTED_BY_OFFLINE_COLOCATED_CHILDREN = new StringId(6641, "Persistent data recovery for region {0} is prevented by offline colocated region{1}");

  public static StringId AbstractDistributionConfig_LOCATOR_SSL_ALIAS_0 = new StringId(6642, "SSL locator communications uses this alias when determining the " + "key to use from the keystore for SSL. Defaults to \"{0}\".");

  public static StringId AbstractDistributionConfig_SSL_ENABLED_COMPONENTS_INVALID_ALIAS_OPTIONS = new StringId(6643, "The alias options for the SSL options provided seem to be invalid. Please check that all required aliases are set");

  public static StringId GEMFIRE_CACHE_SECURITY_MISCONFIGURATION = new StringId(6644, "A server cannot specify its own security-manager or security-post-processor when using cluster configuration");
  public static StringId GEMFIRE_CACHE_SECURITY_MISCONFIGURATION_2 = new StringId(6645, "A server must use cluster configuration when joining a secured cluster.");


  /** Testing strings, messageId 90000-99999 **/

  /**
   * These are simple messages for testing, translated with Babelfish.
   **/
  public static final StringId TESTING_THIS_IS_A_TEST_MESSAGE = new StringId(90000, "This is a test message.");
  public static final StringId TESTING_THIS_MESSAGE_HAS_0_MEMBERS = new StringId(90001, "Please ignore: This message has {0} members.");
  public static final StringId OBJECT_PREFIX = new StringId(90002, "Object_");
  public static final StringId REGION_PREFIX = new StringId(90003, "Region_");
  public static final StringId LISTENER_PREFIX = new StringId(90004, "Listener_");

  public static final StringId DistributedRegion_INITIALIZING_REGION_COMPLETED_0 = new StringId(90005, "Initialization of region {0} completed");


}

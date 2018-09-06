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
package org.apache.geode.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;

/**
 * An interface that implements data serialization for internal GemFire product classes that have a
 * fixed id. The fixed id is used to represent the class, on the wire, at serialization time and
 * used in a switch statement at deserialization time. All the codes should be static final in this
 * class.
 * <p>
 * Implementors MUST have a public zero-arg constructor.
 *
 * <p>
 * Note that this class is for internal use only. Customer classes that want to do something
 * similar should implement a subclass of {@link DataSerializer} or {@link Instantiator}.
 *
 * <p>
 * To add a new DataSerializableFixedID do this following:
 * <ol>
 * <li>Define a constant with an id that is free and put it in <code>DataSerializableFixedID</code>
 * as a "byte". Make sure and update the "unused" comments to no longer mention your new id. If
 * implementing a class used only for tests then there is no need to consume a fixed id and you
 * should use {@link #NO_FIXED_ID}. In this case you can skip steps 3 and 4.
 * <li>Define a method in the class that implements <code>DataSerializableFixedID</code> named
 * {@link #getDSFID} that returns the constant from step 1.
 * <li>Define a private static method in {@link DSFIDFactory} that returns an instance of the class
 * from step 2 usually be calling its zero-arg constructor and then calling fromData(in). See the
 * end of {@link DSFIDFactory} for examples.
 * <li>Add a case statement in {@link DSFIDFactory#create} for the constant from step 1 that calls
 * the method from step 3.
 * <li>Implement {@link #toData} and {@link #fromData} just like you would on a
 * <code>DataSerializer</code>. Make sure you follow the javadocs for these methods to add support
 * for rolling upgrades.
 * </ol>
 *
 * @see DataSerializer
 *
 * @since GemFire 5.7
 */
public interface DataSerializableFixedID extends SerializationVersions {

  // NOTE, codes < -65536 will take 4 bytes to serialize
  // NOTE, codes < -128 will take 2 bytes to serialize

  /*
   * In the class to be serialized, add public FOO(DataInput in) throws IOException,
   * ClassNotFoundException { fromData(in); }
   *
   * public int getDSFID() { return FOO; }
   *
   * In DataSerializableFixedId, allocate an ID for the class byte FOO = -54;
   *
   * In DSFIDFactory, add a case for the new class case FOO: return new FOO(in);
   */
  short CREATE_REGION_MESSAGE_LUCENE = -159;
  short FINAL_CHECK_PASSED_MESSAGE = -158;
  short NETWORK_PARTITION_MESSAGE = -157;
  short SUSPECT_MEMBERS_MESSAGE = -156;

  short HEARTBEAT_RESPONSE = -155;
  short HEARTBEAT_REQUEST = -154;
  short REMOVE_MEMBER_REQUEST = -153;

  short LEAVE_REQUEST_MESSAGE = -152;

  short VIEW_ACK_MESSAGE = -151;
  short INSTALL_VIEW_MESSAGE = -150;
  short GMSMEMBER = -149;
  short NETVIEW = -148;
  short GET_VIEW_REQ = -147;
  short GET_VIEW_RESP = -146;

  short FIND_COORDINATOR_REQ = -145;
  short FIND_COORDINATOR_RESP = -144;

  short JOIN_RESPONSE = -143;
  short JOIN_REQUEST = -142;

  short SNAPPY_COMPRESSED_CACHED_DESERIALIZABLE = -140;

  short GATEWAY_EVENT_IMPL = -136;
  short GATEWAY_SENDER_EVENT_CALLBACK_ARGUMENT = -135;
  short GATEWAY_SENDER_EVENT_IMPL = -134;

  short CLIENT_TOMBSTONE_MESSAGE = -133;

  short R_CLEAR_MSG_REPLY = -132;
  short R_CLEAR_MSG = -131;

  short WAIT_FOR_VIEW_INSTALLATION = -130;

  short DISPATCHED_AND_CURRENT_EVENTS = -129;

  byte DLOCK_QUERY_MESSAGE = -128;
  byte DLOCK_QUERY_REPLY = -127;

  byte CLIENT_HEALTH_STATS = -126;
  byte PR_MANAGE_BACKUP_BUCKET_MESSAGE = -125;
  byte PR_MANAGE_BACKUP_BUCKET_REPLY_MESSAGE = -124;
  byte SIZED_BASED_LOAD_PROBE = -123;

  byte CLIENT_PING_MESSAGE_IMPL = -122;

  byte REMOTE_PUTALL_REPLY_MESSAGE = -121;
  byte VERSION_TAG = -120;

  byte REMOTE_PUTALL_MESSAGE = -119;

  byte ADD_CACHESERVER_PROFILE_UPDATE = -118;
  byte SERVER_INTEREST_REGISTRATION_MESSAGE = -117;
  byte FILTER_PROFILE_UPDATE = -116;
  byte JTA_AFTER_COMPLETION_MESSAGE = -115;
  byte JTA_BEFORE_COMPLETION_MESSAGE = -114;
  byte INVALIDATE_PARTITIONED_REGION_MESSAGE = -113;
  byte TX_REMOTE_COMMIT_MESSAGE = -112;
  byte TX_REMOTE_ROLLBACK_MESSAGE = -111;
  byte PR_PUTALL_REPLY_MESSAGE = -110;
  byte PR_PUTALL_MESSAGE = -109;
  byte RESOURCE_PROFILE_MESSAGE = -108;
  byte RESOURCE_MANAGER_PROFILE = -107;
  byte PR_CREATE_BUCKET_MESSAGE = -106;
  byte PR_CREATE_BUCKET_REPLY_MESSAGE = -105;
  byte DISTRIBUTED_REGION_FUNCTION_MESSAGE = -104;
  byte DISTRIBUTED_REGION_FUNCTION_REPLY_MESSAGE = -103;

  byte MEMBER_FUNCTION_MESSAGE = -102;
  byte MEMBER_FUNCTION_REPLY_MESSAGE = -101;
  byte PARTITION_REGION_CONFIG = -100;
  byte PR_FETCH_KEYS_REPLY_MESSAGE = -99;
  byte PR_DUMP_B2N_REGION_MSG = -98;
  byte PR_DUMP_B2N_REPLY_MESSAGE = -97;
  byte PR_INVALIDATE_MESSAGE = -96;
  byte PR_INVALIDATE_REPLY_MESSAGE = -95;
  byte PR_FUNCTION_MESSAGE = -94;
  byte PR_FUNCTION_REPLY_MESSAGE = -93;

  byte PROFILES_REPLY_MESSAGE = -92;
  byte CACHE_SERVER_PROFILE = -91;
  byte CONTROLLER_PROFILE = -90;

  byte CREATE_REGION_MESSAGE = -89;
  byte DESTROY_PARTITIONED_REGION_MESSAGE = -88;
  byte COMMIT_PROCESS_QUERY_MESSAGE = -87;
  byte COMMIT_PROCESS_QUERY_REPLY_MESSAGE = -86;
  byte DESTROY_REGION_WITH_CONTEXT_MESSAGE = -85;
  byte PUT_ALL_MESSAGE = -84;
  byte CLEAR_REGION_MESSAGE = -83;
  byte INVALIDATE_REGION_MESSAGE = -82;
  byte STATE_MARKER_MESSAGE = -80;
  byte STATE_STABILIZATION_MESSAGE = -79;
  byte STATE_STABILIZED_MESSAGE = -78;
  byte CLIENT_MARKER_MESSAGE_IMPL = -77;
  byte TX_LOCK_UPDATE_PARTICIPANTS_MESSAGE = -76;
  byte TX_ORIGINATOR_RECOVERY_MESSAGE = -75;
  byte TX_ORIGINATOR_RECOVERY_REPLY_MESSAGE = -74;
  byte QUEUE_REMOVAL_MESSAGE = -73;
  byte DLOCK_RECOVER_GRANTOR_MESSAGE = -72;
  byte DLOCK_RECOVER_GRANTOR_REPLY_MESSAGE = -71;
  byte NON_GRANTOR_DESTROYED_REPLY_MESSAGE = -70;
  byte TOMBSTONE_MESSAGE = -69;
  byte IDS_REGISTRATION_MESSAGE = -68;
  byte TX_LOCK_UPDATE_PARTICIPANTS_REPLY_MESSAGE = -67;
  byte STREAMING_REPLY_MESSAGE = -66;
  byte PREFER_BYTES_CACHED_DESERIALIZABLE = -65;
  byte VM_CACHED_DESERIALIZABLE = -64;
  byte GATEWAY_EVENT_IMPL_66 = -63;
  byte SUSPEND_LOCKING_TOKEN = -62;
  byte OBJECT_TYPE_IMPL = -61;
  byte STRUCT_TYPE_IMPL = -60;
  byte COLLECTION_TYPE_IMPL = -59;
  byte TX_LOCK_BATCH = -58;
  byte STORE_ALL_CACHED_DESERIALIZABLE = -57;
  byte GATEWAY_EVENT_CALLBACK_ARGUMENT = -56;
  byte MAP_TYPE_IMPL = -55;
  byte LOCATOR_LIST_REQUEST = -54;
  byte CLIENT_CONNECTION_REQUEST = -53;
  byte QUEUE_CONNECTION_REQUEST = -52;
  byte LOCATOR_LIST_RESPONSE = -51;
  byte CLIENT_CONNECTION_RESPONSE = -50;
  byte QUEUE_CONNECTION_RESPONSE = -49;
  byte CLIENT_REPLACEMENT_REQUEST = -48;

  byte INTEREST_EVENT_MESSAGE = -47;
  byte INTEREST_EVENT_REPLY_MESSAGE = -46;
  byte CLIENT_DENYLIST_MESSAGE = -45;
  byte REMOVE_CLIENT_FROM_DENYLIST_MESSAGE = -44;
  byte GET_ALL_SERVERS_REQUEST = -43;
  byte GET_ALL_SERVRES_RESPONSE = -42;

  byte FIND_REMOTE_TX_REPLY = -41;
  byte FIND_REMOTE_TX_MESSAGE = -40;

  byte R_REMOTE_COMMIT_REPLY_MESSAGE = -39;

  byte R_FETCH_KEYS_REPLY = -38;
  byte R_FETCH_KEYS_MESSAGE = -37;

  byte R_SIZE_MESSAGE = -36;
  byte R_SIZE_REPLY_MESSAGE = -35;

  byte R_FETCH_ENTRY_REPLY_MESSAGE = -34;
  byte R_FETCH_ENTRY_MESSAGE = -33;
  byte R_DESTROY_MESSAGE = -32;
  byte R_INVALIDATE_MESSAGE = -31;
  byte R_INVALIDATE_REPLY_MESSAGE = -30;

  byte R_PUT_MESSAGE = -29;
  byte R_PUT_REPLY_MESSAGE = -28;

  byte R_CONTAINS_MESSAGE = -27;
  byte R_CONTAINS_REPLY_MESSAGE = -26;

  byte R_GET_MESSAGE = -24;
  byte R_GET_REPLY_MESSAGE = -25;

  byte DURABLE_CLIENT_INFO_RESPONSE = -23;

  byte DURABLE_CLIENT_INFO_REQUEST = -22;

  byte CLIENT_INTEREST_MESSAGE = -21;

  byte LATEST_LAST_ACCESS_TIME_MESSAGE = -20;

  public static final byte REMOVE_CACHESERVER_PROFILE_UPDATE = -19;

  // IDs -18 .. -16 are not used

  /**
   * A header byte meaning that the next element in the stream is a <code>VMIdProfile</code>.
   */
  byte VMID_PROFILE_MESSAGE = -15;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>LocalRegion.UUIDProfile</code>.
   */
  byte REGION_UUID_PROFILE_MESSAGE = -14;

  short TX_REMOTE_COMMIT_PHASE1_MESSAGE = -13;
  short TX_BATCH_MESSAGE = -12;
  short TX_CLEANUP_ENTRY_MESSAGE = -11;
  short TX_BATCH_REPLY_MESSAGE = -10;

  byte PR_REMOVE_ALL_MESSAGE = -9;
  byte REMOVE_ALL_MESSAGE = -8;
  byte PR_REMOVE_ALL_REPLY_MESSAGE = -7;
  byte REMOTE_REMOVE_ALL_MESSAGE = -6;
  byte REMOTE_REMOVE_ALL_REPLY_MESSAGE = -5;
  byte DISTTX_COMMIT_MESSAGE = -4;
  byte DISTTX_PRE_COMMIT_MESSAGE = -3;
  byte DISTTX_COMMIT_REPLY_MESSAGE = -2;
  byte DISTTX_PRE_COMMIT_REPLY_MESSAGE = -1;

  byte ILLEGAL = 0;

  // 1 available for reuse. Retired in Geode v1.0
  // byte JGROUPS_VIEW = 1;

  // 2 available for reuse. Retired in Geode v1.0
  // byte JGROUPS_JOIN_RESP = 2;

  byte PUTALL_VERSIONS_LIST = 3;

  byte INITIAL_IMAGE_VERSIONED_OBJECT_LIST = 4;

  byte FIND_VERSION_TAG = 5;

  byte VERSION_TAG_REPLY = 6;

  byte VERSIONED_OBJECT_LIST = 7;

  byte ENUM_ID = 8;
  byte ENUM_INFO = 9;


  /**
   * A header byte meaning that the next element in the stream is a
   * <code>InitialImageOperation.EventStateMessage</code>.
   */
  byte REGION_STATE_MESSAGE = 10;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>ClientInstantiatorMessage</code>.
   */
  byte CLIENT_INSTANTIATOR_MESSAGE = 11;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>InternalInstantiator.RegistrationMessage</code>.
   */
  byte REGISTRATION_MESSAGE = 12;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>InternalInstantiator.RegistrationContextMessage</code>.
   */
  byte REGISTRATION_CONTEXT_MESSAGE = 13;

  /** More Query Result Classes */
  // PRQueryProcessor.EndOfBucket
  byte END_OF_BUCKET = 14;
  byte RESULTS_BAG = 15;
  byte STRUCT_BAG = 16;

  byte BUCKET_PROFILE = 17;
  byte PARTITION_PROFILE = 18;

  byte ROLE_EVENT = 19;
  byte CLIENT_REGION_EVENT = 20;

  byte CONCURRENT_HASH_MAP = 21;
  byte FIND_DURABLE_QUEUE = 22;
  byte FIND_DURABLE_QUEUE_REPLY = 23;
  byte CACHE_SERVER_LOAD_MESSAGE = 24;

  /**
   * A header byte meaning that the next element in the stream is a <code>ObjectPartList</code>.
   */
  byte OBJECT_PART_LIST = 25;

  byte REGION = 26;

  /****** Query Result Classes *******/
  byte RESULTS_COLLECTION_WRAPPER = 27;
  byte RESULTS_SET = 28;
  byte SORTED_RESULT_SET = 29;
  byte SORTED_STRUCT_SET = 30;
  byte UNDEFINED = 31;
  byte STRUCT_IMPL = 32;
  byte STRUCT_SET = 33;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>ClearRegionWithContextMessage</code>.
   */
  byte CLEAR_REGION_MESSAGE_WITH_CONTEXT = 34;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>ClientUpdateMessage</code>.
   */
  byte CLIENT_UPDATE_MESSAGE = 35;

  /**
   * A header byte meaning that the next element in the stream is a <code>EventID</code>.
   */
  byte EVENT_ID = 36;

  byte INTEREST_RESULT_POLICY = 37;
  /**
   * A header byte meaning that the next element in the stream is a
   * <code>ClientProxyMembershipID</code>.
   */
  byte CLIENT_PROXY_MEMBERSHIPID = 38;

  byte PR_BUCKET_BACKUP_MESSAGE = 39;
  byte SERVER_BUCKET_PROFILE = 40;
  byte PR_BUCKET_PROFILE_UPDATE_MESSAGE = 41;
  byte PR_BUCKET_SIZE_MESSAGE = 42;
  byte PR_CONTAINS_KEY_VALUE_MESSAGE = 43;
  byte PR_DUMP_ALL_PR_CONFIG_MESSAGE = 44;
  byte PR_DUMP_BUCKETS_MESSAGE = 45;
  byte PR_FETCH_ENTRIES_MESSAGE = 46;
  byte PR_FETCH_ENTRY_MESSAGE = 47;
  byte PR_FETCH_KEYS_MESSAGE = 48;
  byte PR_FLUSH_MESSAGE = 49;
  byte PR_IDENTITY_REQUEST_MESSAGE = 50;
  byte PR_IDENTITY_UPDATE_MESSAGE = 51;
  byte PR_INDEX_CREATION_MSG = 52;
  byte PR_MANAGE_BUCKET_MESSAGE = 53;
  byte PR_PRIMARY_REQUEST_MESSAGE = 54;
  byte PR_PRIMARY_REQUEST_REPLY_MESSAGE = 55;
  byte PR_SANITY_CHECK_MESSAGE = 56;
  byte PR_PUT_REPLY_MESSAGE = 57;
  byte PR_QUERY_MESSAGE = 58;
  byte PR_REMOVE_INDEXES_MESSAGE = 59;
  byte PR_REMOVE_INDEXES_REPLY_MESSAGE = 60;
  byte PR_SIZE_MESSAGE = 61;
  byte PR_SIZE_REPLY_MESSAGE = 62;
  byte PR_BUCKET_SIZE_REPLY_MESSAGE = 63;
  byte PR_CONTAINS_KEY_VALUE_REPLY_MESSAGE = 64;
  byte PR_FETCH_ENTRIES_REPLY_MESSAGE = 65;
  byte PR_FETCH_ENTRY_REPLY_MESSAGE = 66;
  byte PR_IDENTITY_REPLY_MESSAGE = 67;
  byte PR_INDEX_CREATION_REPLY_MSG = 68;
  byte PR_MANAGE_BUCKET_REPLY_MESSAGE = 69;

  // 70 available for reuse - retired in Geode v1.0
  // byte IP_ADDRESS = 70;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>UpdateOperation.UpdateMessage</code>.
   */
  byte UPDATE_MESSAGE = 71;

  /**
   * A header byte meaning that the next element in the stream is a <code>ReplyMessage</code>.
   */
  byte REPLY_MESSAGE = 72;

  /** <code>DestroyMessage</code> */
  byte PR_DESTROY = 73;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>CreateRegionReplyMessage</code>.
   */
  byte CREATE_REGION_REPLY_MESSAGE = 74;

  byte QUERY_MESSAGE = 75;
  byte RESPONSE_MESSAGE = 76;
  byte NET_SEARCH_REQUEST_MESSAGE = 77;
  byte NET_SEARCH_REPLY_MESSAGE = 78;
  byte NET_LOAD_REQUEST_MESSAGE = 79;
  byte NET_LOAD_REPLY_MESSAGE = 80;
  byte NET_WRITE_REQUEST_MESSAGE = 81;
  byte NET_WRITE_REPLY_MESSAGE = 82;

  // DLockRequestProcessor
  byte DLOCK_REQUEST_MESSAGE = 83;
  byte DLOCK_RESPONSE_MESSAGE = 84;
  // DLockReleaseMessage
  byte DLOCK_RELEASE_MESSAGE = 85;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>SystemMemberCacheMessage</code>.
   */
  // added for feature requests #32887
  byte ADMIN_CACHE_EVENT_MESSAGE = 86;

  byte CQ_ENTRY_EVENT = 87;

  // InitialImageOperation
  byte REQUEST_IMAGE_MESSAGE = 88;
  byte IMAGE_REPLY_MESSAGE = 89;
  byte IMAGE_ENTRY = 90;

  // CloseCacheMessage
  byte CLOSE_CACHE_MESSAGE = 91;

  byte DISTRIBUTED_MEMBER = 92;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>UpdateOperation.UpdateWithContextMessage</code>.
   */
  byte UPDATE_WITH_CONTEXT_MESSAGE = 93;

  // GrantorRequestProcessor
  byte GRANTOR_REQUEST_MESSAGE = 94;
  byte GRANTOR_INFO_REPLY_MESSAGE = 95;

  // StartupMessage
  byte STARTUP_MESSAGE = 96;

  // StartupResponseMessage
  byte STARTUP_RESPONSE_MESSAGE = 97;

  // ShutdownMessage
  byte SHUTDOWN_MESSAGE = 98;

  // DestroyRegionOperation
  byte DESTROY_REGION_MESSAGE = 99;

  byte PR_PUT_MESSAGE = 100;

  // InvalidateOperation
  byte INVALIDATE_MESSAGE = 101;

  // DestroyOperation
  byte DESTROY_MESSAGE = 102;

  // DistributionAdvisor
  byte DA_PROFILE = 103;

  // CacheDistributionAdvisor
  byte CACHE_PROFILE = 104;

  // EntryEventImpl
  byte ENTRY_EVENT = 105;

  // UpdateAttributesProcessor
  byte UPDATE_ATTRIBUTES_MESSAGE = 106;
  byte PROFILE_REPLY_MESSAGE = 107;

  // RegionEventImpl
  byte REGION_EVENT = 108;

  // TXId
  byte TRANSACTION_ID = 109;

  byte TX_COMMIT_MESSAGE = 110;

  byte HA_PROFILE = 111;

  byte ELDER_INIT_MESSAGE = 112;
  byte ELDER_INIT_REPLY_MESSAGE = 113;
  byte DEPOSE_GRANTOR_MESSAGE = 114;

  /**
   * A header byte meaning that the next element in the stream is a <code>HAEventWrapper</code>.
   */
  byte HA_EVENT_WRAPPER = 115;

  byte DLOCK_RELEASE_REPLY = 116;
  byte DLOCK_REMOTE_TOKEN = 117;

  // TXCommitMessage.CommitProcessForTXIdMessage
  byte COMMIT_PROCESS_FOR_TXID_MESSAGE = 118;

  byte FILTER_PROFILE = 119;

  byte PR_GET_MESSAGE = 120;

  // TXLockIdImpl
  byte TRANSACTION_LOCK_ID = 121;
  // TXCommitMessage.CommitProcessForLockIdMessage
  byte COMMIT_PROCESS_FOR_LOCKID_MESSAGE = 122;

  // NonGrantorDestroyedProcessor.NonGrantorDestroyedMessage (dlock)
  byte NON_GRANTOR_DESTROYED_MESSAGE = 123;

  // Token.EndOfStream
  byte END_OF_STREAM_TOKEN = 124;

  /** {@link org.apache.geode.internal.cache.partitioned.GetMessage.GetReplyMessage} */
  byte PR_GET_REPLY_MESSAGE = 125;

  /** {@link org.apache.geode.internal.cache.Node} */
  byte PR_NODE = 126;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>DestroyOperation.DestroyWithContextMessage</code>.
   */
  byte DESTROY_WITH_CONTEXT_MESSAGE = 127;

  // NOTE, CODES > 127 will take two bytes to serialize

  short PR_FETCH_PARTITION_DETAILS_MESSAGE = 128;
  short PR_FETCH_PARTITION_DETAILS_REPLY = 129;
  short PR_DEPOSE_PRIMARY_BUCKET_MESSAGE = 130;
  short PR_DEPOSE_PRIMARY_BUCKET_REPLY = 131;
  short PR_BECOME_PRIMARY_BUCKET_MESSAGE = 132;
  short PR_BECOME_PRIMARY_BUCKET_REPLY = 133;
  short PR_REMOVE_BUCKET_MESSAGE = 134;
  short PR_REMOVE_BUCKET_REPLY = 135;
  short PR_MOVE_BUCKET_MESSAGE = 136;
  short PR_MOVE_BUCKET_REPLY = 137;
  // Geode-5401, message changed from remove transaction to expire transactions.
  short EXPIRE_CLIENT_TRANSACTIONS = 138;

  short REGION_VERSION_VECTOR = 139;

  short INVALIDATE_WITH_CONTEXT_MESSAGE = 140;

  short TOKEN_INVALID = 141;
  short TOKEN_LOCAL_INVALID = 142;
  short TOKEN_DESTROYED = 143;
  short TOKEN_REMOVED = 144;
  short TOKEN_REMOVED2 = 145;

  short STARTUP_RESPONSE_WITHVERSION_MESSAGE = 146;
  short SHUTDOWN_ALL_GATEWAYHUBS_REQUEST = 147;

  short TOKEN_TOMBSTONE = 149;
  short PR_DESTROY_REPLY_MESSAGE = 150;
  short R_DESTROY_REPLY_MESSAGE = 151;

  short CLI_FUNCTION_RESULT = 152;

  short JMX_MANAGER_PROFILE = 153;
  short JMX_MANAGER_PROFILE_MESSAGE = 154;

  short R_FETCH_VERSION_MESSAGE = 155;
  short R_FETCH_VERSION_REPLY = 156;

  short PR_TOMBSTONE_MESSAGE = 157;

  short UPDATE_ENTRY_VERSION_MESSAGE = 158;
  short PR_UPDATE_ENTRY_VERSION_MESSAGE = 159;

  short SHUTDOWN_ALL_GATEWAYSENDERS_REQUEST = 160;
  short SHUTDOWN_ALL_GATEWAYSENDERS_RESPONSE = 161;

  short SHUTDOWN_ALL_GATEWAYRECEIVERS_REQUEST = 162;
  short SHUTDOWN_ALL_GATEWAYRECEIVERS_RESPONSE = 163;

  short TX_COMMIT_MESSAGE_701 = 164;
  short PR_FETCH_BULK_ENTRIES_MESSAGE = 165;
  short PR_FETCH_BULK_ENTRIES_REPLY_MESSAGE = 166;
  short NWAY_MERGE_RESULTS = 167;
  short CUMULATIVE_RESULTS = 168;
  short DISTTX_ROLLBACK_MESSAGE = 169;
  short DISTTX_ROLLBACK_REPLY_MESSAGE = 170;
  // 171..999 unused

  short ADD_HEALTH_LISTENER_REQUEST = 1000;
  short ADD_HEALTH_LISTENER_RESPONSE = 1001;
  short ADD_STAT_LISTENER_REQUEST = 1002;
  short ADD_STAT_LISTENER_RESPONSE = 1003;
  short ADMIN_CONSOLE_DISCONNECT_MESSAGE = 1004;
  short ADMIN_CONSOLE_MESSAGE = 1005;
  short ADMIN_FAILURE_RESPONSE = 1006;
  short ALERT_LEVEL_CHANGE_MESSAGE = 1007;
  short ALERT_LISTENER_MESSAGE = 1008;
  short APP_CACHE_SNAPSHOT_MESSAGE = 1009;
  short BRIDGE_SERVER_REQUEST = 1010;
  short BRIDGE_SERVER_RESPONSE = 1011;
  short CACHE_CONFIG_REQUEST = 1012;
  short CACHE_CONFIG_RESPONSE = 1013;
  short CACHE_INFO_REQUEST = 1014;
  short CACHE_INFO_RESPONSE = 1015;
  short CANCELLATION_MESSAGE = 1016;
  short CANCEL_STAT_LISTENER_REQUEST = 1017;
  short CANCEL_STAT_LISTENER_RESPONSE = 1018;
  short DESTROY_ENTRY_MESSAGE = 1019;
  short ADMIN_DESTROY_REGION_MESSAGE = 1020;
  short FETCH_DIST_LOCK_INFO_REQUEST = 1021;
  short FETCH_DIST_LOCK_INFO_RESPONSE = 1022;
  short FETCH_HEALTH_DIAGNOSIS_REQUEST = 1023;
  short FETCH_HEALTH_DIAGNOSIS_RESPONSE = 1024;
  short FETCH_HOST_REQUEST = 1025;
  short FETCH_HOST_RESPONSE = 1026;
  short FETCH_RESOURCE_ATTRIBUTES_REQUEST = 1027;
  short FETCH_RESOURCE_ATTRIBUTES_RESPONSE = 1028;
  short FETCH_STATS_REQUEST = 1029;
  short FETCH_STATS_RESPONSE = 1030;
  short FETCH_SYS_CFG_REQUEST = 1031;
  short FETCH_SYS_CFG_RESPONSE = 1032;
  short FLUSH_APP_CACHE_SNAPSHOT_MESSAGE = 1033;
  short HEALTH_LISTENER_MESSAGE = 1034;
  short LICENSE_INFO_REQUEST = 1035;
  short LICENSE_INFO_RESPONSE = 1036;
  short OBJECT_DETAILS_REQUEST = 1037;
  short OBJECT_DETAILS_RESPONSE = 1038;
  short OBJECT_NAMES_REQUEST = 1039;
  short OBJECT_NAMES_RESPONSE = 1040;
  short REGION_ATTRIBUTES_REQUEST = 1041;
  short REGION_ATTRIBUTES_RESPONSE = 1042;
  short REGION_REQUEST = 1043;
  short REGION_RESPONSE = 1044;
  short REGION_SIZE_REQUEST = 1045;
  short REGION_SIZE_RESPONSE = 1046;
  short REGION_STATISTICS_REQUEST = 1047;
  short REGION_STATISTICS_RESPONSE = 1048;
  short REMOVE_HEALTH_LISTENER_REQUEST = 1049;
  short REMOVE_HEALTH_LISTENER_RESPONSE = 1050;
  short RESET_HEALTH_STATUS_REQUEST = 1051;
  short RESET_HEALTH_STATUS_RESPONSE = 1052;
  short ROOT_REGION_REQUEST = 1053;
  short ROOT_REGION_RESPONSE = 1054;
  short SNAPSHOT_RESULT_MESSAGE = 1055;
  short STAT_LISTENER_MESSAGE = 1056;
  short STORE_SYS_CFG_REQUEST = 1057;
  short STORE_SYS_CFG_RESPONSE = 1058;
  short SUB_REGION_REQUEST = 1059;
  short SUB_REGION_RESPONSE = 1060;
  short TAIL_LOG_REQUEST = 1061;
  short TAIL_LOG_RESPONSE = 1062;
  short VERSION_INFO_REQUEST = 1063;
  short VERSION_INFO_RESPONSE = 1064;
  short STAT_ALERTS_MGR_ASSIGN_MESSAGE = 1065;
  short UPDATE_ALERTS_DEFN_MESSAGE = 1066;
  short REFRESH_MEMBER_SNAP_REQUEST = 1067;
  short REFRESH_MEMBER_SNAP_RESPONSE = 1068;
  short REGION_SUB_SIZE_REQUEST = 1069;
  short REGION_SUB_SIZE_RESPONSE = 1070;
  short CHANGE_REFRESH_INT_MESSAGE = 1071;
  short ALERTS_NOTIF_MESSAGE = 1072;
  short STAT_ALERT_DEFN_NUM_THRESHOLD = 1073;
  short STAT_ALERT_DEFN_GAUGE_THRESHOLD = 1074;
  short STAT_ALERT_NOTIFICATION = 1075;
  short FILTER_INFO_MESSAGE = 1076;
  short REQUEST_FILTERINFO_MESSAGE = 1077;
  short REQUEST_RVV_MESSAGE = 1078;
  short RVV_REPLY_MESSAGE = 1079;

  short CLIENT_MEMBERSHIP_MESSAGE = 1080;
  // 1,081...1,199 reserved for more admin msgs
  short PR_FUNCTION_STREAMING_MESSAGE = 1201;
  short MEMBER_FUNCTION_STREAMING_MESSAGE = 1202;
  short DR_FUNCTION_STREAMING_MESSAGE = 1203;
  short FUNCTION_STREAMING_REPLY_MESSAGE = 1204;
  short FUNCTION_STREAMING_ORDERED_REPLY_MESSAGE = 1205;
  short REQUEST_SYNC_MESSAGE = 1206;

  // 1,209..1,999 unused

  short HIGH_PRIORITY_ACKED_MESSAGE = 2000;
  short SERIAL_ACKED_MESSAGE = 2001;
  short CLIENT_DATASERIALIZER_MESSAGE = 2002;

  // 2003..2098 unused

  short BUCKET_COUNT_LOAD_PROBE = 2099;
  short PERSISTENT_MEMBERSHIP_VIEW_REQUEST = 2100;
  short PERSISTENT_MEMBERSHIP_VIEW_REPLY = 2101;
  short PERSISTENT_STATE_QUERY_REQUEST = 2102;
  short PERSISTENT_STATE_QUERY_REPLY = 2103;
  short PREPARE_NEW_PERSISTENT_MEMBER_REQUEST = 2104;
  short MISSING_PERSISTENT_IDS_REQUEST = 2105;
  short MISSING_PERSISTENT_IDS_RESPONSE = 2106;
  short REVOKE_PERSISTENT_ID_REQUEST = 2107;
  short REVOKE_PERSISTENT_ID_RESPONSE = 2108;
  short REMOVE_PERSISTENT_MEMBER_REQUEST = 2109;
  short PERSISTENT_MEMBERSHIP_FLUSH_REQUEST = 2110;
  short SHUTDOWN_ALL_REQUEST = 2111;
  short SHUTDOWN_ALL_RESPONSE = 2112;
  short END_BUCKET_CREATION_MESSAGE = 2113;

  short FINISH_BACKUP_REQUEST = 2114;
  short FINISH_BACKUP_RESPONSE = 2115;
  short PREPARE_BACKUP_REQUEST = 2116;
  short BACKUP_RESPONSE = 2117;
  short COMPACT_REQUEST = 2118;
  short COMPACT_RESPONSE = 2119;
  short FLOW_CONTROL_PERMIT_MESSAGE = 2120;

  short OBJECT_PART_LIST66 = 2121;
  short LINKED_RESULTSET = 2122;
  short LINKED_STRUCTSET = 2123;
  short PR_ALL_BUCKET_PROFILES_UPDATE_MESSAGE = 2124;

  short SERIALIZED_OBJECT_PART_LIST = 2125;
  short FLUSH_TO_DISK_REQUEST = 2126;
  short FLUSH_TO_DISK_RESPONSE = 2127;

  short CHECK_TYPE_REGISTRY_STATE = 2128;
  short PREPARE_REVOKE_PERSISTENT_ID_REQUEST = 2129;
  short MISSING_PERSISTENT_IDS_RESPONSE_662 = 2130;

  short PERSISTENT_VERSION_TAG = 2131;
  short PERSISTENT_RVV = 2132;
  short DISK_STORE_ID = 2133;

  short SNAPSHOT_PACKET = 2134;
  short SNAPSHOT_RECORD = 2135;

  short FLOW_CONTROL_ACK = 2136;
  short FLOW_CONTROL_ABORT = 2137;

  short REMOTE_LOCATOR_RESPONSE = 2138;
  short LOCATOR_JOIN_MESSAGE = 2139;

  short PARALLEL_QUEUE_BATCH_REMOVAL_MESSAGE = 2140;
  short PARALLEL_QUEUE_BATCH_REMOVAL_REPLY = 2141;

  // 2141 unused
  short REMOTE_LOCATOR_PING_REQUEST = 2142;
  short REMOTE_LOCATOR_PING_RESPONSE = 2143;
  short GATEWAY_SENDER_PROFILE = 2144;
  short REMOTE_LOCATOR_JOIN_REQUEST = 2145;
  short REMOTE_LOCATOR_JOIN_RESPONSE = 2146;
  short REMOTE_LOCATOR_REQUEST = 2147;


  short BATCH_DESTROY_MESSAGE = 2148;

  short MANAGER_STARTUP_MESSAGE = 2149;

  short JMX_MANAGER_LOCATOR_REQUEST = 2150;
  short JMX_MANAGER_LOCATOR_RESPONSE = 2151;

  short MGMT_COMPACT_REQUEST = 2152;
  short MGMT_COMPACT_RESPONSE = 2153;

  short MGMT_FEDERATION_COMPONENT = 2154;

  short LOCATOR_STATUS_REQUEST = 2155;
  short LOCATOR_STATUS_RESPONSE = 2156;
  short RELEASE_CLEAR_LOCK_MESSAGE = 2157;
  short NULL_TOKEN = 2158;

  short CONFIGURATION_RESPONSE = 2160;

  short PARALLEL_QUEUE_REMOVAL_MESSAGE = 2161;

  short PR_QUERY_TRACE_INFO = 2162;

  short INDEX_CREATION_DATA = 2163;

  short SERVER_PING_MESSAGE = 2164;
  short PR_DESTROY_ON_DATA_STORE_MESSAGE = 2165;

  short DIST_TX_OP = 2166;
  short DIST_TX_PRE_COMMIT_RESPONSE = 2167;
  short DIST_TX_THIN_ENTRY_STATE = 2168;

  short LUCENE_CHUNK_KEY = 2169;
  short LUCENE_FILE = 2170;
  short LUCENE_FUNCTION_CONTEXT = 2171;
  short LUCENE_STRING_QUERY_PROVIDER = 2172;
  short LUCENE_TOP_ENTRIES_COLLECTOR_MANAGER = 2173;
  short LUCENE_ENTRY_SCORE = 2174;
  short LUCENE_TOP_ENTRIES = 2175;
  short LUCENE_TOP_ENTRIES_COLLECTOR = 2176;
  short WAIT_UNTIL_FLUSHED_FUNCTION_CONTEXT = 2177;
  short DESTROY_LUCENE_INDEX_MESSAGE = 2178;
  short LUCENE_PAGE_RESULTS = 2179;
  short LUCENE_RESULT_STRUCT = 2180;
  short GATEWAY_SENDER_QUEUE_ENTRY_SYNCHRONIZATION_MESSAGE = 2181;
  short GATEWAY_SENDER_QUEUE_ENTRY_SYNCHRONIZATION_ENTRY = 2182;
  short ABORT_BACKUP_REQUEST = 2183;

  // NOTE, codes > 65535 will take 4 bytes to serialize

  /**
   * This special code is a way for an implementor if this interface to say that it does not have a
   * fixed id. In that case its class name is serialized. Currently only test classes just return
   * this code.
   */
  int NO_FIXED_ID = Integer.MAX_VALUE;

  //////////////// END CODES ////////////

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  int getDSFID();

  /**
   * Writes the state of this object as primitive data to the given <code>DataOutput</code>.<br>
   * <br>
   * Note: For rolling upgrades, if there is a change in the object format from previous version,
   * add a new toDataPre_GFE_X_X_X_X() method and add an entry for the current {@link Version} in
   * the getSerializationVersions array of the implementing class. e.g. if msg format changed in
   * version 80, create toDataPre_GFE_8_0_0_0, add Version.GFE_80 to the getSerializationVersions
   * array and copy previous toData contents to this newly created toDataPre_GFE_X_X_X_X() method.
   *
   * @throws IOException A problem occurs while writing to <code>out</code>
   */
  void toData(DataOutput out) throws IOException;

  /**
   * Reads the state of this object as primitive data from the given <code>DataInput</code>. <br>
   * <br>
   * Note: For rolling upgrades, if there is a change in the object format from previous version,
   * add a new fromDataPre_GFE_X_X_X_X() method and add an entry for the current {@link Version} in
   * the getSerializationVersions array of the implementing class. e.g. if msg format changed in
   * version 80, create fromDataPre_GFE_8_0_0_0, add Version.GFE_80 to the getSerializationVersions
   * array and copy previous fromData contents to this newly created fromDataPre_GFE_X_X_X_X()
   * method.
   *
   * @throws IOException A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException A class could not be loaded while reading from <code>in</code>
   */
  void fromData(DataInput in) throws IOException, ClassNotFoundException;


}

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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.*;

import java.io.*;

/**
 * An interface that implements data serialization
 * for internal GemFire product classes that have a fixed id.
 * The fixed id is used
 * to represent the class, on the wire, at serialization time and used
 * in a switch statement at deserialization time.
 * All the codes should be static final in this class.
 * <p> Implementors MUST have a public zero-arg constructor.
 *
 * <p> Note that this class is for internal use only. Customer classes
 * that want to do something similiar should implement a subclass of
 * {@link DataSerializer} or {@link Instantiator}.
 *
 * <p>
 * To add a new DataSerializableFixedID do this following:
 * <ol>
 * <li> Define a constant with an id that is free and put it in
 *    <code>DataSerializableFixedID</code> as a "public static final byte".
 *    Make sure and update the "unused" comments to no longer mention your new id.
 *    If implementing a class used only for tests then there is no need to consume
 *    a fixed id and you should use {@link #NO_FIXED_ID}. In this case you can skip
 *    steps 3 and 4.
 * <li> Define a method in the class that
 *    implements <code>DataSerializableFixedID</code>
 *    named {@link #getDSFID} that returns the constant from step 1.
 * <li> Define a private static method in {@link DSFIDFactory} that returns an instance
 *    of the class from step 2 usually be calling its zero-arg constructor
 *    and then calling fromData(in).
 *    See the end of {@link DSFIDFactory} for examples.
 * <li> Add a case statement in {@link DSFIDFactory#create} for the constant
 *    from step 1 that calls the method from step 3.
 * <li> Implement {@link #toData} and {@link #fromData} just like you
 *    would on a <code>DataSerializer</code>. Make sure you follow the javadocs 
 *    for these methods to add support for rolling upgrades.
 * </ol>
 *
 * @see DataSerializer
 *
 * @author Darrel Schneider
 * @since 5.7
 */
public interface DataSerializableFixedID extends SerializationVersions {

  // NOTE, codes < -65536 will take 4 bytes to serialize
  // NOTE, codes < -128 will take 2 bytes to serialize

  /* In the class to be serialized, add
  public FOO(DataInput in) throws IOException, ClassNotFoundException {
    fromData(in);
  }
  
  public int getDSFID() {
    return FOO;
  }
  
  In DataSerializableFixedId, allocate an ID for the class
  public static final byte FOO = -54;

  In DSFIDFactory, add a case for the new class
    case FOO:
      return new FOO(in);
  */
  public static final short HDFS_GATEWAY_EVENT_IMPL = -141;
  public static final short SNAPPY_COMPRESSED_CACHED_DESERIALIZABLE = -140;
  
  public static final short GATEWAY_EVENT_IMPL = -136;
  public static final short GATEWAY_SENDER_EVENT_CALLBACK_ARGUMENT = -135;
  public static final short GATEWAY_SENDER_EVENT_IMPL = -134;

  public static final short CLIENT_TOMBSTONE_MESSAGE = -133;
  
  public static final short R_REGION_OP_REPLY = -132;
  public static final short R_REGION_OP = -131;
  
  public static final short WAIT_FOR_VIEW_INSTALLATION = -130;
  
  public static final short DISPATCHED_AND_CURRENT_EVENTS = -129;
  
  public static final byte DLOCK_QUERY_MESSAGE = -128;
  public static final byte DLOCK_QUERY_REPLY = -127;

  public static final byte CLIENT_HEALTH_STATS = -126;
  public static final byte PR_MANAGE_BACKUP_BUCKET_MESSAGE = -125;
  public static final byte PR_MANAGE_BACKUP_BUCKET_REPLY_MESSAGE = -124;
  public static final byte SIZED_BASED_LOAD_PROBE = -123;

  public static final byte CLIENT_PING_MESSAGE_IMPL = -122;
  
  public static final byte REMOTE_PUTALL_REPLY_MESSAGE = -121;
  public static final byte VERSION_TAG = -120;

  public static final byte REMOTE_PUTALL_MESSAGE = -119;

  public static final byte ADD_CACHESERVER_PROFILE_UPDATE = -118;  
  public static final byte SERVER_INTEREST_REGISTRATION_MESSAGE = -117;
  public static final byte FILTER_PROFILE_UPDATE = -116;
  // [sumedh] below two IDs are no longer used in new TX model and will be
  // removed at some point after SQLF upmerge
  public static final byte JTA_AFTER_COMPLETION_MESSAGE = -115;
  public static final byte JTA_BEFORE_COMPLETION_MESSAGE = -114;
  public static final byte INVALIDATE_PARTITIONED_REGION_MESSAGE = -113;
  public static final byte TX_REMOTE_COMMIT_MESSAGE = -112;
  public static final byte TX_REMOTE_ROLLBACK_MESSAGE = -111;
  public static final byte PR_PUTALL_REPLY_MESSAGE = -110;
  public static final byte PR_PUTALL_MESSAGE = -109;
  public static final byte RESOURCE_PROFILE_MESSAGE = -108;
  public static final byte RESOURCE_MANAGER_PROFILE = -107;
  public static final byte PR_CREATE_BUCKET_MESSAGE = -106;
  public static final byte PR_CREATE_BUCKET_REPLY_MESSAGE = -105;
  public static final byte DISTRIBUTED_REGION_FUNCTION_MESSAGE = -104; 
  public static final byte DISTRIBUTED_REGION_FUNCTION_REPLY_MESSAGE = -103;
  
  public static final byte MEMBER_FUNCTION_MESSAGE = -102; 
  public static final byte MEMBER_FUNCTION_REPLY_MESSAGE = -101;   
  public static final byte PARTITION_REGION_CONFIG = -100;
  public static final byte PR_FETCH_KEYS_REPLY_MESSAGE = -99;
  public static final byte PR_DUMP_B2N_REGION_MSG = -98;
  public static final byte PR_DUMP_B2N_REPLY_MESSAGE = -97;
  public static final byte PR_INVALIDATE_MESSAGE = -96;
  public static final byte PR_INVALIDATE_REPLY_MESSAGE = -95;
  public static final byte PR_FUNCTION_MESSAGE = -94;
  public static final byte PR_FUNCTION_REPLY_MESSAGE = -93;

  public static final byte PROFILES_REPLY_MESSAGE = -92;
  public static final byte CACHE_SERVER_PROFILE = -91;
  public static final byte CONTROLLER_PROFILE = -90;

  public static final byte CREATE_REGION_MESSAGE = -89;
  public static final byte DESTROY_PARTITIONED_REGION_MESSAGE = -88;
  // [sumedh] below two IDs are no longer used in new TX model and will be
  // removed at some point after SQLF upmerge
  public static final byte COMMIT_PROCESS_QUERY_MESSAGE = -87;
  public static final byte COMMIT_PROCESS_QUERY_REPLY_MESSAGE = -86;
  public static final byte DESTROY_REGION_WITH_CONTEXT_MESSAGE = -85;
  public static final byte PUT_ALL_MESSAGE = -84;
  public static final byte CLEAR_REGION_MESSAGE = -83;
  public static final byte INVALIDATE_REGION_MESSAGE = -82;
  public static final byte SEND_QUEUE_MESSAGE = -81;
  public static final byte STATE_MARKER_MESSAGE = -80;
  public static final byte STATE_STABILIZATION_MESSAGE = -79;
  public static final byte STATE_STABILIZED_MESSAGE = -78;
  public static final byte CLIENT_MARKER_MESSAGE_IMPL = -77;
  // [sumedh] below three IDs are no longer used in new TX model and will be
  // removed at some point after SQLF upmerge
  public static final byte TX_LOCK_UPDATE_PARTICIPANTS_MESSAGE = -76;
  public static final byte TX_ORIGINATOR_RECOVERY_MESSAGE = -75;
  public static final byte TX_ORIGINATOR_RECOVERY_REPLY_MESSAGE = -74;
  public static final byte QUEUE_REMOVAL_MESSAGE = -73;
  public static final byte DLOCK_RECOVER_GRANTOR_MESSAGE = -72;
  public static final byte DLOCK_RECOVER_GRANTOR_REPLY_MESSAGE = -71;
  public static final byte NON_GRANTOR_DESTROYED_REPLY_MESSAGE = -70;
  public static final byte TOMBSTONE_MESSAGE = -69;
  public static final byte IDS_REGISTRATION_MESSAGE = -68;
  // [sumedh] below ID is no longer used in new TX model and will be
  // removed at some point after SQLF upmerge
  public static final byte TX_LOCK_UPDATE_PARTICIPANTS_REPLY_MESSAGE = -67;
  public static final byte STREAMING_REPLY_MESSAGE = -66;
  public static final byte PREFER_BYTES_CACHED_DESERIALIZABLE = -65;
  public static final byte VM_CACHED_DESERIALIZABLE = -64;
  public static final byte GATEWAY_EVENT_IMPL_66 = -63;
  public static final byte SUSPEND_LOCKING_TOKEN = -62;
  public static final byte OBJECT_TYPE_IMPL = -61;
  public static final byte STRUCT_TYPE_IMPL = -60;
  public static final byte COLLECTION_TYPE_IMPL = -59;
  public static final byte TX_LOCK_BATCH = -58;
  public static final byte STORE_ALL_CACHED_DESERIALIZABLE = -57;
  public static final byte GATEWAY_EVENT_CALLBACK_ARGUMENT = -56;
  public static final byte MAP_TYPE_IMPL = -55;
  public static final byte LOCATOR_LIST_REQUEST = -54;
  public static final byte CLIENT_CONNECTION_REQUEST = -53;
  public static final byte QUEUE_CONNECTION_REQUEST = -52;
  public static final byte LOCATOR_LIST_RESPONSE = -51;
  public static final byte CLIENT_CONNECTION_RESPONSE = -50;
  public static final byte QUEUE_CONNECTION_RESPONSE = -49;
  public static final byte CLIENT_REPLACEMENT_REQUEST = -48;
  
  public static final byte INTEREST_EVENT_MESSAGE = -47;
  public static final byte INTEREST_EVENT_REPLY_MESSAGE = -46;
  public static final byte CLIENT_BLACKLIST_MESSAGE = -45;
  public static final byte REMOVE_CLIENT_FROM_BLACKLIST_MESSAGE = -44;
  public static final byte GET_ALL_SERVERS_REQUEST = -43;
  public static final byte GET_ALL_SERVRES_RESPONSE = -42;

  // [sumedh] below two IDs are no longer used in new TX model and will be
  // removed at some point after SQLF upmerge
  public static final byte FIND_REMOTE_TX_REPLY = -41;
  public static final byte FIND_REMOTE_TX_MESSAGE = -40;

  public static final byte R_REMOTE_COMMIT_REPLY_MESSAGE = -39;

  public static final byte R_FETCH_KEYS_REPLY = -38;
  public static final byte R_FETCH_KEYS_MESSAGE = -37;

  public static final byte R_SIZE_MESSAGE = -36;
  public static final byte R_SIZE_REPLY_MESSAGE = -35;

  public static final byte R_FETCH_ENTRY_REPLY_MESSAGE = -34;
  public static final byte R_FETCH_ENTRY_MESSAGE = -33;
  public static final byte R_DESTROY_MESSAGE = -32;
  public static final byte R_INVALIDATE_MESSAGE = -31;
  public static final byte R_INVALIDATE_REPLY_MESSAGE = -30;
  
  public static final byte R_PUT_MESSAGE = -29;
  public static final byte R_PUT_REPLY_MESSAGE = -28;
  
  public static final byte R_CONTAINS_MESSAGE = -27;
  public static final byte R_CONTAINS_REPLY_MESSAGE = -26;
  
  public static final byte R_GET_MESSAGE = -24;
  public static final byte R_GET_REPLY_MESSAGE = -25;

  public static final byte DURABLE_CLIENT_INFO_RESPONSE = -23;
  
  public static final byte DURABLE_CLIENT_INFO_REQUEST = -22;
  
  public static final byte CLIENT_INTEREST_MESSAGE = -21;

  /**
   * A header byte meaning that the next element in the stream is a
   * type meant for SQL Fabric.
   */
  public static final byte SQLF_TYPE = -20;

  /**
   * A header byte meaning that the next element in the stream is a
   * DVD object used for SQL Fabric.
   */
  public static final byte SQLF_DVD_OBJECT = -19;
  
  /**
   * A header byte meaning that the next element in the stream is a
   * GlobalRowLocation object used for SQL Fabric.
   */
  public static final byte SQLF_GLOBAL_ROWLOC = -18;
  
  /**
   * A header byte meaning that the next element in the stream is a
   * GemFireKey object used for SQL Fabric.
   */
  public static final byte SQLF_GEMFIRE_KEY = -17;
  
  /**
   * A header byte meaning that the next element in the stream is a
   * FormatibleBitSet object in SQLFabric.
   */
  public static final byte SQLF_FORMATIBLEBITSET = -16;

  // IDs -15 .. -10 are not used in trunk yet but only in SQLFire, so marking
  // as used so that GemFire does not use them until the SQLF upmerge else
  // there will be big problems in backward compatibility after upmerge which
  // is required for both >= SQLF 1.1 and >= GFE 7.1

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>VMIdProfile</code>.
   */
  public static final byte VMID_PROFILE_MESSAGE = -15;

  /**
   * A header byte meaning that the next element in the stream is a
   * <code>LocalRegion.UUIDProfile</code>.
   */
  public static final byte REGION_UUID_PROFILE_MESSAGE = -14;

  public static final short TX_REMOTE_COMMIT_PHASE1_MESSAGE = -13;
  public static final short TX_BATCH_MESSAGE = -12;
  public static final short TX_CLEANUP_ENTRY_MESSAGE = -11;
  public static final short TX_BATCH_REPLY_MESSAGE = -10;

  public static final byte PR_REMOVE_ALL_MESSAGE = -9;
  public static final byte REMOVE_ALL_MESSAGE = -8;
  public static final byte PR_REMOVE_ALL_REPLY_MESSAGE = -7;
  public static final byte REMOTE_REMOVE_ALL_MESSAGE = -6;
  public static final byte REMOTE_REMOVE_ALL_REPLY_MESSAGE = -5;
  public static final byte DISTTX_COMMIT_MESSAGE = -4;
  public static final byte DISTTX_PRE_COMMIT_MESSAGE = -3;
  public static final byte DISTTX_COMMIT_REPLY_MESSAGE = -2;
  public static final byte DISTTX_PRE_COMMIT_REPLY_MESSAGE = -1;
  
  public static final byte ILLEGAL = 0;

  public static final byte JGROUPS_VIEW = 1;
  
  public static final byte JGROUPS_JOIN_RESP = 2;
  
  public static final byte PUTALL_VERSIONS_LIST = 3;

  public static final byte INITIAL_IMAGE_VERSIONED_OBJECT_LIST = 4;

  public static final byte FIND_VERSION_TAG = 5;
  
  public static final byte VERSION_TAG_REPLY = 6;
  
  public static final byte VERSIONED_OBJECT_LIST = 7;

  public static final byte ENUM_ID = 8;
  public static final byte ENUM_INFO = 9;

  
  /** 
   * A header byte meaning that the next element in the stream is 
   * a <code>InitialImageOperation.EventStateMessage</code>. 
   */ 
  public static final byte REGION_STATE_MESSAGE = 10;
  
  /** A header byte meaning that the next element in the stream is a
   * <code>ClientInstantiatorMessage</code>.
   */
  public static final byte CLIENT_INSTANTIATOR_MESSAGE = 11;

  /** A header byte meaning that the next element in the stream is a
   * <code>InternalInstantiator.RegistrationMessage</code>.
   */
  public static final byte REGISTRATION_MESSAGE = 12;

  /** A header byte meaning that the next element in the stream is a
   * <code>InternalInstantiator.RegistrationContextMessage</code>.
   */
  public static final byte REGISTRATION_CONTEXT_MESSAGE = 13;

  /** More Query Result Classes */
  // PRQueryProcessor.EndOfBucket
  public static final byte END_OF_BUCKET = 14;
  public static final byte RESULTS_BAG = 15;
  public static final byte STRUCT_BAG = 16;

  public static final byte BUCKET_PROFILE = 17;
  public static final byte PARTITION_PROFILE = 18;

  public static final byte ROLE_EVENT = 19;
  public static final byte CLIENT_REGION_EVENT = 20;

  public static final byte CONCURRENT_HASH_MAP = 21;
  public static final byte FIND_DURABLE_QUEUE = 22;
  public static final byte FIND_DURABLE_QUEUE_REPLY = 23;
  public static final byte CACHE_SERVER_LOAD_MESSAGE = 24;

  /** A header byte meaning that the next element in the stream is a
   * <code>ObjectPartList</code>.
   */
  public static final byte OBJECT_PART_LIST = 25;

  public static final byte REGION = 26;

  /****** Query Result Classes *******/
  public static final byte RESULTS_COLLECTION_WRAPPER = 27;
  public static final byte RESULTS_SET = 28;
  public static final byte SORTED_RESULT_SET = 29;
  public static final byte SORTED_STRUCT_SET = 30;
  public static final byte UNDEFINED = 31;
  public static final byte STRUCT_IMPL = 32;
  public static final byte STRUCT_SET = 33;

  /** A header byte meaning that the next element in the stream is a
   * <code>ClearRegionWithContextMessage</code>. */
  public static final byte CLEAR_REGION_MESSAGE_WITH_CONTEXT = 34;

  /** A header byte meaning that the next element in the stream is a
   * <code>ClientUpdateMessage</code>. */
  public static final byte CLIENT_UPDATE_MESSAGE = 35;

  /** A header byte meaning that the next element in the stream is a
   * <code>EventID</code>. */
  public static final byte EVENT_ID = 36;

  public static final byte INTEREST_RESULT_POLICY = 37;
  /** A header byte meaning that the next element in the stream is a
   * <code>ClientProxyMembershipID</code>. */
  public static final byte CLIENT_PROXY_MEMBERSHIPID = 38;

  public static final byte PR_BUCKET_BACKUP_MESSAGE = 39;
  public static final byte SERVER_BUCKET_PROFILE = 40;
  public static final byte PR_BUCKET_PROFILE_UPDATE_MESSAGE = 41;
  public static final byte PR_BUCKET_SIZE_MESSAGE = 42;
  public static final byte PR_CONTAINS_KEY_VALUE_MESSAGE = 43;
  public static final byte PR_DUMP_ALL_PR_CONFIG_MESSAGE = 44;
  public static final byte PR_DUMP_BUCKETS_MESSAGE = 45;
  public static final byte PR_FETCH_ENTRIES_MESSAGE = 46;
  public static final byte PR_FETCH_ENTRY_MESSAGE = 47;
  public static final byte PR_FETCH_KEYS_MESSAGE = 48;
  public static final byte PR_FLUSH_MESSAGE = 49;
  public static final byte PR_IDENTITY_REQUEST_MESSAGE = 50;
  public static final byte PR_IDENTITY_UPDATE_MESSAGE = 51;
  public static final byte PR_INDEX_CREATION_MSG = 52;
  public static final byte PR_MANAGE_BUCKET_MESSAGE = 53;
  public static final byte PR_PRIMARY_REQUEST_MESSAGE = 54;
  public static final byte PR_PRIMARY_REQUEST_REPLY_MESSAGE = 55;
  public static final byte PR_SANITY_CHECK_MESSAGE = 56;
  public static final byte PR_PUT_REPLY_MESSAGE = 57;
  public static final byte PR_QUERY_MESSAGE = 58;
  public static final byte PR_REMOVE_INDEXES_MESSAGE = 59;
  public static final byte PR_REMOVE_INDEXES_REPLY_MESSAGE = 60;
  public static final byte PR_SIZE_MESSAGE = 61;
  public static final byte PR_SIZE_REPLY_MESSAGE = 62;
  public static final byte PR_BUCKET_SIZE_REPLY_MESSAGE = 63;
  public static final byte PR_CONTAINS_KEY_VALUE_REPLY_MESSAGE = 64;
  public static final byte PR_FETCH_ENTRIES_REPLY_MESSAGE = 65;
  public static final byte PR_FETCH_ENTRY_REPLY_MESSAGE = 66;
  public static final byte PR_IDENTITY_REPLY_MESSAGE = 67;
  public static final byte PR_INDEX_CREATION_REPLY_MSG = 68;
  public static final byte PR_MANAGE_BUCKET_REPLY_MESSAGE = 69;
  
  /** A header byte meaning that the next element in the stream is a
   * <code>IpAddress</code>. */
  public static final byte IP_ADDRESS = 70;

  /** A header byte meaning that the next element in the stream is a
   * <code>UpdateOperation.UpdateMessage</code>. */
  public static final byte UPDATE_MESSAGE = 71;

  /** A header byte meaning that the next element in the stream is a
   * <code>ReplyMessage</code>. */
  public static final byte REPLY_MESSAGE = 72;

  /** <code>DestroyMessage</code> */
  public static final byte PR_DESTROY = 73;

  /** A header byte meaning that the next element in the stream is a
   * <code>CreateRegionReplyMessage</code>. */
  public static final byte CREATE_REGION_REPLY_MESSAGE = 74;

  public static final byte QUERY_MESSAGE = 75;
  public static final byte RESPONSE_MESSAGE = 76;
  public static final byte NET_SEARCH_REQUEST_MESSAGE = 77;
  public static final byte NET_SEARCH_REPLY_MESSAGE = 78;
  public static final byte NET_LOAD_REQUEST_MESSAGE = 79;
  public static final byte NET_LOAD_REPLY_MESSAGE = 80;
  public static final byte NET_WRITE_REQUEST_MESSAGE = 81;
  public static final byte NET_WRITE_REPLY_MESSAGE = 82;

  // DLockRequestProcessor
  public static final byte DLOCK_REQUEST_MESSAGE = 83;
  public static final byte DLOCK_RESPONSE_MESSAGE = 84;
  // DLockReleaseMessage
  public static final byte DLOCK_RELEASE_MESSAGE = 85;

  /** A header byte meaning that the next element in the stream is a
   * <code>SystemMemberCacheMessage</code>. */
  //added for feature requests #32887
  public static final byte ADMIN_CACHE_EVENT_MESSAGE = 86;
  
  public static final byte CQ_ENTRY_EVENT = 87;
  
  // InitialImageOperation
  public static final byte REQUEST_IMAGE_MESSAGE = 88;
  public static final byte IMAGE_REPLY_MESSAGE = 89;
  public static final byte IMAGE_ENTRY = 90;

  // CloseCacheMessage
  public static final byte CLOSE_CACHE_MESSAGE = 91;

  public static final byte DISTRIBUTED_MEMBER = 92;

  /** A header byte meaning that the next element in the stream is a
   * <code>UpdateOperation.UpdateWithContextMessage</code>. */
  public static final byte UPDATE_WITH_CONTEXT_MESSAGE = 93;

  // GrantorRequestProcessor
  public static final byte GRANTOR_REQUEST_MESSAGE = 94;
  public static final byte GRANTOR_INFO_REPLY_MESSAGE = 95;

  // StartupMessage
  public static final byte STARTUP_MESSAGE = 96;

  // StartupResponseMessage
  public static final byte STARTUP_RESPONSE_MESSAGE = 97;

  // ShutdownMessage
  public static final byte SHUTDOWN_MESSAGE = 98;

  // DestroyRegionOperation
  public static final byte DESTROY_REGION_MESSAGE = 99;

  public static final byte PR_PUT_MESSAGE = 100;

  // InvalidateOperation
  public static final byte INVALIDATE_MESSAGE = 101;

  // DestroyOperation
  public static final byte DESTROY_MESSAGE = 102;

  // DistributionAdvisor
  public static final byte DA_PROFILE = 103;

  // CacheDistributionAdvisor
  public static final byte CACHE_PROFILE = 104;

  // EntryEventImpl
  public static final byte ENTRY_EVENT = 105;

  // UpdateAttributesProcessor
  public static final byte UPDATE_ATTRIBUTES_MESSAGE = 106;
  public static final byte PROFILE_REPLY_MESSAGE = 107;

  // RegionEventImpl
  public static final byte REGION_EVENT = 108;

  // TXId
  public static final byte TRANSACTION_ID = 109;

  // [sumedh] below ID is no longer used in new TX model and will be
  // removed at some point after SQLF upmerge
  public static final byte TX_COMMIT_MESSAGE = 110;

  public static final byte HA_PROFILE = 111;
    
  public static final byte ELDER_INIT_MESSAGE = 112;
  public static final byte ELDER_INIT_REPLY_MESSAGE = 113;
  public static final byte DEPOSE_GRANTOR_MESSAGE = 114;

  /** A header byte meaning that the next element in the stream is a
   * <code>HAEventWrapper</code>. */
  public static final byte HA_EVENT_WRAPPER = 115;

  public static final byte DLOCK_RELEASE_REPLY = 116;
  public static final byte DLOCK_REMOTE_TOKEN = 117;

  // TXCommitMessage.CommitProcessForTXIdMessage
  public static final byte COMMIT_PROCESS_FOR_TXID_MESSAGE = 118;

  public static final byte FILTER_PROFILE = 119;

  public static final byte PR_GET_MESSAGE = 120;

  // [sumedh] below two IDs are no longer used in new TX model and will be
  // removed at some point after SQLF upmerge
  // TXLockIdImpl
  public static final byte TRANSACTION_LOCK_ID = 121;
  // TXCommitMessage.CommitProcessForLockIdMessage
  public static final byte COMMIT_PROCESS_FOR_LOCKID_MESSAGE = 122;

  // NonGrantorDestroyedProcessor.NonGrantorDestroyedMessage (dlock)
  public static final byte NON_GRANTOR_DESTROYED_MESSAGE = 123;

  // Token.EndOfStream
  public static final byte END_OF_STREAM_TOKEN = 124;

  /** {@link com.gemstone.gemfire.internal.cache.partitioned.GetMessage.GetReplyMessage} */
  public static final byte PR_GET_REPLY_MESSAGE = 125;

  /** {@link com.gemstone.gemfire.internal.cache.Node} */
  public static final byte PR_NODE = 126;

  /** A header byte meaning that the next element in the stream is a
   * <code>DestroyOperation.DestroyWithContextMessage</code>. */
  public static final byte DESTROY_WITH_CONTEXT_MESSAGE = 127;

  // NOTE, CODES > 127 will take two bytes to serialize

  public static final short PR_FETCH_PARTITION_DETAILS_MESSAGE = 128;
  public static final short PR_FETCH_PARTITION_DETAILS_REPLY = 129;
  public static final short PR_DEPOSE_PRIMARY_BUCKET_MESSAGE = 130;
  public static final short PR_DEPOSE_PRIMARY_BUCKET_REPLY = 131;
  public static final short PR_BECOME_PRIMARY_BUCKET_MESSAGE = 132;
  public static final short PR_BECOME_PRIMARY_BUCKET_REPLY = 133;
  public static final short PR_REMOVE_BUCKET_MESSAGE = 134;
  public static final short PR_REMOVE_BUCKET_REPLY = 135;
  public static final short PR_MOVE_BUCKET_MESSAGE = 136;
  public static final short PR_MOVE_BUCKET_REPLY = 137;
  public static final short TX_MANAGER_REMOVE_TRANSACTIONS = 138;

  public static final short REGION_VERSION_VECTOR = 139;
  
  public static final short INVALIDATE_WITH_CONTEXT_MESSAGE = 140;
  
  public static final short TOKEN_INVALID = 141;
  public static final short TOKEN_LOCAL_INVALID = 142;
  public static final short TOKEN_DESTROYED = 143;
  public static final short TOKEN_REMOVED = 144;
  public static final short TOKEN_REMOVED2 = 145;

  public static final short STARTUP_RESPONSE_WITHVERSION_MESSAGE = 146;
  public static final short SHUTDOWN_ALL_GATEWAYHUBS_REQUEST = 147;
  
  public static final short TOKEN_TOMBSTONE = 149;
  public static final short PR_DESTROY_REPLY_MESSAGE = 150;
  public static final short R_DESTROY_REPLY_MESSAGE = 151;

  public static final short CLI_FUNCTION_RESULT = 152;
  
  public static final short JMX_MANAGER_PROFILE = 153;
  public static final short JMX_MANAGER_PROFILE_MESSAGE = 154;

  public static final short R_FETCH_VERSION_MESSAGE = 155;
  public static final short R_FETCH_VERSION_REPLY = 156;
  
  public static final short PR_TOMBSTONE_MESSAGE = 157;
  
  public static final short UPDATE_ENTRY_VERSION_MESSAGE = 158;
  public static final short PR_UPDATE_ENTRY_VERSION_MESSAGE = 159;
  
  public static final short SHUTDOWN_ALL_GATEWAYSENDERS_REQUEST = 160;
  public static final short SHUTDOWN_ALL_GATEWAYSENDERS_RESPONSE = 161;
  
  public static final short SHUTDOWN_ALL_GATEWAYRECEIVERS_REQUEST = 162;
  public static final short SHUTDOWN_ALL_GATEWAYRECEIVERS_RESPONSE = 163;

  public static final short TX_COMMIT_MESSAGE_701 = 164;
  public static final short PR_FETCH_BULK_ENTRIES_MESSAGE = 165;
  public static final short PR_FETCH_BULK_ENTRIES_REPLY_MESSAGE = 166;
  public static final short NWAY_MERGE_RESULTS = 167;
  public static final short CUMULATIVE_RESULTS = 168;
  public static final short DISTTX_ROLLBACK_MESSAGE = 169;
  public static final short DISTTX_ROLLBACK_REPLY_MESSAGE = 170;
  // 171..999 unused

  public static final short ADD_HEALTH_LISTENER_REQUEST = 1000;
  public static final short ADD_HEALTH_LISTENER_RESPONSE = 1001;
  public static final short ADD_STAT_LISTENER_REQUEST = 1002;
  public static final short ADD_STAT_LISTENER_RESPONSE = 1003;
  public static final short ADMIN_CONSOLE_DISCONNECT_MESSAGE = 1004;
  public static final short ADMIN_CONSOLE_MESSAGE = 1005;
  public static final short ADMIN_FAILURE_RESPONSE = 1006;
  public static final short ALERT_LEVEL_CHANGE_MESSAGE = 1007;
  public static final short ALERT_LISTENER_MESSAGE = 1008;
  public static final short APP_CACHE_SNAPSHOT_MESSAGE = 1009;
  public static final short BRIDGE_SERVER_REQUEST = 1010;
  public static final short BRIDGE_SERVER_RESPONSE = 1011;
  public static final short CACHE_CONFIG_REQUEST = 1012;
  public static final short CACHE_CONFIG_RESPONSE = 1013;
  public static final short CACHE_INFO_REQUEST = 1014;
  public static final short CACHE_INFO_RESPONSE = 1015;
  public static final short CANCELLATION_MESSAGE = 1016;
  public static final short CANCEL_STAT_LISTENER_REQUEST = 1017;
  public static final short CANCEL_STAT_LISTENER_RESPONSE = 1018;
  public static final short DESTROY_ENTRY_MESSAGE = 1019;
  public static final short ADMIN_DESTROY_REGION_MESSAGE = 1020;
  public static final short FETCH_DIST_LOCK_INFO_REQUEST = 1021;
  public static final short FETCH_DIST_LOCK_INFO_RESPONSE = 1022;
  public static final short FETCH_HEALTH_DIAGNOSIS_REQUEST = 1023;
  public static final short FETCH_HEALTH_DIAGNOSIS_RESPONSE = 1024;
  public static final short FETCH_HOST_REQUEST = 1025;
  public static final short FETCH_HOST_RESPONSE = 1026;
  public static final short FETCH_RESOURCE_ATTRIBUTES_REQUEST = 1027;
  public static final short FETCH_RESOURCE_ATTRIBUTES_RESPONSE = 1028;
  public static final short FETCH_STATS_REQUEST = 1029;
  public static final short FETCH_STATS_RESPONSE = 1030;
  public static final short FETCH_SYS_CFG_REQUEST = 1031;
  public static final short FETCH_SYS_CFG_RESPONSE = 1032;
  public static final short FLUSH_APP_CACHE_SNAPSHOT_MESSAGE = 1033;
  public static final short HEALTH_LISTENER_MESSAGE = 1034;
  public static final short LICENSE_INFO_REQUEST = 1035;
  public static final short LICENSE_INFO_RESPONSE = 1036;
  public static final short OBJECT_DETAILS_REQUEST = 1037;
  public static final short OBJECT_DETAILS_RESPONSE = 1038;
  public static final short OBJECT_NAMES_REQUEST = 1039;
  public static final short OBJECT_NAMES_RESPONSE = 1040;
  public static final short REGION_ATTRIBUTES_REQUEST = 1041;
  public static final short REGION_ATTRIBUTES_RESPONSE = 1042;
  public static final short REGION_REQUEST = 1043;
  public static final short REGION_RESPONSE = 1044;
  public static final short REGION_SIZE_REQUEST = 1045;
  public static final short REGION_SIZE_RESPONSE = 1046;
  public static final short REGION_STATISTICS_REQUEST = 1047;
  public static final short REGION_STATISTICS_RESPONSE = 1048;
  public static final short REMOVE_HEALTH_LISTENER_REQUEST = 1049;
  public static final short REMOVE_HEALTH_LISTENER_RESPONSE = 1050;
  public static final short RESET_HEALTH_STATUS_REQUEST = 1051;
  public static final short RESET_HEALTH_STATUS_RESPONSE = 1052;
  public static final short ROOT_REGION_REQUEST = 1053;
  public static final short ROOT_REGION_RESPONSE = 1054;
  public static final short SNAPSHOT_RESULT_MESSAGE = 1055;
  public static final short STAT_LISTENER_MESSAGE = 1056;
  public static final short STORE_SYS_CFG_REQUEST = 1057;
  public static final short STORE_SYS_CFG_RESPONSE = 1058;
  public static final short SUB_REGION_REQUEST = 1059;
  public static final short SUB_REGION_RESPONSE = 1060;
  public static final short TAIL_LOG_REQUEST = 1061;
  public static final short TAIL_LOG_RESPONSE = 1062;
  public static final short VERSION_INFO_REQUEST = 1063;
  public static final short VERSION_INFO_RESPONSE = 1064;
  public static final short STAT_ALERTS_MGR_ASSIGN_MESSAGE = 1065;
  public static final short UPDATE_ALERTS_DEFN_MESSAGE = 1066;
  public static final short REFRESH_MEMBER_SNAP_REQUEST = 1067;
  public static final short REFRESH_MEMBER_SNAP_RESPONSE = 1068;
  public static final short REGION_SUB_SIZE_REQUEST = 1069;
  public static final short REGION_SUB_SIZE_RESPONSE = 1070;
  public static final short CHANGE_REFRESH_INT_MESSAGE = 1071;
  public static final short ALERTS_NOTIF_MESSAGE = 1072;
  public static final short STAT_ALERT_DEFN_NUM_THRESHOLD = 1073;
  public static final short STAT_ALERT_DEFN_GAUGE_THRESHOLD = 1074;
  public static final short STAT_ALERT_NOTIFICATION = 1075;
  public static final short FILTER_INFO_MESSAGE = 1076;
  public static final short REQUEST_FILTERINFO_MESSAGE = 1077;
  public static final short REQUEST_RVV_MESSAGE = 1078;
  public static final short RVV_REPLY_MESSAGE = 1079;

  public static final short CLIENT_MEMBERSHIP_MESSAGE = 1080;
  // 1,081...1,199 reserved for more admin msgs
  public static final short PR_FUNCTION_STREAMING_MESSAGE = 1201;
  public static final short MEMBER_FUNCTION_STREAMING_MESSAGE = 1202;
  public static final short DR_FUNCTION_STREAMING_MESSAGE = 1203;
  public static final short FUNCTION_STREAMING_REPLY_MESSAGE = 1204;
  public static final short FUNCTION_STREAMING_ORDERED_REPLY_MESSAGE = 1205;
  public static final short REQUEST_SYNC_MESSAGE = 1206;

  // 1,209..1,999 unused

  public static final short HIGH_PRIORITY_ACKED_MESSAGE = 2000;
  public static final short SERIAL_ACKED_MESSAGE = 2001;
  public static final short CLIENT_DATASERIALIZER_MESSAGE=2002;
  
  //2003..2099 unused
  
  public static final short PERSISTENT_MEMBERSHIP_VIEW_REQUEST =2100;
  public static final short PERSISTENT_MEMBERSHIP_VIEW_REPLY = 2101;
  public static final short PERSISTENT_STATE_QUERY_REQUEST = 2102;
  public static final short PERSISTENT_STATE_QUERY_REPLY = 2103;
  public static final short PREPARE_NEW_PERSISTENT_MEMBER_REQUEST = 2104;
  public static final short MISSING_PERSISTENT_IDS_REQUEST = 2105;
  public static final short MISSING_PERSISTENT_IDS_RESPONSE = 2106;
  public static final short REVOKE_PERSISTENT_ID_REQUEST = 2107;
  public static final short REVOKE_PERSISTENT_ID_RESPONSE = 2108;
  public static final short REMOVE_PERSISTENT_MEMBER_REQUEST = 2109;
  public static final short PERSISTENT_MEMBERSHIP_FLUSH_REQUEST = 2110;
  public static final short SHUTDOWN_ALL_REQUEST = 2111;
  public static final short SHUTDOWN_ALL_RESPONSE = 2112;
  public static final short END_BUCKET_CREATION_MESSAGE = 2113;
  
  public static final short FINISH_BACKUP_REQUEST = 2114;
  public static final short FINISH_BACKUP_RESPONSE = 2115;
  public static final short PREPARE_BACKUP_REQUEST = 2116;
  public static final short PREPARE_BACKUP_RESPONSE = 2117;
  public static final short COMPACT_REQUEST = 2118;
  public static final short COMPACT_RESPONSE = 2119;
  public static final short FLOW_CONTROL_PERMIT_MESSAGE = 2120;

  public static final short OBJECT_PART_LIST66 = 2121;
  public static final short LINKED_RESULTSET = 2122;
  public static final short LINKED_STRUCTSET = 2123;
  public static final short PR_ALL_BUCKET_PROFILES_UPDATE_MESSAGE = 2124;
  
  public static final short SERIALIZED_OBJECT_PART_LIST = 2125;
  public static final short FLUSH_TO_DISK_REQUEST = 2126;
  public static final short FLUSH_TO_DISK_RESPONSE= 2127;

  public static final short CHECK_TYPE_REGISTRY_STATE= 2128;
  public static final short  PREPARE_REVOKE_PERSISTENT_ID_REQUEST = 2129;
  public static final short MISSING_PERSISTENT_IDS_RESPONSE_662 = 2130;

  public static final short PERSISTENT_VERSION_TAG = 2131;
  public static final short PERSISTENT_RVV = 2132;
  public static final short DISK_STORE_ID = 2133;
  
  public static final short SNAPSHOT_PACKET = 2134;
  public static final short SNAPSHOT_RECORD = 2135;
  
  public static final short FLOW_CONTROL_ACK = 2136;
  public static final short FLOW_CONTROL_ABORT = 2137;

  public static final short REMOTE_LOCATOR_RESPONSE = 2138;
  public static final short LOCATOR_JOIN_MESSAGE = 2139;

  public static final short  PARALLEL_QUEUE_BATCH_REMOVAL_MESSAGE= 2140;
  public static final short PARALLEL_QUEUE_BATCH_REMOVAL_REPLY = 2141;

  // 2141 unused
  public static final short REMOTE_LOCATOR_PING_REQUEST = 2142;
  public static final short REMOTE_LOCATOR_PING_RESPONSE = 2143;
  public static final short GATEWAY_SENDER_PROFILE = 2144;
  public static final short REMOTE_LOCATOR_JOIN_REQUEST = 2145;
  public static final short REMOTE_LOCATOR_JOIN_RESPONSE = 2146;
  public static final short REMOTE_LOCATOR_REQUEST = 2147;
  

  public static final short BATCH_DESTROY_MESSAGE = 2148;
  
  public static final short MANAGER_STARTUP_MESSAGE = 2149;

  public static final short JMX_MANAGER_LOCATOR_REQUEST = 2150;
  public static final short JMX_MANAGER_LOCATOR_RESPONSE = 2151;

  public static final short MGMT_COMPACT_REQUEST = 2152;
  public static final short MGMT_COMPACT_RESPONSE = 2153;
  
  public static final short MGMT_FEDERATION_COMPONENT = 2154;

  public static final short LOCATOR_STATUS_REQUEST = 2155;
  public static final short LOCATOR_STATUS_RESPONSE = 2156;
  public static final short RELEASE_CLEAR_LOCK_MESSAGE = 2157;
  public static final short NULL_TOKEN = 2158;
  
  public static final short CONFIGURATION_REQUEST = 2159;
  public static final short CONFIGURATION_RESPONSE = 2160;
  
  public static final short PARALLEL_QUEUE_REMOVAL_MESSAGE = 2161;
  
  public static final short PR_QUERY_TRACE_INFO = 2162;

  public static final short INDEX_CREATION_DATA = 2163;
    
  public static final short SERVER_PING_MESSAGE = 2164;
  public static final short PR_DESTROY_ON_DATA_STORE_MESSAGE = 2165;
  
  public static final short DIST_TX_OP = 2166;
  public static final short DIST_TX_PRE_COMMIT_RESPONSE = 2167;
  public static final short DIST_TX_THIN_ENTRY_STATE = 2168;
  
  public static final short LUCENE_CHUNK_KEY = 2169;
  public static final short LUCENE_FILE = 2170;
  public static final short LUCENE_FUNCTION_CONTEXT = 2171;
  public static final short LUCENE_STRING_QUERY_PROVIDER = 2172;
  public static final short LUCENE_TOP_ENTRIES_COLLECTOR_MANAGER = 2173;
  public static final short LUCENE_ENTRY_SCORE = 2174;
  public static final short LUCENE_TOP_ENTRIES = 2175;
  public static final short LUCENE_TOP_ENTRIES_COLLECTOR = 2176;
  
  // NOTE, codes > 65535 will take 4 bytes to serialize
  
  /**
   * This special code is a way for an implementor if this interface
   * to say that it does not have a fixed id.
   * In that case its class name is serialized.
   * Currently only test classes just return this code.
   */
  public static final int NO_FIXED_ID = Integer.MAX_VALUE;

  //////////////// END CODES ////////////

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  public int getDSFID();

  /**
   * Writes the state of this object as primitive data to the given
   * <code>DataOutput</code>.<br>
   * <br>
   * Note: For rolling upgrades, if there is a change in the object format from
   * previous version, add a new toDataPre_GFE_X_X_X_X() method and add an entry for the
   * current {@link Version} in the getSerializationVersions array of the implementing
   * class. e.g. if msg format changed in version 80, create toDataPre_GFE_8_0_0_0, add
   * Version.GFE_80 to the getSerializationVersions array and copy previous toData contents 
   * to this newly created toDataPre_GFE_X_X_X_X() method.
   * <p>
   * For GemFireXD use "GFXD" (or whatever we decide on as a product identifier
   * in Version) instead of "GFE" in method names.
   * @throws IOException
   *           A problem occurs while writing to <code>out</code>
   */
  public void toData(DataOutput out) throws IOException;

  /**
   * Reads the state of this object as primitive data from the given
   * <code>DataInput</code>. <br>
   * <br>
   * Note: For rolling upgrades, if there is a change in the object format from
   * previous version, add a new fromDataPre_GFE_X_X_X_X() method and add an entry for
   * the current {@link Version} in the getSerializationVersions array of the implementing
   * class. e.g. if msg format changed in version 80, create fromDataPre_GFE_8_0_0_0, add
   * Version.GFE_80 to the getSerializationVersions array  and copy previous fromData 
   * contents to this newly created fromDataPre_GFE_X_X_X_X() method.
   * <p>
   * For GemFireXD use "GFXD" (or whatever we decide on as a product identifier
   * in Version) instead of "GFE" in method names.
   * 
   * @throws IOException
   *           A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *           A class could not be loaded while reading from <code>in</code>
   */
  public void fromData(DataInput in) throws IOException, ClassNotFoundException;


}

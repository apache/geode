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
package org.apache.geode.internal.cache.tier;

/**
 * Pre-defined message types supported by the system
 *
 * @since GemFire 2.0.2
 */
public class MessageType {
  /** An invalid message, typically due to problems receiving a complete message */
  public static final int INVALID = -1;

  /** A request for a value from a region */
  public static final int REQUEST = 0;

  /** The value from a region */
  public static final int RESPONSE = 1;

  /** An acception occurred while accessing a region */
  public static final int EXCEPTION = 2;

  /** The get request data was bad (null key, for example) */
  public static final int REQUESTDATAERROR = 3;

  // public static final int DATANOTFOUNDERROR = 4;

  /** A ping request */
  public static final int PING = 5;

  /** A ping reply */
  public static final int REPLY = 6;

  /**
   * Put data into the cache
   *
   * @since GemFire 3.5
   */
  public static final int PUT = 7;

  /**
   * The put request data was bad (null key or value, for example)
   *
   * @since GemFire 3.5
   */
  public static final int PUT_DATA_ERROR = 8;

  /**
   * Destroy an entry from the cache
   *
   * @since GemFire 3.5
   */
  public static final int DESTROY = 9;

  /**
   * The destroy request data was bad (null key, for example)
   *
   * @since GemFire 3.5
   */
  public static final int DESTROY_DATA_ERROR = 10;

  /**
   * Destroy a region from the cache
   *
   * @since GemFire 3.5
   */
  public static final int DESTROY_REGION = 11;

  /**
   * The destroy region request data was bad (null region name, for example)
   *
   * @since GemFire 3.5
   */
  public static final int DESTROY_REGION_DATA_ERROR = 12;

  /**
   * A client wished to be notified of updates
   *
   * @since GemFire 3.5
   */
  public static final int CLIENT_NOTIFICATION = 13;

  /**
   * Information about a client receiving updates has changed
   *
   * @since GemFire 3.5
   */
  public static final int UPDATE_CLIENT_NOTIFICATION = 14;

  /**
   * The receiver (which is an edge client in this case) should locally invalidate a piece of data
   *
   * @since GemFire 3.5
   */
  public static final int LOCAL_INVALIDATE = 15;

  /**
   * The receiver (which is an edge client in this case) should locally destroy a piece of data
   *
   * @since GemFire 3.5
   */
  public static final int LOCAL_DESTROY = 16;

  /**
   * The receiver (which is an edge client in this case) should locally destroy a region
   *
   * @since GemFire 3.5
   */
  public static final int LOCAL_DESTROY_REGION = 17;

  /**
   * A message to close the client connection
   *
   * @since GemFire 3.5
   */
  public static final int CLOSE_CONNECTION = 18;

  /**
   * A message to process a batch of messages
   *
   * @since GemFire 4.1.1
   */
  public static final int PROCESS_BATCH = 19;

  /**
   * A message to register interest in a specific key
   *
   * @since GemFire 4.1.2
   */
  public static final int REGISTER_INTEREST = 20;

  /**
   * The register interest request data was bad (null region name, for example)
   *
   * @since GemFire 4.1.2
   */
  public static final int REGISTER_INTEREST_DATA_ERROR = 21;

  /**
   * A message to unregister interest in a specific key
   *
   * @since GemFire 4.1.2
   */
  public static final int UNREGISTER_INTEREST = 22;

  /**
   * The unregister interest request data was bad (null region name, for example)
   *
   * @since GemFire 4.1.2
   */
  public static final int UNREGISTER_INTEREST_DATA_ERROR = 23;

  /**
   * A message to register interest in a specific list of keys
   *
   * @since GemFire 4.1.2
   */
  public static final int REGISTER_INTEREST_LIST = 24;

  /**
   * A message to unregister interest in a specific list of keys
   *
   * @since GemFire 4.1.2
   */
  public static final int UNREGISTER_INTEREST_LIST = 25;

  /**
   * An unknown message type. This is being used for responses.
   *
   * @since GemFire 4.1.2
   */
  public static final int UNKNOWN_MESSAGE_TYPE_ERROR = 26;

  /**
   * The receiver (which is an edge client in this case) should locally create a piece of data
   *
   * @since GemFire 4.1.2
   */
  public static final int LOCAL_CREATE = 27;

  /**
   * The receiver (which is an edge client in this case) should locally update a piece of data
   *
   * @since GemFire 4.1.2
   */
  public static final int LOCAL_UPDATE = 28;

  public static final int CREATE_REGION = 29;
  public static final int CREATE_REGION_DATA_ERROR = 30;

  /**
   * A message to make primary server
   *
   * @since GemFire 5.1
   */
  public static final int MAKE_PRIMARY = 31;

  /**
   * Response message type from primary server
   *
   * @since GemFire 5.1
   */
  public static final int RESPONSE_FROM_PRIMARY = 32;

  /**
   * Response message type from secondary server
   *
   * @since GemFire 5.1
   */
  public static final int RESPONSE_FROM_SECONDARY = 33;

  /**
   * A query request message
   *
   * <p>
   * author gregp
   *
   * @since GemFire 4.1.1
   */
  public static final int QUERY = 34;

  public static final int QUERY_DATA_ERROR = 35;

  public static final int CLEAR_REGION = 36;

  public static final int CLEAR_REGION_DATA_ERROR = 37;

  public static final int CONTAINS_KEY = 38;

  public static final int CONTAINS_KEY_DATA_ERROR = 39;

  public static final int KEY_SET = 40;

  public static final int KEY_SET_DATA_ERROR = 41;

  public static final int EXECUTECQ_MSG_TYPE = 42;

  public static final int EXECUTECQ_WITH_IR_MSG_TYPE = 43;

  public static final int STOPCQ_MSG_TYPE = 44;

  public static final int CLOSECQ_MSG_TYPE = 45;

  public static final int CLOSECLIENTCQS_MSG_TYPE = 46;

  public static final int CQDATAERROR_MSG_TYPE = 47;

  public static final int GETCQSTATS_MSG_TYPE = 48;

  public static final int MONITORCQ_MSG_TYPE = 49;

  public static final int CQ_EXCEPTION_TYPE = 50;

  /**
   * A message to register the Instantiators.
   *
   * @since GemFire 5.0
   */
  public static final int REGISTER_INSTANTIATORS = 51;

  public static final int PERIODIC_ACK = 52;

  public static final int CLIENT_READY = 53;

  public static final int CLIENT_MARKER = 54;

  public static final int INVALIDATE_REGION = 55;

  public static final int PUTALL = 56;

  public static final int GET_ALL_DATA_ERROR = 58;

  public static final int EXECUTE_REGION_FUNCTION = 59;

  public static final int EXECUTE_REGION_FUNCTION_RESULT = 60;

  public static final int EXECUTE_REGION_FUNCTION_ERROR = 61;

  public static final int EXECUTE_FUNCTION = 62;

  public static final int EXECUTE_FUNCTION_RESULT = 63;

  public static final int EXECUTE_FUNCTION_ERROR = 64;

  public static final int CLIENT_REGISTER_INTEREST = 65;

  public static final int CLIENT_UNREGISTER_INTEREST = 66;

  public static final int REGISTER_DATASERIALIZERS = 67;

  public static final int REQUEST_EVENT_VALUE = 68;

  public static final int REQUEST_EVENT_VALUE_ERROR = 69;

  public static final int PUT_DELTA_ERROR = 70;

  public static final int GET_CLIENT_PR_METADATA = 71;

  public static final int RESPONSE_CLIENT_PR_METADATA = 72;

  public static final int GET_CLIENT_PARTITION_ATTRIBUTES = 73;

  public static final int RESPONSE_CLIENT_PARTITION_ATTRIBUTES = 74;

  public static final int GET_CLIENT_PR_METADATA_ERROR = 75;

  public static final int GET_CLIENT_PARTITION_ATTRIBUTES_ERROR = 76;

  public static final int USER_CREDENTIAL_MESSAGE = 77;

  public static final int REMOVE_USER_AUTH = 78;

  public static final int EXECUTE_REGION_FUNCTION_SINGLE_HOP = 79;

  public static final int QUERY_WITH_PARAMETERS = 80;

  public static final int SIZE = 81;
  public static final int SIZE_ERROR = 82;

  public static final int INVALIDATE = 83;
  public static final int INVALIDATE_ERROR = 84;

  public static final int COMMIT = 85;
  public static final int COMMIT_ERROR = 86;

  public static final int ROLLBACK = 87;

  public static final int TX_FAILOVER = 88;

  public static final int GET_ENTRY = 89;

  public static final int TX_SYNCHRONIZATION = 90;

  public static final int GET_FUNCTION_ATTRIBUTES = 91;

  public static final int GET_PDX_TYPE_BY_ID = 92;

  public static final int GET_PDX_ID_FOR_TYPE = 93;

  public static final int ADD_PDX_TYPE = 94;

  public static final int GET_ALL_FOR_RI = 95; // this message type is not used after 6.x

  public static final int ADD_PDX_ENUM = 96;
  public static final int GET_PDX_ID_FOR_ENUM = 97;
  public static final int GET_PDX_ENUM_BY_ID = 98;

  public static final int SERVER_TO_CLIENT_PING = 99;

  public static final int GET_ALL_70 = 100;

  /** gets the pdx type definitions @since GemFire 7.0 */
  public static final int GET_PDX_TYPES = 101;

  /** gets the pdx enum definitions @since GemFire 7.0 */
  public static final int GET_PDX_ENUMS = 102;

  public static final int TOMBSTONE_OPERATION = 103;

  public static final int GATEWAY_RECEIVER_COMMAND = 104;

  public static final int GETDURABLECQS_MSG_TYPE = 105;

  public static final int GET_DURABLE_CQS_DATA_ERROR = 106;

  public static final int GET_ALL_WITH_CALLBACK = 107;

  public static final int PUT_ALL_WITH_CALLBACK = 108;

  public static final int REMOVE_ALL = 109;
  /**
   * Must be equal to last valid message id.
   */
  private static final int LAST_VALID_MESSAGE_ID = REMOVE_ALL;


  public static boolean validate(int messageType) {
    if (messageType == -1 || messageType == 4) {
      return false;
    }
    boolean valid = (messageType >= 0 && messageType <= LAST_VALID_MESSAGE_ID);
    // logger.warning("Message type is valid: " + valid);
    return valid;
  }

  public static String getString(int type) {
    switch (type) {
      case INVALID:
        return "INVALID";
      case REQUEST:
        return "REQUEST";
      case RESPONSE:
        return "RESPONSE";
      case EXCEPTION:
        return "EXCEPTION";
      case REQUESTDATAERROR:
        return "REQUESTDATAERROR";
      case PING:
        return "PING";
      case REPLY:
        return "REPLY";
      case PUT:
        return "PUT";
      case PUTALL:
        return "PUTALL";
      case PUT_DATA_ERROR:
        return "PUT_DATA_ERROR";
      case DESTROY:
        return "DESTROY";
      case DESTROY_DATA_ERROR:
        return "DESTROY_DATA_ERROR";
      case DESTROY_REGION:
        return "DESTROY_REGION";
      case DESTROY_REGION_DATA_ERROR:
        return "DESTROY_REGION_DATA_ERROR";
      case CLIENT_NOTIFICATION:
        return "CLIENT_NOTIFICATION";
      case UPDATE_CLIENT_NOTIFICATION:
        return "UPDATE_CLIENT_NOTIFICATION";
      case LOCAL_INVALIDATE:
        return "LOCAL_INVALIDATE";
      case LOCAL_DESTROY:
        return "LOCAL_DESTROY";
      case LOCAL_DESTROY_REGION:
        return "LOCAL_DESTROY_REGION";
      case CLOSE_CONNECTION:
        return "CLOSE_CONNECTION";
      case PROCESS_BATCH:
        return "PROCESS_BATCH";
      case REGISTER_INTEREST:
        return "REGISTER_INTEREST";
      case REGISTER_INTEREST_DATA_ERROR:
        return "REGISTER_INTEREST_DATA_ERROR";
      case UNREGISTER_INTEREST:
        return "UNREGISTER_INTEREST";
      case REGISTER_INTEREST_LIST:
        return "REGISTER_INTEREST_LIST";
      case UNREGISTER_INTEREST_LIST:
        return "UNREGISTER_INTEREST_LIST";
      case UNKNOWN_MESSAGE_TYPE_ERROR:
        return "UNKNOWN_MESSAGE_TYPE_ERROR";
      case LOCAL_CREATE:
        return "LOCAL_CREATE";
      case CREATE_REGION:
        return "CREATE_REGION";
      case CREATE_REGION_DATA_ERROR:
        return "CREATE_REGION_DATA_ERROR";
      case MAKE_PRIMARY:
        return "MAKE_PRIMARY";
      case RESPONSE_FROM_PRIMARY:
        return "RESPONSE_FROM_PRIMARY";
      case RESPONSE_FROM_SECONDARY:
        return "RESPONSE_FROM_SECONDARY";
      case QUERY:
        return "QUERY";
      case QUERY_WITH_PARAMETERS:
        return "QUERY_WITH_PARAMETERS";
      case EXECUTECQ_MSG_TYPE:
        return "EXECUTECQ";
      case EXECUTECQ_WITH_IR_MSG_TYPE:
        return "EXECUTECQ_WITH_IR";
      case STOPCQ_MSG_TYPE:
        return "STOPCQ";
      case CLOSECQ_MSG_TYPE:
        return "CLOSECQ";
      case CLOSECLIENTCQS_MSG_TYPE:
        return "CLOSECLIENTCQS";
      case CQDATAERROR_MSG_TYPE:
        return "CQDATAERROR";
      case GETCQSTATS_MSG_TYPE:
        return "GETCQSTATS";
      case MONITORCQ_MSG_TYPE:
        return "MONITORCQ";
      case CQ_EXCEPTION_TYPE:
        return "CQ_EXCEPTION_TYPE";
      case QUERY_DATA_ERROR:
        return "QUERY_DATA_ERROR";
      case CLEAR_REGION:
        return "CLEAR_REGION";
      case CLEAR_REGION_DATA_ERROR:
        return "CLEAR_REGION_DATA_ERROR";
      case CONTAINS_KEY:
        return "CONTAINS_KEY";
      case CONTAINS_KEY_DATA_ERROR:
        return "CONTAINS_KEY_DATA_ERROR";
      case KEY_SET:
        return "KEY_SET";
      case KEY_SET_DATA_ERROR:
        return "KEY_SET_DATA_ERROR";
      case REGISTER_INSTANTIATORS:
        return "REGISTER_INSTANTIATORS";
      case PERIODIC_ACK:
        return "PERIODIC_ACK";
      case CLIENT_READY:
        return "CLIENT_READY";
      case CLIENT_MARKER:
        return "CLIENT_MARKER";
      case INVALIDATE_REGION:
        return "INVALIDATE_REGION";
      case EXECUTE_REGION_FUNCTION:
        return "EXECUTE_REGION_FUNCTION";
      case EXECUTE_REGION_FUNCTION_RESULT:
        return "EXECUTE_REGION_FUNCTION_RESULT";
      case EXECUTE_REGION_FUNCTION_ERROR:
        return "EXECUTE_REGION_FUNCTION_ERROR";
      case EXECUTE_FUNCTION:
        return "EXECUTE_FUNCTION";
      case EXECUTE_FUNCTION_RESULT:
        return "EXECUTE_FUNCTION_RESULT";
      case EXECUTE_FUNCTION_ERROR:
        return "EXECUTE_FUNCTION_ERROR";
      case CLIENT_REGISTER_INTEREST:
        return "CLIENT_REGISTER_INTEREST";
      case CLIENT_UNREGISTER_INTEREST:
        return "CLIENT_UNREGISTER_INTEREST";
      case REGISTER_DATASERIALIZERS:
        return "REGISTER_DATASERIALIZERS";
      case GET_CLIENT_PR_METADATA:
        return "GET_CLIENT_PR_METADATA";
      case RESPONSE_CLIENT_PR_METADATA:
        return "RESPONSE_CLIENT_PR_METADATA";
      case GET_CLIENT_PARTITION_ATTRIBUTES:
        return "GET_CLIENT_PARTITION_ATTRIBUTES";
      case RESPONSE_CLIENT_PARTITION_ATTRIBUTES:
        return "RESPONSE_CLIENT_PARTITION_ATTRIBUTES";
      case GET_CLIENT_PR_METADATA_ERROR:
        return "GET_CLIENT_PR_METADATA_ERROR";
      case GET_CLIENT_PARTITION_ATTRIBUTES_ERROR:
        return "GET_CLIENT_PARTITION_ATTRIBUTES_ERROR";
      case USER_CREDENTIAL_MESSAGE:
        return "USER_CREDENTIAL_MESSAGE";
      case REMOVE_USER_AUTH:
        return "REMOVE_USER_AUTH";
      case EXECUTE_REGION_FUNCTION_SINGLE_HOP:
        return "EXECUTE_REGION_FUNCTION_SINGLE_HOP";
      case GET_PDX_ID_FOR_TYPE:
        return "GET_PDX_ID_FOR_TYPE";
      case GET_PDX_TYPE_BY_ID:
        return "GET_PDX_TYPE_BY_ID";
      case ADD_PDX_TYPE:
        return "ADD_PDX_TYPE";
      case ADD_PDX_ENUM:
        return "ADD_PDX_ENUM";
      case GET_PDX_ID_FOR_ENUM:
        return "GET_PDX_ID_FOR_ENUM";
      case GET_PDX_ENUM_BY_ID:
        return "GET_PDX_ENUM_BY_ID";
      case SIZE:
        return "SIZE";
      case SIZE_ERROR:
        return "SIZE_ERROR";
      case INVALIDATE:
        return "INVALIDATE";
      case INVALIDATE_ERROR:
        return "INVALIDATE_ERROR";
      case COMMIT:
        return "COMMIT";
      case COMMIT_ERROR:
        return "COMMIT_ERROR";
      case ROLLBACK:
        return "ROLLBACK";
      case TX_FAILOVER:
        return "TX_FAILOVER";
      case GET_ENTRY:
        return "GET_ENTRY";
      case TX_SYNCHRONIZATION:
        return "TX_SYNCHRONIZATION";
      case GET_FUNCTION_ATTRIBUTES:
        return "GET_FUNCTION_ATTRIBUTES";
      case GET_ALL_FOR_RI:
        return "GET_ALL_FOR_RI";
      case SERVER_TO_CLIENT_PING:
        return "SERVER_TO_CLIENT_PING";
      case GATEWAY_RECEIVER_COMMAND:
        return "GATEWAY_RECEIVER_COMMAND";
      case TOMBSTONE_OPERATION:
        return "TOMBSTONE_OPERATION";
      case GET_PDX_TYPES:
        return "GET_PDX_TYPES";
      case GET_PDX_ENUMS:
        return "GET_PDX_ENUMS";
      case GET_ALL_70:
        return "GET_ALL_70";
      case GETDURABLECQS_MSG_TYPE:
        return "GETDURABLECQS_MSG_TYPE";
      case GET_DURABLE_CQS_DATA_ERROR:
        return "GET_DURABLE_CQS_DATA_ERROR";
      case GET_ALL_WITH_CALLBACK:
        return "GET_ALL_WITH_CALLBACK";
      case PUT_ALL_WITH_CALLBACK:
        return "PUT_ALL_WITH_CALLBACK";
      case REMOVE_ALL:
        return "REMOVE_ALL";
      default:
        return Integer.toString(type);
    }
  }
}

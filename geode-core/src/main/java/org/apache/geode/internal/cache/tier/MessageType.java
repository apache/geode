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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.annotations.Immutable;

/**
 * Pre-defined message types supported by the system
 *
 * @since GemFire 2.0.2
 */
public enum MessageType {
  /** An invalid message, typically due to problems receiving a complete message */
  INVALID(-1),

  /** A request for a value from a region */
  REQUEST(0),

  /** The value from a region */
  RESPONSE(1),

  /** An exception occurred while accessing a region */
  EXCEPTION(2),

  /** The get request data was bad (null key, for example) */
  REQUESTDATAERROR(3),

  // DATANOTFOUNDERROR(4),

  /** A ping request */
  PING(5),

  /** A ping reply */
  REPLY(6),

  /**
   * Put data into the cache
   *
   * @since GemFire 3.5
   */
  PUT(7),

  /**
   * The put request data was bad (null key or value, for example)
   *
   * @since GemFire 3.5
   */
  PUT_DATA_ERROR(8),

  /**
   * Destroy an entry from the cache
   *
   * @since GemFire 3.5
   */
  DESTROY(9),

  /**
   * The destroy request data was bad (null key, for example)
   *
   * @since GemFire 3.5
   */
  DESTROY_DATA_ERROR(10),

  /**
   * Destroy a region from the cache
   *
   * @since GemFire 3.5
   */
  DESTROY_REGION(11),

  /**
   * The destroy region request data was bad (null region name, for example)
   *
   * @since GemFire 3.5
   */
  DESTROY_REGION_DATA_ERROR(12),

  /**
   * A client wished to be notified of updates
   *
   * @since GemFire 3.5
   */
  CLIENT_NOTIFICATION(13),

  /**
   * Information about a client receiving updates has changed
   *
   * @since GemFire 3.5
   */
  UPDATE_CLIENT_NOTIFICATION(14),

  /**
   * The receiver (which is an edge client in this case) should locally invalidate a piece of data
   *
   * @since GemFire 3.5
   */
  LOCAL_INVALIDATE(15),

  /**
   * The receiver (which is an edge client in this case) should locally destroy a piece of data
   *
   * @since GemFire 3.5
   */
  LOCAL_DESTROY(16),

  /**
   * The receiver (which is an edge client in this case) should locally destroy a region
   *
   * @since GemFire 3.5
   */
  LOCAL_DESTROY_REGION(17),

  /**
   * A message to close the client connection
   *
   * @since GemFire 3.5
   */
  CLOSE_CONNECTION(18),

  /**
   * A message to process a batch of messages
   *
   * @since GemFire 4.1.1
   */
  PROCESS_BATCH(19),

  /**
   * A message to register interest in a specific key
   *
   * @since GemFire 4.1.2
   */
  REGISTER_INTEREST(20),

  /**
   * The register interest request data was bad (null region name, for example)
   *
   * @since GemFire 4.1.2
   */
  REGISTER_INTEREST_DATA_ERROR(21),

  /**
   * A message to unregister interest in a specific key
   *
   * @since GemFire 4.1.2
   */
  UNREGISTER_INTEREST(22),

  /**
   * The unregister interest request data was bad (null region name, for example)
   *
   * @since GemFire 4.1.2
   */
  UNREGISTER_INTEREST_DATA_ERROR(23),

  /**
   * A message to register interest in a specific list of keys
   *
   * @since GemFire 4.1.2
   */
  REGISTER_INTEREST_LIST(24),

  /**
   * A message to unregister interest in a specific list of keys
   *
   * @since GemFire 4.1.2
   */
  UNREGISTER_INTEREST_LIST(25),

  /**
   * An unknown message type. This is being used for responses.
   *
   * @since GemFire 4.1.2
   */
  UNKNOWN_MESSAGE_TYPE_ERROR(26),

  /**
   * The receiver (which is an edge client in this case) should locally create a piece of data
   *
   * @since GemFire 4.1.2
   */
  LOCAL_CREATE(27),

  /**
   * The receiver (which is an edge client in this case) should locally update a piece of data
   *
   * @since GemFire 4.1.2
   */
  LOCAL_UPDATE(28),

  CREATE_REGION(29),
  CREATE_REGION_DATA_ERROR(30),

  /**
   * A message to make primary server
   *
   * @since GemFire 5.1
   */
  MAKE_PRIMARY(31),

  /**
   * Response message type from primary server
   *
   * @since GemFire 5.1
   */
  RESPONSE_FROM_PRIMARY(32),

  /**
   * Response message type from secondary server
   *
   * @since GemFire 5.1
   */
  RESPONSE_FROM_SECONDARY(33),

  /**
   * A query request message
   *
   * @since GemFire 4.1.1
   */
  QUERY(34),

  QUERY_DATA_ERROR(35),

  CLEAR_REGION(36),

  CLEAR_REGION_DATA_ERROR(37),

  CONTAINS_KEY(38),

  CONTAINS_KEY_DATA_ERROR(39),

  KEY_SET(40),

  KEY_SET_DATA_ERROR(41),

  EXECUTECQ(42),

  EXECUTECQ_WITH_IR(43),

  STOPCQ(44),

  CLOSECQ(45),

  CLOSECLIENTCQS(46),

  CQDATAERROR(47),

  GETCQSTATS(48),

  MONITORCQ(49),

  CQ_EXCEPTION_TYPE(50),

  /**
   * A message to register the Instantiators.
   *
   * @since GemFire 5.0
   */
  REGISTER_INSTANTIATORS(51),

  PERIODIC_ACK(52),

  CLIENT_READY(53),

  CLIENT_MARKER(54),

  INVALIDATE_REGION(55),

  PUTALL(56),

  GET_ALL_DATA_ERROR(58),

  EXECUTE_REGION_FUNCTION(59),

  EXECUTE_REGION_FUNCTION_RESULT(60),

  EXECUTE_REGION_FUNCTION_ERROR(61),

  EXECUTE_FUNCTION(62),

  EXECUTE_FUNCTION_RESULT(63),

  EXECUTE_FUNCTION_ERROR(64),

  CLIENT_REGISTER_INTEREST(65),

  CLIENT_UNREGISTER_INTEREST(66),

  REGISTER_DATASERIALIZERS(67),

  REQUEST_EVENT_VALUE(68),

  REQUEST_EVENT_VALUE_ERROR(69),

  PUT_DELTA_ERROR(70),

  GET_CLIENT_PR_METADATA(71),

  RESPONSE_CLIENT_PR_METADATA(72),

  GET_CLIENT_PARTITION_ATTRIBUTES(73),

  RESPONSE_CLIENT_PARTITION_ATTRIBUTES(74),

  GET_CLIENT_PR_METADATA_ERROR(75),

  GET_CLIENT_PARTITION_ATTRIBUTES_ERROR(76),

  USER_CREDENTIAL_MESSAGE(77),

  REMOVE_USER_AUTH(78),

  EXECUTE_REGION_FUNCTION_SINGLE_HOP(79),

  QUERY_WITH_PARAMETERS(80),

  SIZE(81),
  SIZE_ERROR(82),

  INVALIDATE(83),
  INVALIDATE_ERROR(84),

  COMMIT(85),
  COMMIT_ERROR(86),

  ROLLBACK(87),

  TX_FAILOVER(88),

  GET_ENTRY(89),

  TX_SYNCHRONIZATION(90),

  GET_FUNCTION_ATTRIBUTES(91),

  GET_PDX_TYPE_BY_ID(92),

  GET_PDX_ID_FOR_TYPE(93),

  ADD_PDX_TYPE(94),

  GET_ALL_FOR_RI(95), // this message type is not used after 6.x

  ADD_PDX_ENUM(96),
  GET_PDX_ID_FOR_ENUM(97),
  GET_PDX_ENUM_BY_ID(98),

  SERVER_TO_CLIENT_PING(99),

  GET_ALL_70(100),

  /** gets the pdx type definitions @since GemFire 7.0 */
  GET_PDX_TYPES(101),

  /** gets the pdx enum definitions @since GemFire 7.0 */
  GET_PDX_ENUMS(102),

  TOMBSTONE_OPERATION(103),

  GATEWAY_RECEIVER_COMMAND(104),

  GETDURABLECQS(105),

  GET_DURABLE_CQS_DATA_ERROR(106),

  GET_ALL_WITH_CALLBACK(107),

  PUT_ALL_WITH_CALLBACK(108),

  REMOVE_ALL(109),

  /**
   * @since Geode 1.15
   */
  CLIENT_RE_AUTHENTICATE(110);


  public final int id;

  MessageType(final int id) {
    this.id = id;
  }

  @Immutable
  private static final MessageType[] messageTypes = new MessageType[111];

  static {
    for (final MessageType messageType : values()) {
      if (messageType == INVALID) {
        continue;
      }

      final MessageType otherMessageType = messageTypes[messageType.id];
      if (null != (otherMessageType)) {
        throw new IllegalStateException(
            String.format("Duplicate MessageType id %d found in %s and %s", messageType.id,
                messageType, otherMessageType));
      }

      messageTypes[messageType.id] = messageType;
    }
  }

  private static @Nullable MessageType getMessageType(final int id) {
    if (id == INVALID.id) {
      return INVALID;
    } else if (id >= 0 && id < messageTypes.length) {
      return messageTypes[id];
    }
    return null;
  }

  public static @NotNull MessageType valueOf(final int id) {
    final MessageType messageType = getMessageType(id);
    if (messageType == null) {
      throw new EnumConstantNotPresentException(MessageType.class, String.valueOf(id));
    }
    return messageType;
  }

  public static boolean validate(final int id) {
    final MessageType messageType = getMessageType(id);
    return null != messageType && INVALID != messageType;
  }

}

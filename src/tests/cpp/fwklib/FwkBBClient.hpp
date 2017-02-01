#pragma once

#ifndef APACHE_GEODE_GUARD_8443d3966c0b05a9e05eafa7ca28e4c7
#define APACHE_GEODE_GUARD_8443d3966c0b05a9e05eafa7ca28e4c7

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

/**
  * @file    FwkBBClient.hpp
  * @since   1.0
  * @version 1.0
  * @see
  */


#include "FwkBB.hpp"
#include "UDPIpc.hpp"

#include <ace/OS.h>

// ----------------------------------------------------------------------------
//  Some widely used BBs and counters

static std::string CLIENTBB("CLIENTBB");
static std::string CLIENTCOUNT("ClientCount");

// ----------------------------------------------------------------------------

#define CLIENT_WAIT_TIME_FOR_REPLY 5  // seconds

// ----------------------------------------------------------------------------

namespace apache {
namespace geode {
namespace client {
namespace testframework {
/** @class FwkBBClient
  * @brief Framework BB client
  */
class FwkBBClient {
 public:
  FwkBBClient(std::string serverAddr) : m_client(serverAddr), m_messageId(0) {}

  ~FwkBBClient() {}

  /** @brief dump all data
    * @retval result of dump
    */
  std::string dump();

  /** @brief dump BB data
    * @param BBName name of BB
    * @retval result of dump
    */
  std::string dump(const std::string& BBName);

  /** @brief clear BB data
    * @param BBName name of BB
    */
  void clear(const std::string& BBName);

  /** @brief get BB key value
    * @param BBName name of BB
    * @param Key name of Key
    * @retval value
    */
  std::string getString(const std::string& BBName, const std::string& Key);

  /** @brief get BB counter value
    * @param BBName name of BB
    * @param Key name of counter
    * @retval value of counter
    */
  int64_t get(const std::string& BBName, const std::string& Key);

  /** @brief set BB key value
    * @param BBName name of BB
    * @param Key name of key
    * @param Value value to set
    */
  void set(const std::string& BBName, const std::string& Key,
           const std::string& Value);

  /** @brief set BB counter value
    * @param BBName name of BB
    * @param Key name of counter
    * @param Value value to add to counter
    * @retval value of counter
    */
  void set(const std::string& BBName, const std::string& Key,
           const int64_t Value);

  /** @brief add BB counter value
    * @param BBName name of BB
    * @param Key name of counter
    * @param Value value to add to counter
    * @retval value after add
    */
  int64_t add(const std::string& BBName, const std::string& Key,
              const int64_t Value);

  /** @brief subtract BB counter value
    * @param BBName name of BB
    * @param Key name of counter
    * @param Value value to subtract from counter
    * @retval value after subtract
    */
  int64_t subtract(const std::string& BBName, const std::string& Key,
                   const int64_t Value);

  /** @brief increment BB counter value by 1
    * @param BBName name of BB
    * @param Key name of counter
    * @retval value after increment
    */
  int64_t increment(const std::string& BBName, const std::string& Key);

  /** @brief decrement BB counter value by 1
    * @param BBName name of BB
    * @param Key name of counter
    * @retval value after decrement
    */
  int64_t decrement(const std::string& BBName, const std::string& Key);

  /** @brief zero BB counter value to 0
    * @param BBName name of BB
    * @param Key name of counter
    */
  void zero(const std::string& BBName, const std::string& Key);

  /** @brief setIfGreater BB counter value is greater
    * @param BBName name of BB
    * @param Key name of counter
    * @param Value value to set
    * @param piResult
    * @retval value after setIfGreater
    */
  int64_t setIfGreater(const std::string& BBName, const std::string& Key,
                       const int64_t Value);

  /** @brief setIfLess BB counter value is less
    * @param BBName name of BB
    * @param Key name of counter
    * @param Value value to set
    * @retval value after setIfLess
    */
  int64_t setIfLess(const std::string& BBName, const std::string& Key,
                    const int64_t Value);

 private:
  /** @brief get new message id
     */
  uint32_t getNewMessageId() { return ++m_messageId; };

  /** @brief get current message id
    */
  uint32_t getCurrentMessageId() { return m_messageId; };

  /** @brief send a message to server
    * @param message message to send
    * @retval true = Success, false = Failed
    */
  void sendToServer(FwkBBMessage& message);

 private:
  UDPMessageClient m_client;
  std::string m_result;
  uint32_t m_messageId;
};

// ----------------------------------------------------------------------------

}  // namespace  testframework
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_8443d3966c0b05a9e05eafa7ca28e4c7

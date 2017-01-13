/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    FwkBBClient.hpp
  * @since   1.0
  * @version 1.0
  * @see
  */

#ifndef __FWK_BB_CLIENT_HPP__
#define __FWK_BB_CLIENT_HPP__

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

namespace gemfire {
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
}  // namepace gemfire

#endif  // __FWK_BB_CLIENT_HPP__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    FwkBBServer.hpp
  * @since   1.0
  * @version 1.0
  * @see
  */

#ifndef __FWK_BB_SERVER_HPP__
#define __FWK_BB_SERVER_HPP__

#include "FwkBB.hpp"
#include "UDPIpc.hpp"

#include <string>
#include <map>

#include <ace/OS.h>

// ----------------------------------------------------------------------------

namespace gemfire {
namespace testframework {

// ----------------------------------------------------------------------------

/** @class NameKeyPair
  * @brief basic name/key pair
  */
class NameKeyPair {
 public:
  NameKeyPair(std::string sName, std::string sKey)
      : m_name(sName), m_key(sKey){};

  /** @brief get name */
  const std::string& getName() const { return m_name; };

  /** @brief get value */
  const std::string& getKey() const { return m_key; };

 private:
  std::string m_name;
  std::string m_key;
};

// ----------------------------------------------------------------------------

/** @class NameKeyCmp
  * @brief name/key compare
  */
class NameKeyCmp {
 public:
  /** @brief Compares two NameKeyPair objects */
  bool operator()(const NameKeyPair& p1, const NameKeyPair& p2) const {
    return ((p1.getName().compare(p2.getName()) * 2) +
            p1.getKey().compare(p2.getKey())) < 0;
  }
};

// ----------------------------------------------------------------------------

typedef std::map<NameKeyPair, std::string, NameKeyCmp> NameKeyMap;
typedef std::map<NameKeyPair, int64_t, NameKeyCmp> NameCounterMap;

// ----------------------------------------------------------------------------

/** @class FwkBBServer
  * @brief Framework BB server
  */
class FwkBBServer {
 public:
  FwkBBServer() {}
  ~FwkBBServer();

  //  void start( uint32_t port );
  //  void stop();

  /** @brief Clear all server data */
  void clear();

  void onDump(FwkBBMessage& message, FwkBBMessage& reply);
  void onClearBB(FwkBBMessage& message);
  void onGet(FwkBBMessage& message, FwkBBMessage& reply);
  void onSet(FwkBBMessage& message);
  void onAdd(FwkBBMessage& message, FwkBBMessage& reply);
  void onSubtract(FwkBBMessage& message, FwkBBMessage& reply);
  void onIncrement(FwkBBMessage& message, FwkBBMessage& reply);
  void onDecrement(FwkBBMessage& message, FwkBBMessage& reply);
  void onZero(FwkBBMessage& message);
  void onSetIfGreater(FwkBBMessage& message, FwkBBMessage& reply);
  void onSetIfLess(FwkBBMessage& message, FwkBBMessage& reply);

  // ----------------------------------------------------------------------------

  void dump(std::string& result);
  void dump(const std::string& BBName, std::string& result);
  void clearBB(const std::string& BBName);
  std::string getString(const std::string& BBName, const std::string& Key);
  int64_t get(const std::string& BBName, const std::string& Key);
  void set(const std::string& BBName, const std::string& Key,
           const std::string& Value);
  void set(const std::string& BBName, const std::string& Key,
           const int64_t Value);
  int64_t add(const std::string& BBName, const std::string& Key,
              const int64_t Value);
  int64_t subtract(const std::string& BBName, const std::string& Key,
                   const int64_t Value);
  int64_t increment(const std::string& BBName, const std::string& Key);
  int64_t decrement(const std::string& BBName, const std::string& Key);
  void zero(const std::string& BBName, const std::string& Key);
  int64_t setIfGreater(const std::string& BBName, const std::string& Key,
                       const int64_t Value);
  int64_t setIfLess(const std::string& BBName, const std::string& Key,
                    const int64_t Value);

 private:
  NameKeyMap m_nameKeyMap;
  NameCounterMap m_nameCounterMap;

  //  Service * m_farm;
};

// ----------------------------------------------------------------------------

class BBProcessor : public ServiceTask {
 private:
  UDPMessageQueues* m_queues;
  FwkBBServer* m_server;

 public:
  BBProcessor(UDPMessageQueues* shared, FwkBBServer* server)
      : ServiceTask(shared), m_queues(shared), m_server(server) {}

  virtual ~BBProcessor() {}

  virtual int doTask() {
    while (*m_run) {
      try {
        UDPMessage* msg = m_queues->getInbound();
        if (msg != NULL) {
          // Construct the FwkBBMessage
          FwkBBMessage message;
          message.fromMessageStream(msg->getMessage());
          // Construct the reply
          FwkBBMessage reply(BB_SET_ACK_COMMAND);
          reply.setId(message.getId());
          // Process the message
          switch (message.getCmdChar()) {
            case 'C':  // BB_CLEAR_COMMAND
              m_server->onClearBB(message);
              break;
            case 'd':  // BB_DUMP_COMMAND
              m_server->onDump(message, reply);
              break;
            case 'g':  // BB_GET_COMMAND
              m_server->onGet(message, reply);
              break;
            case 's':  // BB_SET_COMMAND
              m_server->onSet(message);
              break;
            case 'A':  // BB_ADD_COMMAND
              m_server->onAdd(message, reply);
              break;
            case 'S':  // BB_SUBTRACT_COMMAND
              m_server->onSubtract(message, reply);
              break;
            case 'I':  // BB_INCREMENT_COMMAND
              m_server->onIncrement(message, reply);
              break;
            case 'D':  // BB_DECREMENT_COMMAND
              m_server->onDecrement(message, reply);
              break;
            case 'z':  // BB_ZERO_COMMAND
              m_server->onZero(message);
              break;
            case 'G':  // BB_SET_IF_GREATER_COMMAND
              m_server->onSetIfGreater(message, reply);
              break;
            case 'L':  // BB_SET_IF_LESS_COMMAND
              m_server->onSetIfLess(message, reply);
              break;
            default:
              break;
          }
          // Construct response
          msg->setMessage(reply.toMessageStream());
          m_queues->putOutbound(msg);
        }
      } catch (FwkException& ex) {
        FWKSEVERE(
            "BBProcessor::doTask() caught exception: " << ex.getMessage());
      } catch (...) {
        FWKSEVERE("BBProcessor::doTask() caught unknown exception");
      }
    }
    return 0;
  }
  virtual void initialize() {}
  virtual void finalize() {}
};

}  // namespace  testframework
}  // namepace gemfire

#endif  // __FWK_BB_SERVER_HPP__

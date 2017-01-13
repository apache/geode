/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    UdpIpc.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __UDPIPC_HPP__
#define __UDPIPC_HPP__

// ----------------------------------------------------------------------------
#include <gfcpp/GemfireCppCache.hpp>

#include "fwklib/FrameworkTest.hpp"

namespace gemfire {
namespace testframework {

// ----------------------------------------------------------------------------

class UdpIpc : public FrameworkTest {
 public:
  UdpIpc(const char* initArgs) : FrameworkTest(initArgs) {}

  virtual ~UdpIpc() {}

  void checkTest(const char* taskId);

  void doService();
  void doClient();
};

class TestProcessor : public ServiceTask {
 private:
  UDPMessageQueues* m_queues;
  bool m_sendReply;

 public:
  TestProcessor(UDPMessageQueues* shared, bool sendReply = false)
      : ServiceTask(shared), m_sendReply(sendReply) {
    m_queues = dynamic_cast<UDPMessageQueues*>(m_shared);
  }

  virtual ~TestProcessor() {}

  int doTask() {
    std::string str("A result for you.");
    while (*m_run) {
      UDPMessage* msg = m_queues->getInbound();
      if (msg != NULL) {
        if (m_sendReply) {
          msg->setMessage(str);
          m_queues->putOutbound(msg);
        }
      }
    }
    return 0;
  }
  virtual void initialize() {}
  virtual void finalize() {}
};

}  // namespace testframework
}  // namespace gemfire
// ----------------------------------------------------------------------------

#endif  // __UDPIPC_HPP__

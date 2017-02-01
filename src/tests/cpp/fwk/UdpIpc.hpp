#pragma once

#ifndef APACHE_GEODE_GUARD_ebe55397742f4c3cb6d6e6eaf7338769
#define APACHE_GEODE_GUARD_ebe55397742f4c3cb6d6e6eaf7338769

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
  * @file    UdpIpc.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------


// ----------------------------------------------------------------------------
#include <gfcpp/GeodeCppCache.hpp>

#include "fwklib/FrameworkTest.hpp"

namespace apache {
namespace geode {
namespace client {
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
}  // namespace client
}  // namespace geode
}  // namespace apache
// ----------------------------------------------------------------------------


#endif // APACHE_GEODE_GUARD_ebe55397742f4c3cb6d6e6eaf7338769

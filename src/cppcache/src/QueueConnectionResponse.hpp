#pragma once

#ifndef GEODE_QUEUECONNECTIONRESPONSE_H_
#define GEODE_QUEUECONNECTIONRESPONSE_H_

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
#include <list>
#include "ServerLocationResponse.hpp"
#include <gfcpp/DataInput.hpp>
#include "ServerLocation.hpp"
namespace apache {
namespace geode {
namespace client {
class QueueConnectionResponse : public ServerLocationResponse {
 public:
  QueueConnectionResponse()
      : ServerLocationResponse(),
        /* adongre
         * CID 28940: Uninitialized scalar field (UNINIT_CTOR) *
         */
        m_durableQueueFound(false) {}
  virtual QueueConnectionResponse* fromData(DataInput& input);
  virtual int8_t typeId() const;
  virtual uint32_t objectSize() const;
  virtual std::list<ServerLocation> getServers() { return m_list; }
  virtual bool isDurableQueueFound() { return m_durableQueueFound; }
  static Serializable* create() { return new QueueConnectionResponse(); }
  virtual ~QueueConnectionResponse(){};

 private:
  void readList(DataInput& input);
  void operator=(const QueueConnectionResponse& val){};
  std::list<ServerLocation> m_list;
  bool m_durableQueueFound;
};
typedef SharedPtr<QueueConnectionResponse> QueueConnectionResponsePtr;
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_QUEUECONNECTIONRESPONSE_H_

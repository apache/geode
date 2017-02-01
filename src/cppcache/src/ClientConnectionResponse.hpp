#pragma once

#ifndef GEODE_CLIENTCONNECTIONRESPONSE_H_
#define GEODE_CLIENTCONNECTIONRESPONSE_H_

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
#include "ServerLocationResponse.hpp"
#include "ServerLocation.hpp"
#include <gfcpp/SharedPtr.hpp>
namespace apache {
namespace geode {
namespace client {
class ClientConnectionResponse : public ServerLocationResponse {
 public:
  ClientConnectionResponse()
      : ServerLocationResponse()
        /* adongre
         * CID 28927: Uninitialized scalar field (UNINIT_CTOR)
         */
        ,
        m_serverFound(false) {}
  virtual ClientConnectionResponse* fromData(DataInput& input);
  virtual int8_t typeId() const;
  virtual uint32_t objectSize() const;
  virtual ServerLocation getServerLocation() const;
  void printInfo() { m_server.printInfo(); }
  static Serializable* create() { return new ClientConnectionResponse(); }
  virtual ~ClientConnectionResponse() {}
  bool serverFound() { return m_serverFound; }

 private:
  bool m_serverFound;
  ServerLocation m_server;
};
typedef SharedPtr<ClientConnectionResponse> ClientConnectionResponsePtr;
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_CLIENTCONNECTIONRESPONSE_H_

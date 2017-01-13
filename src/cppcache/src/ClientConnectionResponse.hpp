/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __CLIENT_CONNECTION_RESPONSE__
#define __CLIENT_CONNECTION_RESPONSE__
#include "ServerLocationResponse.hpp"
#include "ServerLocation.hpp"
#include <gfcpp/SharedPtr.hpp>
namespace gemfire {
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
}
#endif

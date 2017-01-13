/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __QUEUE_CONNECTION_RESPONSE__
#define __QUEUE_CONNECTION_RESPONSE__
#include <list>
#include "ServerLocationResponse.hpp"
#include <gfcpp/DataInput.hpp>
#include "ServerLocation.hpp"
namespace gemfire {
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
}
#endif

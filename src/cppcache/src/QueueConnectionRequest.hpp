/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __QUEUE_CONNECTION_REQUEST__
#define __QUEUE_CONNECTION_REQUEST__
#include "ServerLocationRequest.hpp"
#include "ServerLocation.hpp"
#include "ClientProxyMembershipID.hpp"
#include <set>
#include <string>
namespace gemfire {
class QueueConnectionRequest : public ServerLocationRequest {
 public:
  QueueConnectionRequest(const ClientProxyMembershipID& memId,
                         const std::set<ServerLocation>& excludedServers,
                         int redundantCopies, bool findDurable,
                         std::string serverGp = "")
      : ServerLocationRequest(),
        m_membershipID(memId),
        m_excludedServers(excludedServers),
        m_redundantCopies(redundantCopies),
        m_findDurable(findDurable),
        m_serverGp(serverGp) {}  // No need for default constructor as creating
                                 // request with it does not make sense.
  virtual void toData(DataOutput& output) const;
  virtual QueueConnectionRequest* fromData(DataInput& input);
  virtual int8_t typeId() const;
  virtual uint32_t objectSize() const;
  virtual std::set<ServerLocation> getExcludedServer() const;
  virtual const ClientProxyMembershipID& getProxyMemberShipId() const;
  virtual int getRedundentCopies() const;
  virtual bool isFindDurable() const;
  virtual ~QueueConnectionRequest() {}

 private:
  QueueConnectionRequest(const QueueConnectionRequest&);
  void operator=(const QueueConnectionRequest&);
  void writeSetOfServerLocation(DataOutput& output) const;
  const ClientProxyMembershipID& m_membershipID;
  const std::set<ServerLocation>& m_excludedServers;
  const int m_redundantCopies;
  const bool m_findDurable;
  std::string m_serverGp;
};
}
#endif

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __THINCLIENT_POOL_STICKY_DM__
#define __THINCLIENT_POOL_STICKY_DM__
#include "ThinClientPoolDM.hpp"
#include "ThinClientStickyManager.hpp"
namespace gemfire {
class ThinClientPoolStickyDM : public ThinClientPoolDM {
 public:
  ThinClientPoolStickyDM(const char* name, PoolAttributesPtr poolAttrs,
                         TcrConnectionManager& connManager)
      : ThinClientPoolDM(name, poolAttrs, connManager) {
    m_sticky = true;
  }
  virtual ~ThinClientPoolStickyDM() {
    // m_manager->closeAllStickyConnections();
    // delete m_manager; m_manager = NULL;
  }
  virtual bool canItBeDeletedNoImpl(TcrConnection* conn);

 protected:
  virtual void cleanStickyConnections(volatile bool& isRunning);
  virtual TcrConnection* getConnectionFromQueueW(
      GfErrType* error, std::set<ServerLocation>&, bool isBGThread,
      TcrMessage& request, int8_t& version, bool& match, bool& connFound,
      const BucketServerLocationPtr& serverLocation = NULLPTR);
  virtual void putInQueue(TcrConnection* conn, bool isBGThread,
                          bool isTransaction = false);
  virtual void setStickyNull(bool isBGThread);
  virtual bool canItBeDeleted(TcrConnection* conn);
  virtual void releaseThreadLocalConnection();
  virtual void setThreadLocalConnection(TcrConnection* conn);

  // virtual void cleanStickyConnections(volatile bool& isRunning);
};
typedef SharedPtr<ThinClientPoolStickyDM> ThinClientPoolStickyDMPtr;
}
#endif

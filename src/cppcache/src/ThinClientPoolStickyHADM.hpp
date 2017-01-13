/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __THINCLIENT_POOL_STICKY_HA_DM__
#define __THINCLIENT_POOL_STICKY_HA_DM__
#include "ThinClientPoolHADM.hpp"

namespace gemfire {
class ThinClientPoolStickyHADM : public ThinClientPoolHADM {
 public:
  ThinClientPoolStickyHADM(const char* name, PoolAttributesPtr poolAttrs,
                           TcrConnectionManager& connManager)
      : ThinClientPoolHADM(name, poolAttrs, connManager) {
    // m_manager = new ThinClientStickyManager( this );
    m_sticky = true;
  }
  virtual ~ThinClientPoolStickyHADM() {
    /*m_manager->closeAllStickyConnections();
    delete m_manager; m_manager = NULL;*/
  }
  /*bool canItBeDeletedNoImpl(TcrConnection* conn );
protected:
  virtual void cleanStickyConnections(volatile bool& isRunning);
  virtual TcrConnection* getConnectionFromQueueW( GfErrType* error,
    std::set< ServerLocation >&, bool isBGThread, TcrMessage & request, int8_t&
version, bool & dummy, const BucketServerLocationPtr& serverLocation = NULLPTR
);
  virtual void putInQueue(TcrConnection* conn,  bool isBGThread, bool
isTransaction = false );
  virtual void setStickyNull( bool isBGThread );
  virtual bool canItBeDeleted(TcrConnection* conn);
  virtual void releaseThreadLocalConnection();
  virtual void setThreadLocalConnection(TcrConnection* conn);
*/
  // virtual void cleanStickyConnections(volatile bool& isRunning);
  // ThinClientStickyManager* m_manager;
};
typedef SharedPtr<ThinClientPoolStickyHADM> ThinClientPoolStickyHADMPtr;
}
#endif

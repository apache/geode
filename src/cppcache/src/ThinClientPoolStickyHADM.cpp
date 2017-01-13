/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ThinClientPoolStickyHADM.hpp"
#include "TssConnectionWrapper.hpp"
#include <algorithm>
using namespace gemfire;
/*TcrConnection* ThinClientPoolStickyHADM::getConnectionFromQueueW( GfErrType*
error,
  std::set< ServerLocation >& excludeServers, bool isBGThread, TcrMessage &
request, int8_t& version, bool & dummy, const BucketServerLocationPtr&
serverLocation )
{
  TcrConnection* conn = NULL;
  if( isBGThread ){
    conn = ThinClientPoolDM::getConnectionFromQueueW( error, excludeServers,
isBGThread, request, version, dummy, serverLocation);
    return conn;
  }

  m_manager->getStickyConnection(conn , error, excludeServers,
request.forTransaction());
  return conn;
}
void ThinClientPoolStickyHADM::putInQueue(TcrConnection* conn, bool isBGThread,
bool isTransaction )
{
 if( !isBGThread )
   m_manager->setStickyConnection( conn, isTransaction );
 else
   ThinClientPoolDM::putInQueue( conn, isBGThread, isTransaction);
}
void ThinClientPoolStickyHADM::setStickyNull( bool isBGThread )
{
   if( !isBGThread ) m_manager->setStickyConnection( NULL, false );
}

void ThinClientPoolStickyHADM::cleanStickyConnections(volatile bool& isRunning)
{
  if (!isRunning) {
    return;
  }
   m_manager->cleanStaleStickyConnection();
}

bool ThinClientPoolStickyHADM::canItBeDeleted(TcrConnection* conn)
{
  return m_manager->canThisConnBeDeleted( conn );
}
void ThinClientPoolStickyHADM::releaseThreadLocalConnection()
{
  m_manager->releaseThreadLocalConnection();
}
void ThinClientPoolStickyHADM::setThreadLocalConnection(TcrConnection* conn)
{
  m_manager->addStickyConnection(conn);
}
bool ThinClientPoolStickyHADM::canItBeDeletedNoImpl(TcrConnection* conn )
{
  return ThinClientPoolDM::canItBeDeleted( conn );
}*/

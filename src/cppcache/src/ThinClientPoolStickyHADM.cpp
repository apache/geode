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
#include "ThinClientPoolStickyHADM.hpp"
#include "TssConnectionWrapper.hpp"
#include <algorithm>
using namespace apache::geode::client;
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

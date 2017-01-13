/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "ServerLocation.hpp"
#include <ace/OS_NS_stdio.h>

namespace gemfire {
void ServerLocation::makeEpString() {
  if (m_serverName != NULLPTR) {
    char epstring[1024] = {0};
    ACE_OS::snprintf(epstring, 1024, "%s:%d", m_serverName->asChar(), m_port);
    m_epString = epstring;
  }
}
}

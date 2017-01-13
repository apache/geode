/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "PoolXmlCreation.hpp"

using namespace gemfire;

/*
void PoolXmlCreation::addLocator(const char * host, const char * port)
{
  locatorhosts.push_back(host);
  locatorports.push_back(port);
}

void PoolXmlCreation::addServer(const char * host, const char * port)
{
  serverhosts.push_back(host);
  serverports.push_back(port);
}
*/

PoolPtr PoolXmlCreation::create() {
  return poolFactory->create(poolName.c_str());
}

PoolXmlCreation::PoolXmlCreation(const char* name, PoolFactoryPtr factory) {
  poolName = name;
  poolFactory = factory;
}

PoolXmlCreation::~PoolXmlCreation() {}

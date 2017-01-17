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

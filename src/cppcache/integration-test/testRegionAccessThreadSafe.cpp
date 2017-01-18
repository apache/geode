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
#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include <gfcpp/GeodeCppCache.hpp>
#include <ace/Task.h>
#include <ace/Recursive_Thread_Mutex.h>

using namespace apache::geode::client;

class GetRegionThread : public ACE_Task_Base {
 public:
  bool m_running;
  std::string m_path;
  std::string m_subPath;
  bool m_regionCreateDone;
  bool m_subRegionCreateDone;
  ACE_Recursive_Thread_Mutex m_mutex;
  GetRegionThread(const char* path, const char* subPath)
      : m_running(false),
        m_path(path),
        m_subPath(subPath),
        m_regionCreateDone(false),
        m_subRegionCreateDone(false),
        m_mutex() {}
  int svc(void) {
    while (m_running == true) {
      SLEEP(40);
      try {
        RegionPtr rptr = getHelper()->getRegion(m_path.c_str());
        if (rptr != NULLPTR) {
          ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_mutex);
          ASSERT(m_regionCreateDone == true, "regionCreate Not Done");
        }
      } catch (Exception& ex) {
        LOG(ex.getMessage());
        continue;
      } catch (std::exception& ex) {
        LOG(ex.what());
        continue;
      } catch (...) {
        LOG("unknown exception");
        continue;
      }
      try {
        RegionPtr rptr = getHelper()->getRegion(m_subPath.c_str());
        if (rptr != NULLPTR) {
          ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_mutex);
          ASSERT(m_subRegionCreateDone == true, "subRegionCreate Not Done");
          return 0;
        }
      } catch (Exception& ex) {
        LOG(ex.getMessage());
      } catch (std::exception& ex) {
        LOG(ex.what());
      } catch (...) {
        LOG("getRegion: unknown exception");
      }
    }
    return 0;
  }
  void setRegionFlag() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_mutex);
    m_regionCreateDone = true;
  }
  void setSubRegionFlag() {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_mutex);
    m_subRegionCreateDone = true;
  }
  void start() {
    m_running = true;
    activate();
  }
  void stop() {
    m_running = false;
    wait();
  }
};

static int numberOfLocators = 1;
bool isLocalServer = true;
bool isLocator = true;
const char* locHostPort =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
GetRegionThread* getThread = NULL;
RegionPtr regionPtr;
DUNIT_TASK(s1p1, Setup)
  {
    CacheHelper::initLocator(1);
    CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                            locHostPort);
    LOG("SERVER started");
  }
ENDTASK

/* setup a normal region */
DUNIT_TASK(s2p2, CreateNormalRegion)
  {
    initClientWithPool(true, "__TEST_POOL1__", locHostPort, "ServerGroup1",
                       NULLPTR, 0, true);
    LOG("create normal region");
    getThread =
        new GetRegionThread("DistRegionAck", "DistRegionAck/AuthSubregion");
    getThread->start();
    regionPtr = getHelper()->createPooledRegion(
        "DistRegionAck", USE_ACK, locHostPort, "__TEST_POOL1__", true, true);
    getThread->setRegionFlag();
    AttributesFactory af;
    RegionAttributesPtr rattrsPtr = af.createRegionAttributes();
    getThread->setSubRegionFlag();
    LOG("create normal region successful");
  }
END_TASK(CreateNormalRegion)

DUNIT_TASK(s2p2, CloseCache2)
  {
    getThread->stop();
    delete getThread;
    cleanProc();
  }
END_TASK(CloseCache2)

DUNIT_TASK(s1p1, CloseCache)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER closed");
  }
ENDTASK

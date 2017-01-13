/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

/**
* @file    UdpIpc.cpp
* @since   1.0
* @version 1.0
* @see
*
*/

// ----------------------------------------------------------------------------

#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/gfcpp_globals.hpp>

#include "fwklib/UDPIpc.hpp"
#include "fwk/UdpIpc.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"

#include "fwklib/FwkExport.hpp"

using namespace gemfire;
using namespace gemfire::testframework;

static UdpIpc *g_test = NULL;

// ----------------------------------------------------------------------------

TESTTASK initialize(const char *initArgs) {
  int32_t result = FWK_SUCCESS;
  if (g_test == NULL) {
    FWKINFO("Initializing Fwk library.");
    try {
      g_test = new UdpIpc(initArgs);
    } catch (const FwkException &ex) {
      FWKSEVERE("initialize: caught exception: " << ex.getMessage());
      result = FWK_SEVERE;
    }
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO("Finalizing Fwk library.");
  if (g_test != NULL) {
    g_test->cacheFinalize();
    delete g_test;
    g_test = NULL;
  }
  return result;
}

// ----------------------------------------------------------------------------

void UdpIpc::checkTest(const char *taskId) {
  SpinLockGuard guard(m_lck);
  setTask(taskId);
  if (m_cache == NULLPTR) {
    PropertiesPtr pp = Properties::create();

    cacheInitialize(pp);

    // UdpIpc specific initialization
    // none
  }
}

// ----------------------------------------------------------------------------

TESTTASK doShowEndPoints(const char *taskId) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest(taskId);
  std::string bb("GFE_BB");
  std::string key("EndPoints");
  std::string epts = g_test->bbGetString(bb, key);
  if (epts.empty()) {
    FWKSEVERE("EndPoints are not set in BB.");
    result = FWK_SEVERE;
  } else {
    FWKINFO("EndPoints are: " << epts);
  }

  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doService(const char *taskId) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest(taskId);

  g_test->doService();

  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doClient(const char *taskId) {
  int32_t result = FWK_SUCCESS;
  g_test->checkTest(taskId);

  g_test->doClient();

  return result;
}

// ----------------------------------------------------------------------------

void UdpIpc::doService() {
  bool expectResponse = g_test->getBoolValue("expectResponse");

  int32_t port = g_test->getIntValue("port");
  port = (port < 0) ? 3212 : port;

  int32_t totThreads = g_test->getIntValue("totThreads");
  totThreads = (totThreads < 0) ? 10 : totThreads;

  int32_t inThreads = g_test->getIntValue("inThreads");
  inThreads = (inThreads < 0) ? (totThreads / 3) : inThreads;

  int32_t procThreads = g_test->getIntValue("procThreads");
  procThreads = (procThreads < 0) ? (totThreads / 3) : procThreads;

  int32_t outThreads = g_test->getIntValue("outThreads");
  outThreads = (outThreads < 0) ? (totThreads / 3) : outThreads;

  std::string label = g_test->getStringValue("label");
  if (label.empty()) {
    label = "BBqueues";
  }

  int32_t timedInterval = getTimeValue("timedInterval");
  if (timedInterval <= 0) {
    timedInterval = 120;
  } else {
    timedInterval += 60;
  }

  char *fqdn = ACE_OS::getenv("GF_FQDN");
  if (fqdn == NULL) {
    FWKEXCEPTION("GF_FQDN not set in the environment.");
  }

  UDPMessageQueues *shared = new UDPMessageQueues(label);
  Receiver recv(shared, port);
  TestProcessor serv(shared, expectResponse);
  Responder resp(shared, port);

  Service farm(totThreads);

  try {
    uint32_t thrds = farm.runThreaded(&recv, inThreads);
    FWKINFO("Server running recv on " << thrds << " threads.");

    thrds = farm.runThreaded(&serv, procThreads);
    FWKINFO("Server running serv on " << thrds << " threads.");

    thrds = farm.runThreaded(&resp, outThreads);
    FWKINFO("Server running resp on " << thrds << " threads.");

    char buff[512];
    sprintf(buff, "%s:%d", fqdn, port);
    std::string key("ServerAddr");
    std::string val(buff);
    g_test->bbSet(label, key, val);

    perf::sleepSeconds(timedInterval);

    FWKINFO("Time to stop.");
    farm.stopThreads();
  } catch (FwkException &ex) {
    FWKSEVERE("Caught exception " << ex.getMessage());
  }

  FWKINFO("Server stopped.");
  perf::sleepSeconds(1);
  delete shared;
}

void UdpIpc::doClient() {
  bool expectResponse = g_test->getBoolValue("expectResponse");

  std::string label = g_test->getStringValue("label");
  if (label.empty()) {
    label = "BBqueues";
  }

  int32_t timedInterval = getTimeValue("timedInterval");
  if (timedInterval <= 0) timedInterval = 60;

  std::string serverAddr;
  int32_t tries = 60;
  std::string key("ServerAddr");
  while (serverAddr.empty() && (--tries > 0)) {
    perf::sleepSeconds(1);
    serverAddr = g_test->bbGetString(label, key);
  }
  if (serverAddr.empty()) {
    FWKEXCEPTION("Server address is not set in BB " << label << ".");
  } else {
    FWKINFO("Server address is: " << serverAddr);
  }

  int32_t msgCnt = 0;
  try {
    UDPMessageClient clnt(serverAddr);
    UDPMessage msg;

    const ACE_Time_Value wait(10);
    FWKINFO("Start");
    ACE_Time_Value now = ACE_OS::gettimeofday();
    ACE_Time_Value interval(timedInterval);
    ACE_Time_Value end = now + interval;
    std::string str("Just junk");
    while (end > now) {
      msg.setMessage(str);
      msg.setCmd(ACK_REQUEST);
      msg.sendTo(clnt.getConn(), clnt.getServer());
      msgCnt++;
      if (expectResponse) {
        msg.receiveFrom(clnt.getConn(), &wait);
        if (msg.length() == 0) {
          FWKINFO("NULL response.");
        } else {
          std::string reply = msg.getMessage();
          if (reply == "A result for you.") {
            // nevermind
          } else {
            FWKWARN("Reply was not as expected: " << reply);
          }
        }
      }
      now = ACE_OS::gettimeofday();
    }
  } catch (FwkException &ex) {
    FWKSEVERE("Caught exception " << ex.getMessage());
  }
  FWKINFO("Stop");
  FWKINFO("Client sent " << msgCnt << " messages");
}

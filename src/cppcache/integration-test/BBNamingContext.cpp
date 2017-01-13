/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _WIN32

#include "BBNamingContext.hpp"

#include <ace/ACE.h>
#include "fwklib/FwkBBServer.hpp"
#include "fwklib/FwkBBClient.hpp"
#include "fwklib/FwkStrCvt.hpp"
#include <fwklib/FwkException.hpp>

#define ERR_MAX 10

using namespace gemfire::testframework;

static int hashcode(char* str) {
  if (str == NULL) {
    return 0;
  }
  int localHash = 0;

  int prime = 31;
  char* data = str;
  for (int i = 0; i < 50 && (data[i] != '\0'); i++) {
    localHash = prime * localHash + data[i];
  }
  if (localHash > 0) return localHash;
  return -1 * localHash;
}

static int getRandomNum() {
  char* testName = ACE_OS::getenv("TESTNAME");

  int seed = hashcode(testName) + 11;

  printf("seed for BBPort process %d\n", seed);
  // The integration tests rely on the pseudo-random
  // number generator being seeded with a very particular
  // value specific to the test by way of the test name.
  // Whilst this approach is pessimal, it can not be
  // remedied as the test depend upon it.
  ACE_OS::srand(seed);
  return (ACE_OS::rand() % 49999) + 14000;
}

int G_BBPORT = getRandomNum();

class BBNamingContextClientImpl {
  FwkBBClient* m_bbc;
  uint32_t m_errCount;
  bool checkValue(const std::string& k, const std::string& k1,
                  const std::string& value);

 public:
  BBNamingContextClientImpl();
  ~BBNamingContextClientImpl();
  void open();
  void close();
  void dump();
  int rebind(const char* key, const char* value, char* type);
  int resolve(const char* key, char* value, char* type);
};

class BBNamingContextServerImpl {
  FwkBBServer* m_bbServer;
  UDPMessageQueues* m_shared;
  STReceiver* m_recv;
  BBProcessor* m_serv;
  Responder* m_resp;
  Service* m_farm;

 public:
  BBNamingContextServerImpl();
  ~BBNamingContextServerImpl();
};
//
// Impls:
//
BBNamingContextClientImpl::BBNamingContextClientImpl()
    : m_bbc(NULL), m_errCount(0) {}
BBNamingContextClientImpl::~BBNamingContextClientImpl() { close(); }
void BBNamingContextClientImpl::open() {
  try {
    // char * bbPort = ACE_OS::getenv( "BB_PORT" );

    char temp[8];
    char* bbPort = ACE_OS::itoa(G_BBPORT, temp, 10);

    char buf[1024];
    ACE_OS::sprintf(buf, "localhost:%s", bbPort);
    fprintf(stdout, "Blackboard client is talking on %s\n", buf);
    fflush(stdout);
    m_bbc = new FwkBBClient(buf);
  } catch (FwkException& e) {
    FWKEXCEPTION("create bb client encounted Exception: " << e.getMessage());
  } catch (...) {
    FWKEXCEPTION("create bb client unknow exception\n");
  }
}
void BBNamingContextClientImpl::close() {
  if (m_bbc != NULL) {
    delete m_bbc;
    m_bbc = NULL;
  }
}
int BBNamingContextClientImpl::rebind(const char* key, const char* value,
                                      char* type) {
  // fprintf(stdout, "bind: key=%s, value=%s\n", key, value);
  if (m_bbc == NULL) {
    return -1;
  }
  if (m_errCount > ERR_MAX) {
    close();
    return -1;
  }

  try {
    std::string k(key);
    std::string k1("1");
    std::string v(value);
    m_bbc->set(k, k1, v);
    if (false == checkValue(k, k1, value)) {
      m_errCount++;
    } else {
      if (m_errCount > 0) {
        m_errCount = 0;
      }
      return 0;
    }
  } catch (FwkException& e) {
    FWKEXCEPTION(" rebind encounted Exception: " << e.getMessage());
    m_errCount++;
  } catch (...) {
    FWKEXCEPTION("rebind unknown exception\n");
    m_errCount++;
  }
  return -1;
}
void BBNamingContextClientImpl::dump() {
  if (m_bbc == NULL) {
    return;
  }
  if (m_errCount > ERR_MAX) {
    close();
    return;
  }
  try {
    std::string bb = m_bbc->dump();
    FWKINFO("Dump Blackboard " << bb);
    if (m_errCount > 0) {
      m_errCount = 0;
    }
  } catch (FwkException& e) {
    FWKEXCEPTION("create dump encounted Exception: " << e.getMessage());
    m_errCount++;
  } catch (...) {
    FWKEXCEPTION("dump unknown exception\n");
    m_errCount++;
  }
}
int BBNamingContextClientImpl::resolve(const char* key, char* value,
                                       char* type) {
  // fprintf(stdout, "resolve: key=%s\n", key);fflush(stdout);
  if (m_bbc == NULL) {
    return -1;
  }
  if (m_errCount > ERR_MAX) {
    close();
    return -1;
  }
  try {
    std::string k(key);
    std::string k1("1");
    std::string v = m_bbc->getString(k, k1);
    // fprintf(stdout, "resolve: got value %s for key=%s\n", v.c_str(),
    // key);fflush(stdout);
    ACE_OS::strcpy(value, v.c_str());
    if (m_errCount > 0) {
      m_errCount = 0;
    }
    return v.length() == 0 ? -1 : 0;
  } catch (FwkException& e) {
    FWKEXCEPTION("create resolve encounted Exception: " << e.getMessage());
    m_errCount++;
  } catch (...) {
    FWKEXCEPTION("resolve unknown exception\n");
    m_errCount++;
  }
  return -1;
}
bool BBNamingContextClientImpl::checkValue(const std::string& k,
                                           const std::string& k1,
                                           const std::string& value) {
  bool valid = false;
  try {
    std::string v = m_bbc->getString(k, k1);
    if (value == v) valid = true;
  } catch (FwkException& e) {
    FWKEXCEPTION("create resolve encounted Exception: " << e.getMessage());
  } catch (...) {
    FWKEXCEPTION("resolve unknown exception\n");
  }
  return valid;
}

BBNamingContextServerImpl::BBNamingContextServerImpl() {
  try {
    // char * bbPort = ACE_OS::getenv( "BB_PORT" );
    char temp[8];
    char* bbPort = ACE_OS::itoa(G_BBPORT, temp, 10);
    FwkStrCvt bPort(bbPort);
    uint32_t prt = bPort.toUInt32();
    fprintf(stdout, "Blackboard server is on port:%u\n", prt);
    fflush(stdout);
    m_bbServer = new FwkBBServer();
    m_shared = new UDPMessageQueues("BBQueues");
    m_recv = new STReceiver(m_shared, prt);
    m_serv = new BBProcessor(m_shared, m_bbServer);
    m_resp = new Responder(m_shared, prt);
    m_farm = new Service(3);
    uint32_t thrds = m_farm->runThreaded(m_recv, 1);
    thrds = m_farm->runThreaded(m_serv, 1);
    thrds = m_farm->runThreaded(m_resp, 1);
  } catch (FwkException& e) {
    FWKEXCEPTION("create bb server encounted Exception: " << e.getMessage());
  } catch (...) {
    FWKSEVERE("create bb server unknown exception\n");
  }
}

BBNamingContextServerImpl::~BBNamingContextServerImpl() {
  delete m_farm;
  delete m_bbServer;
  delete m_shared;
  delete m_recv;
  delete m_serv;
  delete m_resp;
}
//
// client
//
BBNamingContextClient::BBNamingContextClient() {
  m_impl = new BBNamingContextClientImpl();
}
BBNamingContextClient::~BBNamingContextClient() {
  if (m_impl) {
    delete m_impl;
    m_impl = NULL;
  }
}
void BBNamingContextClient::open() { m_impl->open(); }
void BBNamingContextClient::close() { m_impl->close(); }
void BBNamingContextClient::dump() { m_impl->dump(); }
int BBNamingContextClient::rebind(const char* key, const char* value,
                                  char* type) {
  return m_impl->rebind(key, value, type);
}
int BBNamingContextClient::resolve(const char* key, char* value, char* type) {
  return m_impl->resolve(key, value, type);
}
//
// server
//
BBNamingContextServer::BBNamingContextServer() {
  m_impl = new BBNamingContextServerImpl();
}
BBNamingContextServer::~BBNamingContextServer() {
  if (m_impl != NULL) {
    delete m_impl;
    m_impl = NULL;
  }
}

#endif

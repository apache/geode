/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    FwkBBServer.cpp
  * @since   1.0
  * @version 1.0
  * @see
  */

// ----------------------------------------------------------------------------

#include "FwkBBServer.hpp"
#include "FwkStrCvt.hpp"
#include "FwkLog.hpp"
#include "UDPIpc.hpp"

#include <set>

using namespace gemfire;
using namespace gemfire::testframework;

// ----------------------------------------------------------------------------

FwkBBServer::~FwkBBServer() {
  //  stop();
  clear();
}

////
///----------------------------------------------------------------------------
//
// void FwkBBServer::stop() {
//  m_farm->stopThreads();
//  clear();
//  delete m_farm;
//}
//
////
///----------------------------------------------------------------------------
//
// void FwkBBServer::start( uint32_t port ) {
//  UDPMessageQueues * shared = new UDPMessageQueues( "BlackBoard" );
//
//  m_farm = new Service( 10 );
//
//  Receiver recv( shared, port );
//  BBProcessor serv( shared, this );
//  Responder resp( shared, port );
//
//  uint32_t thrds = m_farm->runThreaded( &recv, 5 );
//  thrds = m_farm->runThreaded( &resp, 4 );
//  thrds = m_farm->runThreaded( &serv, 1 );
//}

// ----------------------------------------------------------------------------

void FwkBBServer::clear() {
  m_nameKeyMap.clear();
  m_nameCounterMap.clear();
}

// ----------------------------------------------------------------------------
// <start><id>IIII<c>dump<end>
//    bool dump(std::string& result);
// <start><id>IIII<c>dump<p>BBName<end>
//    bool dump(const char* BBName, std::string& result)
// ----------------------------------------------------------------------------

void FwkBBServer::onDump(FwkBBMessage& message, FwkBBMessage& reply) {
  std::string sParameter1 = message.getParameter(0);

  std::string response;
  if (sParameter1.size()) {
    dump(sParameter1, response);
  } else {  // dump all
    dump(response);
  }
  reply.setResult(response);
}

void FwkBBServer::onClearBB(FwkBBMessage& message) {
  std::string sParameter1 = message.getParameter(0);

  if (sParameter1.size()) clearBB(sParameter1);
}

// ----------------------------------------------------------------------------
// <start><id>IIII<c>get<p>key<p>BBName<p>Key<end>
//   bool get(const char* BBName, const char* Key, std::string& result)
// <start><id>IIII<c>get<p>counter<p>BBName<p>Key<end>
//   bool get(const char* BBName, const char* Key,
//     int64_t* piResult)
// ----------------------------------------------------------------------------

void FwkBBServer::onGet(FwkBBMessage& message, FwkBBMessage& reply) {
  std::string sParameter1 = message.getParameter(0);
  std::string sParameter2 = message.getParameter(1);
  std::string sParameter3 = message.getParameter(2);

  if (sParameter1.size() && sParameter2.size() && sParameter3.size()) {
    if (sParameter1 == "key") {
      reply.setResult(getString(sParameter2, sParameter3));
    } else if (sParameter1 == "counter") {
      int64_t result = get(sParameter2, sParameter3);
      reply.setResult(FwkStrCvt(result).toString());
    }
  }
}

// ----------------------------------------------------------------------------
// <start><id>IIII<c>set<p>key<p>BBName<p>Key<p>pszValue<end>
// bool set(const char* BBName, const char* Key, const char* pszValue)
// <start><id>IIII<c>set<p>counter<p>BBName<p>Key<p>Value<end>
// bool set(const char* BBName, const char* Key,
//   const int64_t Value)
// ----------------------------------------------------------------------------

void FwkBBServer::onSet(FwkBBMessage& message) {
  std::string sParameter1 = message.getParameter(0);
  std::string sParameter2 = message.getParameter(1);
  std::string sParameter3 = message.getParameter(2);
  std::string sParameter4 = message.getParameter(3);

  if (sParameter1.size() && sParameter2.size() && sParameter3.size() &&
      sParameter4.size()) {
    if (sParameter1 == "key") {
      set(sParameter2, sParameter3, sParameter4);
    } else if (sParameter1 == "counter") {
      int64_t Value = FwkStrCvt(sParameter4.c_str()).toInt64();
      set(sParameter2, sParameter3, Value);
    }
  }
}

// ----------------------------------------------------------------------------
// <start><id>IIII<c>add<p>BBName<p>Key<p>Value<end>
// bool add(const char* BBName, const char* Key,
//   const int64_t Value, int64_t* piResult)
// ----------------------------------------------------------------------------

void FwkBBServer::onAdd(FwkBBMessage& message, FwkBBMessage& reply) {
  int64_t result = 0;
  std::string sParameter1 = message.getParameter(0);
  std::string sParameter2 = message.getParameter(1);
  std::string sParameter3 = message.getParameter(2);

  if (sParameter1.size() && sParameter2.size() && sParameter3.size()) {
    int64_t Value = FwkStrCvt(sParameter3).toInt64();
    result = add(sParameter1, sParameter2, Value);
  }

  reply.setResult(FwkStrCvt(result).toString());
}

// ----------------------------------------------------------------------------
// <start><id>IIII<c>subtract<p>BBName<p>Key<p>Value<end>
// bool subtract(const char* BBName, const char* Key,
//   const int64_t Value, int64_t* piResult)
// ----------------------------------------------------------------------------

void FwkBBServer::onSubtract(FwkBBMessage& message, FwkBBMessage& reply) {
  int64_t result = 0;
  std::string sParameter1 = message.getParameter(0);
  std::string sParameter2 = message.getParameter(1);
  std::string sParameter3 = message.getParameter(2);

  if (sParameter1.size() && sParameter2.size() && sParameter3.size()) {
    int64_t Value = FwkStrCvt(sParameter3).toInt64();
    result = subtract(sParameter1, sParameter2, Value);
  }

  reply.setResult(FwkStrCvt(result).toString());
}

// ----------------------------------------------------------------------------
// <start><id>IIII<c>increment<p>BBName<p>Key<p>Value<end>
// bool increment(const char* BBName, const char* Key,
//   int64_t* piResult)
// ----------------------------------------------------------------------------

void FwkBBServer::onIncrement(FwkBBMessage& message, FwkBBMessage& reply) {
  int64_t result = 0;
  std::string sParameter1 = message.getParameter(0);
  std::string sParameter2 = message.getParameter(1);

  if (sParameter1.size() && sParameter2.size()) {
    result = increment(sParameter1, sParameter2);
  }

  reply.setResult(FwkStrCvt(result).toString());
}

// ----------------------------------------------------------------------------
// <start><id>IIII<c>decrement<p>BBName<p>Key<p>Value<end>
// bool decrement(const char* BBName, const char* Key,
//   int64_t* piResult)
// ----------------------------------------------------------------------------

void FwkBBServer::onDecrement(FwkBBMessage& message, FwkBBMessage& reply) {
  int64_t result = 0;
  std::string sParameter1 = message.getParameter(0);
  std::string sParameter2 = message.getParameter(1);

  if (sParameter1.size() && sParameter2.size()) {
    result = decrement(sParameter1, sParameter2);
  }

  reply.setResult(FwkStrCvt(result).toString());
}

// ----------------------------------------------------------------------------
// <start><id>IIII<c>zero<p>BBName<p>Key<end>
// bool zero(const char* BBName, const char* Key)
// ----------------------------------------------------------------------------

void FwkBBServer::onZero(FwkBBMessage& message) {
  std::string sParameter1 = message.getParameter(0);
  std::string sParameter2 = message.getParameter(1);

  if (sParameter1.size() && sParameter2.size()) zero(sParameter1, sParameter2);
}

// ----------------------------------------------------------------------------
// <start><id>IIII<c>setIfGreater<p>BBName<p>Key<p>Value<end>
// bool setIfGreater(const char* BBName,
//   const char* Key, const int64_t Value, int64_t* piResult)
// ----------------------------------------------------------------------------

void FwkBBServer::onSetIfGreater(FwkBBMessage& message, FwkBBMessage& reply) {
  int64_t result = 0;
  std::string sParameter1 = message.getParameter(0);
  std::string sParameter2 = message.getParameter(1);
  std::string sParameter3 = message.getParameter(2);

  if (sParameter1.size() && sParameter2.size() && sParameter3.size()) {
    int64_t Value = FwkStrCvt(sParameter3).toInt64();
    result = setIfGreater(sParameter1, sParameter2, Value);
  }

  reply.setResult(FwkStrCvt(result).toString());
}

// ----------------------------------------------------------------------------
// <start><id>IIII<c>setIfLess<p>BBName<p>Key<p>Value<end>
// bool setIfLess(const char* BBName,
//   const char* Key, const int64_t Value, int64_t* piResult)
// ----------------------------------------------------------------------------

void FwkBBServer::onSetIfLess(FwkBBMessage& message, FwkBBMessage& reply) {
  int64_t result = 0;
  std::string sParameter1 = message.getParameter(0);
  std::string sParameter2 = message.getParameter(1);
  std::string sParameter3 = message.getParameter(2);

  if (sParameter1.size() && sParameter2.size() && sParameter3.size()) {
    int64_t Value = FwkStrCvt(sParameter3).toInt64();
    result = setIfLess(sParameter1, sParameter2, Value);
  }

  reply.setResult(FwkStrCvt(result).toString());
}

// ----------------------------------------------------------------------------

void FwkBBServer::dump(std::string& result) {
  // set of all bb's
  std::set<std::string> bbSet;

  // get all bbNames in nameKeyMap
  NameKeyMap::iterator nameKeyIt = m_nameKeyMap.begin();
  while (nameKeyIt != m_nameKeyMap.end()) {
    bbSet.insert(nameKeyIt->first.getName());
    nameKeyIt++;
  }

  // get all bbnames in nameCounterMap
  NameCounterMap::iterator nameCounterIt = m_nameCounterMap.begin();
  while (nameCounterIt != m_nameCounterMap.end()) {
    bbSet.insert(nameCounterIt->first.getName());
    nameCounterIt++;
  }

  // dump all bb's
  std::set<std::string>::iterator bbIt = bbSet.begin();
  while (bbIt != bbSet.end()) {
    std::string bb(*bbIt);
    std::string response;
    dump(bb, response);
    result += response;
    bbIt++;
  }
}

// ----------------------------------------------------------------------------

void FwkBBServer::dump(const std::string& BBName, std::string& result) {
  result = "\nBlack Board: ";
  result += BBName;
  result += "\n";

  // get keys
  NameKeyMap::iterator nameKeyIt = m_nameKeyMap.begin();

  while (nameKeyIt != m_nameKeyMap.end()) {
    if (nameKeyIt->first.getName() == BBName) {
      result += "  Key: ";
      result += nameKeyIt->first.getKey();
      result += " Value: ";
      result += nameKeyIt->second;
      result += "\n";
    }

    nameKeyIt++;
  }

  // get counters
  NameCounterMap::iterator nameCounterIt = m_nameCounterMap.begin();

  while (nameCounterIt != m_nameCounterMap.end()) {
    if (nameCounterIt->first.getName() == BBName) {
      result += "  Counter: ";
      result += nameCounterIt->first.getKey();
      result += " Value: ";
      result += FwkStrCvt(nameCounterIt->second).toString();
      result += "\n";
    }

    nameCounterIt++;
  }
}

// ----------------------------------------------------------------------------

void FwkBBServer::clearBB(const std::string& BBName) {
  NameKeyMap::iterator nameKeyIt = m_nameKeyMap.begin();
  NameKeyMap::iterator prevK = m_nameKeyMap.begin();
  while (nameKeyIt != m_nameKeyMap.end()) {
    //    FWKINFO( "Looking at: " << nameKeyIt->first.getName() << "  Key: " <<
    //    nameKeyIt->first.getKey() );
    if (nameKeyIt->first.getName() == BBName) {
      //    FWKINFO( "Calling erase." );
      m_nameKeyMap.erase(nameKeyIt);
      nameKeyIt = prevK;
    } else {
      prevK = nameKeyIt;
    }
    nameKeyIt++;
  }

  NameCounterMap::iterator nameCounterIt = m_nameCounterMap.begin();
  NameCounterMap::iterator prevC = m_nameCounterMap.begin();
  while (nameCounterIt != m_nameCounterMap.end()) {
    //    FWKINFO( "Looking at: " << nameCounterIt->first.getName() << "
    //    Counter: " << nameCounterIt->first.getKey() );
    if (nameCounterIt->first.getName() == BBName) {
      //    FWKINFO( "Calling erase." );
      m_nameCounterMap.erase(nameCounterIt);
      nameCounterIt = prevC;
    } else {
      prevC = nameCounterIt;
    }
    nameCounterIt++;
  }
}

// ----------------------------------------------------------------------------

std::string FwkBBServer::getString(const std::string& BBName,
                                   const std::string& Key) {
  std::string result;
  NameKeyPair nameKeyPair(BBName, Key);

  NameKeyMap::iterator it = m_nameKeyMap.find(nameKeyPair);
  if (it != m_nameKeyMap.end()) {  // if found
    result = it->second;
  }

  return result;
}

// ----------------------------------------------------------------------------

int64_t FwkBBServer::get(const std::string& BBName, const std::string& Key) {
  int64_t result = 0;
  NameKeyPair nameKeyPair(BBName, Key);

  NameCounterMap::iterator it = m_nameCounterMap.find(nameKeyPair);
  if (it != m_nameCounterMap.end()) {  // if found
    result = it->second;
  } else {  // not found, set to Value;
    m_nameCounterMap[nameKeyPair] = 0;
  }

  return result;
}

// ----------------------------------------------------------------------------

void FwkBBServer::set(const std::string& BBName, const std::string& Key,
                      const std::string& Value) {
  NameKeyPair nameKeyPair(BBName, Key);

  NameKeyMap::iterator it = m_nameKeyMap.find(nameKeyPair);
  if (it != m_nameKeyMap.end()) {  // if found
    it->second = std::string(Value);
  } else {  // not found, set to Value;
    m_nameKeyMap[nameKeyPair] = std::string(Value);
  }
}

// ----------------------------------------------------------------------------

void FwkBBServer::set(const std::string& BBName, const std::string& Key,
                      const int64_t Value) {
  NameKeyPair nameKeyPair(BBName, Key);

  NameCounterMap::iterator it = m_nameCounterMap.find(nameKeyPair);
  if (it != m_nameCounterMap.end()) {  // if found
    it->second = Value;
  } else {  // not found, set to Value;
    m_nameCounterMap[nameKeyPair] = Value;
  }
}

// ----------------------------------------------------------------------------

int64_t FwkBBServer::add(const std::string& BBName, const std::string& Key,
                         const int64_t Value) {
  int64_t result = Value;
  NameKeyPair nameKeyPair(BBName, Key);
  NameCounterMap::iterator it = m_nameCounterMap.find(nameKeyPair);
  if (it != m_nameCounterMap.end()) {  // if found
    it->second = it->second + Value;
    result = it->second;
  } else {  // not found, set to Value;
    m_nameCounterMap[nameKeyPair] = Value;
  }

  return result;
}

// ----------------------------------------------------------------------------

int64_t FwkBBServer::subtract(const std::string& BBName, const std::string& Key,
                              const int64_t Value) {
  return add(BBName, Key, Value * -1);
}

// ----------------------------------------------------------------------------

int64_t FwkBBServer::increment(const std::string& BBName,
                               const std::string& Key) {
  return add(BBName, Key, 1);
}

// ----------------------------------------------------------------------------

int64_t FwkBBServer::decrement(const std::string& BBName,
                               const std::string& Key) {
  return add(BBName, Key, -1);
}

// ----------------------------------------------------------------------------

void FwkBBServer::zero(const std::string& BBName, const std::string& Key) {
  set(BBName, Key, 0ll);
}

// ----------------------------------------------------------------------------

int64_t FwkBBServer::setIfGreater(const std::string& BBName,
                                  const std::string& Key, const int64_t Value) {
  int64_t result = Value;
  NameKeyPair nameKeyPair(BBName, Key);

  NameCounterMap::iterator it = m_nameCounterMap.find(nameKeyPair);
  if (it != m_nameCounterMap.end()) {  // if found
    if (Value > it->second) it->second = Value;
    result = it->second;
  } else {  // not found, set to Value;
    m_nameCounterMap[nameKeyPair] = Value;
  }

  return result;
}

// ----------------------------------------------------------------------------

int64_t FwkBBServer::setIfLess(const std::string& BBName,
                               const std::string& Key, const int64_t Value) {
  int64_t result = Value;

  NameKeyPair nameKeyPair(BBName, Key);

  NameCounterMap::iterator it = m_nameCounterMap.find(nameKeyPair);
  if (it != m_nameCounterMap.end()) {  // if found
    if (Value < it->second) it->second = Value;
    result = it->second;
  } else {  // not found, set to Value;
    m_nameCounterMap[nameKeyPair] = Value;
  }

  return result;
}

// ----------------------------------------------------------------------------

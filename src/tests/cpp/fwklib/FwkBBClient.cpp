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

/**
  * @file    FwkBBClient.cpp
  * @since   1.0
  * @version 1.0
  * @see
  */

// ----------------------------------------------------------------------------

#include "FwkBBClient.hpp"
#include "FwkLog.hpp"
#include "FwkStrCvt.hpp"

#include "ace/Synch.h"
#include "ace/OS.h"

using namespace apache::geode::client;
using namespace apache::geode::client::testframework;

// ----------------------------------------------------------------------------

void FwkBBClient::sendToServer(FwkBBMessage& message) {
  message.setId(FwkStrCvt(getNewMessageId()).toString());
  std::string msg = message.toMessageStream();
  if (msg.empty()) {
    FWKEXCEPTION("Empty message in FwkBBClient::sendToServer");
  }

  int32_t tries = 5;
  bool done = false;
  ACE_Time_Value timeout(CLIENT_WAIT_TIME_FOR_REPLY);
  int32_t sCnt = 0;
  int32_t rCnt = 0;
  int32_t nrCnt = 0;
  int32_t eCnt = 0;
  while (!done && (tries-- > 0)) {
    try {
      UDPMessage udpMsg(msg);
      udpMsg.setSender(m_client.getServer());
      if (!udpMsg.send(m_client.getConn())) {
        sCnt++;
        FWKEXCEPTION("Send failed in FwkBBClient::sendToServer");
      }
      if (!udpMsg.receiveFrom(m_client.getConn(), &timeout)) {
        rCnt++;
        FWKEXCEPTION("Receive failed in FwkBBClient::sendToServer");
      }
      if (udpMsg.length() == 0) {
        nrCnt++;
        FWKEXCEPTION("No Reply from server in FwkBBClient::sendToServer");
      }
      message.fromMessageStream(udpMsg.getMessage());
      m_result.clear();
      m_result = message.getResult();
      done = true;
    } catch (FwkException& /* ex */) {
      eCnt++;
      // FWKSEVERE( "Caught exception in FwkBBClient::sendToServer: " <<
      // ex.getMessage() << ", tried to send: " << msg.substr( 0, 50 ) );
    }
  }
  if (!done) {
    FWKSEVERE("FwkBBClient::sendToServer: FAILED, ( "
              << eCnt << ", " << sCnt << ", " << rCnt << ", " << nrCnt
              << " ) tried to send: " << msg.substr(0, 50));
  }
}

// ----------------------------------------------------------------------------

std::string FwkBBClient::dump() {
  FwkBBMessage message(BB_DUMP_COMMAND);
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
  return m_result;
}

// ----------------------------------------------------------------------------

std::string FwkBBClient::dump(const std::string& BBName) {
  FwkBBMessage message(BB_DUMP_COMMAND);
  message.addParameter(BBName);
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
  return m_result;
}

// ----------------------------------------------------------------------------

void FwkBBClient::clear(const std::string& BBName) {
  FwkBBMessage message(BB_CLEAR_COMMAND);
  message.addParameter(BBName);
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
}

// ----------------------------------------------------------------------------

std::string FwkBBClient::getString(const std::string& BBName,
                                   const std::string& Key) {
  FwkBBMessage message(BB_GET_COMMAND);
  message.addParameter("key");
  message.addParameter(BBName);
  message.addParameter(Key);
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
  return m_result;
}

// ----------------------------------------------------------------------------

int64_t FwkBBClient::get(const std::string& BBName, const std::string& Key) {
  FwkBBMessage message(BB_GET_COMMAND);
  message.addParameter("counter");
  message.addParameter(BBName);
  message.addParameter(Key);
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
  return FwkStrCvt::toInt64(m_result.c_str());
}

// ----------------------------------------------------------------------------

void FwkBBClient::set(const std::string& BBName, const std::string& Key,
                      const std::string& Value) {
  FwkBBMessage message(BB_SET_COMMAND);
  message.addParameter("key");
  message.addParameter(BBName);
  message.addParameter(Key);
  message.addParameter(Value);
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
}

// ----------------------------------------------------------------------------

void FwkBBClient::set(const std::string& BBName, const std::string& Key,
                      const int64_t Value) {
  FwkBBMessage message(BB_SET_COMMAND);
  message.addParameter("counter");
  message.addParameter(BBName);
  message.addParameter(Key);
  message.addParameter(FwkStrCvt(Value).toString());
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
}

// ----------------------------------------------------------------------------

int64_t FwkBBClient::add(const std::string& BBName, const std::string& Key,
                         const int64_t Value) {
  FwkBBMessage message(BB_ADD_COMMAND);
  message.addParameter(BBName);
  message.addParameter(Key);
  message.addParameter(FwkStrCvt(Value).toString());
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
  return FwkStrCvt::toInt64(m_result.c_str());
}

// ----------------------------------------------------------------------------

int64_t FwkBBClient::subtract(const std::string& BBName, const std::string& Key,
                              const int64_t Value) {
  FwkBBMessage message(BB_SUBTRACT_COMMAND);
  message.addParameter(BBName);
  message.addParameter(Key);
  message.addParameter(FwkStrCvt(Value).toString());
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
  return FwkStrCvt::toInt64(m_result.c_str());
}

// ----------------------------------------------------------------------------

int64_t FwkBBClient::increment(const std::string& BBName,
                               const std::string& Key) {
  FwkBBMessage message(BB_INCREMENT_COMMAND);
  message.addParameter(BBName);
  message.addParameter(Key);
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
  return FwkStrCvt::toInt64(m_result.c_str());
}

// ----------------------------------------------------------------------------

int64_t FwkBBClient::decrement(const std::string& BBName,
                               const std::string& Key) {
  FwkBBMessage message(BB_DECREMENT_COMMAND);
  message.addParameter(BBName);
  message.addParameter(Key);
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
  return FwkStrCvt::toInt64(m_result.c_str());
}

// ----------------------------------------------------------------------------

void FwkBBClient::zero(const std::string& BBName, const std::string& Key) {
  FwkBBMessage message(BB_ZERO_COMMAND);
  message.addParameter(BBName);
  message.addParameter(Key);
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
}

// ----------------------------------------------------------------------------

int64_t FwkBBClient::setIfGreater(const std::string& BBName,
                                  const std::string& Key, const int64_t Value) {
  FwkBBMessage message(BB_SET_IF_GREATER_COMMAND);
  message.addParameter(BBName);
  message.addParameter(Key);
  message.addParameter(FwkStrCvt(Value).toString());
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
  return FwkStrCvt::toInt64(m_result.c_str());
}

// ----------------------------------------------------------------------------

int64_t FwkBBClient::setIfLess(const std::string& BBName,
                               const std::string& Key, const int64_t Value) {
  FwkBBMessage message(BB_SET_IF_LESS_COMMAND);
  message.addParameter(BBName);
  message.addParameter(Key);
  message.addParameter(FwkStrCvt(Value).toString());
  try {
    sendToServer(message);
  } catch (FwkException& ex) {
    ex.getMessage();
  }
  return FwkStrCvt::toInt64(m_result.c_str());
}

// ----------------------------------------------------------------------------

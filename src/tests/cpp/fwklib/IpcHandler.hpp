#pragma once

#ifndef APACHE_GEODE_GUARD_450b17d5d954d8270b6906f1510c355c
#define APACHE_GEODE_GUARD_450b17d5d954d8270b6906f1510c355c

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


#include <ace/SOCK_Stream.h>
#include <ace/OS.h>

#include <gfcpp/gf_base.hpp>

#include <string>

namespace apache {
namespace geode {
namespace client {
namespace testframework {

enum IpcMsg {
  IPC_NULL = 0xabc0,
  IPC_EXITING = 0xabc1,  // no response, client is exiting
  IPC_ERROR = 0xabc2,    // never sent
  IPC_ACK = 0xabc3,      // c<>d : single byte, no response
  IPC_PING = 0xabc4,     // d->c : single byte, no response
  //  IPC_STOP = 0xabc5, // d->c : single byte, expect ack
  IPC_EXIT = 0xabc6,  // d->c : single byte, expect ack
  IPC_RUN = 0xabc7,   // d->c : task id, expect ack
                      //  IPC_SYNC = 0xabc8, // c->d : single byte, expect ack
                      //  IPC_GO = 0xabc9,   // d->c : single byte, expect ack
  IPC_DONE = 0xabca   // c->d : single byte, expect ack
};

class IpcHandler {
 private:
  ACE_SOCK_Stream *m_io;

  bool checkPipe();
  int32_t readInt(int32_t waitSeconds);
  bool sendIpcMsg(IpcMsg msg, int32_t waitSeconds = 0);
  bool sendBuffer(IpcMsg msg, const char *str);
  std::string readString(int32_t waitSeconds);
  IpcMsg readIpcMsg(int32_t waitSeconds);

 public:
  IpcHandler(const ACE_INET_Addr &driver, int32_t maxWaitSecs = 0);

  inline IpcHandler(ACE_SOCK_Stream *io) : m_io(io) {
    ACE_OS::signal(SIGPIPE, SIG_IGN);  // Ignore broken pipe
  }

  ~IpcHandler();

  void close();

  IpcMsg getIpcMsg(int32_t waitSeconds, std::string &result);
  IpcMsg getIpcMsg(int32_t waitSeconds);
  char *checkBuffer(int32_t size);

  bool sendTask(char *task) { return sendBuffer(IPC_RUN, task); }

  bool sendResult(char *result) { return sendBuffer(IPC_DONE, result); }

  inline bool sendExit() {
    return sendIpcMsg(IPC_EXIT);
    close();
  }
};

}  // namespace testframework
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_450b17d5d954d8270b6906f1510c355c

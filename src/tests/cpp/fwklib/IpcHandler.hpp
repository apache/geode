/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __IpcHandler_hpp__
#define __IpcHandler_hpp__

#include <ace/SOCK_Stream.h>
#include <ace/OS.h>

#include <gfcpp/gf_base.hpp>

#include <string>

namespace gemfire {
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

}  // testframework
}  // gemfire

#endif  // __IpcHandler_hpp__

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __TcpIpc_hpp__
#define __TcpIpc_hpp__

#include <gfcpp/gf_base.hpp>
#include <ace/SOCK_Stream.h>
#include <ace/SOCK_Acceptor.h>
#include <string>

namespace gemfire {
namespace testframework {

class TcpIpc {
 private:
  ACE_SOCK_Stream* m_io;
  std::string m_ipaddr;

  void init(int32_t sockBufferSize = 0);
  void clearNagle(int32_t sock);
  int32_t setSize(int32_t sock, int32_t flag, int32_t size);
  int32_t getSize(int32_t sock, int32_t flag);

 public:
  TcpIpc(std::string& ipaddr, int32_t sockBufferSize = 0) : m_ipaddr(ipaddr) {
    init(sockBufferSize);
  }
  TcpIpc(char* ipaddr, int32_t sockBufferSize = 0) : m_ipaddr(ipaddr) {
    init(sockBufferSize);
  }

  TcpIpc(int32_t sockBufferSize = 0) { init(sockBufferSize); }

  ~TcpIpc();

  void close();

  bool listen(int32_t waitSecs = 0);
  bool accept(ACE_SOCK_Acceptor* acceptor, int32_t waitSecs = 0);
  bool connect(int32_t waitSecs = 0);

  int32_t readBuffer(char** buffer, int32_t waitSecs = 0);
  int32_t sendBuffer(char* buffer, int32_t length, int32_t waitSecs = 0) {
    char* buffs[1];
    buffs[0] = buffer;
    int32_t lengths[1];
    lengths[0] = length;
    return sendBuffers(1, buffs, lengths, waitSecs);
  }
  int32_t sendBuffers(int32_t cnt, char* buffers[], int32_t lengths[],
                      int32_t waitSecs = 0);
};

}  // testframework
}  // gemfire

#endif  // __TcpIpc_hpp__

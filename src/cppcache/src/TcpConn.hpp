/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __TcpConn_hpp__
#define __TcpConn_hpp__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Log.hpp>
#include "Connector.hpp"

#include <ace/SOCK_Stream.h>
#include <ace/OS.h>

namespace gemfire {

#ifdef WIN32

#define TCPLEVEL IPPROTO_TCP

#else

#include <sys/types.h>
#include <sys/socket.h>

#define TCPLEVEL SOL_TCP

#endif

class CPPCACHE_EXPORT TcpConn : public Connector {
 private:
  ACE_SOCK_Stream* m_io;

 protected:
  ACE_INET_Addr m_addr;
  uint32_t m_waitSeconds;

  int32_t m_maxBuffSizePool;

  enum SockOp { SOCK_READ, SOCK_WRITE };

  void clearNagle(ACE_SOCKET sock);
  int32_t maxSize(ACE_SOCKET sock, int32_t flag, int32_t size);

  virtual int32_t socketOp(SockOp op, char* buff, int32_t len,
                           uint32_t waitSeconds);

  virtual void createSocket(ACE_SOCKET sock);

 public:
  static int m_chunkSize;

  static int setChunkSize() {
    // Attempt to set chunk size to nearest OS page size
    // for perf improvement
    int pageSize = ACE_OS::getpagesize();
    if (pageSize > 16000000)
      return 16000000;
    else if (pageSize > 0)
      return pageSize + (16000000 / pageSize) * pageSize;

    return 16000000;
  }

  TcpConn();
  TcpConn(const char* hostname, int32_t port,
          uint32_t waitSeconds = DEFAULT_CONNECT_TIMEOUT,
          int32_t maxBuffSizePool = 0);
  TcpConn(const char* ipaddr, uint32_t waitSeconds = DEFAULT_CONNECT_TIMEOUT,
          int32_t maxBuffSizePool = 0);

  virtual ~TcpConn() { close(); }

  // Close this tcp connection
  virtual void close();

  void init();

  // Listen
  void listen(const char* hostname, int32_t port,
              uint32_t waitSeconds = DEFAULT_READ_TIMEOUT_SECS);
  void listen(const char* ipaddr,
              uint32_t waitSeconds = DEFAULT_READ_TIMEOUT_SECS);

  virtual void listen(ACE_INET_Addr addr,
                      uint32_t waitSeconds = DEFAULT_READ_TIMEOUT_SECS);

  // connect
  void connect(const char* hostname, int32_t port,
               uint32_t waitSeconds = DEFAULT_CONNECT_TIMEOUT);
  void connect(const char* ipaddr,
               uint32_t waitSeconds = DEFAULT_CONNECT_TIMEOUT);

  virtual void connect();

  int32_t receive(char* buff, int32_t len, uint32_t waitSeconds,
                  uint32_t waitMicroSeconds);
  int32_t send(const char* buff, int32_t len, uint32_t waitSeconds,
               uint32_t waitMicroSeconds);

  virtual void setOption(int32_t level, int32_t option, void* val,
                         int32_t len) {
    GF_DEV_ASSERT(m_io != NULL);

    if (m_io->set_option(level, option, val, len) == -1) {
      int32_t lastError = ACE_OS::last_error();
      LOGERROR("Failed to set option, errno: %d: %s", lastError,
               ACE_OS::strerror(lastError));
    }
  }

  void setIntOption(int32_t level, int32_t option, int32_t val) {
    setOption(level, option, (void*)&val, sizeof(int32_t));
  }

  void setBoolOption(int32_t level, int32_t option, bool val) {
    setOption(level, option, (void*)&val, sizeof(bool));
  }

  virtual uint16 getPort();
};
}  // gemfire

#endif  // __TcpConn_hpp__

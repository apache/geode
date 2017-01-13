/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __TcpSslConn_hpp__
#define __TcpSslConn_hpp__

#include "TcpConn.hpp"
#include <ace/DLL.h>
#include "../../cryptoimpl/GFSsl.hpp"

namespace gemfire {

class TcpSslConn : public TcpConn {
 private:
  GFSsl* m_ssl;
  ACE_DLL m_dll;
  // adongre: Added for Ticket #758
  // Pass extra parameter for the password
  typedef void* (*gf_create_SslImpl)(ACE_SOCKET, const char*, const char*,
                                     const char*);
  typedef void (*gf_destroy_SslImpl)(void*);

  GFSsl* getSSLImpl(ACE_SOCKET sock, const char* pubkeyfile,
                    const char* privkeyfile);

 protected:
  int32_t socketOp(SockOp op, char* buff, int32_t len, uint32_t waitSeconds);

  void createSocket(ACE_SOCKET sock);

 public:
  TcpSslConn() : TcpConn(), m_ssl(NULL){};

  TcpSslConn(const char* hostname, int32_t port,
             uint32_t waitSeconds = DEFAULT_CONNECT_TIMEOUT,
             int32_t maxBuffSizePool = 0)
      : TcpConn(hostname, port, waitSeconds, maxBuffSizePool), m_ssl(NULL){};

  TcpSslConn(const char* ipaddr, uint32_t waitSeconds = DEFAULT_CONNECT_TIMEOUT,
             int32_t maxBuffSizePool = 0)
      : TcpConn(ipaddr, waitSeconds, maxBuffSizePool), m_ssl(NULL){};

  // TODO:  Watch out for virt dtor calling virt methods!

  virtual ~TcpSslConn() {}

  // Close this tcp connection
  void close();

  // Listen
  void listen(ACE_INET_Addr addr,
              uint32_t waitSeconds = DEFAULT_READ_TIMEOUT_SECS);

  // connect
  void connect();

  void setOption(int32_t level, int32_t option, void* val, int32_t len) {
    GF_DEV_ASSERT(m_ssl != NULL);

    if (m_ssl->setOption(level, option, val, len) == -1) {
      int32_t lastError = ACE_OS::last_error();
      LOGERROR("Failed to set option, errno: %d: %s", lastError,
               ACE_OS::strerror(lastError));
    }
  }

  uint16 getPort();
};
}  // gemfire

#endif  // __TcpSslConn_hpp__

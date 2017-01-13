/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _SSLIMPL_HPP_INCLUDED_
#define _SSLIMPL_HPP_INCLUDED_

#include <gfcpp/gf_base.hpp>
#include "ace/ACE.h"
#include "ace/OS.h"
#include <ace/INET_Addr.h>
#include <ace/SOCK_IO.h>
#include <ace/SSL/SSL_SOCK_Connector.h>
#include <ace/SSL/SSL_SOCK_Acceptor.h>
#include <ace/OS.h>
#include <ace/Recursive_Thread_Mutex.h>
#include "ace/Time_Value.h"
#include "GFSsl.hpp"

class SSLImpl : public GFSsl {
 private:
  ACE_SSL_SOCK_Stream* m_io;
  static ACE_Recursive_Thread_Mutex s_mutex;
  volatile static bool s_initialized;

 public:
  SSLImpl(ACE_SOCKET sock, const char* pubkeyfile, const char* privkeyfile,
          const char* password);
  virtual ~SSLImpl();

  int setOption(int, int, void*, int);
  int listen(ACE_INET_Addr, unsigned);
  int connect(ACE_INET_Addr, unsigned);
  ssize_t recv(void*, size_t, const ACE_Time_Value*, size_t*);
  ssize_t send(const void*, size_t, const ACE_Time_Value*, size_t*);
  int getLocalAddr(ACE_Addr&);
  void close();
};

extern "C" {
CPPCACHE_EXPORT void* gf_create_SslImpl(ACE_SOCKET sock, const char* pubkeyfile,
                                        const char* privkeyfile,
                                        const char* pemPassword);
CPPCACHE_EXPORT void gf_destroy_SslImpl(void* impl);
}

#endif  // _SSLIMPL_HPP_INCLUDED_

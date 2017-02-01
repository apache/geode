#pragma once

#ifndef GEODE_SSLSOCKSTREAM_H_
#define GEODE_SSLSOCKSTREAM_H_

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

#include <ace/ACE.h>
#include <ace/DLL.h>
#include <ace/INET_Addr.h>

namespace apache {
namespace geode {
namespace client {

class SslSockStream {
 private:
  SslSockStream();
  SslSockStream(const SslSockStream &);

  ACE_DLL m_dll;

  void *m_ctx;
  ACE_SOCKET m_sock;
  const char *m_pubkey;
  const char *m_privkey;

  typedef void *(*gf_initSslImpl_Type)(ACE_SOCKET, const char *, const char *);
  typedef void (*gf_clearSslImpl_Type)(void *);
  typedef int (*gf_set_option_Type)(void *, int, int, void *, int);
  typedef int (*gf_listen_Type)(void *, ACE_INET_Addr, unsigned);
  typedef int (*gf_connect_Type)(void *, ACE_INET_Addr, unsigned);
  typedef ssize_t (*gf_recv_n_Type)(void *, void *, size_t,
                                    const ACE_Time_Value *, size_t *);
  typedef ssize_t (*gf_send_n_Type)(void *, const void *, size_t,
                                    const ACE_Time_Value *, size_t *);
  typedef int (*gf_get_local_addr_Type)(void *, ACE_Addr &);

#define DECLARE_SSL_FUNC_PTR(OrigName) OrigName##_Type OrigName##_Ptr;

  DECLARE_SSL_FUNC_PTR(gf_initSslImpl)
  DECLARE_SSL_FUNC_PTR(gf_clearSslImpl)
  DECLARE_SSL_FUNC_PTR(gf_set_option)
  DECLARE_SSL_FUNC_PTR(gf_listen)
  DECLARE_SSL_FUNC_PTR(gf_connect)
  DECLARE_SSL_FUNC_PTR(gf_recv_n)
  DECLARE_SSL_FUNC_PTR(gf_send_n)
  DECLARE_SSL_FUNC_PTR(gf_get_local_addr)

  void initACESSLFuncPtrs();
  void *getACESSLFuncPtr(const char *function_name);

 protected:
 public:
  SslSockStream(ACE_SOCKET, const char *, const char *);
  ~SslSockStream();

  void init();

  int set_option(int level, int option, void *optval, int optlen) const;

  int listen(ACE_INET_Addr addr, unsigned waitSeconds);

  int connect(ACE_INET_Addr ipaddr, unsigned waitSeconds);

  ssize_t recv_n(void *buf, size_t len, const ACE_Time_Value *timeout = 0,
                 size_t *bytes_transferred = 0) const;

  ssize_t send_n(const void *buf, size_t len, const ACE_Time_Value *timeout = 0,
                 size_t *bytes_transferred = 0) const;

  int get_local_addr(ACE_Addr &) const;

  int close();

};  // class SslSockStream
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_SSLSOCKSTREAM_H_

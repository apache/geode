/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "SslSockStream.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include <ace/OS_NS_stdio.h>

using namespace gemfire;

namespace gemfire {

void *SslSockStream::getACESSLFuncPtr(const char *function_name) {
  void *func = m_dll.symbol(function_name);
  if (func == NULL) {
    char msg[1000];
    ACE_OS::snprintf(msg, 1000, "cannot find function %s in library %s",
                     function_name, "cryptoImpl");
    LOGERROR(msg);
    throw IllegalStateException(msg);
  }
  return func;
}

void SslSockStream::initACESSLFuncPtrs() {
  const char *libName = "cryptoImpl";
  if (m_dll.open(libName, ACE_DEFAULT_SHLIB_MODE, 0) == -1) {
    int32_t lastError = ACE_OS::last_error();
    LOGERROR("Failed to open cryptoImpl . Errno: %d : %s", lastError,
             ACE_OS::strerror(lastError));
    char msg[1000] = {0};
    ACE_OS::snprintf(msg, 1000, "cannot open library: %s", libName);
    LOGERROR(msg);
    throw FileNotFoundException(msg);
  }

#define ASSIGN_SSL_FUNC_PTR(OrigName) \
  OrigName##_Ptr = (OrigName##_Type)getACESSLFuncPtr(#OrigName);

  ASSIGN_SSL_FUNC_PTR(gf_initSslImpl)
  ASSIGN_SSL_FUNC_PTR(gf_clearSslImpl)
  ASSIGN_SSL_FUNC_PTR(gf_set_option)
  ASSIGN_SSL_FUNC_PTR(gf_listen)
  ASSIGN_SSL_FUNC_PTR(gf_connect)
  ASSIGN_SSL_FUNC_PTR(gf_recv_n)
  ASSIGN_SSL_FUNC_PTR(gf_send_n)
  ASSIGN_SSL_FUNC_PTR(gf_get_local_addr)
}

SslSockStream::SslSockStream(ACE_SOCKET sock, const char *pubkey,
                             const char *privkey)
    : m_ctx(NULL),
      m_sock(sock),
      m_pubkey(pubkey),
      m_privkey(privkey),
      gf_initSslImpl_Ptr(NULL),
      gf_clearSslImpl_Ptr(NULL),
      gf_set_option_Ptr(NULL),
      gf_listen_Ptr(NULL),
      gf_connect_Ptr(NULL),
      gf_recv_n_Ptr(NULL),
      gf_send_n_Ptr(NULL),
      gf_get_local_addr_Ptr(NULL) {}

void SslSockStream::init() {
  initACESSLFuncPtrs();
  m_ctx = gf_initSslImpl_Ptr(m_sock, m_pubkey, m_privkey);
  LOGDEBUG("Got %p as SSL socket context address", m_ctx);
}

int SslSockStream::set_option(int level, int option, void *optval,
                              int optlen) const {
  return gf_set_option_Ptr(m_ctx, level, option, optval, optlen);
}

int SslSockStream::listen(ACE_INET_Addr addr, unsigned waitSeconds) {
  return gf_listen_Ptr(m_ctx, addr, waitSeconds);
}

int SslSockStream::connect(ACE_INET_Addr ipaddr, unsigned waitSeconds) {
  return gf_connect_Ptr(m_ctx, ipaddr, waitSeconds);
}

ssize_t SslSockStream::recv_n(void *buf, size_t len,
                              const ACE_Time_Value *timeout,
                              size_t *bytes_transferred) const {
  return gf_recv_n_Ptr(m_ctx, buf, len, timeout, bytes_transferred);
}

ssize_t SslSockStream::send_n(const void *buf, size_t len,
                              const ACE_Time_Value *timeout,
                              size_t *bytes_transferred) const {
  return gf_send_n_Ptr(m_ctx, buf, len, timeout, bytes_transferred);
}

int SslSockStream::get_local_addr(ACE_Addr &addr) const {
  return gf_get_local_addr_Ptr(m_ctx, addr);
}

int SslSockStream::close() {
  gf_clearSslImpl_Ptr(m_ctx);
  m_ctx = NULL;
  return 0;
}

SslSockStream::~SslSockStream() { close(); }

}  // namespace gemfire

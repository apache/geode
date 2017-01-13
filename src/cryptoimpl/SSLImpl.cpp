/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "SSLImpl.hpp"

#include <ace/INET_Addr.h>
#include <ace/SOCK_IO.h>
#include <ace/Guard_T.h>
#include <ace/SSL/SSL_SOCK_Connector.h>
#include <ace/SSL/SSL_SOCK_Acceptor.h>
#include <ace/OS.h>

ACE_Recursive_Thread_Mutex SSLImpl::s_mutex;
volatile bool SSLImpl::s_initialized = false;

void *gf_create_SslImpl(ACE_SOCKET sock, const char *pubkeyfile,
                        const char *privkeyfile, const char *pemPassword) {
  return (void *)new SSLImpl(sock, pubkeyfile, privkeyfile, pemPassword);
}

void gf_destroy_SslImpl(void *impl) {
  SSLImpl *theLib = reinterpret_cast<SSLImpl *>(impl);
  delete theLib;
}

extern "C" {

// adongre: Added for Ticket #758
static int pem_passwd_cb(char *buf, int size, int rwflag, void *passwd) {
  strncpy(buf, (char *)passwd, size);
  buf[size - 1] = '\0';
  return (strlen(buf));
}
}

SSLImpl::SSLImpl(ACE_SOCKET sock, const char *pubkeyfile,
                 const char *privkeyfile, const char *password) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(SSLImpl::s_mutex);

  if (SSLImpl::s_initialized == false) {
    ACE_SSL_Context *sslctx = ACE_SSL_Context::instance();
    SSL_CTX *opensslctx = sslctx->context();

    if (SSL_CTX_set_cipher_list(opensslctx, "eNULL:DEFAULT") == 0) {
      // if it fails here error is caught at connect.
    }
    // sslctx->set_mode(ACE_SSL_Context::SSLv23_client);
    sslctx->load_trusted_ca(pubkeyfile);
    // adongre: Added for Ticket #758
    if (strlen(password) > 0) {
      SSL_CTX_set_default_passwd_cb(sslctx->context(), pem_passwd_cb);
      SSL_CTX_set_default_passwd_cb_userdata(sslctx->context(),
                                             const_cast<char *>(password));
    }
    sslctx->private_key(privkeyfile);
    sslctx->certificate(privkeyfile);
    SSLImpl::s_initialized = true;
  }
  m_io = new ACE_SSL_SOCK_Stream();
  m_io->set_handle((ACE_HANDLE)sock);
}

SSLImpl::~SSLImpl() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(SSLImpl::s_mutex);

  if (m_io != NULL) {
    delete m_io;
  }
}

void SSLImpl::close() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(SSLImpl::s_mutex);

  if (m_io != NULL) {
    m_io->close();
  }
}

int SSLImpl::setOption(int level, int option, void *optval, int optlen) {
  return m_io->set_option(level, option, optval, optlen);
}

int SSLImpl::listen(ACE_INET_Addr addr, unsigned waitSeconds) {
  ACE_SSL_SOCK_Acceptor listener(addr, 1);
  int32_t retVal = 0;
  if (waitSeconds > 0) {
    ACE_Time_Value wtime(waitSeconds);
    retVal = listener.accept(*m_io, 0, &wtime);
  } else {
    retVal = listener.accept(*m_io, 0);
  }
  return retVal;
}

int SSLImpl::connect(ACE_INET_Addr ipaddr, unsigned waitSeconds) {
  ACE_SSL_SOCK_Connector conn;
  int32_t retVal = 0;
  if (waitSeconds > 0) {
    // passing wait time as microseconds
    ACE_Time_Value wtime(0, waitSeconds);
    retVal = conn.connect(*m_io, ipaddr, &wtime);
  } else {
    retVal = conn.connect(*m_io, ipaddr);
  }
  return retVal;
}

ssize_t SSLImpl::recv(void *buf, size_t len, const ACE_Time_Value *timeout = 0,
                      size_t *bytes_transferred = 0) {
  return m_io->recv_n(buf, len, 0, timeout, bytes_transferred);
}

ssize_t SSLImpl::send(const void *buf, size_t len,
                      const ACE_Time_Value *timeout = 0,
                      size_t *bytes_transferred = 0) {
  return m_io->send_n(buf, len, 0, timeout, bytes_transferred);
}

int SSLImpl::getLocalAddr(ACE_Addr &addr) { return m_io->get_local_addr(addr); }

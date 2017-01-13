/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "TcpSslConn.hpp"

#include <gfcpp/SystemProperties.hpp>
#include <gfcpp/DistributedSystem.hpp>
#include "../../cryptoimpl/GFSsl.hpp"

using namespace gemfire;

GFSsl* TcpSslConn::getSSLImpl(ACE_SOCKET sock, const char* pubkeyfile,
                              const char* privkeyfile) {
  const char* libName = "cryptoImpl";
  if (m_dll.open(libName, RTLD_NOW | RTLD_GLOBAL, 0) == -1) {
    char msg[1000] = {0};
    ACE_OS::snprintf(msg, 1000, "cannot open library: %s", libName);
    LOGERROR(msg);
    throw FileNotFoundException(msg);
  }

  gf_create_SslImpl func =
      reinterpret_cast<gf_create_SslImpl>(m_dll.symbol("gf_create_SslImpl"));
  if (func == NULL) {
    char msg[1000];
    ACE_OS::snprintf(msg, 1000,
                     "cannot find function %s in library gf_create_SslImpl",
                     "cryptoImpl");
    LOGERROR(msg);
    throw IllegalStateException(msg);
  }
  // adongre: Added for Ticket #758
  const char* pemPassword =
      DistributedSystem::getSystemProperties()->sslKeystorePassword();

  return reinterpret_cast<GFSsl*>(
      func(sock, pubkeyfile, privkeyfile, pemPassword));
}

void TcpSslConn::createSocket(ACE_SOCKET sock) {
  SystemProperties* props = DistributedSystem::getSystemProperties();
  const char* pubkeyfile = props->sslTrustStore();
  const char* privkeyfile = props->sslKeyStore();
  LOGDEBUG("Creating SSL socket stream");
  m_ssl = getSSLImpl(sock, pubkeyfile, privkeyfile);
}

void TcpSslConn::listen(ACE_INET_Addr addr, uint32_t waitSeconds) {
  GF_DEV_ASSERT(m_ssl != NULL);

  int32_t retVal = m_ssl->listen(addr, waitSeconds);

  if (retVal == -1) {
    char msg[256];
    int32_t lastError = ACE_OS::last_error();
    if (lastError == ETIME || lastError == ETIMEDOUT) {
      /* adongre
       * Coverity - II
       * CID 29271: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
       * "sprintf" can cause a
       * buffer overflow when done incorrectly. Because sprintf() assumes an
       * arbitrarily long string,
       * callers must be careful not to overflow the actual space of the
       * destination.
       * Use snprintf() instead, or correct precision specifiers.
       * Fix : using ACE_OS::snprintf
       */
      // sprintf( msg, "TcpSslConn::listen Attempt to listen timed out after %d
      // seconds.", waitSeconds );
      ACE_OS::snprintf(
          msg, 256,
          "TcpSslConn::listen Attempt to listen timed out after %d seconds.",
          waitSeconds);
      throw TimeoutException(msg);
    }
    // sprintf( msg, "TcpSslConn::listen failed with errno: %d: %s", lastError,
    // ACE_OS::strerror(lastError) );
    ACE_OS::snprintf(msg, 255, "TcpSslConn::listen failed with errno: %d: %s",
                     lastError, ACE_OS::strerror(lastError));
    throw GemfireIOException(msg);
  }
}

void TcpSslConn::connect() {
  GF_DEV_ASSERT(m_ssl != NULL);

  ACE_OS::signal(SIGPIPE, SIG_IGN);  // Ignore broken pipe

  // m_ssl->init();

  uint32_t waitSeconds = m_waitSeconds;

  // passing waittime as microseconds
  if (DistributedSystem::getSystemProperties()->readTimeoutUnitInMillis()) {
    waitSeconds = waitSeconds * 1000;
  } else {
    waitSeconds = waitSeconds * (1000 * 1000);
  }

  LOGDEBUG("Connecting SSL socket stream to %s:%d waiting %d sec",
           m_addr.get_host_name(), m_addr.get_port_number(), m_waitSeconds);

  int32_t retVal = m_ssl->connect(m_addr, waitSeconds);

  if (retVal == -1) {
    char msg[256];
    int32_t lastError = ACE_OS::last_error();
    if (lastError == ETIME || lastError == ETIMEDOUT) {
      ACE_OS::snprintf(
          msg, 256,
          "TcpSslConn::connect Attempt to connect timed out after %d seconds.",
          m_waitSeconds);
      // this is only called by constructor, so we must delete m_ssl
      GF_SAFE_DELETE(m_ssl);
      throw TimeoutException(msg);
    }
    ACE_OS::snprintf(msg, 256, "TcpSslConn::connect failed with errno: %d: %s",
                     lastError, ACE_OS::strerror(lastError));
    // this is only called by constructor, so we must delete m_ssl
    GF_SAFE_DELETE(m_ssl);
    throw GemfireIOException(msg);
  }
}

void TcpSslConn::close() {
  if (m_ssl != NULL) {
    m_ssl->close();
    gf_destroy_SslImpl func = reinterpret_cast<gf_destroy_SslImpl>(
        m_dll.symbol("gf_destroy_SslImpl"));
    func((void*)m_ssl);
    m_ssl = NULL;
  }
}

int32_t TcpSslConn::socketOp(TcpConn::SockOp op, char* buff, int32_t len,
                             uint32_t waitSeconds) {
  {
    GF_DEV_ASSERT(m_ssl != NULL);
    GF_DEV_ASSERT(buff != NULL);

#if GF_DEVEL_ASSERTS == 1
    if (len <= 0) {
      LOGERROR(
          "TcpSslConn::socketOp called with a length of %d specified. "
          "No operation performed.",
          len);
      GF_DEV_ASSERT(false);
    }
#endif
    // passing wait time as micro seconds
    ACE_Time_Value waitTime(0, waitSeconds);
    ACE_Time_Value endTime(ACE_OS::gettimeofday() + waitTime);
    ACE_Time_Value sleepTime(0, 100);
    size_t readLen = 0;
    ssize_t retVal;
    bool errnoSet = false;

    int32_t sendlen = len;
    int32_t totalsend = 0;

    while (len > 0 && waitTime > ACE_Time_Value::zero) {
      if (len > m_chunkSize) {
        sendlen = m_chunkSize;
        len -= m_chunkSize;
      } else {
        sendlen = len;
        len = 0;
      }
      do {
        if (op == SOCK_READ) {
          retVal = m_ssl->recv(buff, sendlen, &waitTime, &readLen);
        } else {
          retVal = m_ssl->send(buff, sendlen, &waitTime, &readLen);
        }
        sendlen -= static_cast<int32_t>(readLen);
        totalsend += static_cast<int32_t>(readLen);
        if (retVal < 0) {
          int32_t lastError = ACE_OS::last_error();
          if (lastError == EAGAIN) {
            ACE_OS::sleep(sleepTime);
          } else {
            errnoSet = true;
            break;
          }
        } else if (retVal == 0 && readLen == 0) {
          ACE_OS::last_error(EPIPE);
          errnoSet = true;
          break;
        }

        buff += readLen;

        waitTime = endTime - ACE_OS::gettimeofday();
        if (waitTime <= ACE_Time_Value::zero) break;
      } while (sendlen > 0);
      if (errnoSet) break;
    }

    if (len > 0 && !errnoSet) {
      ACE_OS::last_error(ETIME);
    }

    GF_DEV_ASSERT(len >= 0);
    return totalsend;
  }
}

uint16_t TcpSslConn::getPort() {
  GF_DEV_ASSERT(m_ssl != NULL);

  ACE_INET_Addr localAddr;
  m_ssl->getLocalAddr(*(ACE_Addr*)&localAddr);
  return localAddr.get_port_number();
}

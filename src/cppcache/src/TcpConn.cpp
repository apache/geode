/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#include "TcpConn.hpp"
#include <gfcpp/DistributedSystem.hpp>
#include <gfcpp/SystemProperties.hpp>
#include <gfcpp/Log.hpp>

#include <memory.h>

#include <ace/INET_Addr.h>
#include <ace/SOCK_IO.h>
#include <ace/SOCK_Connector.h>
#include <ace/SOCK_Acceptor.h>
#include <ace/OS.h>

using namespace gemfire;

int TcpConn::m_chunkSize = TcpConn::setChunkSize();

void TcpConn::clearNagle(ACE_SOCKET sock) {
  int32_t val = 1;
#ifdef WIN32
  const char *param = (const char *)&val;
#else
  const void *param = (const void *)&val;
#endif
  int32_t plen = sizeof(val);

  if (0 != setsockopt(sock, IPPROTO_TCP, 1, param, plen)) {
    int32_t lastError = ACE_OS::last_error();
    LOGERROR("Failed to set TCP_NODELAY on socket. Errno: %d: %s", lastError,
             ACE_OS::strerror(lastError));
  }
}

int32_t TcpConn::maxSize(ACE_SOCKET sock, int32_t flag, int32_t size) {
  int32_t val = 0;
#ifdef _WIN32
  const char *cparam = (const char *)&val;
  char *param = (char *)&val;
#else
  const void *cparam = (const void *)&val;
  void *param = (void *)&val;
#endif
  socklen_t plen = sizeof(val);
  socklen_t clen = sizeof(val);

  static int32_t max = 32000;
  if (m_maxBuffSizePool <= 0) {
    SystemProperties *props = DistributedSystem::getSystemProperties();
    if (props != NULL) {
      max = props->maxSocketBufferSize();
    }
  } else {
    max = m_maxBuffSizePool;
  }
  int32_t inc = 32120;
  val = size - (3 * inc);
  if (val < 0) val = 0;
  if (size == 0) size = max;
  int32_t red = 0;
  int32_t lastRed = -1;
  while (lastRed != red) {
    lastRed = red;
    val += inc;
    if (0 != setsockopt(sock, SOL_SOCKET, flag, cparam, clen)) {
      int32_t lastError = ACE_OS::last_error();
      LOGERROR("Failed to set socket options. Errno: %d : %s ", lastError,
               ACE_OS::strerror(lastError));
    }
    if (0 != getsockopt(sock, SOL_SOCKET, flag, param, &plen)) {
      int32_t lastError = ACE_OS::last_error();
      LOGERROR(
          "Failed to get buffer size for flag %d on socket. Errno: %d : %s",
          flag, lastError, ACE_OS::strerror(lastError));
    }
#ifdef _LINUX
    val /= 2;
#endif
    if ((val >= max) || (val >= size)) continue;
    red = val;
  }
  return val;
}

void TcpConn::createSocket(ACE_SOCKET sock) {
  LOGDEBUG("Creating plain socket stream");
  m_io = new ACE_SOCK_Stream((ACE_HANDLE)sock);
  // m_io->enable(ACE_NONBLOCK);
}

void TcpConn::init() {
  /* adongre
   * CID 28736: Improper use of negative value (NEGATIVE_RETURNS)
   * Function "socket(2, 1, 0)" returns a negative number.
   * Assigning: unsigned variable "sock" = "socket".
   *
   * CID 28737: Unsigned compared against 0 (NO_EFFECT)
   * This less-than-zero comparison of an unsigned value is never true. "sock <
   * 0U".
   */
  ACE_SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
  // if ( sock < 0 ) {
  if (sock == -1) {
    int32_t lastError = ACE_OS::last_error();
    LOGERROR("Failed to create socket. Errno: %d: %s", lastError,
             ACE_OS::strerror(lastError));
    char msg[256];
    ACE_OS::snprintf(msg, 256, "TcpConn::connect failed with errno: %d: %s",
                     lastError, ACE_OS::strerror(lastError));
    throw GemfireIOException(msg);
  }

  clearNagle(sock);

  static int32_t readSize = 0;
  static int32_t writeSize = 0;
  int32_t originalReadSize = readSize;
  readSize = maxSize(sock, SO_SNDBUF, readSize);
  if (originalReadSize != readSize) {
    // This should get logged once at startup and again only if it changes
    LOGINFO("Using socket send buffer size of %d.", readSize);
  }
  int32_t originalWriteSize = writeSize;
  writeSize = maxSize(sock, SO_RCVBUF, writeSize);
  if (originalWriteSize != writeSize) {
    // This should get logged once at startup and again only if it changes
    LOGINFO("Using socket receive buffer size of %d.", writeSize);
  }

  createSocket(sock);

  connect();
}

TcpConn::TcpConn() : m_io(NULL), m_waitSeconds(0), m_maxBuffSizePool(0) {}

TcpConn::TcpConn(const char *ipaddr, uint32_t waitSeconds,
                 int32_t maxBuffSizePool)
    : m_io(NULL),
      m_addr(ipaddr),
      m_waitSeconds(waitSeconds),
      m_maxBuffSizePool(maxBuffSizePool) {}

TcpConn::TcpConn(const char *hostname, int32_t port, uint32_t waitSeconds,
                 int32_t maxBuffSizePool)
    : m_io(NULL),
      m_addr(port, hostname),
      m_waitSeconds(waitSeconds),
      m_maxBuffSizePool(maxBuffSizePool) {}

void TcpConn::listen(const char *hostname, int32_t port, uint32_t waitSeconds) {
  ACE_INET_Addr addr(port, hostname);
  listen(addr, waitSeconds);
}

void TcpConn::listen(const char *ipaddr, uint32_t waitSeconds) {
  ACE_INET_Addr addr(ipaddr);
  listen(addr, waitSeconds);
}

void TcpConn::listen(ACE_INET_Addr addr, uint32_t waitSeconds) {
  GF_DEV_ASSERT(m_io != NULL);

  ACE_SOCK_Acceptor listener(addr, 1);
  int32_t retVal = 0;
  if (waitSeconds > 0) {
    ACE_Time_Value wtime(waitSeconds);
    retVal = listener.accept(*m_io, 0, &wtime);
  } else {
    retVal = listener.accept(*m_io, 0);
  }
  if (retVal == -1) {
    char msg[256];
    int32_t lastError = ACE_OS::last_error();
    if (lastError == ETIME || lastError == ETIMEDOUT) {
      /* adongre
       * Coverity - II
      * CID 29270: Calling risky function (SECURE_CODING)[VERY RISKY]. Using
      * "sprintf" can cause a
      * buffer overflow when done incorrectly. Because sprintf() assumes an
      * arbitrarily long string,
      * callers must be careful not to overflow the actual space of the
      * destination.
      * Use snprintf() instead, or correct precision specifiers.
      * Fix : using ACE_OS::snprintf
      */
      ACE_OS::snprintf(
          msg, 256,
          "TcpConn::listen Attempt to listen timed out after %d seconds.",
          waitSeconds);
      throw TimeoutException(msg);
    }
    ACE_OS::snprintf(msg, 256, "TcpConn::listen failed with errno: %d: %s",
                     lastError, ACE_OS::strerror(lastError));
    throw GemfireIOException(msg);
  }
}

void TcpConn::connect(const char *hostname, int32_t port,
                      uint32_t waitSeconds) {
  ACE_INET_Addr addr(port, hostname);
  m_addr = addr;
  m_waitSeconds = waitSeconds;
  connect();
}

void TcpConn::connect(const char *ipaddr, uint32_t waitSeconds) {
  ACE_INET_Addr addr(ipaddr);
  m_addr = addr;
  m_waitSeconds = waitSeconds;
  connect();
}

void TcpConn::connect() {
  GF_DEV_ASSERT(m_io != NULL);

  ACE_INET_Addr ipaddr = m_addr;
  uint32_t waitSeconds = m_waitSeconds;

  ACE_OS::signal(SIGPIPE, SIG_IGN);  // Ignore broken pipe

  // passing waittime as microseconds
  if (DistributedSystem::getSystemProperties()->readTimeoutUnitInMillis()) {
    waitSeconds = waitSeconds * 1000;
  } else {
    waitSeconds = waitSeconds * (1000 * 1000);
  }

  LOGFINER("Connecting plain socket stream to %s:%d waiting %d micro sec",
           ipaddr.get_host_name(), ipaddr.get_port_number(), waitSeconds);

  ACE_SOCK_Connector conn;
  int32_t retVal = 0;
  if (waitSeconds > 0) {
    // passing waittime as microseconds
    ACE_Time_Value wtime(0, waitSeconds);
    retVal = conn.connect(*m_io, ipaddr, &wtime);
  } else {
    retVal = conn.connect(*m_io, ipaddr);
  }
  if (retVal == -1) {
    char msg[256];
    int32_t lastError = ACE_OS::last_error();
    if (lastError == ETIME || lastError == ETIMEDOUT) {
      ACE_OS::snprintf(
          msg, 256,
          "TcpConn::connect Attempt to connect timed out after %d seconds.",
          waitSeconds);
      //  this is only called by constructor, so we must delete m_io
      GF_SAFE_DELETE(m_io);
      throw TimeoutException(msg);
    }
    ACE_OS::snprintf(msg, 256, "TcpConn::connect failed with errno: %d: %s",
                     lastError, ACE_OS::strerror(lastError));
    //  this is only called by constructor, so we must delete m_io
    GF_SAFE_DELETE(m_io);
    throw GemfireIOException(msg);
  }
  int rc = this->m_io->enable(ACE_NONBLOCK);
  if (-1 == rc) {
    char msg[250];
    int32_t lastError = ACE_OS::last_error();
    ACE_OS::snprintf(msg, 256, "TcpConn::NONBLOCK: %d: %s", lastError,
                     ACE_OS::strerror(lastError));

    LOGINFO(msg);
  }
}

void TcpConn::close() {
  if (m_io != NULL) {
    m_io->close();
    GF_SAFE_DELETE(m_io);
  }
}

int32_t TcpConn::receive(char *buff, int32_t len, uint32_t waitSeconds,
                         uint32_t waitMicroSeconds) {
  return socketOp(SOCK_READ, buff, len, waitSeconds);
}

int32_t TcpConn::send(const char *buff, int32_t len, uint32_t waitSeconds,
                      uint32_t waitMicroSeconds) {
  return socketOp(SOCK_WRITE, const_cast<char *>(buff), len, waitSeconds);
}

int32_t TcpConn::socketOp(TcpConn::SockOp op, char *buff, int32_t len,
                          uint32_t waitSeconds) {
  {
    /*{
      ACE_HANDLE handle = m_io->get_handle();
       int val = ACE::get_flags (handle);

      if (ACE_BIT_DISABLED (val, ACE_NONBLOCK))
      {
        //ACE::set_flags (handle, ACE_NONBLOCK);
        LOGINFO("Flag is not set");
      }else
      {
          LOGINFO("Flag is set");
      }
    }*/

    GF_DEV_ASSERT(m_io != NULL);
    GF_DEV_ASSERT(buff != NULL);

#if GF_DEVEL_ASSERTS == 1
    if (len <= 0) {
      LOGERROR(
          "TcpConn::socketOp called with a length of %d specified. "
          "No operation performed.",
          len);
      GF_DEV_ASSERT(false);
    }
#endif

    ACE_Time_Value waitTime(0, waitSeconds /*now its in microSeconds*/);
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
          retVal = m_io->recv_n(buff, sendlen, &waitTime, &readLen);
        } else {
          retVal = m_io->send_n(buff, sendlen, &waitTime, &readLen);
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
        if (sendlen == 0) break;
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

//  Return the local port for this TCP connection.
uint16_t TcpConn::getPort() {
  GF_DEV_ASSERT(m_io != NULL);

  ACE_INET_Addr localAddr;
  m_io->get_local_addr(*(ACE_Addr *)&localAddr);
  return localAddr.get_port_number();
}

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#include "TcpIpc.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/FwkLog.hpp"

#include <memory.h>
#include <errno.h>

#include <ace/INET_Addr.h>
#include <ace/SOCK_IO.h>
#include <ace/SOCK_Connector.h>
#include <ace/SOCK_Acceptor.h>
#include <ace/OS.h>

using namespace gemfire;
using namespace gemfire::testframework;

void TcpIpc::clearNagle(int32_t sock) {
  int32_t val = 1;
#ifdef WIN32
  const char *param = (const char *)&val;
#else
  const void *param = (const void *)&val;
#endif
  int32_t plen = sizeof(param);

  if (0 != setsockopt(sock, IPPROTO_TCP, 1, param, plen)) {
    FWKSEVERE("Failed to set NAGLE on socket.  Errno: " << errno);
  }
}

int32_t TcpIpc::getSize(int32_t sock, int32_t flag) {
  int32_t val = 0;
#ifdef _WIN32
  char *param = (char *)&val;
#else
  void *param = (void *)&val;
#endif
  socklen_t plen = sizeof(val);

  if (0 != getsockopt(sock, SOL_SOCKET, flag, param, &plen)) {
    FWKSEVERE("Failed to get buff size for flag "
              << flag << " on socket.  Errno: " << errno);
  }
#ifdef _LINUX
  val /= 2;
#endif
  return val;
}

int32_t TcpIpc::setSize(int32_t sock, int32_t flag, int32_t size) {
  int32_t val = 0;
  if (size <= 0) return 0;

#ifdef _WIN32
  const char *cparam = (const char *)&val;
  char *param = (char *)&val;
#else
  const void *cparam = (const void *)&val;
  void *param = (void *)&val;
#endif
  socklen_t plen = sizeof(val);
  socklen_t clen = sizeof(val);

  int32_t inc = 32120;
  val = size - (3 * inc);
  if (val < 0) val = 0;
  int32_t red = 0;
  int32_t lastRed = -1;
  while (lastRed != red) {
    lastRed = red;
    val += inc;
    setsockopt(sock, SOL_SOCKET, flag, cparam, clen);
    if (0 != getsockopt(sock, SOL_SOCKET, flag, param, &plen)) {
      FWKSEVERE("Failed to get buff size for flag "
                << flag << " on socket.  Errno: " << errno);
    }
#ifdef _LINUX
    val /= 2;
#endif
    if (val < size) red = val;
  }
  return val;
}

void TcpIpc::init(int32_t sockBufferSize) {
  int32_t sock = (int32_t)socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    FWKSEVERE("Failed to create socket.  Errno: " << errno);
  }

  if (sockBufferSize > 0) {
    clearNagle(sock);

    int32_t size = setSize(sock, SO_SNDBUF, sockBufferSize);
    size = setSize(sock, SO_RCVBUF, sockBufferSize);
  } else {
    int32_t size = getSize(sock, SO_SNDBUF);
    size = getSize(sock, SO_RCVBUF);
  }
  m_io = new ACE_SOCK_Stream((ACE_HANDLE)sock);
  ACE_OS::signal(SIGPIPE, SIG_IGN);  // Ignore broken pipe
}

bool TcpIpc::listen(int32_t waitSecs) {
  if (m_ipaddr.empty()) {
    FWKSEVERE("Listen failed, address not set.");
    return false;
  }
  ACE_INET_Addr addr(m_ipaddr.c_str());
  ACE_SOCK_Acceptor listener(addr, 1);

  if (listener.accept(*m_io, 0, new ACE_Time_Value(waitSecs)) != 0) {
    FWKSEVERE("Accept failed with errno: " << errno);
    return false;
  }
  return true;
}

bool TcpIpc::accept(ACE_SOCK_Acceptor *acceptor, int32_t waitSecs) {
  if (acceptor->accept(*m_io, 0, new ACE_Time_Value(waitSecs)) != 0) {
    FWKSEVERE("Accept failed with errno: " << errno);
    return false;
  }
  return true;
}

bool TcpIpc::connect(int32_t waitSecs) {
  if (m_ipaddr.empty()) {
    FWKSEVERE("Connect failed, address not set.");
    return false;
  }
  ACE_INET_Addr driver(m_ipaddr.c_str());
  ACE_SOCK_Connector conn;
  int32_t retVal = -1;
  while ((retVal == -1) && (waitSecs-- > 0)) {
    perf::sleepSeconds(1);
    errno = 0;
    retVal = conn.connect(*m_io, driver);
  }
  if (retVal == -1) {
    FWKSEVERE("Attempt to connect failed, errno: " << errno);
    return false;
  }
  return true;
}

TcpIpc::~TcpIpc() { close(); }

void TcpIpc::close() {
  if (m_io != NULL) {
    m_io->close();
    delete m_io;
    m_io = NULL;
  }
}

int32_t TcpIpc::readBuffer(char **buffer, int32_t waitSecs) {
  ACE_Time_Value wtime(waitSecs);
  iovec buffs;
  buffs.iov_base = NULL;
  buffs.iov_len = 0;
  int32_t red = static_cast<int32_t>(m_io->recvv(&buffs, &wtime));
  if ((red == -1) && ((errno == ECONNRESET) || (errno == EPIPE))) {
    FWKEXCEPTION("During attempt to read: Connection failure errno: " << errno);
  }
  if (red == -1) {
  }
  *buffer = reinterpret_cast<char *>(buffs.iov_base);
  return buffs.iov_len;
}

int32_t TcpIpc::sendBuffers(int32_t cnt, char *buffers[], int32_t lengths[],
                            int32_t waitSecs) {
  ACE_Time_Value wtime(waitSecs);
  int32_t tot = 0;
  if (cnt > 2) {
    FWKEXCEPTION("During attempt to write: Too many buffers passed in.");
  }
  iovec buffs[2];
  for (int32_t idx = 0; idx < cnt; idx++) {
#ifdef _LINUX
    buffs[idx].iov_base = (void *)buffers[idx];
#else
    buffs[idx].iov_base = buffers[idx];
#endif
    buffs[idx].iov_len = lengths[idx];
    tot += lengths[idx];
  }
  int32_t wrote = static_cast<int32_t>(m_io->sendv(buffs, cnt, &wtime));
  if ((wrote == -1) && ((errno == ECONNRESET) || (errno == EPIPE))) {
    FWKEXCEPTION(
        "During attempt to write: Connection failure errno: " << errno);
  }
  if (tot != wrote) {
    FWKSEVERE("Failed to write all bytes attempted, wrote "
              << wrote << ", attempted " << tot);
  }
  return wrote;
}

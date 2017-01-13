/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#include "IpcHandler.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/FwkLog.hpp"

#include <memory.h>
#include <errno.h>

#include <ace/INET_Addr.h>
#include <ace/SOCK_IO.h>
#include <ace/SOCK_Connector.h>
#include <ace/OS.h>

using namespace gemfire;
using namespace gemfire::testframework;

// This class is not thread safe, and its use in the test framework
// does not require it to be.
IpcHandler::IpcHandler(const ACE_INET_Addr &driver, int32_t waitSeconds)
    : m_io(new ACE_SOCK_Stream()) {
  ACE_OS::signal(SIGPIPE, SIG_IGN);  // Ignore broken pipe
  ACE_SOCK_Connector conn;
  ACE_Time_Value wtime(waitSeconds, 1000);
  int32_t retVal = conn.connect(*m_io, driver, &wtime);
  if (retVal == -1) {
#ifdef WIN32
    errno = WSAGetLastError();
#endif
    FWKEXCEPTION("Attempt to connect failed, error: " << errno);
  }
}

IpcHandler::~IpcHandler() {
  sendIpcMsg(IPC_EXITING);
  close();
  checkBuffer(-12);
}

void IpcHandler::close() {
  if (m_io != 0) {
    m_io->close();
    delete m_io;
    m_io = 0;
  }
}

bool IpcHandler::checkPipe() {
  static ACE_Time_Value next;
  if (m_io == NULL) return false;
  ACE_Time_Value now = ACE_OS::gettimeofday();
  if (next < now) {
    ACE_Time_Value interval(60);
    next = now + interval;
    if (!sendIpcMsg(IPC_NULL)) return false;
  }
  return true;
}

IpcMsg IpcHandler::readIpcMsg(int32_t waitSeconds) {
  IpcMsg result = IPC_NULL;
  int32_t red = readInt(waitSeconds);
  if (red != -1) {
    result = static_cast<IpcMsg>(red);
    if (result == IPC_NULL) {
      return readIpcMsg(waitSeconds);  // skip past nulls
    }
  }
  return result;
}

int32_t IpcHandler::readInt(int32_t waitSeconds) {
  int32_t result = -1;
  if (!checkPipe()) {
    FWKEXCEPTION("Connection failure, error: " << errno);
  }

  ACE_Time_Value *wtime = new ACE_Time_Value(waitSeconds, 1000);
  int32_t redInt = -1;
  int32_t red =
      static_cast<int32_t>(m_io->recv_n((void *)&redInt, 4, 0, wtime));
  delete wtime;
  if (red == 4) {
    result = ntohl(redInt);
    //    FWKDEBUG( "Received " << result );
  } else {
    if (red == -1) {
#ifdef WIN32
      errno = WSAGetLastError();
#endif
      if ((errno > 0) && (errno != ETIME)) {
        FWKEXCEPTION("Read failure, error: " << errno);
      }
    }
  }
  return result;
}

char *IpcHandler::checkBuffer(int32_t len) {
  static int32_t length = 512;
  static char *buffer = NULL;

  if (len == -12) {
    delete[] buffer;
    buffer = NULL;
    return buffer;
  }

  if (length < len) {
    length = len + 32;
    if (buffer != NULL) {
      delete[] buffer;
      buffer = NULL;
    }
  }

  if (buffer == NULL) {
    buffer = new char[length];
  }

  memset(buffer, 0, length);
  return buffer;
}

std::string IpcHandler::readString(int32_t waitSeconds) {
  int32_t length = readInt(waitSeconds);
  if (length == -1) {
    FWKEXCEPTION(
        "Failed to read string, length not available, errno: " << errno);
  }

  char *buffer = checkBuffer(length);

  ACE_Time_Value *wtime = new ACE_Time_Value(waitSeconds, 1000);

  int32_t red =
      static_cast<int32_t>(m_io->recv((void *)buffer, length, 0, wtime));
  delete wtime;
  if (red <= 0) {
    if (red < 0) {
#ifdef WIN32
      errno = WSAGetLastError();
#endif
    } else {
      errno = EWOULDBLOCK;
    }
    FWKEXCEPTION("Failed to read string from socket, errno: " << errno);
  }
  //  FWKDEBUG( "Received " << buffer );
  return buffer;
}

IpcMsg IpcHandler::getIpcMsg(int32_t waitSeconds, std::string &str) {
  IpcMsg msg = readIpcMsg(waitSeconds);
  switch (msg) {
    case IPC_PING:
    case IPC_NULL:  // null should never be seen here
    case IPC_ACK:
    case IPC_EXITING:
      break;  // nothing required
    case IPC_DONE:
    case IPC_RUN:  // Need to read the rest
      str = readString(waitSeconds);
      if (!str.empty()) sendIpcMsg(IPC_ACK);
      break;
    case IPC_ERROR:
    case IPC_EXIT:
    default:
      sendIpcMsg(IPC_ACK);
      break;
  }
  return msg;
}

IpcMsg IpcHandler::getIpcMsg(int32_t waitSeconds) {
  IpcMsg msg = readIpcMsg(waitSeconds);
  switch (msg) {
    case IPC_PING:
    case IPC_NULL:  // null should never be seen here
    case IPC_ACK:
    case IPC_EXITING:
      break;  // nothing required
    case IPC_DONE:
    case IPC_RUN:  // Need to read the rest
      break;
    case IPC_ERROR:
    case IPC_EXIT:
    default:
      sendIpcMsg(IPC_ACK);
      break;
  }
  return msg;
}

bool IpcHandler::sendIpcMsg(IpcMsg msg, int32_t waitSeconds) {
  int32_t writeInt = htonl(msg);
  //  FWKDEBUG( "Sending " << ( int32_t )msg );
  ACE_Time_Value tv(waitSeconds, 1000);
  int32_t wrote =
      static_cast<int32_t>(m_io->send((const void *)&writeInt, 4, &tv));
  if (wrote == -1) {
#ifdef WIN32
    errno = WSAGetLastError();
#endif
    if (errno > 0) {
      FWKEXCEPTION("Send failure, error: " << errno);
    }
  }
  if (wrote == 4) {
    switch (msg) {
      case IPC_NULL:
      case IPC_ERROR:
      case IPC_ACK:
      case IPC_RUN:
      case IPC_DONE:
        return true;
        break;
      default:
        msg = getIpcMsg(60);
        if (msg == IPC_ACK) return true;
        break;
    }
  }
  return false;
}

bool IpcHandler::sendBuffer(IpcMsg msg, const char *str) {
  int32_t length = static_cast<int32_t>(strlen(str));
  char *buffer = checkBuffer(length);
  *reinterpret_cast<IpcMsg *>(buffer) = static_cast<IpcMsg>(htonl(msg));
  *reinterpret_cast<int32_t *>(buffer + 4) = htonl(length);
  strcpy((buffer + 8), str);

  //  FWKDEBUG( "Sending " << ( int32_t )msg << "  and string: " << str );
  length += 8;
  int32_t wrote =
      static_cast<int32_t>(m_io->send((const void *)buffer, length));
  if (wrote == -1) {
#ifdef WIN32
    errno = WSAGetLastError();
#endif
    if (errno > 0) {
      FWKEXCEPTION("Send failure, error: " << errno);
    }
  }
  if (wrote == length) {
    if (getIpcMsg(180) != IPC_ACK) {
      FWKEXCEPTION("Send was not ACK'ed.");
    }
    return true;
  }
  FWKEXCEPTION("Tried to write " << length << " bytes, only wrote " << wrote);
  return false;
}

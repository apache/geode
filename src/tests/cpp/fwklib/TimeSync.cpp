/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#include "fwklib/TimeSync.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/PaceMeter.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <memory.h>
#include <errno.h>
#include <sys/types.h>
#ifndef WIN32
#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#endif

using namespace gemfire;
using namespace gemfire::testframework;

// Our configuration parameters
#define TIME_SYNC_ADDR "224.0.37.37"
#define TIME_SYNC_PORT 9631
#define TIME_SYNC_TTL 3

// ----------------------------------------------------------------------------

static const short int wrd = 0x0001;
static const char* byt = reinterpret_cast<const char*>(&wrd);

#define isNetworkOrder() (byt[1] == 1)

#define NSWAP_8(x) ((x)&0xff)
#define NSWAP_16(x) ((NSWAP_8(x) << 8) | NSWAP_8((x) >> 8))
#define NSWAP_32(x) ((NSWAP_16(x) << 16) | NSWAP_16((x) >> 16))
#define NSWAP_64(x) ((NSWAP_32(x) << 32) | NSWAP_32((x) >> 32))

long long htonl64(long long value) {
  if (isNetworkOrder()) return value;
  return NSWAP_64(value);
}

long long ntohl64(long long value) {
  if (isNetworkOrder()) return value;
  return NSWAP_64(value);
}

// ----------------------------------------------------------------------------

int32_t TimeSync::svc() {
  if (m_delta == NULL) {
    sendTimeSync();
  } else {
    recvTimeSync();
  }
  return 0;
}

// ----------------------------------------------------------------------------

void TimeSync::sendTimeSync() {
  int32_t sock = (int32_t)socket(AF_INET, SOCK_DGRAM, 0);
  if (sock < 0) {
    FWKSEVERE("Failed to create socket for sending TimeSync messages.  Errno: "
              << errno);
    return;
  }

// To control how many times a packet will be forwarded by routers:
#ifdef _SOLARIS
  unsigned char val = TIME_SYNC_TTL;
#else
  int32_t val = TIME_SYNC_TTL;
#endif
#ifdef WIN32
  int32_t retVal = setsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL,
                              (const char*)&val, sizeof(val));
#else
  int32_t retVal = setsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL,
                              (const void*)&val, sizeof(val));
#endif
  if (retVal != 0) {
    FWKSEVERE("Failed to set ttl on socket.  Errno: " << errno);
    return;
  }

  struct sockaddr_in addr;
  socklen_t addrLen = sizeof(struct sockaddr_in);
  memset((void*)&addr, 0, addrLen);
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(TIME_SYNC_ADDR);
  addr.sin_port = htons(m_port);

  int32_t tvLen = sizeof(int64_t);
  do {
    ACE_Time_Value tval = ACE_OS::gettimeofday();
    int64_t now = htonl64(timevalMicros(tval));
#ifdef WIN32
    retVal = sendto(sock, (const char*)&now, tvLen, 0,
                    (const struct sockaddr*)&addr, addrLen);
#else
    retVal = sendto(sock, (const void*)&now, tvLen, 0,
                    reinterpret_cast<const struct sockaddr*>(&addr), addrLen);
#endif
    if (retVal == -1) {
      FWKSEVERE("Failed to send TimeSync message.  Errno: " << errno);
    } else {
      if (!m_logged) {
        m_logged = true;
        FWKINFO("Sending time sync messages at " << TIME_SYNC_ADDR << ":"
                                                 << TIME_SYNC_PORT);
      }
    }
    // FWKINFO( "TimeSync sent: " << now );
    perf::sleepSeconds(TIME_SYNC_PAUSE_SECONDS);
  } while (!m_done);
}

// ----------------------------------------------------------------------------

void TimeSync::recvTimeSync() {
  int32_t sock = (int32_t)socket(AF_INET, SOCK_DGRAM, 0);
  if (sock < 0) {
    FWKSEVERE(
        "Failed to create socket for receiving TimeSync messages.  Errno: "
        << errno);
    return;
  }

  // To allow binding multiple applications to the same IP group address:
  int32_t val = 1;
#ifdef WIN32
  int32_t retVal = setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (const char*)&val,
                              sizeof(val));
#else
  int32_t retVal = setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (const void*)&val,
                              sizeof(val));
#endif
  if (retVal != 0) {
    FWKSEVERE("Failed to set socket option SO_REUSEADDR.  Errno: " << errno);
    return;
  }

  struct sockaddr_in addr;
  socklen_t addrLen = sizeof(struct sockaddr_in);
  memset((void*)&addr, 0, addrLen);
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(m_port);

  retVal = bind(sock, reinterpret_cast<struct sockaddr*>(&addr), addrLen);
  if (retVal != 0) {
    FWKSEVERE(
        "Failed to bind to socket for receiving TimeSync messages.  Errno: "
        << errno);
    return;
  }

  // use setsockopt() to request that the kernel join a multicast group
  struct ip_mreq mreq;
  mreq.imr_multiaddr.s_addr = inet_addr(TIME_SYNC_ADDR);
  mreq.imr_interface.s_addr = htonl(INADDR_ANY);
#ifdef WIN32
  retVal = setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, (const char*)&mreq,
                      sizeof(mreq));
#else
  retVal = setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, (const void*)&mreq,
                      sizeof(mreq));
#endif
  if (retVal != 0) {
    FWKSEVERE("Failed to join multicast group.  Errno: " << errno);
    return;
  }

  int64_t tval;
  int32_t tvLen = sizeof(int64_t);
  struct sockaddr_in sender;
  socklen_t senderLen = sizeof(struct sockaddr_in);
  memset((void*)&sender, 0, senderLen);

  int32_t cnt = 0;
  int64_t total = 0;
  while (!m_done) {
    tval = 0;
#ifdef WIN32
    retVal = recvfrom(sock, (char*)&tval, tvLen, 0, (struct sockaddr*)&sender,
                      &senderLen);
#else
    retVal = recvfrom(sock, (void*)&tval, tvLen, 0,
                      reinterpret_cast<struct sockaddr*>(&sender), &senderLen);
#endif
    switch (retVal) {
      case -1:  // error!
        if ((errno > 0) && (errno != EAGAIN)) {
          FWKSEVERE(
              "Failed while receiving TimeSync message.  Errno: " << errno);
          return;
        }
        break;
      case 8:  // process the sync message
      {
        ACE_Time_Value now = ACE_OS::gettimeofday();
        int64_t curr = timevalMicros(now);
        tval = ntohl64(tval);
        int64_t diff = (tval - curr);
        total += diff;
        cnt++;
        if (cnt == 10) {
          int32_t avg = static_cast<int32_t>(total / cnt);
          if (*m_delta != avg) {
            *m_delta = avg;
            if (m_report) {
              FWKINFO("Current delta is: " << *m_delta);
            }
          }
          cnt = 0;
          total = 0;
        }
        if (!m_logged) {
          m_logged = true;
          FWKINFO("Receiving time sync messages at " << TIME_SYNC_ADDR << ":"
                                                     << TIME_SYNC_PORT);
        }
      } break;
      case 0:   // nothing received, no-op, fall thru
      default:  // junk??
        break;
    }
  }
}

// ----------------------------------------------------------------------------

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <memory.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>

typedef int int32_t;
typedef long long int64_t;

// Some useful conversion constants
#define NANOS_PER_MICRO 1000
#define MICROS_PER_MILLI 1000
#define NANOS_PER_MILLI NANOS_PER_MICRO * MICROS_PER_MILLI

#define MILLIS_PER_SECOND 1000  
#define MICROS_PER_SECOND MICROS_PER_MILLI * MILLIS_PER_SECOND
#define NANOS_PER_SECOND NANOS_PER_MICRO * MICROS_PER_SECOND

// Our configuration parameters
#define TIME_SYNC_ADDR  "224.0.37.27"
#define TIME_SYNC_PORT 9631
#define TIME_SYNC_PAUSE_MILLIS 10
#define TIME_SYNC_TTL 1

// Our sleep time calculated from the parameters
#define TIME_SYNC_SLEEP_NANOS TIME_SYNC_PAUSE_MILLIS * NANOS_PER_MILLI
#define TIME_SYNC_AVERAGE_INTERVAL MILLIS_PER_SECOND / TIME_SYNC_PAUSE_MILLIS

// ----------------------------------------------------------------------------

void sendTimeSync() {
  int32_t sock = socket( AF_INET, SOCK_DGRAM, 0 );
  if ( sock < 0 ) {
    printf( "%s  %d\n", "Failed to create socket for sending TimeSync messages.", errno );
    return;
  }
  
  // To control how many times a packet will be forwarded by routers:
  int32_t val = TIME_SYNC_TTL;
  int32_t retVal = setsockopt( sock, IPPROTO_IP, IP_MULTICAST_TTL,
    ( const void * )&val, sizeof( val ) );
  if ( retVal != 0 ) {
    printf( "%s  %d\n", "Failed to set ttl on socket.", errno );
    return;
  }

  struct sockaddr_in addr;
  socklen_t addrLen = sizeof( struct sockaddr_in );
  memset( ( void * )&addr, 0, addrLen );
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr( TIME_SYNC_ADDR ); 
  addr.sin_port = htons( TIME_SYNC_PORT );
  
  struct timeval tv;
  int32_t tvLen = sizeof( tv );
  
  bool done = false;
  while ( !done ) {
    retVal = gettimeofday( &tv, 0 );
    if ( retVal == 0 ) {
      retVal = sendto( sock, ( const void * )&tv, tvLen, 0, 
        ( const struct sockaddr * )&addr, addrLen );
      if ( retVal == -1 ) {
        printf( "%s  %d\n", "Failed to send TimeSync message.", errno );
        return;
      }
      printf( "." );
      fflush( stdout );
      struct timespec ts;
      ts.tv_sec = 0;
      ts.tv_nsec = TIME_SYNC_SLEEP_NANOS;
      nanosleep( &ts, 0 );
    }
    else {
      printf( "%s  %d\n", "Failed to get time of day to send.", errno );
      return;
    }
  }
}

// ----------------------------------------------------------------------------

void recvTimeSync( int64_t * delta ) {
  int32_t sock = socket( AF_INET, SOCK_DGRAM, 0 );
  if ( sock < 0 ) {
    printf( "%s  %d\n", "Failed to create socket for receiving TimeSync messages.", errno );
    return;
  }
  
  // To allow binding multiple applications to the same IP group address:
  int32_t val = 1;
  int32_t retVal = setsockopt( sock, SOL_SOCKET, SO_REUSEADDR,
    ( const void * )&val, sizeof( val ) );
  if ( retVal != 0 ) {
    printf( "%s  %d\n", "Failed to set socket option SO_REUSEADDR.", errno );
    return;
  }

  struct sockaddr_in addr;
  socklen_t addrLen = sizeof( struct sockaddr_in );
  memset( ( void * )&addr, 0, addrLen );
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl( INADDR_ANY ); 
  addr.sin_port = htons( TIME_SYNC_PORT );
  
  retVal = bind( sock, ( struct sockaddr * )&addr, addrLen );
  if ( retVal != 0 ) {
    printf( "%s  %d\n", "Failed to bind to socket for receiving TimeSync messages.", errno );
    return;
  }
  
   // use setsockopt() to request that the kernel join a multicast group 
   struct ip_mreq mreq;
   mreq.imr_multiaddr.s_addr = inet_addr( TIME_SYNC_ADDR );
   mreq.imr_interface.s_addr = htonl( INADDR_ANY );
   retVal = setsockopt( sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof( mreq ) );
   if ( retVal != 0 ) {
    printf( "%s  %d\n", "Failed to multicast group.", errno );
    return;
   }

  struct timeval now;
  struct timeval tv;
  int32_t tvLen = sizeof( tv );
  struct sockaddr_in sender;
  socklen_t senderLen = sizeof( struct sockaddr_in );
  memset( ( void * )&sender, 0, senderLen );
  
  bool done = false;
  int32_t cnt = 0;
  int64_t total = 0;
  while ( !done ) {
    retVal = recvfrom( sock, ( void * )&tv, tvLen, 0, ( struct sockaddr * )&sender, &senderLen );
    if ( retVal == -1 ) {
      printf( "%s  %d\n", "Failed while receiving TimeSync message.", errno );
      return;
    }
    else {
      gettimeofday( &now, 0 );
      int64_t curr = now.tv_sec;
      curr = ( curr * MICROS_PER_SECOND ) + now.tv_usec;
      int64_t src = tv.tv_sec;
      src = ( src * MICROS_PER_SECOND ) + tv.tv_usec;
      
      int64_t diff = src - curr;
      total += diff;
      cnt++;
      if ( ( cnt % 100 ) == 0 ) {
        int64_t avg = total / cnt;
        if ( *delta != avg ) {
          *delta = avg;
          printf( "\nCurrent delta is: %lld  ", *delta );
        }
        if ( cnt == 10000 ) {
          cnt = 1001;
          total = avg * cnt;
        }
      }
      printf( "." );
      fflush( stdout );
    }
  }
}

// ----------------------------------------------------------------------------

int main( int32_t argc, char * argv[] )
{
  if ( argc > 1 ) {
    sendTimeSync();
  }
  else {
    int64_t delta = 0;
    recvTimeSync( &delta );
  }

  return 0;
}

// ----------------------------------------------------------------------------



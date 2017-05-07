/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>
#include <memory.h>
#include <stdlib.h>
#include <string.h>

bool done = false;

struct TwoLongs {
  long a;
  long b;
};

union netTV {
  struct timeval ntv;
  struct TwoLongs tl;
};

extern "C" void goAway( int ignore ) {
  fflush( stdout );
  exit( 0 );
}

void respond() {
  const int len = 24;
  char buff[len];
  
  int port = 27015;
  int sock;
  
  sockaddr_in raddr;
  sockaddr_in saddr;
  socklen_t addrSize = sizeof( saddr );

  memset( ( void * )&raddr, 0, addrSize );
  memset( ( void * )&saddr, 0, addrSize );

  //-----------------------------------------------
  // Create a socket for datagrams
  sock = socket( AF_INET, SOCK_DGRAM, IPPROTO_UDP );

  //-----------------------------------------------
  // Bind the socket to any address and the specified port.
  raddr.sin_family = AF_INET;
  raddr.sin_port = htons( port );
  raddr.sin_addr.s_addr = htonl( INADDR_ANY );

  bind( sock, ( const sockaddr * )&raddr, addrSize );

  //-----------------------------------------------
  // Call the recvfrom function to receive datagrams
  // on the bound socket.
  printf( "Ready\n" );
  fflush( stdout );
  
  union netTV tv, * nv = ( union netTV * )buff;
    
  while ( !done ) {
    recvfrom( sock, buff, len, 0, ( sockaddr * )&saddr, &addrSize );
    int ign = gettimeofday( &( tv.ntv ), 0 );
    nv->tl.a = htonl( tv.tl.a );
    nv->tl.b = htonl( tv.tl.b );
    sendto( sock, buff, len, 0, ( sockaddr * )&saddr, addrSize );
    
    printf(".");
  }

  //-----------------------------------------------
  // Clean up and exit.
  close( sock );
  printf( "Done.\n" );
}

void request( char * host ) {
  const int len = 24;
  char buff[len];
  
  int port = 27015;
  int sock;
  
  sockaddr_in raddr;
  sockaddr_in saddr;
  socklen_t addrSize = sizeof( saddr );

  memset( ( void * )&raddr, 0, addrSize );
  memset( ( void * )&saddr, 0, addrSize );

  struct hostent * hent = gethostbyname( host );
  if ( hent == 0 ) {
      printf( "Couldn't locate host entry for %s\n", host );
      fflush( stdout );
      return;
  }
  
  char thisHost[128];
  gethostname( thisHost, 128 );
  char * dot = strchr( thisHost, '.' );
  if ( dot != 0 )
    *dot = 0;

  //-----------------------------------------------
  // Create a socket for datagrams
  sock = socket( AF_INET, SOCK_DGRAM, IPPROTO_UDP );

  //-----------------------------------------------
  // Set the send address to the specified host and port.
  saddr.sin_family = AF_INET;
  saddr.sin_port = htons( port );
  memcpy( ( void * )&saddr.sin_addr.s_addr, ( void * )hent->h_addr, hent->h_length );

  //-----------------------------------------------
  struct timeval start;
  struct timeval end;
  
  int iters = 1000;
  int peerBefore = 0;
  unsigned long long int totalDelta = 0;
  
  union netTV tv, * nv = ( union netTV * )buff;
  
  for ( int i = 0; i < iters; i++ ) {
    fflush( stdout );
    
    gettimeofday( &start, 0 );
    sendto( sock, buff, len, 0, ( sockaddr * )&saddr, addrSize );
    recvfrom( sock, buff, len, 0, ( sockaddr * )&raddr, &addrSize );
    gettimeofday( &end, 0 );
    
    tv.tl.a = ntohl( nv->tl.a );
    tv.tl.b = ntohl( nv->tl.b );
    
    // Translate the timevals into long long ints
    unsigned long long int reportedStart = ( start.tv_sec * 1000000ll ) + start.tv_usec;
    unsigned long long int reportedMiddle = ( tv.ntv.tv_sec * 1000000ll ) + tv.ntv.tv_usec;
    unsigned long long int reportedEnd = ( end.tv_sec * 1000000ll ) + end.tv_usec;
    
    // How long did it take to interact with peer ( one way )?
    unsigned long long int overhead = ( reportedEnd - reportedStart ) / 2;
    
    // What did we expect the middle to be?
    unsigned long long int expectedMiddle = reportedStart + overhead;
    
    // What did we expect the end to be?
    unsigned long long int expectedEnd = reportedMiddle + overhead;
    
    // If clocks were in sync, reportedMiddle would be = expectedMiddle
    long long int obDiff = expectedMiddle - reportedMiddle;  
    
    // if obDiff < 0, mtime was before stime
    if ( obDiff < 0 ) 
      peerBefore++;
    else
      peerBefore--;
    
    // If clocks were perfectly in sync, etime would always be > mtime
    long long int ibDiff = expectedEnd - reportedEnd;  
    
    
    // Add the absolute values of the inbound and outbound times, subtract the 
    // overhead, and divide by two for average delta between clocks
    long long int delta = ( llabs( obDiff ) + llabs( ibDiff ) ) / 2 ;
    totalDelta += llabs( delta );
  }
  
  printf( "%s is %s %s %llu microseconds.\n", thisHost, 
    ( ( peerBefore > 0 ) ? "behind" : "ahead of" ), host, totalDelta / iters );

  //-----------------------------------------------
  // Clean up and exit.
  close( sock );
}

// To compile:
//    Linux: g++ checkSync -o checkSync
//    Solaris: CC checkSync -o checkSync -lsocket -lnsl
//    Windows: cc checkSync -o checkSync

// To run:
//    On hostA: checkSync
//    On hostB: checkSync 
//    On hostC: checkSync hostA hostB

int main( int argc, char * argv[] ) {
  
  signal( SIGINT, goAway );
  
  if (argc < 2) {
    respond();
  }
  else {
    for ( int i = 1; i < argc; i++ )
      request( argv[i] );
  }
  printf( "Done.\n" );
  return 0;
}


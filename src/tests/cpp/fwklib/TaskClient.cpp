/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#include "TaskClient.hpp"

#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <ctype.h>
#ifndef WIN32
#include <unistd.h>
#endif

using namespace gemfire;
using namespace gemfire::testframework;

#ifdef WIN32

#define popen _popen
#define pclose _pclose
#define MODE "wt"

#else  // linux, et. al.

#define MODE "w"

#endif  // WIN32

FILE* TaskClient::m_pipe = (FILE*)0;
uint32_t TaskClient::m_refCnt = 0;

void TaskClient::writePipe(const char* cmd) {
  if (m_pipe == (FILE*)0) {
    openPipe();
  }
  fwrite(cmd, 1, strlen(cmd), m_pipe);
  fflush(m_pipe);
}

void TaskClient::openPipe() {
  const char* cmd = "bash -c \"piper >> piper_%d.log 2>&1\"";
  char cbuf[1024];
  int32_t pid = ACE_OS::getpid();

  sprintf(cbuf, cmd, pid);
  m_pipe = popen(cbuf, MODE);
  if (m_pipe == (FILE*)0) {
    FWKEXCEPTION("Failed to open pipe, errno: " << errno);
  }
}

void TaskClient::start() {
  m_busy = false;
  FWKINFO("Starting client " << m_id << " name: " << m_name << " on "
                             << m_host);
  char buf[1024];
  if (m_runsTasks) {
    sprintf(buf, "goHost %s startClient %d %s %s %d\n", m_host, m_id,
            m_testFile, m_logDirectory, m_port);
    writePipe(buf);
    ACE_SOCK_Stream* conn = new ACE_SOCK_Stream();
    if (m_listener->accept(*conn, 0, new ACE_Time_Value(120)) == 0) {
      FWKINFO("Client " << m_id << " connected.");
      m_ipc = new IpcHandler(conn);
    } else {
      FWKSEVERE("Accept failed with errno: " << errno);
      FWKEXCEPTION("Client " << m_id << " failed to register.");
    }
  } else {
    if (m_program == NULL) {
      FWKEXCEPTION("Program not specified for Client " << m_id);
    } else {
      const char* args = (m_arguments == NULL) ? "" : m_arguments;
      sprintf(buf, "goHost %s startClient %d %s %s %d \"%s\" \"%s\"\n", m_host,
              m_id, "none", m_logDirectory, 0, m_program, args);
      writePipe(buf);
    }
  }
}

void TaskClient::kill() {
  FWKINFO("Stopping client " << m_id << " name: " << m_name << " on "
                             << m_host);
  char buf[1024];
  sprintf(buf, "goHost %s stopProcess %d\n", m_host, m_id);
  writePipe(buf);
  m_busy = false;
}

// void TaskClient::killAll( std::string & host ) {
//  FWKINFO( "Stopping clients on " << host );
//  char buf[1024];
//  sprintf( buf, "goHost %s stopAll -l \n", host.c_str() );
//  writePipe( buf );
//}

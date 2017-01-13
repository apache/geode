/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __TaskClient_hpp__
#define __TaskClient_hpp__

#include "PerfFwk.hpp"
#include "FwkLog.hpp"

#include "IpcHandler.hpp"
#include "FwkObjects.hpp"

#include <ace/Task.h>
#include <ace/SOCK_Acceptor.h>

#include <string>

namespace gemfire {
namespace testframework {

#ifdef WIN32

#define pclose _pclose

#endif  // WIN32

class TaskClient : public ACE_Task_Base {
 private:
  char* m_host;
  int32_t m_port;
  char* m_logDirectory;
  char* m_testFile;
  IpcHandler* m_ipc;
  int32_t m_id;
  bool m_busy;
  bool m_assigned;
  std::string m_name;
  ACE_SOCK_Acceptor* m_listener;
  std::string m_taskId;
  bool m_runsTasks;
  char* m_program;
  char* m_arguments;
  static FILE* m_pipe;
  static uint32_t m_refCnt;
  std::string m_hostGroup;

  void openPipe();
  void writePipe(const char* cmd);

 public:
  TaskClient(int32_t port, const char* logDir, const char* testFile,
             ACE_SOCK_Acceptor* listener, int32_t id, const char* name = " ")
      : m_host(NULL),
        m_port(port),
        m_logDirectory(strdup(logDir)),
        m_testFile(strdup(testFile)),
        m_ipc(0),
        m_id(id),
        m_busy(false),
        m_assigned(false),
        m_name(name),
        m_listener(listener),
        m_runsTasks(true),
        m_program(NULL),
        m_arguments(NULL),
        m_hostGroup(FwkClientSet::m_defaultGroup) {
    m_refCnt++;
  }

  TaskClient(const char* logDir, int32_t id, const char* program,
             const char* arguments, const char* name = " ")
      : m_host(NULL),
        m_port(0),
        m_logDirectory(strdup(logDir)),
        m_testFile(NULL),
        m_ipc(0),
        m_id(id),
        m_busy(false),
        m_assigned(false),
        m_name(name),
        m_listener(NULL),
        m_runsTasks(false),
        m_program((program == NULL) ? NULL : strdup(program)),
        m_arguments((arguments == NULL) ? NULL : strdup(arguments)),
        m_hostGroup(FwkClientSet::m_defaultGroup) {
    m_refCnt++;
  }

  ~TaskClient() {
    m_refCnt--;
    if ((m_refCnt < 1) && (m_pipe != (FILE*)0)) {
      // FWKINFO( "Closing pipe." );
      const char* pexit = "exit\n";
      writePipe(pexit);
      perf::sleepSeconds(3);
      pclose(m_pipe);
      m_pipe = (FILE*)0;
      // FWKINFO( "Pipe closed." );
    }
    if (m_host != NULL) {
      free(m_host);
      m_host = NULL;
    }
    if (m_logDirectory != NULL) {
      free(m_logDirectory);
      m_logDirectory = NULL;
    }
    if (m_testFile != NULL) {
      free(m_testFile);
      m_testFile = NULL;
    }
    if (m_ipc != NULL) {
      delete m_ipc;
      m_ipc = NULL;
    }
  }

  int32_t getId() { return m_id; }
  const char* getLogDirectory() { return m_logDirectory; }
  bool isBusy() { return m_busy; }
  void setBusy(bool busy) { m_busy = busy; }
  bool isAssigned() { return m_assigned; }
  void setAssigned(bool assigned) { m_assigned = assigned; }
  std::string getName() { return m_name; }
  std::string getTaskId() { return m_taskId; }

  void setHostGroup(const std::string& grp) { m_hostGroup = grp; }
  std::string getHostGroup() { return m_hostGroup; }

  bool sendExit() { return m_ipc->sendExit(); }
  bool sendTask(char* taskId) {
    m_taskId = taskId;
    return m_ipc->sendTask(taskId);
  }

  IpcMsg getIpcMsg(int32_t waitSeconds, std::string& result) {
    IpcMsg msg = m_ipc->getIpcMsg(waitSeconds, result);
    if (msg == IPC_DONE) m_busy = false;
    return msg;
  }

  IpcMsg getIpcMsg(int32_t waitSeconds) {
    IpcMsg msg = m_ipc->getIpcMsg(waitSeconds);
    if (msg == IPC_DONE) m_busy = false;
    return msg;
  }

  const char* getHost() { return m_host; }
  void setHost(const char* host) {
    if (host != NULL) {
      if (m_host != NULL) free(m_host);
      m_host = strdup(host);
    }
  }

  void start();
  void kill();
  //  void killAll( std::string & host );

  bool runsTasks() { return m_runsTasks; }

};  // TaskClient

}  // testframework
}  // gemfire

#endif  // __TaskClient_hpp__

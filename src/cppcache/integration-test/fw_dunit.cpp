/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifdef WIN32
// ace.dll was built with FD_SETSIZE of 1024, so ensure it stays that way.
#undef FD_SETSIZE
#define FD_SETSIZE 1024
#if WINVER == 0x0500
#undef _WINSOCKAPI_
#include <WinSock2.h>
#endif
#endif

#ifdef USE_SMARTHEAP
#include <smrtheap.h>
#endif

#include "TimeBomb.hpp"

#include <ace/ACE.h>

// #include <gfcpp/GemfireCppCache.hpp>

#include <typeinfo>

#include <string>
#include <list>
#include <map>

// SW: Switching to framework BB on linux also since it is much faster.
#ifndef _WIN32
// On solaris, when ACE_Naming_Context maps file to memory using fixed mode, it
// interfere with malloc/brk system calls later cause failure. For now, we use
// the Black Board from the regression test framework. When the ACE problem is
// fixed in a new release we'll go back to original code by undefining
// SOLARIS_USE_BB
#define SOLARIS_USE_BB 1
#endif

#define KEY_MAX 128
#define VALUE_MAX 128

#ifdef SOLARIS_USE_BB
#include "BBNamingContext.hpp"
#else
#include <ace/Naming_Context.h>
#endif

#include <ace/Guard_T.h>
#include <ace/Get_Opt.h>
#include <ace/Time_Value.h>
#include <ace/SV_Semaphore_Complex.h>

#include "fw_spawn.hpp"
#include "fwklib/FwkException.hpp"

#define __DUNIT_NO_MAIN__
#include "fw_dunit.hpp"

typedef unsigned int uint32_t;

ACE_TCHAR* g_programName = NULL;
uint32_t g_masterPid = 0;

ClientCleanup gClientCleanup;

namespace dunit {

void HostWaitForDebugger() {
  int done = 0;
  LOG("host wait for debugger.");
  while (!done) {
    sleep(1);
  }
}

void setupCRTOutput() {
#ifdef _WIN32
#ifdef DEBUG
  int reportMode = _CRTDBG_MODE_FILE | _CRTDBG_MODE_WNDW;
  _CrtSetReportMode(_CRT_ASSERT, reportMode);
  _CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDERR);
  _CrtSetReportMode(_CRT_ERROR, reportMode);
  _CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDERR);
  _CrtSetReportMode(_CRT_WARN, reportMode);
  _CrtSetReportFile(_CRT_WARN, _CRTDBG_FILE_STDERR);
  SetErrorMode(SEM_FAILCRITICALERRORS);
#endif
#endif
}

void getTimeStr(char* bufPtr) {
  ACE_TCHAR timestamp[64] = {0};  // only 35 needed here
  ACE::timestamp(timestamp, sizeof timestamp);
  // timestamp is like "Tue May 17 2005 12:54:22.546780"
  // for our purpose we just want "12:54:22.546780"
  strcpy(bufPtr, &timestamp[16]);
}

// some common values..
#define SLAVE_STATE_UNKNOWN 0
#define SLAVE_STATE_READY 1
#define SLAVE_STATE_DONE 2
#define SLAVE_STATE_TASK_ACTIVE 3
#define SLAVE_STATE_TASK_COMPLETE 4
#define SLAVE_STATE_SCHEDULED 5

void log(std::string s, int lineno, const char* filename);

/** Naming service for sharing data between processes. */
class NamingContextImpl : virtual public NamingContext {
 private:
#ifdef SOLARIS_USE_BB
  BBNamingContextClient
#else
  ACE_Naming_Context
#endif
      m_context;

  void millisleep(int msec) {
    ACE_Time_Value sleepTime;
    sleepTime.msec(msec);
    ACE_OS::sleep(sleepTime);
  }

  int checkResult(int result, const char* func) {
    if (result == -1) {
      LOGMASTER("NamingCtx operation failed for:");
      LOGMASTER(func);
      LOGMASTER("Dump follows:");
      dump();
      throw - 1;
    }
    return result;
  }

 public:
  NamingContextImpl() : m_context() {
    open();
    LOGMASTER("Naming context ready.");
  }

  virtual ~NamingContextImpl() {
    m_context.close();
    ACE_OS::unlink(ACE_OS::getenv("TESTNAME"));
  }

  /**
   * Share a string value, return -1 if there is a failure to store value,
   * otherwise returns 0.
   */
  virtual int rebind(const char* key, const char* value) {
    int res = -1;
    int attempts = 10;
    while ((res = m_context.rebind(key, value, (char*)"")) == -1 &&
           attempts--) {
      millisleep(10);
    }
    return checkResult(res, "rebind");
  }

  /**
   * Share an int value, return -1 if there is a failure to store value,
   * otherwise returns 0.
   */
  virtual int rebind(const char* key, int value) {
    char buf[VALUE_MAX] = {0};
    ACE_OS::sprintf(buf, "%d", value);
    int res = rebind(key, (const char*)buf);
    return res;
  }

  /**
   * retreive a value by key, storing the result in the users buf. If the key
   * is not found, the buf will contain the empty string "".
   */
  virtual void getValue(const char* key, char* buf) {
#ifdef SOLARIS_USE_BB
    char value[VALUE_MAX] = {0};
    char type[VALUE_MAX] = {0};
#else
    char* value = NULL;
    char* type = NULL;
#endif

    int res = -1;
    // we should not increase attempts to avoid increasing test run times.
    int attempts = 3;
    while ((res = m_context.resolve(key, value, type)) != 0 && attempts--) {
      // we should not increase sleep to avoid increasing test run times.
      millisleep(5);
    }

    if (res != 0) {
      strcpy(buf, "");
      return;
    }
    ACE_OS::strcpy(buf, value);
  }

  /**
   * return the value by key, as an int using the string to int conversion
   * rules of atoi.
   */
  virtual int getIntValue(const char* key) {
    char value[VALUE_MAX] = {0};
    getValue(key, value);
    if (ACE_OS::strcmp(value, "") == 0) return 0;
    return ACE_OS::atoi(value);
  }

  void open(bool local = true) {
#ifdef SOLARIS_USE_BB
    m_context.open();
#else
    ACE_Name_Options* name_options = m_context.name_options();
    name_options->process_name(getContextName().c_str());
    name_options->namespace_dir(".");
    if (local)
      name_options->context(ACE_Naming_Context::PROC_LOCAL);
    else
      name_options->context(ACE_Naming_Context::NET_LOCAL);
    // std::string dbName = ACE_DEFAULT_LOCALNAME;
    // dbName.append(ACE_OS::getenv("TESTNAME"));

    name_options->database(ACE_OS::getenv("TESTNAME"));
    checkResult(m_context.open(name_options->context(), 0), "open");
#endif
    LOGMASTER("Naming context opened.");
  }

  std::string getContextName() {
    char buf[1024] = {0};
    ACE_OS::sprintf(buf, "dunit.context.%s%d", ACE::basename(g_programName),
                    g_masterPid);
    std::string b_str(buf);
    return b_str;
  }

  std::string getMutexName() {
    char buf[1024] = {0};
    ACE_OS::sprintf(buf, "dunit.mutex.%s%d", ACE::basename(g_programName),
                    g_masterPid);
    std::string b_str(buf);
    return b_str;
  }

  /** print out all the entries' keys and values in the naming context. */
  virtual void dump() {
#ifdef SOLARIS_USE_BB
    m_context.dump();
#else
    ACE_BINDING_SET set;
    if (this->m_context.list_name_entries(set, "") != 0) {
      char buf[1000] = {0};
      ACE_OS::sprintf(buf, "There is nothing in the naming context.");
      LOGMASTER(buf);
    } else {
      ACE_BINDING_ITERATOR set_iterator(set);
      for (ACE_Name_Binding* entry = 0; set_iterator.next(entry) != 0;
           set_iterator.advance()) {
        ACE_Name_Binding binding(*entry);
        char buf[1000] = {0};
        ACE_OS::sprintf(buf, "%s => %s", binding.name_.char_rep(),
                        binding.value_.char_rep());
        LOGMASTER(buf);
      }
    }
#endif
  }

  void resetContext() {
    char buf[30] = {0};
    sprintf(buf, "%d", ACE_OS::getpid());

    int res1 = -1;
    int attempts1 = 10;
    while ((res1 = m_context.rebind("Driver", buf)) == -1 && attempts1--) {
      millisleep(10);
    }
    checkResult(res1, "rebind1");

    int res2 = -1;
    int attempts2 = 10;
    while ((res2 = m_context.rebind("SlaveId", "0")) == -1 && attempts2--) {
      millisleep(10);
    }
    checkResult(res2, "rebind2");

    LOGMASTER("Naming context reset.");
  }
};

/** uniquely represent each different slave. */
class SlaveId {
 private:
  uint32_t m_id;
  static const char* m_idNames[];

 public:
  explicit SlaveId(uint32_t id) { m_id = id; }

  int getId() { return m_id; }

  const char* getIdName() { return m_idNames[m_id]; }

  /** return the system id for this process */
  int getSystem() { return 1; }
  /** return the process id for this system. */
  int getProcOnSys() { return ((m_id % 2) == 0) ? 2 : 1; }
};

const char* SlaveId::m_idNames[] = {"none", "s1p1", "s1p2", "s2p1", "s2p2"};

/** method for letting Task discover its name through RTTI. */
std::string Task::typeName() { return std::string(typeid(*this).name()); }

typedef std::list<Task*> TaskList;

/** contains a queue of Task* for each SlaveId. */
class TaskQueues {
 private:
  std::map<int, TaskList> m_qmap;
  std::list<int> m_schedule;

  TaskQueues() : m_qmap(), m_schedule() {}

  void registerTask(SlaveId sId, Task* task) {
    m_qmap[sId.getId()].push_back(task);
    m_schedule.push_back(sId.getId());
  }

  Task* nextTask(SlaveId& sId) {
    TaskList* tasks = &(m_qmap[sId.getId()]);
    if (tasks->empty()) {
      return NULL;
    }
    Task* task = tasks->front();
    if (task != NULL) {
      char logmsg[1024] = {0};
      sprintf(logmsg, "received task: %s ", task->m_taskName.c_str());
      LOG(logmsg);
      tasks->pop_front();
    }
    return task;
  }

  int nextSlaveId() {
    if (m_schedule.empty()) {
      return 0;
    }
    int sId = m_schedule.front();
    char logmsg[1024] = {0};
    sprintf(logmsg, "Next slave id is : %d", sId);
    LOGMASTER(logmsg);
    m_schedule.pop_front();
    return sId;
  }

  static TaskQueues* taskQueues;

 public:
  static void addTask(SlaveId sId, Task* task) {
    if (taskQueues == NULL) {
      taskQueues = new TaskQueues();
    }
    taskQueues->registerTask(sId, task);
  }

  static int getSlaveId() {
    ASSERT(taskQueues != NULL, "failure to initialize fw_dunit module.");
    return taskQueues->nextSlaveId();
  }

  static Task* getTask(SlaveId sId) {
    ASSERT(taskQueues != NULL, "failure to initialize fw_dunit module.");
    return taskQueues->nextTask(sId);
  }
};

TaskQueues* TaskQueues::taskQueues = NULL;

/** register task with slave. */
void Task::init(int sId) {
  m_id = sId;
  m_taskName = this->typeName();
  TaskQueues::addTask(SlaveId(sId), this);
}

/** main framework entry */
class Dunit {
 private:
  NamingContextImpl m_globals;
  static Dunit* singleton;
  bool m_close_down;

  Dunit() : m_globals(), m_close_down(false) {}

  void resetContext() {
    m_close_down = true;
    m_globals.resetContext();
  }

 public:
  /** call this once just inside main... */
  static void init(bool initContext = false) {
    if (initContext) {
      ACE_OS::unlink("localnames");
      ACE_OS::unlink("name_space_localnames");
      ACE_OS::unlink("backing_store_localnames");
    }
    singleton = new Dunit();
    if (initContext) {
      singleton->resetContext();
    }
  }

  /** return the already initialized singleton Dunit instance. */
  static Dunit* getSingleton() {
    ASSERT(singleton != NULL, "singleton not created yet.");
    return singleton;
  }

  /** delete the existing singleton */
  static void close() {
    Dunit* tmp = singleton;
    singleton = NULL;
    delete tmp;
  }

  /** set the next slave id */
  void setNextSlave(SlaveId& sId) { m_globals.rebind("SlaveId", sId.getId()); }

  /** get the next slave id */
  int getNextSlave() { return m_globals.getIntValue("SlaveId"); }

  /** return true if all slaves are to terminate. */
  bool mustQuit() {
    return m_globals.getIntValue("TerminateAllSlaves") ? true : false;
  }

  /** signal all slaves to terminate. */
  void setMustQuit() { m_globals.rebind("TerminateAllSlaves", 1); }

  /** signal to test driver that an error occurred. */
  void setFailed() { m_globals.rebind("Failure", 1); }

  bool getFailed() { return m_globals.getIntValue("Failure") ? true : false; }

  void setSlaveState(SlaveId sId, int state) {
    char key[100] = {0};
    ACE_OS::sprintf(key, "ReadySlave%d", sId.getId());
    m_globals.rebind(key, state);
  }

  int getSlaveState(SlaveId sId) {
    char key[100] = {0};
    ACE_OS::sprintf(key, "ReadySlave%d", sId.getId());
    return m_globals.getIntValue(key);
  }

  void setSlaveTimeout(SlaveId sId, int seconds) {
    char key[100] = {0};
    ACE_OS::sprintf(key, "TimeoutSlave%d", sId.getId());
    m_globals.rebind(key, seconds);
  }

  int getSlaveTimeout(SlaveId sId) {
    char key[100] = {0};
    ACE_OS::sprintf(key, "TimeoutSlave%d", sId.getId());
    return m_globals.getIntValue(key);
  }

  /** return the NamingContext for global (amongst all processes) values. */
  NamingContext* globals() { return &m_globals; }

  ~Dunit() {}
};

#define DUNIT dunit::Dunit::getSingleton()

Dunit* Dunit::singleton = NULL;

void Task::setTimeout(int seconds) {
  if (seconds > 0) {
    DUNIT->setSlaveTimeout(SlaveId(m_id), seconds);
  } else {
    DUNIT->setSlaveTimeout(SlaveId(m_id), TASK_TIMEOUT);
  }
}

class TestProcess : virtual public dunit::Manager {
 private:
  SlaveId m_sId;

 public:
  TestProcess(const ACE_TCHAR* cmdline, uint32_t id)
      : Manager(cmdline), m_sId(id) {}

  SlaveId& getSlaveId() { return m_sId; }

 protected:
 public:
  virtual ~TestProcess() {}
};

/**
 * Container of TestProcess(es) held in driver. each represents one of the
 * legal SlaveIds spawned when TestDriver is created.
 */
class TestDriver {
 private:
  TestProcess* m_slaves[4];
#ifdef SOLARIS_USE_BB
  BBNamingContextServer* m_bbNamingContextServer;
#endif

 public:
  TestDriver() {
#ifdef SOLARIS_USE_BB
    m_bbNamingContextServer = new BBNamingContextServer();
    ACE_OS::sleep(5);
    fprintf(stdout, "Blackboard started\n");
    fflush(stdout);
#endif

    dunit::Dunit::init(true);
    fprintf(stdout, "Master starting slaves.\n");
    for (uint32_t i = 1; i < 5; i++) {
      ACE_TCHAR cmdline[2048] = {0};
      char* profilerCmd = ACE_OS::getenv("PROFILERCMD");
      if (profilerCmd != NULL && profilerCmd[0] != '$' &&
          profilerCmd[0] != '\0') {
        // replace %d's in profilerCmd with PID and slave ID
        char cmdbuf[2048] = {0};
        ACE_OS::sprintf(cmdbuf, profilerCmd, ACE_OS::gettimeofday().msec(),
                        g_masterPid, i);
        ACE_OS::sprintf(cmdline, "%s %s -s%d -m%d", cmdbuf, g_programName, i,
                        g_masterPid);
      } else {
        ACE_OS::sprintf(cmdline, "%s -s%d -m%d", g_programName, i, g_masterPid);
      }
      fprintf(stdout, "%s\n", cmdline);
      m_slaves[i - 1] = new TestProcess(cmdline, i);
    }
    fflush(stdout);
    // start each of the slaves...
    for (uint32_t j = 1; j < 5; j++) {
      m_slaves[j - 1]->doWork();
      ACE_OS::sleep(2);  // do not increase this to avoid precheckin runs taking
                         // much longer.
    }
  }

  ~TestDriver() {
    // kill off any children that have not yet terminated.
    for (uint32_t i = 1; i < 5; i++) {
      if (m_slaves[i - 1]->running() == 1) {
        delete m_slaves[i - 1];  // slave destructor should terminate process.
      }
    }
    dunit::Dunit::close();
#ifdef SOLARIS_USE_BB
    delete m_bbNamingContextServer;
    m_bbNamingContextServer = NULL;
#endif
  }

  int begin() {
    fprintf(stdout, "Master started with pid %d\n", ACE_OS::getpid());
    fflush(stdout);
    waitForReady();
    // dispatch task...

    int nextSlave;
    while ((nextSlave = TaskQueues::getSlaveId()) != 0) {
      SlaveId sId(nextSlave);
      DUNIT->setSlaveState(sId, SLAVE_STATE_SCHEDULED);
      fprintf(stdout, "Set next process to %s\n", sId.getIdName());
      fflush(stdout);
      DUNIT->setNextSlave(sId);
      waitForCompletion(sId);
      // check special conditions.
      if (DUNIT->getFailed()) {
        DUNIT->setMustQuit();
        waitForDone();
        return 1;
      }
    }

    // end all work..
    DUNIT->setMustQuit();
    waitForDone();
    return 0;
  }

  /** wait for an individual slave to finish a task. */
  void waitForCompletion(SlaveId& sId) {
    int secs = DUNIT->getSlaveTimeout(sId);
    DUNIT->setSlaveTimeout(sId, TASK_TIMEOUT);
    if (secs <= 0) secs = TASK_TIMEOUT;
    fprintf(stdout, "Waiting %d seconds for %s to finish task.\n", secs,
            sId.getIdName());
    fflush(stdout);
    ACE_Time_Value end = ACE_OS::gettimeofday();
    ACE_Time_Value offset(secs, 0);
    end += offset;
    while (DUNIT->getSlaveState(sId) != SLAVE_STATE_TASK_COMPLETE) {
      // sleep a bit..
      if (DUNIT->getFailed()) return;
      ACE_Time_Value sleepTime;
      sleepTime.msec(100);
      ACE_OS::sleep(sleepTime);
      checkSlaveDeath();
      ACE_Time_Value now = ACE_OS::gettimeofday();
      if (now >= end) {
        handleTimeout(sId);
        break;
      }
    }
  }

  void handleTimeout() {
    fprintf(stdout, "Error: Timed out waiting for all slaves to be ready.\n");
    fflush(stdout);
    DUNIT->setMustQuit();
    DUNIT->setFailed();
  }

  void handleTimeout(SlaveId& sId) {
    fprintf(stdout, "Error: Timed out waiting for %s to finish task.\n",
            sId.getIdName());
    fflush(stdout);
    DUNIT->setMustQuit();
    DUNIT->setFailed();
  }

  /** wait for all slaves to be done initializing. */
  void waitForReady() {
    fprintf(stdout, "Waiting %d seconds for all slaves to be ready.\n",
            TASK_TIMEOUT);
    fflush(stdout);
    ACE_Time_Value end = ACE_OS::gettimeofday();
    ACE_Time_Value offset(TASK_TIMEOUT, 0);
    end += offset;
    uint32_t readyCount = 0;
    while (readyCount < 4) {
      fprintf(stdout, "Ready Count: %d\n", readyCount);
      fflush(stdout);
      if (DUNIT->getFailed()) return;
      // sleep a bit..
      ACE_Time_Value sleepTime(1);
      //      sleepTime.msec( 10 );
      ACE_OS::sleep(sleepTime);
      readyCount = 0;
      for (uint32_t i = 1; i < 5; i++) {
        int state = DUNIT->getSlaveState(SlaveId(i));
        if (state == SLAVE_STATE_READY) {
          readyCount++;
        }
      }
      checkSlaveDeath();
      ACE_Time_Value now = ACE_OS::gettimeofday();
      if (now >= end) {
        handleTimeout();
        break;
      }
    }
  }

  /** wait for all slaves to be destroyed. */
  void waitForDone() {
    fprintf(stdout, "Waiting %d seconds for all slaves to complete.\n",
            TASK_TIMEOUT);
    fflush(stdout);
    ACE_Time_Value end = ACE_OS::gettimeofday();
    ACE_Time_Value offset(TASK_TIMEOUT, 0);
    end += offset;
    uint32_t doneCount = 0;
    while (doneCount < 4) {
      // if ( DUNIT->getFailed() ) return;
      // sleep a bit..
      ACE_Time_Value sleepTime;
      sleepTime.msec(100);
      ACE_OS::sleep(sleepTime);
      doneCount = 0;
      for (uint32_t i = 1; i < 5; i++) {
        int state = DUNIT->getSlaveState(SlaveId(i));
        if (state == SLAVE_STATE_DONE) {
          doneCount++;
        }
      }
      ACE_Time_Value now = ACE_OS::gettimeofday();
      if (now >= end) {
        handleTimeout();
        break;
      }
    }
  }

  /** test to see that all the slave processes are still around, or throw
      a TestException so the driver doesn't get hung. */
  void checkSlaveDeath() {
    for (uint32_t i = 0; i < 4; i++) {
      if (!m_slaves[i]->running()) {
        char msg[1000] = {0};
        sprintf(msg, "Error: Slave %s terminated prematurely.",
                m_slaves[i]->getSlaveId().getIdName());
        LOG(msg);
        DUNIT->setFailed();
        DUNIT->setMustQuit();
        FAIL(msg);
      }
    }
  }
};

class TestSlave {
 private:
  SlaveId m_sId;

 public:
  static SlaveId* procSlaveId;

  explicit TestSlave(int id) : m_sId(id) {
    procSlaveId = new SlaveId(id);
    dunit::Dunit::init();
    DUNIT->setSlaveState(m_sId, SLAVE_STATE_READY);
  }

  ~TestSlave() {
    DUNIT->setSlaveState(m_sId, SLAVE_STATE_DONE);
    dunit::Dunit::close();
  }

  void begin() {
    fprintf(stdout, "Slave %s started with pid %d\n", m_sId.getIdName(),
            ACE_OS::getpid());
    fflush(stdout);
    SlaveId slaveZero(0);

    // consume tasks of this slaves queue, only when it is his turn..
    while (!DUNIT->mustQuit()) {
      if (DUNIT->getNextSlave() == m_sId.getId()) {
        // set next slave to zero so I don't accidently run twice.
        DUNIT->setNextSlave(slaveZero);
        // do next task...
        Task* task = TaskQueues::getTask(m_sId);
        // perform task.
        if (task != NULL) {
          DUNIT->setSlaveState(m_sId, SLAVE_STATE_TASK_ACTIVE);
          try {
            task->doTask();
            fflush(stdout);
            DUNIT->setSlaveState(m_sId, SLAVE_STATE_TASK_COMPLETE);
          } catch (TestException te) {
            te.print();
            handleError();
            return;
          } catch (...) {
            LOG("Unhandled exception, terminating.");
            handleError();
            return;
          }
        }
      }
      ACE_Time_Value sleepTime;
      sleepTime.msec(100);
      ACE_OS::sleep(sleepTime);
    }
  }

  void handleError() {
    DUNIT->setFailed();
    DUNIT->setMustQuit();
    DUNIT->setSlaveState(m_sId, SLAVE_STATE_TASK_COMPLETE);
  }
};

SlaveId* TestSlave::procSlaveId = NULL;

void sleep(int millis) {
  if (millis == 0) {
    ACE_OS::thr_yield();
  } else {
    ACE_Time_Value sleepTime;
    sleepTime.msec(millis);
    ACE_OS::sleep(sleepTime);
  }
}

void logMaster(std::string s, int lineno, const char* filename) {
  char buf[128] = {0};
  dunit::getTimeStr(buf);

  fprintf(stdout, "[TEST master:pid(%d)] %s at line: %d\n", ACE_OS::getpid(),
          s.c_str(), lineno);
  fflush(stdout);
}

// log a message and print the slave id as well.. used by fw_helper with no
// slave id.
void log(std::string s, int lineno, const char* filename, int id) {
  char buf[128] = {0};
  dunit::getTimeStr(buf);

  fprintf(stdout, "[TEST 0:pid(%d)] %s at line: %d\n", ACE_OS::getpid(),
          s.c_str(), lineno);
  fflush(stdout);
}

// log a message and print the slave id as well..
void log(std::string s, int lineno, const char* filename) {
  char buf[128] = {0};
  dunit::getTimeStr(buf);

  fprintf(stdout, "[TEST %s %s:pid(%d)] %s at line: %d\n", buf,
          (dunit::TestSlave::procSlaveId
               ? dunit::TestSlave::procSlaveId->getIdName()
               : "master"),
          ACE_OS::getpid(), s.c_str(), lineno);
  fflush(stdout);
}

void cleanup() { gClientCleanup.callClientCleanup(); }

int dmain(int argc, ACE_TCHAR* argv[]) {
#ifdef WIN32
  char* envsetting = ACE_OS::getenv("BUG481");
  if (envsetting != NULL && strlen(envsetting) > 0) {
    gemfire::setNewAndDelete(&operator new, & operator delete);
  }
#endif

#ifdef USE_SMARTHEAP
  MemRegisterTask();
#endif
  setupCRTOutput();
  TimeBomb tb(&cleanup);
  // tb->arm(); // leak this on purpose.
  try {
    g_programName = new ACE_TCHAR[2048];
    ACE_OS::strcpy(g_programName, argv[0]);

    const ACE_TCHAR options[] = ACE_TEXT("s:m:");
    ACE_Get_Opt cmd_opts(argc, argv, options);

    int result = 0;

    int slaveId = 0;
    int option = 0;
    while ((option = cmd_opts()) != EOF) {
      switch (option) {
        case 's':
          slaveId = ACE_OS::atoi(cmd_opts.opt_arg());
          fprintf(stdout, "Using process id: %d\n", slaveId);
          fflush(stdout);
          break;
        case 'm':
          g_masterPid = ACE_OS::atoi(cmd_opts.opt_arg());
          fprintf(stdout, "Using master id: %d\n", g_masterPid);
          fflush(stdout);
          break;
        default:
          fprintf(stdout, "ignoring option: %s  with value %s\n",
                  cmd_opts.last_option(), cmd_opts.opt_arg());
          fflush(stdout);
      }
    }

    //  perf::NamingServiceThread nsvc( 12045 );
    //  nsvc.activate( THR_NEW_LWP | THR_DETACHED | THR_DAEMON, 1 );
    //  dunit::Dunit::init( true );
    //
    //  for ( int i = cmd_opts.opt_ind(); i < argc; i++ ) {
    //    char buf[1024], * name, * value;
    //    strcpy( buf, argv[i] );
    //    name = &buf[0];
    //    value = strchr( name, '=' );
    //    if ( value != 0 ) {
    //      *value = '\0';
    //      value++;
    //      // add to context
    //      dunit::globals()->rebind( name, value );
    //    }
    //  }

    // record the master pid if it wasn't passed to us on the command line.
    // the TestDriver will pass this to the child processes.
    // currently this is used for giving a unique per run id to shared
    // resources.
    if (g_masterPid == 0) {
      g_masterPid = ACE_OS::getpid();
    }

    if (slaveId > 0) {
      dunit::TestSlave slave(slaveId);
      slave.begin();
    } else {
      dunit::TestDriver tdriver;
      result = tdriver.begin();
      if (result == 0) {
        printf("#### All Tasks completed successfully. ####\n");
      } else {
        printf("#### FAILED. ####\n");
      }

      fflush(stdout);
    }
    printf("final slave id %d, result %d\n", slaveId, result);
    printf("before calling cleanup %d \n", slaveId);
    gClientCleanup.callClientCleanup();
    printf("after calling cleanup\n");
    return result;

  } catch (dunit::TestException& te) {
    te.print();
  } catch (gemfire::testframework::FwkException& fe) {
    printf("Exception: %s\n", fe.what());
    fflush(stdout);
  } catch (...) {
    printf("Exception: unhandled/unidentified exception reached main.\n");
    fflush(stdout);
    // return 1;
  }
  gClientCleanup.callClientCleanup();
  return 1;
}

/** entry point for test code modules to access the naming service. */
NamingContext* globals() { return DUNIT->globals(); }

};  // namespace dunit

namespace perf {

TimeStamp::TimeStamp(int64_t msec) : m_msec(msec) {}

TimeStamp::TimeStamp() {
  ACE_Time_Value tmp = ACE_OS::gettimeofday();
  m_msec = tmp.msec();
}

TimeStamp::TimeStamp(const TimeStamp& other) : m_msec(other.m_msec) {}

TimeStamp& TimeStamp::operator=(const TimeStamp& other) {
  m_msec = other.m_msec;
  return *this;
}

TimeStamp::~TimeStamp() {}

int64_t TimeStamp::msec() const { return m_msec; }

void TimeStamp::msec(int64_t t) { m_msec = t; }

Record::Record(std::string testName, const long ops, const TimeStamp& start,
               const TimeStamp& stop)
    : m_testName(testName),
      m_operations(ops),
      m_startTime(start),
      m_stopTime(stop) {}

Record::Record()
    : m_testName(""), m_operations(0), m_startTime(0), m_stopTime(0) {}

Record::Record(const Record& other)
    : m_testName(other.m_testName),
      m_operations(other.m_operations),
      m_startTime(other.m_startTime),
      m_stopTime(other.m_stopTime) {}

Record& Record::operator=(const Record& other) {
  m_testName = other.m_testName;
  m_operations = other.m_operations;
  m_startTime = other.m_startTime;
  m_stopTime = other.m_stopTime;
  return *this;
}

void Record::write(gemfire::DataOutput& output) {
  output.writeASCII(m_testName.c_str(),
                    static_cast<uint32_t>(m_testName.length()));
  output.writeInt(m_operations);
  output.writeInt(m_startTime.msec());
  output.writeInt(m_stopTime.msec());
}

void Record::read(gemfire::DataInput& input) {
  char* buf = NULL;
  input.readASCII(&buf);
  m_testName = buf;
  delete[] buf;
  input.readInt(&m_operations);
  int64_t timetmp;
  input.readInt(&timetmp);
  m_startTime.msec(timetmp);
  input.readInt(&timetmp);
  m_stopTime.msec(timetmp);
}

Record::~Record() {}

int Record::elapsed() {
  return static_cast<int>(m_stopTime.msec() - m_startTime.msec());
}

int Record::perSec() {
  return static_cast<int>(((static_cast<double>(1000) * m_operations) /
                           static_cast<double>(elapsed())) +
                          0.5);
}

std::string Record::asString() {
  std::string tmp = m_testName;
  char* buf = new char[1000];
  sprintf(buf, " -- %d ops/sec, ", perSec());
  tmp += buf;
  sprintf(buf, "%d ops, ", static_cast<int>(m_operations));
  tmp += buf;
  sprintf(buf, "%d millis", elapsed());
  tmp += buf;
  return tmp;
}

PerfSuite::PerfSuite(const char* suiteName) : m_suiteName(suiteName) {}

void PerfSuite::addRecord(std::string testName, const long ops,
                          const TimeStamp& start, const TimeStamp& stop) {
  Record tmp(testName, ops, start, stop);
  m_records[testName] = tmp;
  int64_t elapsed ATTR_UNUSED = stop.msec() - start.msec();
  int64_t opspersec ATTR_UNUSED = (1000 * ops) / elapsed;
  fprintf(stdout, "[PerfSuite] %s\n", tmp.asString().c_str());
  fflush(stdout);
}

/** create a file in cwd, named "<suite>_results.<host>" */
void PerfSuite::save() {
  /* Currently having trouble with windows... not useful until the compare
     function is written anyway...

  gemfire::DataOutput output;
  output.writeASCII( m_suiteName.c_str(), m_suiteName.length() );

  char hname[100];
  ACE_OS::hostname( hname, 100 );
  std::string fname = m_suiteName + "_results." + hname;

  output.writeASCII( hname );

  for( RecordMap::iterator iter = m_records.begin(); iter != m_records.end();
  iter++ ) {
    Record record = (*iter).second;
    record.write( output );
  }
  fprintf( stdout, "[PerfSuite] finished serializing results.\n" );
  fflush( stdout );

  fprintf( stdout, "[PerfSuite] writing results to %s\n", fname.c_str() );
  FILE* of = ACE_OS::fopen( fname.c_str(), "a+" );
  if ( of == 0 ) {
    FAIL( "failed to open result file handle for perfSuite." );
  }
  LOG( "opened perf output file for a+" );
  uint32_t len = 0;
  char* buf = (char*) output.getBuffer( &len );
  LOG( "got buffer." );
  ACE_OS::fwrite( buf, len, 1, of );
  LOG( "wrote buffer" );
  ACE_OS::fflush( of );
  LOG( "flushed of" );
  ACE_OS::fclose( of );
  LOG( "closed of" );
  fprintf( stdout, "[PerfSuite] finished saving results file %s\n",
  fname.c_str() );
  fflush( stdout );
  */
}

/** load data saved in $ENV{'baselines'} named "<suite>_baseline.<host>" */
void PerfSuite::compare() {
  char hname[100] = {0};
  ACE_OS::hostname(hname, 100);
  std::string fname = m_suiteName + "_baseline." + hname;
}

ThreadLauncher::ThreadLauncher(int thrCount, Thread& thr)
    : m_thrCount(thrCount),
      m_initSemaphore((-1 * thrCount) + 1),
      m_startSemaphore(0),
      m_stopSemaphore((-1 * thrCount) + 1),
      m_cleanSemaphore(0),
      m_termSemaphore((-1 * thrCount) + 1),
      m_startTime(0),
      m_stopTime(0),
      m_threadDef(thr) {
  m_threadDef.init(this);
}

void ThreadLauncher::go() {
#ifdef WIN32
  int thrAttrs = THR_NEW_LWP | THR_JOINABLE;
#else
  int thrAttrs = THR_NEW_LWP | THR_JOINABLE | THR_INHERIT_SCHED;
#endif
  int res = m_threadDef.activate(thrAttrs, m_thrCount);
  ASSERT(res == 0, "Failed to start threads properly");
  m_initSemaphore.acquire();
  LOG("[ThreadLauncher] all threads ready.");
  m_startTime = new TimeStamp();
  m_startSemaphore.release(m_thrCount);
  m_stopSemaphore.acquire();
  m_stopTime = new TimeStamp();
  m_cleanSemaphore.release(m_thrCount);
  m_termSemaphore.acquire();
  LOG("[ThreadLauncher] joining threads.");
  m_threadDef.wait();
  LOG("[ThreadLauncher] all threads stopped.");
}

ThreadLauncher::~ThreadLauncher() {
  if (m_startTime != 0) {
    delete m_startTime;
  }
  if (m_stopTime != 0) {
    delete m_stopTime;
  }
}

Thread::Thread() : ACE_Task_Base(), m_launcher(NULL), m_used(false) {}

Thread::~Thread() {}

int Thread::svc() {
  m_used = true;
  int res = 0;
  try {
    setup();  // do per thread setup
  } catch (...) {
    LOG("[Thread] unknown exception thrown in setup().");
    res = 1;
  }
  m_launcher->initSemaphore().release();
  m_launcher->startSemaphore().acquire();
  try {
    perftask();  // do measured iterations
  } catch (...) {
    LOG("[Thread] unknown exception thrown in perftask().");
    res = 2;
  }
  m_launcher->stopSemaphore().release();
  m_launcher->cleanSemaphore().acquire();
  try {
    cleanup();  // cleanup after thread.
  } catch (...) {
    LOG("[Thread] unknown exception thrown in cleanup()");
    res = 3;
  }
  m_launcher->termSemaphore().release();
  return res;
}

Semaphore::Semaphore(int count) : m_mutex(), m_cond(m_mutex), m_count(count) {}

Semaphore::~Semaphore() {}

void Semaphore::acquire(int t) {
  ACE_Guard<ACE_Thread_Mutex> _guard(m_mutex);

  while (m_count < t) {
    m_cond.wait();
  }
  m_count -= t;
}

void Semaphore::release(int t) {
  ACE_Guard<ACE_Thread_Mutex> _guard(m_mutex);

  m_count += t;
  if (m_count > 0) {
    m_cond.broadcast();
  }
}

};  // namespace perf

namespace test {

NOCout cout;
NOCout::FLAGS endl = NOCout::endl;
NOCout::FLAGS flush = NOCout::flush;
NOCout::FLAGS hex = NOCout::hex;
NOCout::FLAGS dec = NOCout::dec;
}

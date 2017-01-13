/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __DUNIT_FW_DUNIT_H__
#define __DUNIT_FW_DUNIT_H__

/***

A Dunit framework provides a structure for executing sequential portions of
test code in different processes. This framework provides C++ tests with
a tools to define tasks and associating them to different processes. It
automatically maintains the sequence order that these tasks are to be
executing in. Process startup and termination is handled by the framework.

Task control:

The framework spawns four child processes. Two processes run with GF_DEFAULT
set to distributed system 1, and the other two run with GF_DEFAULT set to
distributed system 2. The processes correspond with the ids:

  s1p1 and s1p2 for system 1, s2p1 and s2p2 for system 2.

Two macros are used to define blocks of test code, and what process it will
run in.

DUNIT_TASK(x,y) - beginning of block to run in process x, with uniq id y.
END_TASK(y)     - ending of block started by previous DUNIT_TASK call.

Tasks are executed in the order, from top to bottom, that they are found in
the test source file. If any task throws an uncaught exception, the framework
will catch the exception and consider it a test failure. If the exception is
of type dunit::TestException, then it will be logged.
(@TODO: catch and log gemfire::Exception types, and std::exceptions. )

DUNIT_TASK actually begins the definition of a subclass of dunit::Task. And the
task Method doTask. The block of code between DUNIT_TASK and END_TASK is
compiled as the body of doTask. END_TASK closes the class definition, while
declaring an instance to be initialized at module load time. This sets up the
schedule of tasks to execute and the process they are to be executed on.

Logging and handle errors:

The logging and error reporting macros include file and line number information.

ASSERT(x,y) - if condition x is false, throw a TestException with message y.
FAIL(y)     - throw a TestException with message y.
LOG(y)      - Log message y.

Sharing test data between process is done with a NamingContext which is set up
for you automatically. Use the globals() function to get a pointer to it.
With the naming context is like a map associating char* values with char*
keys. Convenience methods are made available that take and return int values.
See class NamingContext in this file.

The the covers, the framework is implemented in the fw_dunit.cpp and
fw_spawn.hpp sources. fw_dunit.cpp is compiled as a seperate object module
without dependence on the product. It is in a seperate module that is linked in
two each test executable. The seperation is designed to help keep the test
framework from being corrupt by the product, and prevent the framework from
mucking with the product in unforseen ways.

---------------------------------------
An example dunit test file:
---------------------------------------

#include <fw_dunit.hpp>

DUNIT_TASK(s1p1,setupp1)
{
  // test code... put ten entries...
  globals()->rebind( "entryCount", 10 );
}
END_TASK(setupp1)

DUNIT_TASK(s2p1,validate)
{
  // check results from distribution...
  int expectedEntries = globals()->getIntValue( "entryCount" );
  // ...
  ASSERT( foundEntries == expectedEntries, "wrong entryCount" );
}
END_TASK(validate)

----------------------------------------

*/

// Default task timeout in seconds
#ifndef TASK_TIMEOUT
#define TASK_TIMEOUT 1800
#endif

#ifdef WINVER
#if WINVER == 0x0500
#undef _WINSOCKAPI_
#include <WinSock2.h>
#endif
#endif

#include <string>

#include <ace/ACE.h>
#include <signal.h>

#define ASSERT(x, y)                                   \
  if (!(x)) {                                          \
    raise(SIGABRT);                                    \
    throw dunit::TestException(y, __LINE__, __FILE__); \
  }
#define XASSERT(x)                                      \
  if (!(x)) {                                           \
    raise(SIGABRT);                                     \
    throw dunit::TestException(#x, __LINE__, __FILE__); \
  }
#define FAIL(y) throw dunit::TestException(y, __LINE__, __FILE__)
#define LOG(y) dunit::log(y, __LINE__, __FILE__)
#define LOGMASTER(y) dunit::logMaster(y, __LINE__, __FILE__)
#define SLEEP(x) dunit::sleep(x)

#define SYMJOIN(x, y) SYMJOIN2(x, y)
#define SYMJOIN2(x, y) x##y

#define DCLASSNAME(y) SYMJOIN(SYMJOIN(Task_, y), __LINE__)
#define DMAKE_STR(y) #y
#define DTASKDESC(x, y) DMAKE_STR(x line : y)

#define DCLASSDEF(y) SYMJOIN(Task_, y)
#define DVARNAME(y) SYMJOIN(SYMJOIN(a_, y), __LINE__)

// Create a test task instance, x is the process tag identifying which process
//   to run in. y is a unique task identifier used in generating a class name.
#define DUNIT_TASK(x, y)                                            \
  class DCLASSNAME(y) : virtual public dunit::Task {                \
   public:                                                          \
    DCLASSNAME(y)() { init(x); }                                    \
                                                                    \
   public:                                                          \
    virtual void doTask() {                                         \
      const char* fwtest_Name ATTR_UNUSED = DTASKDESC(y, __LINE__); \
      try {
// Close the class definition produced by DUNIT_TASK macro.
// y is a unique identifier used to generate an instance variable name.
#define END_TASK(y)                         \
  }                                         \
  catch (gemfire::Exception & ex) {         \
    ex.printStackTrace();                   \
    FAIL(ex.getMessage());                  \
  }                                         \
  catch (std::exception & ex) {             \
    FAIL(ex.what());                        \
  }                                         \
  catch (dunit::TestException&) {           \
    throw;                                  \
  }                                         \
  catch (...) {                             \
    FAIL("Unknown exception type caught."); \
  }                                         \
  }                                         \
  }                                         \
  SYMJOIN(a_, __LINE__);
#define ENDTASK                             \
  }                                         \
  catch (gemfire::Exception & ex) {         \
    ex.printStackTrace();                   \
    FAIL(ex.getMessage());                  \
  }                                         \
  catch (std::exception & ex) {             \
    FAIL(ex.what());                        \
  }                                         \
  catch (dunit::TestException&) {           \
    throw;                                  \
  }                                         \
  catch (...) {                             \
    FAIL("Unknown exception type caught."); \
  }                                         \
  }                                         \
  }                                         \
  SYMJOIN(a_, __LINE__);

#define DUNIT_TASK_DEFINITION(x, y)                                 \
  class DCLASSDEF(y) : virtual public dunit::Task {                 \
   public:                                                          \
    DCLASSDEF(y)() { init(x); }                                     \
                                                                    \
   public:                                                          \
    virtual void doTask() {                                         \
      const char* fwtest_Name ATTR_UNUSED = DTASKDESC(y, __LINE__); \
      try {
#define END_TASK_DEFINITION                 \
  }                                         \
  catch (gemfire::Exception & ex) {         \
    ex.printStackTrace();                   \
    FAIL(ex.getMessage());                  \
  }                                         \
  catch (std::exception & ex) {             \
    FAIL(ex.what());                        \
  }                                         \
  catch (dunit::TestException&) {           \
    throw;                                  \
  }                                         \
  catch (...) {                             \
    FAIL("Unknown exception type caught."); \
  }                                         \
  }                                         \
  }                                         \
  ;
#define CALL_TASK(y) \
  DCLASSDEF(y) * DVARNAME(y) ATTR_UNUSED = new DCLASSDEF(y)();

#define DUNIT_MAIN         \
  class DCLASSNAME(Main) { \
   public:                 \
    DCLASSNAME(Main)() {   \
      try {
#define END_MAIN                            \
  }                                         \
  catch (gemfire::Exception & ex) {         \
    ex.printStackTrace();                   \
    FAIL(ex.getMessage());                  \
  }                                         \
  catch (std::exception & ex) {             \
    FAIL(ex.what());                        \
  }                                         \
  catch (dunit::TestException&) {           \
    throw;                                  \
  }                                         \
  catch (...) {                             \
    FAIL("Unknown exception type caught."); \
  }                                         \
  }                                         \
  }                                         \
  SYMJOIN(a_, __LINE__);

// identifiers for the different processes.
#define s1p1 1
#define s1p2 2
#define s2p1 3
#define s2p2 4

namespace dunit {

void logMaster(std::string s, int lineno, const char* filename);
void log(std::string s, int lineno, const char* filename);
void sleep(int millis);
void HostWaitForDebugger();

// on windows this disables the popups.
void setupCRTOutput();

/** base class for all tasks... define a virtual doTest method to be called
 * when task is to be executed. init method sets up the task in the right
 * process queue for the slaves to execute.
 */
class Task {
 public:
  std::string m_taskName;
  int m_id;

  Task() {}
  virtual ~Task() {}

  /** register task with slave. */
  void init(int sId);

  // defined by block of code in DUNIT_TASK macro usage.
  virtual void doTask() = 0;

  void setTimeout(int seconds = 0);

  std::string typeName();
};

/** Shared naming context for storing ints and strings between processes.
 *  To acquire the naming context, use the globals() function which
 *  returns a pointer for the naming context.
 */
class NamingContext {
 public:
  /**
   * Share a string value, return -1 if there is a failure to store value,
   * otherwise returns 0.
   */
  virtual int rebind(const char* key, const char* value) = 0;

  /**
   * Share an int value, return -1 if there is a failure to store value,
   * otherwise returns 0.
   */
  virtual int rebind(const char* key, int value) = 0;

  /**
   * retreive a value by key, storing the result in the users buf. If the key
   * is not found, the buf will contain the empty string "". Make sure the
   * buffer is big enough to hold whatever has have bound.
   */
  virtual void getValue(const char* key, char* buf) = 0;

  /**
   * return the value by key, as an int using the string to int conversion
   * rules of atoi.
   */
  virtual int getIntValue(const char* key) = 0;

  /** dump the entire context in LOG messages. */
  virtual void dump() = 0;
};

extern "C" {

NamingContext* globals();
}

/**
 * Exception type to use when test framework has trouble or if ASSERT and FAIL
 * are triggered. Use the macros ASSERT(cond,message) and FAIL(message) to
 * throw, use TestException.print() to dump the issue to stderr. If not caught
 * by your test code, the framework will catch it and assume the test block
 * failed.
 */
class TestException {
 public:
  TestException(const char* msg, int lineno, const char* filename)
      : m_message((char*)msg), m_lineno(lineno), m_filename((char*)filename) {}

  void print() {
    fprintf(stdout, "#### TestException: %s in %s at line %d\n",
            m_message.c_str(), m_filename, m_lineno);
    fflush(stdout);
  }
  std::string m_message;
  int m_lineno;
  char* m_filename;
};

int dmain(int argc, char* argv[]);

};  // end namespace dunit.

#ifndef __DUNIT_NO_MAIN__

int ACE_TMAIN(int argc, ACE_TCHAR* argv[]) { return dunit::dmain(argc, argv); }

#endif  // __DUNIT_NO_MAIN__

#include "fw_perf.hpp"

#include "no_cout.hpp"

#endif  // __DUNIT_FW_DUNIT_H__

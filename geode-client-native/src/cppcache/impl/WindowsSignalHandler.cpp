/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifdef _WIN32
#include <ace/OS.h>
#include "../SignalHandler.hpp"
#include "WindowsSignalHandler.hpp"


//void * SignalHandler::s_pOldHandler = NULL;

extern "C"
{
  void backtraceHandler(int sig)
  {
    // do nothing... don't handle SEH exceptions this way... only for posix.
  }

  void removeBtHandler()
  {
    SetUnhandledExceptionFilter(NULL);
  }
}
namespace gemfire {
  void createMiniDump(unsigned int u, EXCEPTION_POINTERS* pExp, char* dumpFile, size_t maxLen)
  {
    dumpFile[0] = '\0';
    // firstly see if dbghelp.dll is around and has the function we need
    // look next to gfcppcache.dll first, as the one in System32 might be old
    // (e.g. Windows 2000)
    HMODULE hDll = NULL;
    std::string libDir = CppCacheLibrary::getProductLibDir();

    if (libDir.size() > 0) {
      libDir += "\\DBGHELP.DLL";
      hDll = LoadLibrary(libDir.c_str());
    }

    if (hDll == NULL) {
      // load any version we can
      hDll = LoadLibrary("DBGHELP.DLL");
    }

    MINIDUMP_WRITE_DUMP MiniDumpWriteDump_ = (MINIDUMP_WRITE_DUMP)GetProcAddress(
      hDll, "MiniDumpWriteDump");
    if (MiniDumpWriteDump_) {
      MINIDUMP_EXCEPTION_INFORMATION mExInfo;
      HANDLE hDump_File;

      mExInfo.ThreadId = GetCurrentThreadId();
      mExInfo.ExceptionPointers = pExp;
      mExInfo.ClientPointers = 0;

      const char* dumpLocation = SignalHandler::getCrashDumpLocation();
      int dumpFileEnd;
      if (dumpLocation != NULL && dumpLocation[0] != '\0') {
        dumpFileEnd = ACE_OS::snprintf(dumpFile, maxLen, "%s/%s-%d-%d.dmp",
          dumpLocation, SignalHandler::getCrashDumpPrefix(),
          time(NULL), ACE_OS::getpid());
      }
      else {
        ACE_TCHAR cwd[_MAX_PATH];
        (void)ACE_OS::getcwd(cwd, _MAX_PATH - 1);
        dumpFileEnd = ACE_OS::snprintf(dumpFile, maxLen, "%s/%s-%d-%d.dmp",
          cwd, SignalHandler::getCrashDumpPrefix(),
          time(NULL), ACE_OS::getpid());
      }
      LOGERROR("Generating debug dump in file %s", dumpFile);

      hDump_File = CreateFile(dumpFile, GENERIC_WRITE, 0,
        NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);

      if (hDump_File != INVALID_HANDLE_VALUE) {
        MiniDumpWriteDump_(GetCurrentProcess(), ACE_OS::getpid(),
          hDump_File, MiniDumpWithDataSegs, (pExp ? &mExInfo : NULL), NULL, NULL);

        CloseHandle(hDump_File);
      }
      else {
        LOGERROR("Failed to open file for debug dump: %s", dumpFile);
        ACE_OS::snprintf(dumpFile + dumpFileEnd, maxLen - dumpFileEnd,
          " (failed to open file)");
      }
    }
  }
LONG handleDebugEvent(LPEXCEPTION_POINTERS lpEP)
{

  PVOID exceptionAddress = lpEP->ExceptionRecord->ExceptionAddress;
  DWORD exceptionCode    = lpEP->ExceptionRecord->ExceptionCode;
  char sStatus[100];

  switch (exceptionCode)
  {
    case STATUS_WAIT_0:
      ACE_OS::strncpy(sStatus,"STATUS_WAIT_0", sizeof(sStatus));
      break;
    case STATUS_ABANDONED_WAIT_0:
      ACE_OS::strncpy(sStatus,"STATUS_ABANDONED_WAIT_0", sizeof(sStatus));
      break;
    case STATUS_USER_APC:
      ACE_OS::strncpy(sStatus,"STATUS_USER_APC", sizeof(sStatus));
      break;
    case STATUS_TIMEOUT:
      ACE_OS::strncpy(sStatus,"STATUS_TIMEOUT", sizeof(sStatus));
      break;
    case STATUS_PENDING:
      ACE_OS::strncpy(sStatus,"STATUS_PENDING", sizeof(sStatus));
      break;
    case STATUS_GUARD_PAGE_VIOLATION:
      ACE_OS::strncpy(sStatus,"STATUS_GUARD_PAGE_VIOLATION", sizeof(sStatus));
      break;
    case STATUS_DATATYPE_MISALIGNMENT:
      ACE_OS::strncpy(sStatus,"STATUS_DATATYPE_MISALIGNMENT", sizeof(sStatus));
      break;
    case STATUS_ACCESS_VIOLATION:
      ACE_OS::strncpy(sStatus,"STATUS_ACCESS_VIOLATION", sizeof(sStatus));
      break;
    case STATUS_IN_PAGE_ERROR:
      ACE_OS::strncpy(sStatus,"STATUS_IN_PAGE_ERROR", sizeof(sStatus));
      break;
    case STATUS_NO_MEMORY:
      ACE_OS::strncpy(sStatus,"STATUS_NO_MEMORY", sizeof(sStatus));
      break;
    case STATUS_ILLEGAL_INSTRUCTION:
      ACE_OS::strncpy(sStatus,"STATUS_ILLEGAL_INSTRUCTION", sizeof(sStatus));
      break;
    case STATUS_NONCONTINUABLE_EXCEPTION:
      ACE_OS::strncpy(sStatus,"STATUS_NONCONTINUABLE_EXCEPTION", sizeof(sStatus));
      break;
    case STATUS_INVALID_DISPOSITION:
      ACE_OS::strncpy(sStatus,"STATUS_INVALID_DISPOSITION", sizeof(sStatus));
      break;
    case STATUS_ARRAY_BOUNDS_EXCEEDED:
      ACE_OS::strncpy(sStatus,"STATUS_ARRAY_BOUNDS_EXCEEDED", sizeof(sStatus));
      break;
    case STATUS_FLOAT_DENORMAL_OPERAND:
      ACE_OS::strncpy(sStatus,"STATUS_FLOAT_DENORMAL_OPERAND", sizeof(sStatus));
      break;
    case STATUS_FLOAT_DIVIDE_BY_ZERO:
      ACE_OS::strncpy(sStatus,"STATUS_FLOAT_DIVIDE_BY_ZERO", sizeof(sStatus));
      break;
    case STATUS_FLOAT_INEXACT_RESULT:
      ACE_OS::strncpy(sStatus,"STATUS_FLOAT_INEXACT_RESULT", sizeof(sStatus));
      break;
    case STATUS_FLOAT_INVALID_OPERATION:
      ACE_OS::strncpy(sStatus,"STATUS_FLOAT_INVALID_OPERATION", sizeof(sStatus));
      break;
    case STATUS_FLOAT_OVERFLOW:
      ACE_OS::strncpy(sStatus,"STATUS_FLOAT_OVERFLOW", sizeof(sStatus));
      break;
    case STATUS_FLOAT_STACK_CHECK:
      ACE_OS::strncpy(sStatus,"STATUS_FLOAT_STACK_CHECK", sizeof(sStatus));
      break;
    case STATUS_FLOAT_UNDERFLOW:
      ACE_OS::strncpy(sStatus,"STATUS_FLOAT_UNDERFLOW", sizeof(sStatus));
      break;
    case STATUS_INTEGER_DIVIDE_BY_ZERO:
      ACE_OS::strncpy(sStatus,"STATUS_INTEGER_DIVIDE_BY_ZERO", sizeof(sStatus));
      break;
    case STATUS_INTEGER_OVERFLOW:
      ACE_OS::strncpy(sStatus,"STATUS_INTEGER_OVERFLOW", sizeof(sStatus));
      break;
    case STATUS_PRIVILEGED_INSTRUCTION:
      ACE_OS::strncpy(sStatus,"STATUS_PRIVILEGED_INSTRUCTION", sizeof(sStatus));
      break;
    case STATUS_STACK_OVERFLOW:
      ACE_OS::strncpy(sStatus,"STATUS_STACK_OVERFLOW", sizeof(sStatus));
      break;
    case STATUS_CONTROL_C_EXIT:
      ACE_OS::strncpy(sStatus,"STATUS_CONTROL_C_EXIT", sizeof(sStatus));
      break;
    case STATUS_SEGMENT_NOTIFICATION:
    case STATUS_BREAKPOINT:
    case STATUS_SINGLE_STEP:
      return DBG_EXCEPTION_NOT_HANDLED;
    default:
      ACE_OS::strncpy(sStatus,"STATUS_UNKNOWN(C++ Exception?)", sizeof(sStatus));
      break;
  }

  int pid = ACE_OS::getpid();
  LOGERROR("Debug event %s occurred at %p in process "
      "with pid %d\n", sStatus, exceptionAddress, pid);

  StackTrace st;
  st.print();

  char dumpFile[_MAX_PATH];
  createMiniDump(exceptionCode, lpEP, dumpFile, _MAX_PATH - 1);

#ifdef DEBUG
  return 0;
#else
  if (SignalHandler::s_pOldHandler != NULL) {
    LPTOP_LEVEL_EXCEPTION_FILTER followup = (LPTOP_LEVEL_EXCEPTION_FILTER) SignalHandler::s_pOldHandler;
    return followup(lpEP);
  }
  std::string exMsg = "Internal exception caught due to ";
  exMsg += sStatus;
  exMsg += ". A crash dump has been created in ";
  exMsg += dumpFile;

  throw gemfire::FatalInternalException(exMsg.c_str());
  //exit( 1 );
  //return 1; // no popup...
#endif
}
void SignalHandler::dumpStack(char* dumpFile, size_t maxLen)
{
  createMiniDump(0, NULL, dumpFile, maxLen);
}

void SignalHandler::dumpStack(unsigned int expCode, EXCEPTION_POINTERS* pExp,
    char* dumpFile, size_t maxLen)
{
  createMiniDump(expCode, pExp, dumpFile, maxLen);
}
void SignalHandler::installBacktraceHandler()
{
  std::string waitSeconds = Utils::getEnv( "GF_DEBUG_WAIT" );
  if ( ! waitSeconds.empty( ) ) {
    int waitS = atoi( waitSeconds.c_str() );
    if ( waitS > 0 ) s_waitSeconds = waitS;
  }

  SignalHandler::s_pOldHandler = (void*) SetUnhandledExceptionFilter(
    (LPTOP_LEVEL_EXCEPTION_FILTER)handleDebugEvent);
}

void SignalHandler::removeBacktraceHandler()
{
  ::removeBtHandler();
}

}
#endif


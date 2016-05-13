/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <eh.h>
#include <exception>

#include <windows.h>
#include <dbghelp.h>
#include <tchar.h>
#include <time.h>

#include "StackTrace.hpp"
#include "CppCacheLibrary.hpp"
#include "Utils.hpp"
#include "../ExceptionTypes.hpp"





namespace gemfire {


typedef BOOL (WINAPI *MINIDUMP_WRITE_DUMP)(
  HANDLE hProcess,
  DWORD dwPid,
  HANDLE hFile,
  MINIDUMP_TYPE DumpType,
  CONST PMINIDUMP_EXCEPTION_INFORMATION ExceptionParam,
  CONST PMINIDUMP_USER_STREAM_INFORMATION UserStreamParam,
  CONST PMINIDUMP_CALLBACK_INFORMATION CallbackParam
);
LONG handleDebugEvent(LPEXCEPTION_POINTERS lpEP);



}

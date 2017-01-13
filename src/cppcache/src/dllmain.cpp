#define DLL_MAIN_C

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "CppCacheLibrary.hpp"
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include "Utils.hpp"
#include <gfcpp/Exception.hpp>
#include <ace/TSS_T.h>

void initLibDllEntry(void);

extern "C" {

static bool initgflib() {
  initLibDllEntry();
  return true;
}

static bool initgflibDone = initgflib();

#ifdef _WIN32
#include <windows.h>

BOOL APIENTRY DllMain(HMODULE hModule, DWORD ul_reason_for_call,
                      LPVOID lpReserved) {
  switch (ul_reason_for_call) {
    case DLL_THREAD_DETACH: {
      break;
    }
    case DLL_PROCESS_DETACH: {
      break;
    }
    // Do not do *anything* in attach phase unless absolutely required
    // and absolutely sure that it follows all restrictions; this has
    // caused much grief with AccessViolations in the past.
    case DLL_PROCESS_ATTACH:
    case DLL_THREAD_ATTACH: {
      break;
    }
  }
  return TRUE;
}

LIBEXP void DllMainGetPath(char *result, int maxLen) {
  if (!initgflibDone) {
    result[0] = '\0';
    return;
  }
  HMODULE libgfcppcache_module = GetModuleHandle("gfcppcache.dll");
  if (libgfcppcache_module == 0) {
    libgfcppcache_module = GetModuleHandle("gfcppcache_g.dll");
    if (libgfcppcache_module == 0) {
      libgfcppcache_module = GetModuleHandle("Gemstone.Gemfire.Cache.dll");
    }
  }
  if (libgfcppcache_module != 0) {
    GetModuleFileName(libgfcppcache_module, result, maxLen);
  } else {
    result[0] = '\0';
  }
}

#else
#include <dlfcn.h>

void DllMainGetPath(char *result, int maxLen) {
  if (!initgflibDone) {
    result[0] = '\0';
    return;
  }
  Dl_info dlInfo;
  dladdr((void *)DllMainGetPath, &dlInfo);
  if (realpath(dlInfo.dli_fname, result) == NULL) {
    result[0] = '\0';
  }
}

#endif /* WIN32 */

} /* extern "C" */

#ifdef _WIN32

namespace gemfire {
void CPPCACHE_EXPORT setNewAndDelete(pNew pn, pDelete pd) {
  Utils::s_pNew = pn;
  Utils::s_pDelete = pd;
  Utils::s_setNewAndDelete = true;
}

void setDefaultNewAndDelete() {
  Utils::s_pNew = (pNew)&malloc;      // operator new;
  Utils::s_pDelete = (pDelete)&free;  // operator delete;
}

/*
void setDefaultNewAndDelete_disabled()
{
  HMODULE hModule = GetModuleHandle("msvcrtd");

  if (!hModule) hModule = GetModuleHandle("msvcrt");

  if (hModule)
  {
    // 32-bit versions
    Utils::s_pNew = (pNew) GetProcAddress(hModule, "??2@YAPAXI@Z");
    Utils::s_pDelete = (pDelete) GetProcAddress(hModule, "??3@YAXPAX@Z");

    if (Utils::s_pNew && Utils::s_pDelete) return;

    // 64-bit versions
    Utils::s_pNew = (pNew) GetProcAddress(hModule, "??2@YAPEAX_K@Z");
    Utils::s_pDelete = (pDelete) GetProcAddress(hModule, "??3@YAXPEAX@Z");
  }

  if (!Utils::s_pNew || !Utils::s_pDelete)
  {
    throw IllegalStateException("Could not set default new and delete
operators.");
  }
}
*/

}  // namespace gemfire

#endif  // _WIN32

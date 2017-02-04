/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "CppCacheLibrary.hpp"

#include <ace/OS.h>
#include <ace/ACE.h>
#include <ace/Init_ACE.h>
#include <ace/Log_Msg.h>
#include <ace/Singleton.h>

//#include <StatisticsFactory.hpp>
#include "MapEntry.hpp"
#include "ExpMapEntry.hpp"
#include "LRUMapEntry.hpp"
#include "LRUExpMapEntry.hpp"
#include <gfcpp/CacheFactory.hpp>
#include "SerializationRegistry.hpp"
#include "CacheableToken.hpp"
#include <gfcpp/DataOutput.hpp>
#include "TcrMessage.hpp"
#include "Utils.hpp"
#include "PdxTypeRegistry.hpp"

#include <string>

using namespace apache::geode::client;

namespace apache {
namespace geode {
namespace client {
void gf_log_libinit();
}  // namespace client
}  // namespace geode
}  // namespace apache

CppCacheLibrary::CppCacheLibrary() {
  // TODO: This should catch any exceptions, log it, and bail out..
  // Put initialization code for statics and other such things here.
  try {
    gf_log_libinit();
    EntryFactory::init();
    LRUEntryFactory::init();
    ExpEntryFactory::init();
    LRUExpEntryFactory::init();
    CacheFactory::init();
    CacheableToken::init();
    SerializationRegistry::init();
    // PdxTypeRegistry::init();
    // log( "Finished initializing CppCacheLibrary." );
  } catch (apache::geode::client::Exception& ge) {
    ge.printStackTrace();
    throw;
  }
}

CppCacheLibrary::~CppCacheLibrary() {
  // Put any global clean up code here.
  CacheFactory::cleanup();
  //  PdxTypeRegistry::cleanup();

  ACE::fini();
}

//---------------------------------------------------------------------

typedef ACE_Singleton<CppCacheLibrary, ACE_Recursive_Thread_Mutex> TheLibrary;

// expect this to be called from key Library entry points, or automatically
// if we can... Probably safest to call from DistributedSystem factory method.
// impl type Unit tests may need to call this themselves to ensure the
// internals are prepared. fw_helper framework will handle this.
CppCacheLibrary* CppCacheLibrary::initLib(void) {
  ACE::init();
  return TheLibrary::instance();
}

// this closes ace and triggers the cleanup of the singleton CppCacheLibrary.
void CppCacheLibrary::closeLib(void) {
  // ACE::fini(); This should not happen..... Things might be using ace beyond
  // the life of
  // using geode.
}

// called during DLL initialization
void initLibDllEntry(void) { CppCacheLibrary::initLib(); }

extern "C" {
void DllMainGetPath(char* result, int maxLen);
}

// Returns pathname of product's lib directory, adds 'addon' to it if 'addon' is
// not null.
std::string CppCacheLibrary::getProductLibDir(const char* addon) {
  std::string proddir = CppCacheLibrary::getProductDir();
  proddir += "/lib/";
  if (addon != NULL) {
    proddir += addon;
  }
  return proddir;
}

// return the directory where the library/DLL resides
std::string CppCacheLibrary::getProductLibDir() {
  // otherwise... get the DLL path, and work backwards from it.
  char path[PATH_MAX + 1];
  char* dllNamePtr;

  path[0] = '\0';
  DllMainGetPath(path, PATH_MAX);
#ifdef WIN32
  // windows paths are case insensitive
  for (int i = 0; i < PATH_MAX && path[i] != 0; i++) {
    path[i] = ::tolower(path[i]);
  }
  dllNamePtr = strstr(path, "apache-geode.dll");
  if (dllNamePtr == NULL) {
    dllNamePtr = strstr(path, "apache-geode_g.dll");
    if (dllNamePtr == NULL) {
      dllNamePtr = strstr(path, "Apache.Geode.dll");
    }
  }
#else
  dllNamePtr = strstr(path, "libapache-geode");
#endif
  std::string libDir;
  if (dllNamePtr != NULL) {
    dllNamePtr--;
    // null terminate at the directory containing the library.
    *dllNamePtr = '\0';
    libDir = path;
  }
  return libDir;
}

std::string CppCacheLibrary::getProductDir() {
  // If the environment variable is set, use it.
  // char * gfcppenv = ACE_OS::getenv( "GFCPP" );
  std::string gfcppenv = Utils::getEnv("GFCPP");
  if (gfcppenv.length() > 0) {
    return gfcppenv;
  }

  // otherwise... get the DLL path, and work backwards from it.
  std::string libdirname = getProductLibDir();
  if (libdirname.size() == 0) {
    fprintf(stderr,
            "Cannot determine location of product directory.\n"
            "Please set GFCPP environment variable.\n");
    fflush(stderr);
    throw apache::geode::client::IllegalStateException(
        "Product installation directory "
        "not found. Please set GFCPP environment variable.");
  }
  // replace all '\' with '/' to make everything easier..
  size_t len = libdirname.length() + 1;
  char* slashtmp = new char[len];
  ACE_OS::strncpy(slashtmp, libdirname.c_str(), len);
  for (size_t i = 0; i < libdirname.length(); i++) {
    if (slashtmp[i] == '\\') {
      slashtmp[i] = '/';
    }
  }
  libdirname = slashtmp;
  delete[] slashtmp;
  slashtmp = NULL;

  // check if it is "hidden/lib/debug" and work back from build area.
  size_t hiddenidx = libdirname.find("hidden");
  if (hiddenidx != std::string::npos) {
    // make sure hidden was a whole word...
    hiddenidx--;
    if (libdirname[hiddenidx] == '/' || libdirname[hiddenidx] == '\\') {
      // odds are high hiddenidx terminates osbuild.dir.
      std::string hiddenroute = libdirname.substr(0, hiddenidx) + "/product";
      return hiddenroute;
    }
  }
  // check if bin on windows, and go back one...
  GF_D_ASSERT(libdirname.length() > 4);
#ifdef WIN32
  std::string libpart = "bin";
#else
  std::string libpart = "lib";
#endif
  if (libdirname.substr(libdirname.length() - 3) == libpart) {
    return libdirname.substr(0, libdirname.length() - 4);
  } else {
    return libdirname;
  }
  /*  Bug 728 fix - don't fail here, rather assume user installs product
  manually in place.
  fprintf(stderr, "Cannot determine location of product directory.\n"
      "Please set GFCPP environment variable.\n");
  fflush(stderr);
  throw apache::geode::client::IllegalStateException("Product installation
  directory "
      "not found. Please set GFCPP environment variable.");
      */
}

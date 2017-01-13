/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_CPPCACHELIBRARY_H__
#define __GEMFIRE_IMPL_CPPCACHELIBRARY_H__

#include <gfcpp/gfcpp_globals.hpp>
#include <string>

namespace gemfire {

// initialize GEMFIRE runtime if it has not already been initialized.
class CPPCACHE_EXPORT CppCacheLibrary {
 public:
  // All real initialization happens here.
  CppCacheLibrary();
  // All cleanup goes here.
  virtual ~CppCacheLibrary();

  // Call to this to trigger initialization.
  static CppCacheLibrary* initLib(void);
  // Call to this to trigger cleanup.  initLib and closeLib calls must be in
  // pairs.
  static void closeLib(void);

  // Returns pathname of product's lib directory, adds 'addon' to it if 'addon'
  // is not null.
  static std::string getProductLibDir(const char* addon);

  // Returns the directory where the library/DLL resides
  static std::string getProductLibDir();

  static std::string getProductDir();
};

}; /* namespace gemfire */

#endif  // __GEMFIRE_IMPL_CPPCACHELIBRARY_H__

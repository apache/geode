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

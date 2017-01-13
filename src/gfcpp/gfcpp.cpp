/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define _GF_INCLUDE_ENCRYPT 1

#include <gfcpp/gfcpp_globals.hpp>

#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/CacheFactory.hpp>

using namespace gemfire;

void doVersion() { printf("\n%s\n", CacheFactory::getProductDescription()); }

int main(int argc, char** argv) {
  Log::init(Log::Error, NULL);
  try {
    if (argc > 1) {
      std::string command = argv[1];
      if (command == "version") {
        doVersion();
      }
    }
  } catch (const Exception& ex) {
    ex.printStackTrace();
    fflush(stdout);
    return 1;
  }
  fflush(stdout);
  return 0;
}

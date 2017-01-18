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

#define _GF_INCLUDE_ENCRYPT 1

#include <gfcpp/gfcpp_globals.hpp>

#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/CacheFactory.hpp>

using namespace apache::geode::client;

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

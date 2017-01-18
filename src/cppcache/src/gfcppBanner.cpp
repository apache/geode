#include "gfcppBanner.hpp"
using namespace apache::geode::client;
std::string gfcppBanner::getBanner() {
  std::string str =
      "------------------------------------------------------------------------"
      "---\n"
      "Licensed to the Apache Software Foundation (ASF) under one or more\n"
      "contributor license agreements.  See the NOTICE file distributed with\n"
      "this work for additional information regarding copyright ownership.\n"
      "The ASF licenses this file to You under the Apache License, Version "
      "2.0\n"
      "(the \"License\"); you may not use this file except in compliance with\n"
      "the License.  You may obtain a copy of the License at\n"
      "\n"
      "     http://www.apache.org/licenses/LICENSE-2.0\n"
      "\n"
      "Unless required by applicable law or agreed to in writing, software\n"
      "distributed under the License is distributed on an \"AS IS\" BASIS,\n"
      "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or "
      "implied.\n"
      "See the License for the specific language governing permissions and\n"
      "limitations under the License.\n"
      "------------------------------------------------------------------------"
      "---\n";
  return str;
}

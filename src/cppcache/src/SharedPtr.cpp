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
#include <gfcpp/SharedPtr.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <Utils.hpp>

#include <string>

using namespace apache::geode::client;

void SPEHelper::throwNullPointerException(const char* ptrtype) {
  throw NullPointerException(Utils::demangleTypeName(ptrtype)->asChar(), NULL,
                             true);
}

void SPEHelper::throwClassCastException(const char* msg, const char* fromType,
                                        const char* toType) {
  std::string exMsg(msg);
  exMsg +=
      ((std::string) " from '" + Utils::demangleTypeName(fromType)->asChar() +
       "' to '" + Utils::demangleTypeName(toType)->asChar() + "'.");
  throw ClassCastException(exMsg.c_str());
}

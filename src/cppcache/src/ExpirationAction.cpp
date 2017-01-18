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

#include <gfcpp/ExpirationAction.hpp>

#include <string.h>
#include <ace/OS.h>

using namespace apache::geode::client;

char* ExpirationAction::names[] = {(char*)"INVALIDATE",
                                   (char*)"LOCAL_INVALIDATE", (char*)"DESTROY",
                                   (char*)"LOCAL_DESTROY", (char*)NULL};

ExpirationAction::Action ExpirationAction::fromName(const char* name) {
  uint32_t i = 0;
  while ((names[i] != NULL) || (i <= static_cast<uint32_t>(LOCAL_DESTROY))) {
    if (name && names[i] && ACE_OS::strcasecmp(names[i], name) == 0) {
      return static_cast<Action>(i);
    }
    ++i;
  }
  return INVALID_ACTION;
}

const char* ExpirationAction::fromOrdinal(const int ordinal) {
  if (INVALIDATE <= ordinal && ordinal <= LOCAL_DESTROY) {
    return names[ordinal];
  }
  return NULL;
}

ExpirationAction::ExpirationAction() {}

ExpirationAction::~ExpirationAction() {}

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/ExpirationAction.hpp>

#include <string.h>
#include <ace/OS.h>

using namespace gemfire;

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

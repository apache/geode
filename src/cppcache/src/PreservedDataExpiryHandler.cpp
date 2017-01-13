/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PreservedDataExpiryHandler.cpp
 *
 *  Created on: Apr 5, 2012
 *      Author: npatel
 */
#include "ace/Timer_Queue.h"
#include "ace/Timer_Heap.h"
#include "ace/Reactor.h"
#include "ace/svc_export.h"
#include "ace/Timer_Heap_T.h"
#include "ace/Timer_Queue_Adapters.h"

#include "PreservedDataExpiryHandler.hpp"
#include "PdxTypeRegistry.hpp"

using namespace gemfire;

PreservedDataExpiryHandler::PreservedDataExpiryHandler(
    PdxSerializablePtr pdxObjectPtr, uint32_t duration)
    :  // UNUSED m_duration(duration),
      m_pdxObjectPtr(pdxObjectPtr) {}

int PreservedDataExpiryHandler::handle_timeout(
    const ACE_Time_Value& current_time, const void* arg) {
  WriteGuard guard(PdxTypeRegistry::getPreservedDataLock());
  LOGDEBUG(
      "Entered PreservedDataExpiryHandler "
      "PdxTypeRegistry::getPreserveDataMap().size() = %d",
      PdxTypeRegistry::getPreserveDataMap().size());

  try {
    // remove the entry from the map
    if (PdxTypeRegistry::getPreserveDataMap().contains(m_pdxObjectPtr)) {
      PdxTypeRegistry::getPreserveDataMap().erase(m_pdxObjectPtr);
      LOGDEBUG(
          "PreservedDataExpiry:: preserveData erased entry from map updated "
          "size = %d",
          PdxTypeRegistry::getPreserveDataMap().size());
    } else {
      LOGDEBUG("PreservedDataExpiry:: preserveData does not contains Entry");
    }

  } catch (...) {
    // Ignore whatever exception comes
    LOGDEBUG(
        "PreservedDataExpiry:: Error while Clearing PdxObject and its "
        "preserved data. Ignoring the error");
  }
  return 0;
}

int PreservedDataExpiryHandler::handle_close(ACE_HANDLE, ACE_Reactor_Mask) {
  return 0;
}

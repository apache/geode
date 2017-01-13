/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <ace/File_Lock.h>
#include <ace/Process.h>
#include <ace/SPIPE_Addr.h>

namespace gemfire {

class CPPCACHE_EXPORT ACEDummy {
 public:
  static void useUnusedAceFeatures();
};

void ACEDummy::useUnusedAceFeatures() {
  ACE_File_Lock fLock("/BadFileName", 0);
  ACE_Process proc;
  ACE_SPIPE_Addr addr;
}
}  // namespace gemfire

#ifndef _GEMFIRE_STATISTICS_HOSTSTATHELPERLINUX_HPP_
#define _GEMFIRE_STATISTICS_HOSTSTATHELPERLINUX_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#if defined(_LINUX)
#include <gfcpp/gfcpp_globals.hpp>
#include <string>
#include <sys/sysinfo.h>
#include "ProcessStats.hpp"

/** @file
*/

namespace gemfire_statistics {

/**
 * Linux Implementation to fetch operating system stats.
 *
 */

class HostStatHelperLinux {
 public:
  static void refreshProcess(ProcessStats* processStats);
  // static refreeshSystem(Statistics* stats);

 private:
  static uint8_t m_logStatErrorCountDown;
};
};

#endif  // if def(_LINUX)

#endif  // _GEMFIRE_STATISTICS_HOSTSTATHELPERLINUX_HPP_

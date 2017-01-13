/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <ace/Thread_Mutex.h>
#include <ace/Singleton.h>
#include "NullProcessStats.hpp"
#include "GemfireStatisticsFactory.hpp"
#include "HostStatHelperNull.hpp"
using namespace gemfire_statistics;

/**
 * <P>This class provides the interface for statistics about a
 * Null operating system process that is using a GemFire system.
 *
 */

NullProcessStats::NullProcessStats(int64 pid, const char* name) {}

int64 NullProcessStats::getProcessSize() { return 0; }

int32 NullProcessStats::getCpuUsage() { return 0; }
int64 NullProcessStats::getCPUTime() { return 0; }
int32 NullProcessStats::getNumThreads() { return 0; }
int64 NullProcessStats::getAllCpuTime() { return 0; }

void NullProcessStats::close() {}

NullProcessStats::~NullProcessStats() {}

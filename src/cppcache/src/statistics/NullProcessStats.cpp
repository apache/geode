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
#include <gfcpp/gfcpp_globals.hpp>
#include <ace/Thread_Mutex.h>
#include <ace/Singleton.h>
#include "NullProcessStats.hpp"
#include "GeodeStatisticsFactory.hpp"
#include "HostStatHelperNull.hpp"
using namespace apache::geode::statistics;

/**
 * <P>This class provides the interface for statistics about a
 * Null operating system process that is using a Geode system.
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

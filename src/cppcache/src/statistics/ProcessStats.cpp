/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ProcessStats.hpp"
using namespace gemfire_statistics;

/**
 * Creates a new <code>ProcessStats</code> that wraps the given
 * <code>Statistics</code>.
 */
ProcessStats::ProcessStats() {}

int64 ProcessStats::getProcessSize() { return 0; }

ProcessStats::~ProcessStats() {}

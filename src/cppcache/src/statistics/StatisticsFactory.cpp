/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "GemfireStatisticsFactory.hpp"

namespace gemfire_statistics {

StatisticsFactory* StatisticsFactory::getExistingInstance() {
  return GemfireStatisticsFactory::getExistingInstance();
}
}

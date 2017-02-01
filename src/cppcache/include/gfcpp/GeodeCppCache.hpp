#pragma once

#ifndef GEODE_GFCPP_GEODECPPCACHE_H_
#define GEODE_GFCPP_GEODECPPCACHE_H_

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

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "Cacheable.hpp"
#include "CacheableKey.hpp"
#include "CacheableBuiltins.hpp"
#include "CacheableDate.hpp"
#include "CacheableFileName.hpp"
#include "CacheableObjectArray.hpp"
#include "CacheableString.hpp"
#include "CacheableUndefined.hpp"
#include "CacheFactory.hpp"
#include "Cache.hpp"
#include "GemFireCache.hpp"
#include "CacheAttributes.hpp"
#include "CacheStatistics.hpp"
#include "CqAttributesFactory.hpp"
#include "CqAttributes.hpp"
#include "CqListener.hpp"
#include "CqQuery.hpp"
#include "CqServiceStatistics.hpp"
#include "DataInput.hpp"
#include "DataOutput.hpp"
#include "Delta.hpp"
#include "DistributedSystem.hpp"
#include "EntryEvent.hpp"
#include "Exception.hpp"
#include "ExceptionTypes.hpp"
#include "Execution.hpp"
#include "ExpirationAction.hpp"
#include "ExpirationAttributes.hpp"
#include "FunctionService.hpp"
#include "HashMapT.hpp"
#include "HashSetT.hpp"
#include "Query.hpp"
#include "QueryService.hpp"
#include "RegionEvent.hpp"
#include "Region.hpp"
#include "Pool.hpp"
#include "PoolManager.hpp"
#include "PoolFactory.hpp"
#include "RegionService.hpp"
#include "ResultCollector.hpp"
#include "ResultSet.hpp"
#include "Serializable.hpp"
#include "SharedPtr.hpp"
#include "StructSet.hpp"
#include "UserData.hpp"
#include "VectorT.hpp"
#include "TransactionId.hpp"
#include "UserFunctionExecutionException.hpp"
#include "PdxInstanceFactory.hpp"
#include "PdxInstance.hpp"
#include "WritablePdxInstance.hpp"
#include "PdxWrapper.hpp"
#include "PdxSerializer.hpp"
#include "CacheableEnum.hpp"
#include "CqStatusListener.hpp"
#include "PdxFieldTypes.hpp"

#include "GeodeCppCache.inl"

#endif // GEODE_GFCPP_GEODECPPCACHE_H_

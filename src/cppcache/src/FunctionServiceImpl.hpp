#pragma once

#ifndef GEODE_FUNCTIONSERVICEIMPL_H_
#define GEODE_FUNCTIONSERVICEIMPL_H_

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
#include <gfcpp/gf_types.hpp>
#include "ProxyCache.hpp"
#include <gfcpp/FunctionService.hpp>
/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {
/**
 * @class FunctionService FunctionService.hpp
 * entry point for function execution
 * @see Execution
 */

class CPPCACHE_EXPORT FunctionServiceImpl : public FunctionService {
 public:
  /**
   * This function is used in multiuser mode to execute function on server.
   */
  // virtual ExecutionPtr onServer();

  /**
   * This function is used in multiuser mode to execute function on server.
   */
  // virtual ExecutionPtr onServers();

  virtual ~FunctionServiceImpl() {}

 private:
  FunctionServiceImpl(const FunctionService &);
  FunctionServiceImpl &operator=(const FunctionService &);

  FunctionServiceImpl(ProxyCachePtr proxyCache);

  static FunctionServicePtr getFunctionService(ProxyCachePtr proxyCache);

  ProxyCachePtr m_proxyCache;
  friend class ProxyCache;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_FUNCTIONSERVICEIMPL_H_

#ifndef __GEMFIRE_FUNCTION_SERVICEIMPL_H__
#define __GEMFIRE_FUNCTION_SERVICEIMPL_H__

/*=========================================================================
  * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
  *
  * The specification of function behaviors is found in the corresponding .cpp
 fil
  e.
  *
  *========================================================================
  */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include "ProxyCache.hpp"
#include <gfcpp/FunctionService.hpp>
/**
 * @file
 */

namespace gemfire {
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
}  // namespace gemfire
#endif  //__GEMFIRE_FUNCTION_SERVICEIMPL_H__

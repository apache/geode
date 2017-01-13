#ifndef __GEMFIRE_FUNCTION_SERVICE_H__
#define __GEMFIRE_FUNCTION_SERVICE_H__

/*=========================================================================
  * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
  *
  * The specification of function behaviors is found in the corresponding
  * .cpp file.
  *
  *========================================================================
  */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "Execution.hpp"

/**
 * @file
 */

// macros to resolve ambiguity between PoolPtr and RegionServicePtr
#define GF_TYPE_IS_POOL(T) \
  gemfire::TypeHelper::SuperSubclass<gemfire::Pool, T>::result
#define GF_TYPE_IS_POOL_TYPE(T) \
  gemfire::TypeHelper::YesNoType<GF_TYPE_IS_POOL(T)>::value

namespace gemfire {
/**
 * @class FunctionService FunctionService.hpp
 * entry point for function execution
 * @see Execution
 */

class CPPCACHE_EXPORT FunctionService : public SharedBase {
 public:
  /**
   * Returns a {@link Execution} object that can be used to execute a data
   * dependent function on the specified Region.<br>
   * When invoked from a GemFire client, the method returns an Execution
   * instance that sends a message to one of the connected servers as specified
   * by the {@link Pool} for the region. Depending on the filters setup on the
   * {@link Execution}, the function is executed on all GemFire members that
   * define the data region, or a subset of members.
   * {@link Execution::withFilter(filter)}).
   *
   * @param region
   * If Pool is multiusersecure mode then one need to pass nstance of Region
   * from RegionService.
   *
   * @return Execution
   * @throws NullPointerException
   *                 if the region passed in is NULLPTR
   */
  static ExecutionPtr onRegion(RegionPtr region);

  /**
   * Returns a {@link Execution} object that can be used to execute a data
   * independent function on a server in the provided {@link Pool}.
   * <p>
   * If the server goes down while dispatching or executing the function, an
   * Exception will be thrown.
   * @param pool from which to chose a server for execution
   * @return Execution
   * @throws NullPointerException
   *                 if Pool instance passed in is NULLPTR
   * @throws UnsupportedOperationException
   *                 if Pool is in multiusersecure Mode
   */
  inline static ExecutionPtr onServer(const PoolPtr& pool) {
    return onServerWithPool(pool);
  }

  /**
   * Returns a {@link Execution} object that can be used to execute a data
   * independent function on a server where Cache is attached.
   * <p>
   * If the server goes down while dispatching or executing the function, an
   * Exception will be thrown.
   * @param cache
   *        cache from which to chose a server for execution
   * @return Execution
   * @throws NullPointerException
   *                 if Pool instance passed in is NULLPTR
   * @throws UnsupportedOperationException
   *                 if Pool is in multiusersecure Mode
   */
  inline static ExecutionPtr onServer(const RegionServicePtr& cache) {
    return onServerWithCache(cache);
  }

  template <typename T>
  static ExecutionPtr onServer(const SharedPtr<T>& poolOrCache) {
    return onServer(poolOrCache, GF_TYPE_IS_POOL_TYPE(T));
  }

  /**
   * Returns a {@link Execution} object that can be used to execute a data
   * independent function on all the servers in the provided {@link Pool}.
   * If one of the servers goes down while dispatching or executing the function
   * on the server, an Exception will be thrown.
   *
   * @param pool the set of servers to execute the function
   * @return Execution
   * @throws NullPointerException
   *                 if Pool instance passed in is NULLPTR
   * @throws UnsupportedOperationException
   *                 if Pool is in multiusersecure Mode
   */
  inline static ExecutionPtr onServers(const PoolPtr& pool) {
    return onServersWithPool(pool);
  }

  /**
  * Returns a {@link Execution} object that can be used to execute a data
  * independent function on all the servers where Cache is attached.
  * If one of the servers goes down while dispatching or executing the function
  * on the server, an Exception will be thrown.
  *
  * @param cache
  *        the {@link Cache} where function need to execute.
  * @return Execution
  * @throws NullPointerException
  *                 if Pool instance passed in is NULLPTR
  * @throws UnsupportedOperationException
  *                 if Pool is in multiusersecure Mode
  */
  inline static ExecutionPtr onServers(const RegionServicePtr& cache) {
    return onServersWithCache(cache);
  }

  template <typename T>
  static ExecutionPtr onServers(const SharedPtr<T>& poolOrCache) {
    return onServers(poolOrCache, GF_TYPE_IS_POOL_TYPE(T));
  }

  virtual ~FunctionService() {}

 private:
  static ExecutionPtr onServerWithPool(const PoolPtr& pool);

  static ExecutionPtr onServerWithCache(const RegionServicePtr& cache);

  static ExecutionPtr onServersWithPool(const PoolPtr& pool);

  static ExecutionPtr onServersWithCache(const RegionServicePtr& cache);

  template <typename T>
  static ExecutionPtr onServer(const SharedPtr<T>& pool,
                               TypeHelper::yes_type isPool) {
    return onServerWithPool(pool);
  }

  template <typename T>
  static ExecutionPtr onServer(const SharedPtr<T>& cache,
                               TypeHelper::no_type isPool) {
    return onServerWithCache(cache);
  }

  template <typename T>
  static ExecutionPtr onServers(const SharedPtr<T>& pool,
                                TypeHelper::yes_type isPool) {
    return onServersWithPool(pool);
  }

  template <typename T>
  static ExecutionPtr onServers(const SharedPtr<T>& cache,
                                TypeHelper::no_type isPool) {
    return onServersWithCache(cache);
  }
};

}  // namespace gemfire

#endif  //__GEMFIRE_FUNCTION_SERVICE_H__

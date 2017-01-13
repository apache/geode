#include "FunctionServiceImpl.hpp"
#include "ExecutionImpl.hpp"
#include <gfcpp/PoolManager.hpp>

using namespace gemfire;

FunctionServiceImpl::FunctionServiceImpl(ProxyCachePtr proxyCache) {
  m_proxyCache = proxyCache;
}

FunctionServicePtr FunctionServiceImpl::getFunctionService(
    ProxyCachePtr proxyCache) {
  FunctionServicePtr fPtr(new FunctionServiceImpl(proxyCache));
  return fPtr;
}

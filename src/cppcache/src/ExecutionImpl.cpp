/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include "ExecutionImpl.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "ThinClientRegion.hpp"
#include "ThinClientPoolDM.hpp"
#include "NoResult.hpp"
#include "UserAttributes.hpp"
// using namespace gemfire;

FunctionToFunctionAttributes ExecutionImpl::m_func_attrs;
ACE_Recursive_Thread_Mutex ExecutionImpl::m_func_attrs_lock;
ExecutionPtr ExecutionImpl::withFilter(CacheableVectorPtr routingObj) {
  // ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
  if (routingObj == NULLPTR) {
    throw IllegalArgumentException("Execution::withFilter: filter is null");
  }
  if (m_region == NULLPTR) {
    throw UnsupportedOperationException(
        "Execution::withFilter: FunctionService::onRegion needs to be called "
        "first before calling this function");
  }
  //      m_routingObj = routingObj;
  ExecutionPtr ptr(new ExecutionImpl(  //*this
      routingObj, m_args, m_rc, m_region, m_allServer, m_pool, m_proxyCache));
  return ptr;
}
ExecutionPtr ExecutionImpl::withArgs(CacheablePtr args) {
  // ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
  if (args == NULLPTR) {
    throw IllegalArgumentException("Execution::withArgs: args is null");
  }
  //  m_args = args;
  ExecutionPtr ptr(new ExecutionImpl(  //*this
      m_routingObj, args, m_rc, m_region, m_allServer, m_pool, m_proxyCache));
  return ptr;
}
ExecutionPtr ExecutionImpl::withCollector(ResultCollectorPtr rs) {
  // ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
  if (rs == NULLPTR) {
    throw IllegalArgumentException(
        "Execution::withCollector: collector is null");
  }
  //	m_rc = rs;
  ExecutionPtr ptr(new ExecutionImpl(  //*this
      m_routingObj, m_args, rs, m_region, m_allServer, m_pool, m_proxyCache));
  return ptr;
}

std::vector<int8>* ExecutionImpl::getFunctionAttributes(const char* func) {
  std::map<std::string, std::vector<int8>*>::iterator itr =
      m_func_attrs.find(func);
  if (itr != m_func_attrs.end()) {
    return itr->second;
  }
  return NULL;
}

ResultCollectorPtr ExecutionImpl::execute(const char* func, uint32_t timeout) {
  LOGDEBUG("ExecutionImpl::execute1: ");
  return execute(func, timeout, true);
}

ResultCollectorPtr ExecutionImpl::execute(CacheableVectorPtr& routingObj,
                                          CacheablePtr& args,
                                          ResultCollectorPtr& rs,
                                          const char* func, uint32_t timeout) {
  m_routingObj = routingObj;
  m_args = args;
  m_rc = rs;
  return execute(func, timeout, false);
}

ResultCollectorPtr ExecutionImpl::execute(const char* fn, uint32_t timeout,
                                          bool verifyFuncArgs) {
  std::string func = fn;
  LOGDEBUG("ExecutionImpl::execute: ");
  GuardUserAttribures gua;
  if (m_proxyCache != NULLPTR) {
    LOGDEBUG("ExecutionImpl::execute function on proxy cache");
    gua.setProxyCache(m_proxyCache);
  }
  bool serverHasResult = false;
  bool serverIsHA = false;
  bool serverOptimizeForWrite = false;

  if (verifyFuncArgs) {
    std::vector<int8>* attr = getFunctionAttributes(fn);
    {
      if (attr == NULL) {
        ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_func_attrs_lock);
        GfErrType err = GF_NOERR;
        attr = getFunctionAttributes(fn);
        if (attr == NULL) {
          if (m_region != NULLPTR) {
            err = dynamic_cast<ThinClientRegion*>(m_region.ptr())
                      ->getFuncAttributes(fn, &attr);
          } else if (m_pool != NULLPTR) {
            err = getFuncAttributes(fn, &attr);
          }
          if (err != GF_NOERR) {
            GfErrTypeToException("Execute::GET_FUNCTION_ATTRIBUTES", err);
          }
          if (!attr->empty() && err == GF_NOERR) {
            m_func_attrs[fn] = attr;
          }
        }
      }
    }
    serverHasResult = ((attr->at(0) == 1) ? true : false);
    serverIsHA = ((attr->at(1) == 1) ? true : false);
    serverOptimizeForWrite = ((attr->at(2) == 1) ? true : false);
  }

  LOGDEBUG(
      "ExecutionImpl::execute got functionAttributes from srver for function = "
      "%s serverHasResult = %d "
      " serverIsHA = %d serverOptimizeForWrite = %d ",
      func.c_str(), serverHasResult, serverIsHA, serverOptimizeForWrite);

  if (serverHasResult == false) {
    m_rc = new NoResult();
  } else if (m_rc == NULLPTR) {
    m_rc = new ResultCollector();
  }

  uint8_t isHAHasResultOptimizeForWrite = 0;
  if (serverIsHA) {
    isHAHasResultOptimizeForWrite = isHAHasResultOptimizeForWrite | 1;
  }

  if (serverHasResult) {
    isHAHasResultOptimizeForWrite = isHAHasResultOptimizeForWrite | 2;
  }

  if (serverOptimizeForWrite) {
    isHAHasResultOptimizeForWrite = isHAHasResultOptimizeForWrite | 4;
  }

  LOGDEBUG("ExecutionImpl::execute: isHAHasResultOptimizeForWrite = %d",
           isHAHasResultOptimizeForWrite);
  TXState* txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();

  if (txState != NULL && m_allServer == true) {
    throw UnsupportedOperationException(
        "Execution::execute: Transaction function execution on all servers is "
        "not supported");
  }

  if (m_region != NULLPTR) {
    int32_t retryAttempts = 3;
    if (m_pool != NULLPTR) {
      retryAttempts = m_pool->getRetryAttempts();
    }

    //    if(txState != NULL && !txState->isReplay())
    //    {
    //		VectorOfCacheablePtr args(new VectorOfCacheable());
    //		args->push_back(m_args);
    //		args->push_back(m_routingObj);
    //		args->push_back(m_rc);
    //		args->push_back(CacheableString::create(func));
    //		args->push_back(CacheableInt32::create(timeout));
    //		txState->recordTXOperation(GF_EXECUTE_FUNCTION,
    // m_region==NULLPTR?NULL:m_region->getFullPath(), NULLPTR, args);
    //    }
    //    try{
    if (m_pool != NULLPTR && m_pool->getPRSingleHopEnabled()) {
      ThinClientPoolDM* tcrdm = dynamic_cast<ThinClientPoolDM*>(m_pool.ptr());
      if (tcrdm == NULL) {
        throw IllegalArgumentException(
            "Execute: pool cast to ThinClientPoolDM failed");
      }
      ClientMetadataService* cms = tcrdm->getClientMetaDataService();
      CacheableHashSetPtr failedNodes = CacheableHashSet::create();
      if ((m_routingObj == NULLPTR || m_routingObj->empty()) &&
          txState ==
              NULL) {  // For transactions we should not create multiple threads
        LOGDEBUG("ExecutionImpl::execute: m_routingObj is empty");
        HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>* locationMap =
            cms->groupByServerToAllBuckets(
                m_region,
                /*serverOptimizeForWrite*/ (isHAHasResultOptimizeForWrite & 4));
        if (locationMap == NULL || locationMap->empty()) {
          LOGDEBUG(
              "ExecutionImpl::execute: m_routingObj is empty and locationMap "
              "is also empty so use old FE onRegion");
          dynamic_cast<ThinClientRegion*>(m_region.ptr())
              ->executeFunction(
                  fn, m_args, m_routingObj, isHAHasResultOptimizeForWrite, m_rc,
                  (isHAHasResultOptimizeForWrite & 1) ? retryAttempts : 0,
                  timeout);
          cms->enqueueForMetadataRefresh(m_region->getFullPath(), 0);
        } else {
          LOGDEBUG(
              "ExecutionImpl::execute: withoutFilter and locationMap is not "
              "empty");
          bool reExecute =
              dynamic_cast<ThinClientRegion*>(m_region.ptr())
                  ->executeFunctionSH(fn, m_args, isHAHasResultOptimizeForWrite,
                                      m_rc, locationMap, failedNodes, timeout,
                                      /*allBuckets*/ true);
          delete locationMap;
          if (reExecute) {  // Fallback to old FE onREgion
            if (isHAHasResultOptimizeForWrite & 1) {  // isHA = true
              m_rc->clearResults();
              CacheableVectorPtr rs =
                  dynamic_cast<ThinClientRegion*>(m_region.ptr())
                      ->reExecuteFunction(fn, m_args, m_routingObj,
                                          isHAHasResultOptimizeForWrite, m_rc,
                                          (isHAHasResultOptimizeForWrite & 1)
                                              ? retryAttempts
                                              : 0,
                                          failedNodes, timeout);
            } else {  // isHA = false
              m_rc->clearResults();
              dynamic_cast<ThinClientRegion*>(m_region.ptr())
                  ->executeFunction(
                      fn, m_args, m_routingObj, isHAHasResultOptimizeForWrite,
                      m_rc,
                      (isHAHasResultOptimizeForWrite & 1) ? retryAttempts : 0,
                      timeout);
            }
          }
        }
      } else if (m_routingObj != NULLPTR && m_routingObj->size() == 1) {
        LOGDEBUG("executeFunction onRegion WithFilter size equal to 1 ");
        dynamic_cast<ThinClientRegion*>(m_region.ptr())
            ->executeFunction(
                fn, m_args, m_routingObj, isHAHasResultOptimizeForWrite, m_rc,
                (isHAHasResultOptimizeForWrite & 1) ? retryAttempts : 0,
                timeout);
      } else {
        if (txState == NULL) {
          HashMapT<BucketServerLocationPtr, CacheableHashSetPtr>* locationMap =
              cms->getServerToFilterMapFESHOP(
                  &m_routingObj, m_region, /*serverOptimizeForWrite*/ (
                                     isHAHasResultOptimizeForWrite & 4));
          if (locationMap == NULL || locationMap->empty()) {
            LOGDEBUG(
                "ExecutionImpl::execute: withFilter but locationMap is empty "
                "so use old FE onRegion");
            dynamic_cast<ThinClientRegion*>(m_region.ptr())
                ->executeFunction(
                    fn, m_args, m_routingObj, isHAHasResultOptimizeForWrite,
                    m_rc,
                    (isHAHasResultOptimizeForWrite & 1) ? retryAttempts : 0,
                    timeout);
            cms->enqueueForMetadataRefresh(m_region->getFullPath(), 0);
          } else {
            LOGDEBUG(
                "ExecutionImpl::execute: withFilter and locationMap is not "
                "empty");
            bool reExecute = dynamic_cast<ThinClientRegion*>(m_region.ptr())
                                 ->executeFunctionSH(
                                     fn, m_args, isHAHasResultOptimizeForWrite,
                                     m_rc, locationMap, failedNodes, timeout,
                                     /*allBuckets*/ false);
            delete locationMap;
            if (reExecute) {  // Fallback to old FE onREgion
              if (isHAHasResultOptimizeForWrite & 1) {  // isHA = true
                m_rc->clearResults();
                CacheableVectorPtr rs =
                    dynamic_cast<ThinClientRegion*>(m_region.ptr())
                        ->reExecuteFunction(fn, m_args, m_routingObj,
                                            isHAHasResultOptimizeForWrite, m_rc,
                                            (isHAHasResultOptimizeForWrite & 1)
                                                ? retryAttempts
                                                : 0,
                                            failedNodes, timeout);
              } else {  // isHA = false
                m_rc->clearResults();
                dynamic_cast<ThinClientRegion*>(m_region.ptr())
                    ->executeFunction(
                        fn, m_args, m_routingObj, isHAHasResultOptimizeForWrite,
                        m_rc,
                        (isHAHasResultOptimizeForWrite & 1) ? retryAttempts : 0,
                        timeout);
              }
            }
          }
        } else {  // For transactions use old way
          dynamic_cast<ThinClientRegion*>(m_region.ptr())
              ->executeFunction(
                  fn, m_args, m_routingObj, isHAHasResultOptimizeForWrite, m_rc,
                  (isHAHasResultOptimizeForWrite & 1) ? retryAttempts : 0,
                  timeout);
        }
      }
    } else {  // w/o single hop, Fallback to old FE onREgion
      dynamic_cast<ThinClientRegion*>(m_region.ptr())
          ->executeFunction(
              fn, m_args, m_routingObj, isHAHasResultOptimizeForWrite, m_rc,
              (isHAHasResultOptimizeForWrite & 1) ? retryAttempts : 0, timeout);
    }
    /*    } catch (TransactionDataNodeHasDepartedException e) {
                    if(txState == NULL)
                    {
                            GfErrTypeThrowException("Transaction is NULL",
       GF_CACHE_ILLEGAL_STATE_EXCEPTION);
                    }

                    if(!txState->isReplay())
                            txState->replay(false);
            } catch(TransactionDataRebalancedException e) {
                    if(txState == NULL)
                    {
                            GfErrTypeThrowException("Transaction is NULL",
       GF_CACHE_ILLEGAL_STATE_EXCEPTION);
                    }

                    if(!txState->isReplay())
                            txState->replay(true);
            }
    */
    if (serverHasResult == true) {
      // ExecutionImpl::addResults(m_rc, rs);
      m_rc->endResults();
    }

    return m_rc;
  } else if (m_pool != NULLPTR) {
    if (txState != NULL) {
      throw UnsupportedOperationException(
          "Execution::execute: Transaction function execution on pool is not "
          "supported");
    }
    if (m_allServer == false) {
      executeOnPool(
          func, isHAHasResultOptimizeForWrite,
          (isHAHasResultOptimizeForWrite & 1) ? m_pool->getRetryAttempts() : 0,
          timeout);
      if (serverHasResult == true) {
        // ExecutionImpl::addResults(m_rc, rs);
        m_rc->endResults();
      }
      return m_rc;
    }
    executeOnAllServers(func, isHAHasResultOptimizeForWrite, timeout);
  } else {
    throw IllegalStateException("Execution::execute: should not be here");
  }
  return m_rc;
}

GfErrType ExecutionImpl::getFuncAttributes(const char* func,
                                           std::vector<int8>** attr) {
  ThinClientPoolDM* tcrdm = dynamic_cast<ThinClientPoolDM*>(m_pool.ptr());
  if (tcrdm == NULL) {
    throw IllegalArgumentException(
        "Execute: pool cast to ThinClientPoolDM failed");
  }

  GfErrType err = GF_NOERR;

  // do TCR GET_FUNCTION_ATTRIBUTES
  LOGDEBUG("Tcrmessage request GET_FUNCTION_ATTRIBUTES ");
  std::string funcName(func);
  TcrMessageGetFunctionAttributes request(funcName, tcrdm);
  TcrMessageReply reply(true, tcrdm);
  err = tcrdm->sendSyncRequest(request, reply);
  if (err != GF_NOERR) {
    return err;
  }
  switch (reply.getMessageType()) {
    case TcrMessage::RESPONSE: {
      *attr = reply.getFunctionAttributes();
      break;
    }
    case TcrMessage::EXCEPTION: {
      err = dynamic_cast<ThinClientRegion*>(m_region.ptr())
                ->handleServerException("Region::GET_FUNCTION_ATTRIBUTES",
                                        reply.getException());
      break;
    }
    default: {
      LOGERROR("Unknown message type %d while getting function attributes.",
               reply.getMessageType());
      err = GF_MSG;
    }
  }
  return err;
}

void ExecutionImpl::addResults(ResultCollectorPtr& collector,
                               const CacheableVectorPtr& results) {
  if (results == NULLPTR || collector == NULLPTR) {
    return;
  }

  for (int32_t resultItem = 0; resultItem < results->size(); resultItem++) {
    CacheablePtr result(results->operator[](resultItem));
    collector->addResult(result);
  }
}
void ExecutionImpl::executeOnAllServers(std::string& func, uint8_t getResult,
                                        uint32_t timeout) {
  ThinClientPoolDM* tcrdm = dynamic_cast<ThinClientPoolDM*>(m_pool.ptr());
  if (tcrdm == NULL) {
    throw IllegalArgumentException(
        "Execute: pool cast to ThinClientPoolDM failed");
  }
  CacheableStringPtr exceptionPtr = NULLPTR;
  GfErrType err = tcrdm->sendRequestToAllServers(
      func.c_str(), getResult, timeout, m_args, m_rc, exceptionPtr);
  if (exceptionPtr != NULLPTR && err != GF_NOERR) {
    LOGDEBUG("Execute errorred: %d", err);
    // throw FunctionExecutionException( "Execute: failed to execute function
    // with server." );
    if (err == GF_CACHESERVER_EXCEPTION) {
      throw FunctionExecutionException(
          "Execute: failed to execute function with server.");
    } else {
      GfErrTypeToException("Execute", err);
    }
  }

  if (err == GF_AUTHENTICATION_FAILED_EXCEPTION ||
      err == GF_NOT_AUTHORIZED_EXCEPTION ||
      err == GF_AUTHENTICATION_REQUIRED_EXCEPTION) {
    GfErrTypeToException("Execute", err);
  }

  if (err != GF_NOERR) {
    if (err == GF_CACHESERVER_EXCEPTION) {
      throw FunctionExecutionException(
          "Execute: exception "
          "at the server side: ",
          exceptionPtr->asChar());
    } else {
      LOGDEBUG("Execute errorred with server exception: %d", err);
      throw FunctionExecutionException(
          "Execute: failed to execute function on servers.");
    }
  }
}
CacheableVectorPtr ExecutionImpl::executeOnPool(std::string& func,
                                                uint8_t getResult,
                                                int32_t retryAttempts,
                                                uint32_t timeout) {
  ThinClientPoolDM* tcrdm = dynamic_cast<ThinClientPoolDM*>(m_pool.ptr());
  if (tcrdm == NULL) {
    throw IllegalArgumentException(
        "Execute: pool cast to ThinClientPoolDM failed");
  }
  int32_t attempt = 0;

  // CacheableStringArrayPtr csArray = tcrdm->getServers();

  // if (csArray != NULLPTR && csArray->length() != 0) {
  //  for (int i = 0; i < csArray->length(); i++)
  //  {
  //    CacheableStringPtr cs = csArray[i];
  //    TcrEndpoint *ep = NULL;
  //    /*
  //    std::string endpointStr = Utils::convertHostToCanonicalForm(cs->asChar()
  //    );
  //    */
  //    ep = tcrdm->addEP(cs->asChar());
  //  }
  //}

  // if pools retry attempts are not set then retry once on all available
  // endpoints
  if (retryAttempts == -1) {
    retryAttempts = static_cast<int32_t>(tcrdm->getNumberOfEndPoints());
  }

  while (attempt <= retryAttempts) {
    std::string funcName(func);
    TcrMessageExecuteFunction msg(funcName, m_args, getResult, tcrdm, timeout);
    TcrMessageReply reply(true, tcrdm);
    ChunkedFunctionExecutionResponse* resultCollector(
        new ChunkedFunctionExecutionResponse(reply, (getResult & 2), m_rc));
    reply.setChunkedResultHandler(resultCollector);
    reply.setTimeout(timeout);

    GfErrType err = GF_NOERR;
    err = tcrdm->sendSyncRequest(msg, reply, !(getResult & 1));
    LOGFINE("executeOnPool %d attempt = %d retryAttempts = %d", err, attempt,
            retryAttempts);
    if (err == GF_NOERR &&
        (reply.getMessageType() == TcrMessage::EXCEPTION ||
         reply.getMessageType() == TcrMessage::EXECUTE_FUNCTION_ERROR)) {
      err = ThinClientRegion::handleServerException("Execute",
                                                    reply.getException());
    }
    if (ThinClientBaseDM::isFatalClientError(err)) {
      GfErrTypeToException("ExecuteOnPool:", err);
    } else if (err != GF_NOERR) {
      if (getResult & 1) {
        resultCollector->reset();
        m_rc->clearResults();
        attempt++;
        if (attempt > 0) {
          getResult |= 8;  // Send this on server, so that it can identify that
                           // it is a retry attempt.
        }
        continue;
      } else {
        GfErrTypeToException("ExecuteOnPool:", err);
      }
    }
    // CacheableVectorPtr values =
    // resultCollector->getFunctionExecutionResults();
    /*
    ==25848== 1,610 (72 direct, 1,538 indirect) bytes in 2 blocks are definitely
    lost in loss record 193 of 218
    ==25848==    at 0x4007D75: operator new(unsigned int)
    (vg_replace_malloc.c:313)
    ==25848==    by 0x42B575A:
    gemfire::ExecutionImpl::executeOnPool(stlp_std::basic_string<char,
    stlp_std::char_traits<char>, stlp_std::allocator<char> >&, unsigned char,
    int, unsigned int) (ExecutionImpl.cpp:426)
    ==25848==    by 0x42B65E6: gemfire::ExecutionImpl::execute(char const*,
    bool, unsigned int, bool, bool, bool) (ExecutionImpl.cpp:292)
    ==25848==    by 0x42B7897: gemfire::ExecutionImpl::execute(char const*,
    bool, unsigned int, bool, bool) (ExecutionImpl.cpp:76)
    ==25848==    by 0x8081320: Task_Client1OpTest::doTask() (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPoolExecuteFunctionPrSHOP)
    ==25848==    by 0x808D5CA: dunit::TestSlave::begin() (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPoolExecuteFunctionPrSHOP)
    ==25848==    by 0x8089807: dunit::dmain(int, char**) (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPoolExecuteFunctionPrSHOP)
    ==25848==    by 0x805F9EA: main (in
    /export/pnq-gst-dev01a/users/adongre/cedar_dev_Nov12/build-artifacts/linux/tests/cppcache/testThinClientPoolExecuteFunctionPrSHOP)
    */
    delete resultCollector;
    resultCollector = NULL;

    return NULLPTR;
  }
  return NULLPTR;
}

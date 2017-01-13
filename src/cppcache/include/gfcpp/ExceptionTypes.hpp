#ifndef __GEMFIRE_EXCEPTIONTYPES_H__
#define __GEMFIRE_EXCEPTIONTYPES_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "Exception.hpp"

namespace gemfire {

#define _GF_EXCEPTION_DEF(x)                                              \
  const char _exception_name_##x[] = "gemfire::" #x;                      \
  class x;                                                                \
  typedef SharedPtr<x> x##Ptr;                                            \
  class CPPCACHE_EXPORT x : public gemfire::Exception {                   \
   public:                                                                \
    x(const char* msg1, const char* msg2 = NULL, bool forceStack = false, \
      const ExceptionPtr& cause = NULLPTR)                                \
        : Exception(msg1, msg2, forceStack, cause) {}                     \
    x(const x& other) : Exception(other) {}                               \
    virtual Exception* clone() const {                                    \
      return new x(m_message, m_stack, m_cause);                          \
    }                                                                     \
    virtual ~x() {}                                                       \
    virtual const char* getName() const { return _exception_name_##x; }   \
    virtual void raise() { throw * this; }                                \
                                                                          \
   protected:                                                             \
    x(const CacheableStringPtr& message, const StackTracePtr& stack,      \
      const ExceptionPtr& cause)                                          \
        : Exception(message, stack, cause) {}                             \
                                                                          \
   private:                                                               \
    const x& operator=(const x&);                                         \
  }

/*
 *
 * This is the list of exceptions directly derived from gemfire::Exception.
 *
 */

/**
 *@brief A gemfire assertion exception.
 **/
_GF_EXCEPTION_DEF(AssertionException);

/**
 *@brief Thrown when an argument to a method is illegal.
 **/
_GF_EXCEPTION_DEF(IllegalArgumentException);

/**
 *@brief Thrown when the state of cache is manipulated to be illegal.
 **/
_GF_EXCEPTION_DEF(IllegalStateException);

/**
 *@brief Thrown when an attempt is made to create an existing cache.
 **/
_GF_EXCEPTION_DEF(CacheExistsException);

/**
 *@brief Thrown when the cache xml is incorrect.
 **/
_GF_EXCEPTION_DEF(CacheXmlException);
/**
 *@brief Thrown when a timout occurs.
 **/
_GF_EXCEPTION_DEF(TimeoutException);

/**
 *@brief Thrown when the cache writer aborts the operation.
 **/
_GF_EXCEPTION_DEF(CacheWriterException);

/**
 *@brief Thrown when an attempt is made to create an existing region.
 **/
_GF_EXCEPTION_DEF(RegionExistsException);

/**
 *@brief Thrown when an operation is attempted on a closed cache.
 **/
_GF_EXCEPTION_DEF(CacheClosedException);

/**
 *@brief Thrown when lease of cache proxy has expired.
 **/
_GF_EXCEPTION_DEF(LeaseExpiredException);

/**
 *@brief Thrown when the cache loader aborts the operation.
 **/
_GF_EXCEPTION_DEF(CacheLoaderException);

/**
 *@brief Thrown when an operation is attempted on a destroyed region.
 **/
_GF_EXCEPTION_DEF(RegionDestroyedException);

/**
 *@brief Thrown when an operation is attempted on a destroyed entry.
 **/
_GF_EXCEPTION_DEF(EntryDestroyedException);

/**
 *@brief Thrown when the connecting target is not running.
 **/
_GF_EXCEPTION_DEF(NoSystemException);

/**
 *@brief Thrown when an attempt is made to connect to
 *       DistributedSystem second time.
 **/
_GF_EXCEPTION_DEF(AlreadyConnectedException);

/**
 *@brief Thrown when a non-existing file is accessed.
 **/
_GF_EXCEPTION_DEF(FileNotFoundException);

/**
 *@brief Thrown when an operation is interrupted.
 **/
_GF_EXCEPTION_DEF(InterruptedException);

/**
 *@brief Thrown when an operation unsupported by the
 *       current configuration is attempted.
 **/
_GF_EXCEPTION_DEF(UnsupportedOperationException);

/**
 *@brief Thrown when statistics are invoked for a region where
 *       they are disabled.
 **/
_GF_EXCEPTION_DEF(StatisticsDisabledException);

/**
 *@brief Thrown when a concurrent operation fails.
 **/
_GF_EXCEPTION_DEF(ConcurrentModificationException);

/**
 *@brief An unknown exception occurred.
 **/
_GF_EXCEPTION_DEF(UnknownException);

/**
 *@brief Thrown when a cast operation fails.
 **/
_GF_EXCEPTION_DEF(ClassCastException);

/**
 *@brief Thrown when an operation is attempted on a non-existent entry.
 **/
_GF_EXCEPTION_DEF(EntryNotFoundException);

/**
 *@brief Thrown when there is an input/output error.
 **/
_GF_EXCEPTION_DEF(GemfireIOException);

/**
 *@brief Thrown when gemfire configuration file is incorrect.
 **/
_GF_EXCEPTION_DEF(GemfireConfigException);

/**
 *@brief Thrown when a null argument is provided to a method
 *       where it is expected to be non-null.
 **/
_GF_EXCEPTION_DEF(NullPointerException);

/**
 *@brief Thrown when attempt is made to create an existing entry.
 **/
_GF_EXCEPTION_DEF(EntryExistsException);

/**
 *@brief Thrown when an operation is attempted before connecting
 *       to the distributed system.
 **/
_GF_EXCEPTION_DEF(NotConnectedException);

/**
 *@brief Thrown when there is an error in the cache proxy.
 **/
_GF_EXCEPTION_DEF(CacheProxyException);

/**
 *@brief Thrown when the system cannot allocate any more memory.
 **/
_GF_EXCEPTION_DEF(OutOfMemoryException);

/**
 *@brief Thrown when an attempt is made to release a lock not
 *       owned by the thread.
 **/
_GF_EXCEPTION_DEF(NotOwnerException);

/**
 *@brief Thrown when a region is created in an incorrect scope.
 **/
_GF_EXCEPTION_DEF(WrongRegionScopeException);

/**
 *@brief Thrown when the internal buffer size is exceeded.
 **/
_GF_EXCEPTION_DEF(BufferSizeExceededException);

/**
 *@brief Thrown when a region creation operation fails.
 **/
_GF_EXCEPTION_DEF(RegionCreationFailedException);

/**
 *@brief Thrown when there is a fatal internal exception in gemfire.
  */
_GF_EXCEPTION_DEF(FatalInternalException);

/**
 *@brief Thrown by the persistence manager when a write
 *       fails due to disk failure.
 **/
_GF_EXCEPTION_DEF(DiskFailureException);

/**
 *@brief Thrown by the persistence manager when the data
 *@brief to be read from disk is corrupt.
 **/
_GF_EXCEPTION_DEF(DiskCorruptException);

/**
 *@brief Thrown when persistence manager fails to initialize.
 **/
_GF_EXCEPTION_DEF(InitFailedException);

/**
 *@brief Thrown when persistence manager fails to close properly.
 **/
_GF_EXCEPTION_DEF(ShutdownFailedException);

/**
 *@brief Thrown when an exception occurs on the cache server.
 **/
_GF_EXCEPTION_DEF(CacheServerException);

/**
 *@brief Thrown when bound of array/vector etc. is exceeded.
 **/
_GF_EXCEPTION_DEF(OutOfRangeException);

/**
 *@brief Thrown when query exception occurs at the server.
 **/
_GF_EXCEPTION_DEF(QueryException);

/**
 *@brief Thrown when an unknown message is received from the server.
 **/
_GF_EXCEPTION_DEF(MessageException);

/**
 *@brief Thrown when a non authorized operation is done.
 **/
_GF_EXCEPTION_DEF(NotAuthorizedException);

/**
 *@brief Thrown when authentication fails.
 **/
_GF_EXCEPTION_DEF(AuthenticationFailedException);

/**
 *@brief Thrown when no credentials are provided by client when server expects.
 **/
_GF_EXCEPTION_DEF(AuthenticationRequiredException);

/**
 *@brief Thrown when two durable connect with same Id.
 **/
_GF_EXCEPTION_DEF(DuplicateDurableClientException);

/**
 *@brief Thrown when the cache listener throws an exception.
 **/
_GF_EXCEPTION_DEF(CacheListenerException);
/**
 *@brief Thrown during continuous query execution time.
 **/
_GF_EXCEPTION_DEF(CqException);
/**
 *@brief Thrown if the Cq on which the operaion performed is closed
 **/
_GF_EXCEPTION_DEF(CqClosedException);
/**
 *@brief Thrown if the Cq Query failed
 **/
_GF_EXCEPTION_DEF(CqQueryException);
/**
 *@brief Thrown if a Cq by this name already exists on this client
 **/
_GF_EXCEPTION_DEF(CqExistsException);
/**
 *@brief  Thrown if the query doesnot meet the CQ constraints.
 * E.g.:Query string should refer only one region, join not supported.
 *      The query must be a SELECT statement.
 *      DISTINCT queries are not supported.
 *      Projections are not supported.
 *      Only one iterator in the FROM clause is supported, and it must be a
 *region path.
 *      Bind parameters in the query are not supported for the initial release.
 **/
_GF_EXCEPTION_DEF(CqInvalidException);
/**
 *@brief Thrown if function execution failed
 **/
_GF_EXCEPTION_DEF(FunctionExecutionException);
/**
 *@brief Thrown if the No locators are active to reply for new connection.
 **/
_GF_EXCEPTION_DEF(NoAvailableLocatorsException);
/**
 *@brief Thrown if all connections in the pool are in use.
 **/
_GF_EXCEPTION_DEF(AllConnectionsInUseException);
/**
 *@brief Thrown if Delta could not be applied.
 **/
_GF_EXCEPTION_DEF(InvalidDeltaException);
/**
 *@brief Thrown if a Key is not present in the region.
 **/
_GF_EXCEPTION_DEF(KeyNotFoundException);
/**
* @brief This is for all Exceptions that may be thrown
* by a GemFire transaction.
**/
_GF_EXCEPTION_DEF(TransactionException);
/**
* @brief The RollbackException exception indicates that either the transaction
* has been rolled back or an operation cannot complete because the
* transaction is marked for rollback only.
**/
_GF_EXCEPTION_DEF(RollbackException);
/**
* @brief Thrown when a commit fails due to a write conflict.
* @see CacheTransactionManager#commit
**/
_GF_EXCEPTION_DEF(CommitConflictException);
/**
* @brief Thrown when the transactional data host has shutdown or no longer has
*the data
* being modified by the transaction.
* This can be thrown while doing transactional operations or during commit.
**/
_GF_EXCEPTION_DEF(TransactionDataNodeHasDepartedException);
/**
* @brief Thrown when a {@link RebalanceOperation} occurs concurrently with a
*transaction.
* This can be thrown while doing transactional operations or during commit.
**/
_GF_EXCEPTION_DEF(TransactionDataRebalancedException);

/**
* @brief Thrown if putAll operation with single hop succeeded partially.
**/
_GF_EXCEPTION_DEF(PutAllPartialResultException);

/**
 *@brief Thrown when a version on which delta is based is different than the
 *current version
 **/

extern void CPPCACHE_EXPORT GfErrTypeThrowException(const char* str,
                                                    GfErrType err);

#define GfErrTypeToException(str, err)   \
  {                                      \
    if (err != GF_NOERR) {               \
      GfErrTypeThrowException(str, err); \
    }                                    \
  }
};  // namespace gemfire

#endif  // ifndef __GEMFIRE_EXCEPTIONTYPES_H__

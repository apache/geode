/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <stdio.h>
#include <string>
#include <gfcpp/ExceptionTypes.hpp>
#include <TXState.hpp>
#include <TSSTXStateWrapper.hpp>

namespace gemfire {
void setTSSExceptionMessage(const char* exMsg);
const char* getTSSExceptionMessage();

void GfErrTypeThrowException(const char* str, GfErrType err) {
  std::string func;
  const char* exMsg = getTSSExceptionMessage();
  if (exMsg != NULL && exMsg[0] == '\0') {
    exMsg = NULL;
  } else {
    func.append(str);
    func.append(": ");
    str = func.c_str();
  }
  switch (err) {
    case GF_NOTCON: {
      NotConnectedException ex(
          str, (exMsg != NULL ? exMsg : ": not connected to GemFire"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_MSG: {
      MessageException ex(
          str, (exMsg != NULL ? exMsg
                              : ": message from server could not be handled"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHESERVER_EXCEPTION: {
      CacheServerException ex(
          str,
          (exMsg != NULL ? exMsg : ": exception happened at cache server"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_NOTOWN: {
      NotOwnerException ex(str, (exMsg != NULL ? exMsg : ": not own the lock"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_REGION_NOT_FOUND: {
      CacheServerException ex(
          str, (exMsg != NULL ? exMsg : ": region not found on server"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_REGION_NOT_GLOBAL: {
      IllegalStateException ex(str,
                               (exMsg != NULL ? exMsg : ": region not global"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION: {
      IllegalArgumentException ex(
          str, (exMsg != NULL ? exMsg : ": illegal argument"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_ILLEGAL_STATE_EXCEPTION: {
      IllegalStateException ex(str,
                               (exMsg != NULL ? exMsg : ": illegal State"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_WRITER_EXCEPTION: {
      CacheWriterException ex(
          str, (exMsg != NULL ? exMsg : ": exception on server during write"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHEWRITER_ERROR: {
      CacheWriterException ex(
          str, (exMsg != NULL ? exMsg : ": exception in CacheWriter"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_LOADER_EXCEPTION: {
      CacheLoaderException ex(
          str, (exMsg != NULL ? exMsg : ": exception in CacheLoader"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_LISTENER_EXCEPTION: {
      CacheListenerException ex(
          str, (exMsg != NULL ? exMsg : ": exception in CacheListener"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_REGION_INVALID: {
      RegionDestroyedException ex(
          str, (exMsg != NULL ? exMsg : ": region not valid"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_PROXY: {
      CacheProxyException ex(
          str, (exMsg != NULL ? exMsg : ": error in Cache proxy"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_IOERR: {
      GemfireIOException ex(
          str, (exMsg != NULL ? exMsg : ": Input/Output error in operation"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_ENOENT: {
      NoSystemException ex(str,
                           (exMsg != NULL ? exMsg : ": entity does not exist"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_REGION_KEYS_NOT_STRINGS: {
      IllegalArgumentException ex(
          str,
          (exMsg != NULL ? exMsg : ": region entries do not support C access"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_REGION_ENTRY_NOT_BYTES: {
      IllegalArgumentException ex(
          str,
          (exMsg != NULL ? exMsg
                         : ": existing non-null values was not a byte array"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_TIMEOUT_EXCEPTION: {
      TimeoutException ex(str, (exMsg != NULL ? exMsg : ": timed out"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_TIMOUT: {
      TimeoutException ex(str, (exMsg != NULL ? exMsg : ": timed out"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CLIENT_WAIT_TIMEOUT: {
      TimeoutException ex(
          str,
          (exMsg != NULL ? exMsg
                         : ": timed out, possibly bucket is not available."));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_ENOMEM: {
      OutOfMemoryException ex(str, (exMsg != NULL ? exMsg : ": Out of memory"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_ERANGE: {
      BufferSizeExceededException ex(
          str, (exMsg != NULL ? exMsg : ": Buffer Size Exceeded"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_LEASE_EXPIRED_EXCEPTION: {
      LeaseExpiredException ex(
          str, (exMsg != NULL ? exMsg : ": lock Lease Expired On you"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_REGION_EXISTS_EXCEPTION: {
      RegionExistsException ex(
          str, (exMsg != NULL ? exMsg : ": Named Region Exists"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_ENTRY_NOT_FOUND: {
      EntryNotFoundException ex(str,
                                (exMsg != NULL ? exMsg : ": Entry not found"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_ENTRY_EXISTS: {
      EntryExistsException ex(
          str,
          (exMsg != NULL ? exMsg : ": Entry already exists in the region"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_ENTRY_DESTROYED_EXCEPTION: {
      EntryDestroyedException ex(
          str, (exMsg != NULL ? exMsg : ": Entry has been destroyed"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_REGION_DESTROYED_EXCEPTION: {
      RegionDestroyedException ex(
          str, (exMsg != NULL ? exMsg : ": Named Region Destroyed"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_CLOSED_EXCEPTION: {
      CacheClosedException ex(
          str, (exMsg != NULL ? exMsg : ": Cache has been closed"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_STATISTICS_DISABLED_EXCEPTION: {
      StatisticsDisabledException ex(
          str,
          (exMsg != NULL ? exMsg
                         : ": Statistics have been disabled for the region"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION: {
      ConcurrentModificationException ex(
          str,
          (exMsg != NULL ? exMsg : ": Concurrent modification in the cache"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_NOT_AUTHORIZED_EXCEPTION: {
      NotAuthorizedException ex(
          str, (exMsg != NULL ? exMsg : ": unauthorized operation"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_AUTHENTICATION_FAILED_EXCEPTION: {
      AuthenticationFailedException ex(
          str, (exMsg != NULL ? exMsg : ": authentication failed"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_AUTHENTICATION_REQUIRED_EXCEPTION: {
      AuthenticationRequiredException ex(
          str, (exMsg != NULL ? exMsg : ": no authentication provided"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_DUPLICATE_DURABLE_CLIENT: {
      DuplicateDurableClientException ex(
          str, (exMsg != NULL ? exMsg : ": Duplicate Durable Client Id"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_REMOTE_QUERY_EXCEPTION: {
      QueryException ex(str, (exMsg != NULL ? exMsg : ": Query failed"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_CACHE_LOCATOR_EXCEPTION: {
      ExceptionPtr exCause(new NoAvailableLocatorsException(
          str, (exMsg != NULL ? exMsg : ": No locators available")));
      NotConnectedException ex(
          str, (exMsg != NULL ? exMsg : ": No locators available"), false,
          exCause);
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_ALL_CONNECTIONS_IN_USE_EXCEPTION: {
      AllConnectionsInUseException ex(
          str, (exMsg != NULL ? exMsg : ": All connections are in use"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_FUNCTION_EXCEPTION: {
      FunctionExecutionException ex(
          str, (exMsg != NULL ? exMsg : ": Function execution failed"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_DISKFULL: {
      DiskFailureException ex(str, (exMsg != NULL ? exMsg : ": Disk full"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_ROLLBACK_EXCEPTION: {
      RollbackException ex(
          str, (exMsg != NULL ? exMsg : ": Transaction rolled back"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_COMMIT_CONFLICT_EXCEPTION: {
      CommitConflictException ex(
          str, (exMsg != NULL ? exMsg : ": Commit conflict exception"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_TRANSACTION_DATA_REBALANCED_EXCEPTION: {
      TransactionDataRebalancedException ex(
          str,
          (exMsg != NULL ? exMsg : ": Transaction data rebalanced exception"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_TRANSACTION_DATA_NODE_HAS_DEPARTED_EXCEPTION: {
      TransactionDataNodeHasDepartedException ex(
          str,
          (exMsg != NULL ? exMsg
                         : ": Transaction data node has departed exception"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    case GF_PUTALL_PARTIAL_RESULT_EXCEPTION: {
      PutAllPartialResultException ex(
          str, (exMsg != NULL ? exMsg : ": PutAll Partial exception"));
      setTSSExceptionMessage(NULL);
      throw ex;
    }
    default: {
      char buf[64];
      LOGINFO("error code: %d", err);
      if (exMsg == NULL) {
        ACE_OS::snprintf(buf, 64, "Unknown error code[0x%X]", err);
        exMsg = buf;
      }
      UnknownException ex(str, exMsg);
      setTSSExceptionMessage(NULL);
      throw ex;
    }
  }
}

}  // namespace gemfire

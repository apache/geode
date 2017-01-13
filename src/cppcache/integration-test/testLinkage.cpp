/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testLinkage"

#include <gfcpp/GemfireCppCache.hpp>
#include "fw_helper.hpp"

using namespace gemfire;

/**
 * @brief Test that we can link to all classes.
 */
BEGIN_TEST(LinkageTest)
  // just create one for now...

  AttributesFactory af;

  {
    CacheablePtr cacheablePtr;
    CacheableKeyPtr cacheableKeyPtr;
    RegionPtr regionPtr;
    AttributesMutator am(regionPtr);
    RegionEntryPtr regionEntryPtr;
    CacheableStringPtr cacheableStringPtr;
    CachePtr cachePtr;
    // add other ptr types here...
  }
  {
    Exception e("test message");
    // all exceptions.
    IllegalArgumentException aIllegalArgumentException(
        "IllegalArgumentException");
    IllegalStateException aIllegalStateException("IllegalStateException");
    CacheExistsException aCacheExistsException("CacheExistsException");
    CacheXmlException aCacheXmlException("CacheXmlException");
    TimeoutException aTimeoutException("TimeoutException");
    CacheWriterException aCacheWriterException("CacheWriterException");
    RegionExistsException aRegionExistsException("RegionExistsException");
    CacheClosedException aCacheClosedException("CacheClosedException");
    LeaseExpiredException aLeaseExpiredException("LeaseExpiredException");
    CacheLoaderException aCacheLoaderException("CacheLoaderException");
    RegionDestroyedException aRegionDestroyedException(
        "RegionDestroyedException");
    EntryDestroyedException aEntryDestroyedException("EntryDestroyedException");
    NoSystemException aNoSystemException("NoSystemException");
    AlreadyConnectedException aAlreadyConnectedException(
        "AlreadyConnectedException");
    FileNotFoundException aFileNotFoundException("FileNotFoundException");
    InterruptedException aInterruptedException("InterruptedException");
    UnsupportedOperationException aUnsupportedOperationException(
        "UnsupportedOperationException");
    StatisticsDisabledException aStatisticsDisabledException(
        "StatisticsDisabledException");
    ConcurrentModificationException aConcurrentModificationException(
        "ConcurrentModificationException");
    UnknownException aUnknownException("UnknownException");
    ClassCastException aClassCastException("ClassCastException");
    EntryNotFoundException aEntryNotFoundException("EntryNotFoundException");
    GemfireIOException aGemfireIOException("GemfireIOException");
    GemfireConfigException aGemfireConfigException("GemfireConfigException");
    NullPointerException aNullPointerException("NullPointerException");
    EntryExistsException aEntryExistsException("EntryExistsException");
  }

  CachePtr cachePtr;
  CacheFactoryPtr cacheFactoryPtr = CacheFactory::createCacheFactory();
  cachePtr = cacheFactoryPtr->create();
  // Cache cache;
  ASSERT((!cachePtr->isClosed()), "cache shouldn't be closed.");
  RegionPtr rptr;
  UserDataPtr callback;
  //    CacheListener cl;
  CacheListenerPtr clPtr;
  //    CacheLoader cacheloader;
  CacheLoaderPtr cldPtr;
  //    CacheStatistics cstats; NOT yet...

  //    CacheWriter cwriter;
  CacheWriterPtr cwPtr;
END_TEST(LinkageTest)

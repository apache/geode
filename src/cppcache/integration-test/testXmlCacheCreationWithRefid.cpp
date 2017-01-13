/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testXmlCacheCreationWithRefid"

#include <gfcpp/GemfireCppCache.hpp>

#include "fw_helper.hpp"
#include <string>
#include <math.h>
using namespace gemfire;
using namespace test;

int testXmlCacheCreationWithRefid(const char* fileName) {
  char* host_name = (char*)"XML_CACHE_CREATION_TEST";
  CacheFactoryPtr cacheFactory;
  CachePtr cptr;

  char* path = ACE_OS::getenv("TESTSRC");
  std::string directory(path);

  cout << "create DistributedSytem with name=" << host_name << endl;
  try {
    cacheFactory = CacheFactory::createCacheFactory();
  } catch (Exception& ex) {
    ex.showMessage();
    ex.printStackTrace();
    return -1;
  }

  cout << "Create cache with the configurations provided in "
          "valid_cache_refid.xml"
       << endl;

  try {
    std::string filePath = directory + fileName;
    cptr = cacheFactory->set("cache-xml-file", filePath.c_str())->create();
    if (cptr->getPdxIgnoreUnreadFields() != false) {
      cout << "getPdxIgnoreUnreadFields should return false." << endl;
      return -1;
    } else {
      cout << "getPdxIgnoreUnreadFields returned false." << endl;
    }
  } catch (CacheXmlException& ex) {
    ex.showMessage();
    ex.printStackTrace();
    return -1;
  }

  RegionPtr Root1;
  RegionPtr SubRegion1;
  RegionPtr SubRegion11;
  RegionPtr SubRegion2;

  RegionPtr Root2;
  RegionPtr SubRegion21;

  cout << "Verify whether all the regions are created" << endl;

  try {
    Root1 = cptr->getRegion("Root1");
    SubRegion1 = Root1->getSubregion("SubRegion1");
    SubRegion11 = SubRegion1->getSubregion("SubRegion11");
    SubRegion2 = Root1->getSubregion("SubRegion2");

    Root2 = cptr->getRegion("Root2");
    SubRegion21 = Root2->getSubregion("SubRegion21");
  } catch (...) {
    LOGINFO("Unknown Exception while getting one of the regions");
    return -1;
  }

  cout << "Verify whether region 'SubRegion11' has correct attributes" << endl;

  RegionAttributesPtr atts = SubRegion11->getAttributes();

  if (atts->getCachingEnabled() != true) {
    LOGINFO("Caching is not enabled in SubRegion11");
    return -1;
  }

  if (atts->getInitialCapacity() != 10) {
    LOGINFO("Initial capacity of SubRegion11 is not 10");
    return -1;
  }

  if (atts->getConcurrencyLevel() != 52) {
    LOGINFO("Concurrency level of SubRegion11 is not 52");
    return -1;
  }

  if (fabs(atts->getLoadFactor() - 0.89) > 0.001) {
    LOGINFO("Load factor of SubRegion11 is not 0.89");
    return -1;
  }

  cout << "Verify whether region 'SubRegion2' has correct attributes" << endl;

  atts = SubRegion2->getAttributes();

  if (atts->getCachingEnabled() != true) {
    LOGINFO("Caching is not enabled in SubRegion2");
    return -1;
  }

  if (atts->getInitialCapacity() != 10) {
    LOGINFO("Initial capacity of SubRegion2 is not 10");
    return -1;
  }

  if (fabs(atts->getLoadFactor() - 0.89) > 0.001) {
    LOGINFO("Load factor of SubRegion2 is not 0.89");
    return -1;
  }

  if (atts->getConcurrencyLevel() != 52) {
    LOGINFO("Concurrency level of SubRegion2 is not 52");
    return -1;
  }

  cout << "Verify whether region 'SubRegion21' has correct attributes" << endl;

  atts = SubRegion21->getAttributes();

  if (atts->getCachingEnabled() != true) {
    LOGINFO("Caching is not enabled in SubRegion21");
    return -1;
  }

  if (atts->getInitialCapacity() != 10) {
    LOGINFO("Initial capacity of SubRegion21 is not 10");
    return -1;
  }

  if (fabs(atts->getLoadFactor() - 0.89) > 0.001) {
    LOGINFO("Load factor of SubRegion21 is not 0.89");
    return -1;
  }

  if (atts->getConcurrencyLevel() != 52) {
    LOGINFO("Concurrency level of SubRegion21 is not 52");
    return -1;
  }

  if (atts->getEntryIdleTimeout() != 10) {
    LOGINFO("Entryidletimeout of SubRegion21 is not 10");
    return -1;
  }

  if (atts->getEntryIdleTimeoutAction() != ExpirationAction::INVALIDATE) {
    LOGINFO("Entryidletimeoutaction of SubRegion21 is not invalidate");
    return -1;
  }

  if (atts->getRegionIdleTimeout() != 20) {
    LOGINFO("Regionidletimeout of SubRegion21 is not 20");
    return -1;
  }

  if (atts->getRegionIdleTimeoutAction() != ExpirationAction::DESTROY) {
    LOGINFO("Regionidletimeoutaction of SubRegion21 is not destroy");
    return -1;
  }

  cout << "Verify whether region 'Root2' has correct attributes" << endl;

  atts = Root2->getAttributes();

  if (atts->getCachingEnabled() != true) {
    LOGINFO("Caching is not enabled in Root2");
    return -1;
  }

  if (atts->getInitialCapacity() != 25) {
    LOGINFO("Initial capacity of Root2 is not 10");
    return -1;
  }

  if (fabs(atts->getLoadFactor() - 0.32) > 0.001) {
    LOGINFO("Load factor of Root2 is not 0.0.32");
    return -1;
  }

  if (atts->getConcurrencyLevel() != 16) {
    LOGINFO("Concurrency level of Root2 is not 16");
    return -1;
  }

  if (atts->getEntryIdleTimeout() != 10) {
    LOGINFO("Entryidletimeout of Root2 is not 10");
    return -1;
  }

  if (atts->getEntryIdleTimeoutAction() != ExpirationAction::INVALIDATE) {
    LOGINFO("Entryidletimeoutaction of Root2 is not invalidate");
    return -1;
  }

  if (atts->getEntryTimeToLive() != 0) {
    LOGINFO("Entrytimetolive of Root2 is not 0");
    return -1;
  }

  if (atts->getEntryTimeToLiveAction() != ExpirationAction::LOCAL_INVALIDATE) {
    LOGINFO("Entrytimetoliveaction of Root2 is not local_invalidate");
    return -1;
  }

  if (atts->getRegionIdleTimeout() != 0) {
    LOGINFO("Regionidletimeout of Root2 is not 0");
    return -1;
  }

  if (atts->getRegionIdleTimeoutAction() != ExpirationAction::INVALIDATE) {
    LOGINFO("Regionidletimeoutaction of Root2 is not invalidate");
    return -1;
  }

  if (atts->getRegionTimeToLive() != 0) {
    LOGINFO("Regiontimetolive of Root2 is not 0");
    return -1;
  }

  if (atts->getRegionTimeToLiveAction() != ExpirationAction::DESTROY) {
    LOGINFO("Regiontimetoliveaction of Root2 is not destroy");
    return -1;
  }

  cptr->close();

  return 0;
}

BEGIN_TEST(ValidXmlTestRefid)
  {
    int res = testXmlCacheCreationWithRefid("/valid_cache_refid.xml");
    if (res != 0) {
      FAIL("Test Failed.");
    }

    res = testXmlCacheCreationWithRefid("/valid_cache_region_refid.xml");
    if (res != 0) {
      FAIL("Test Failed.");
    }
  }
END_TEST(ValidXmlTestRefid)

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testXmlCacheCreationWithOverFlow"

#include <gfcpp/GemfireCppCache.hpp>

#include "fw_helper.hpp"
#include <string>

using namespace gemfire;
using namespace test;

int testXmlCacheCreationWithOverflow() {
  char* host_name = (char*)"XML_CACHE_CREATION_TEST";
  CacheFactoryPtr cacheFactory;
  CachePtr cptr;
  const uint32_t totalSubRegionsRoot1 = 2;
  const uint32_t totalRootRegions = 2;

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
          "valid_overflowAttr.xml"
       << endl;

  try {
    std::string filePath = directory + "/non-existent.xml";
    cptr = cacheFactory->set("cache-xml-file", filePath.c_str())->create();
    return -1;
  } catch (CacheXmlException& ex) {
    ex.showMessage();
    ex.printStackTrace();
  } catch (...) {
    LOGINFO("Unknown exception");
    return -1;
  }

  /// return 0;
  try {
    std::string filePath = directory + "/valid_overflowAttr.xml";
    cout << "getPdxIgnoreUnreadFields should return true.1" << endl;
    cptr = cacheFactory->set("cache-xml-file", filePath.c_str())->create();
    if (cptr->getPdxIgnoreUnreadFields() != false) {
      cout << "getPdxIgnoreUnreadFields should return true." << endl;
      return -1;
    } else {
      cout << "getPdxIgnoreUnreadFields returned true." << endl;
    }
    // return 0;
  } catch (Exception& ex) {
    cout << "getPdxIgnoreUnreadFields should return true2." << endl;
    ex.showMessage();
    ex.printStackTrace();
    return -1;
  } catch (...) {
    LOGINFO(" unknown exception");
    return -1;
  }

  VectorOfRegion vrp;
  cout << "Test if number of root regions are correct" << endl;
  cptr->rootRegions(vrp);
  cout << "  vrp.size=" << vrp.size() << endl;

  if (vrp.size() != totalRootRegions) {
    cout << "Number of root regions does not match" << endl;
    return -1;
  }

  cout << "Root regions in Cache :" << endl;
  for (int32_t i = 0; i < vrp.size(); i++) {
    cout << "vc[" << i << "].m_reaPtr=" << vrp.at(i).ptr() << endl;
    cout << "vc[" << i << "]=" << vrp.at(i)->getName() << endl;
  }
  RegionPtr regPtr1 = vrp.at(0);

  uint32_t i ATTR_UNUSED = 0;
  VectorOfRegion vr;
  cout << "Test if the number of sub regions with the root region Root1 are "
          "correct"
       << endl;
  regPtr1->subregions(true, vr);
  cout << "  vr.size=" << vr.size() << endl;
  if (vr.size() != totalSubRegionsRoot1) {
    cout << "Number of Subregions does not match" << endl;
    return -1;
  }

  cout << "get subregions from the root region :" << vrp.at(0)->getName()
       << endl;
  for (int32_t i = 0; i < vr.size(); i++) {
    cout << "vc[" << i << "].m_reaPtr=" << vr.at(i).ptr() << endl;
    cout << "vc[" << i << "]=" << vr.at(i)->getName() << endl;
  }

  cout << "Test if the nesting of regions is correct" << endl;
  const char* parentName;
  const char* childName;
  RegionPtr regPtr2 = vrp.at(1);
  VectorOfRegion vsr;
  regPtr2->subregions(true, vsr);
  for (uint32_t i = 0; i < static_cast<uint32_t>(vsr.size()); i++) {
    Region* regPtr = vsr.at(i).ptr();
    childName = regPtr->getName();

    RegionPtr x = regPtr->getParentRegion();
    parentName = (x.ptr())->getName();
    if (strcmp(childName, "SubSubRegion221") == 0) {
      if (strcmp(parentName, "SubRegion22") != 0) {
        cout << "Incorrect parent: tree structure not formed correctly" << endl;
        return -1;
      }
    }
  }
  cout << "****Correct region tree structure created from valid_cache.xml****"
       << endl;

  vr.clear();
  vrp.clear();

  cout << "Test the attributes of region" << endl;

  RegionAttributesPtr raPtr = regPtr1->getAttributes();
  RegionAttributes* regAttr = raPtr.ptr();
  cout << "Attributes of root region Root1 are : " << endl;

  bool cachingEnabled = regAttr->getCachingEnabled();
  cout << "Caching-enabled :true" << endl;
  if (!cachingEnabled) {
    return -1;
  }
  int lruEL = regAttr->getLruEntriesLimit();
  cout << "lru-entries-limit : 35" << endl;
  if (lruEL != 35) {
    return -1;
  }
  int concurrency = regAttr->getConcurrencyLevel();
  cout << "concurrency-level : 10" << endl;
  if (concurrency != 10) {
    return -1;
  }
  int initialCapacity = regAttr->getInitialCapacity();
  cout << "initial-capacity : 25" << endl;
  if (initialCapacity != 25) {
    return -1;
  }
  int regionIdleTO = regAttr->getRegionIdleTimeout();
  cout << "RegionIdleTimeout:20 " << endl;
  if (regionIdleTO != 20) {
    return -1;
  }

  ExpirationAction::Action action1 = regAttr->getRegionIdleTimeoutAction();
  cout << "RegionIdleTimeoutAction : Destroy" << endl;
  if (action1 != ExpirationAction::DESTROY) {
    return -1;
  }
  const DiskPolicyType::PolicyType type = regAttr->getDiskPolicy();
  cout << "DiskPolicy : overflows" << endl;
  if (type != DiskPolicyType::OVERFLOWS) {
    cout << " diskpolicy is not overflows " << endl;
    return -1;
  }

  const char* lib = regAttr->getPersistenceLibrary();
  const char* libFun = regAttr->getPersistenceFactory();
  printf(" persistence library1 = %s\n", lib);
  printf(" persistence function1 = %s\n", libFun);
  cout << "persistence library = " << regAttr->getPersistenceLibrary() << endl;
  cout << "persistence function = " << regAttr->getPersistenceFactory() << endl;
  PropertiesPtr pconfig = regAttr->getPersistenceProperties();
  if (pconfig != NULLPTR) {
    cout << " persistence property is not null" << endl;
    cout << " persistencedir = "
         << pconfig->find("PersistenceDirectory")->asChar() << endl;
    cout << " pagesize = " << pconfig->find("PageSize")->asChar() << endl;
    cout << " maxpagecount = " << pconfig->find("MaxPageCount")->asChar()
         << endl;
  }
  cout << "****Attributes of Root1 are correctly set****" << endl;

  RegionAttributesPtr raPtr2 = regPtr2->getAttributes();
  RegionAttributes* regAttr2 = raPtr2.ptr();
  const char* lib2 = regAttr2->getPersistenceLibrary();
  const char* libFun2 = regAttr2->getPersistenceFactory();
  printf(" persistence library2 = %s\n", lib2);
  printf(" persistence function2 = %s\n", libFun2);
  cout << "persistence library = " << regAttr2->getPersistenceLibrary() << endl;
  cout << "persistence function = " << regAttr2->getPersistenceFactory()
       << endl;
  PropertiesPtr pconfig2 = regAttr2->getPersistenceProperties();
  if (pconfig2 != NULLPTR) {
    cout << " persistence property is not null for Root2" << endl;
    cout << " persistencedir2 = "
         << pconfig2->find("PersistenceDirectory")->asChar() << endl;
    cout << " pagesize2 = " << pconfig->find("PageSize")->asChar() << endl;
    cout << " maxpagecount2 = " << pconfig->find("MaxPageCount")->asChar()
         << endl;
  }
  cout << "Destroy region" << endl;
  try {
    regPtr1->localDestroyRegion();
    regPtr2->localDestroyRegion();
  } catch (Exception& ex) {
    ex.showMessage();
    ex.printStackTrace();
    return -1;
  }

  regPtr1 = NULLPTR;
  regPtr2 = NULLPTR;

  if (!cptr->isClosed()) {
    cptr->close();
    cptr = NULLPTR;
  }
  ////////////////////////////testing of cache.xml completed///////////////////

  cout << "Create cache with the configurations provided in the "
          "invalid_overflowAttr1.xml."
       << endl;
  cout << "This is a well-formed xml....attributes not provided for "
          "persistence manager. exception should be thrown"
       << endl;

  try {
    std::string filePath = directory + "/invalid_overflowAttr1.xml";
    cptr = cacheFactory->set("cache-xml-file", filePath.c_str())->create();
    return -1;
  } catch (Exception& ex) {
    cout << endl;
    ex.showMessage();
    ex.printStackTrace();
  }

  ///////////////testing of invalid_cache1.xml completed///////////////////

  cout << "Create cache with the configurations provided in the "
          "invalid_overflowAttr2.xml."
       << endl;
  cout << " This is a well-formed xml....attribute values is not provided for "
          "persistence library name......should throw an exception"
       << endl;

  try {
    std::string filePath = directory + "/invalid_overflowAttr2.xml";
    cptr = cacheFactory->set("cache-xml-file", filePath.c_str())->create();
    return -1;
  } catch (CacheXmlException& ex) {
    cout << endl;
    ex.showMessage();
    ex.printStackTrace();
  }

  ///////////////testing of invalid_cache2.xml completed///////////////////

  cout << "Create cache with the configurations provided in the "
          "invalid_overflowAttr3.xml."
       << endl;

  cout << "This is a well-formed xml....but region-attributes for persistence "
          "invalid......should throw an exception"
       << endl;

  try {
    std::string filePath = directory + "/invalid_overflowAttr3.xml";
    cptr = cacheFactory->set("cache-xml-file", filePath.c_str())->create();
    return -1;
  } catch (Exception& ex) {
    cout << endl;
    ex.showMessage();
    ex.printStackTrace();
  }

  ///////////////testing of invalid_cache3.xml completed///////////////////
  cout << "disconnecting..." << endl;
  try {
    cout << "just before disconnecting..." << endl;
    if (cptr != NULLPTR && !cptr->isClosed()) {
      cptr->close();
      cptr = NULLPTR;
    }
  } catch (Exception& ex) {
    ex.showMessage();
    ex.printStackTrace();
    return -1;
  }
  cout << "done with test" << endl;
  cout << "Test successful!" << endl;
  return 0;
}

BEGIN_TEST(ValidXmlTestOverFlow)
  {
    int res = testXmlCacheCreationWithOverflow();
    if (res != 0) {
      FAIL("Test Failed.");
    }
  }
END_TEST(ValidXmlTestOverFlow)

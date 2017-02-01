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

#define ROOT_NAME "testCache"

#include <gfcpp/GeodeCppCache.hpp>
#include <CacheRegionHelper.hpp>
#include "fw_helper.hpp"

using namespace apache::geode::client;

using namespace test;
BEGIN_TEST(CacheFunction)
  char* host_name = (char*)"TESTCACHE";
  uint16_t port ATTR_UNUSED = 0;
  const uint32_t totalSubRegions = 3;
  char* regionName = (char*)"TESTCACHE_ROOT_REGION";
  char* subRegionName1 = (char*)"TESTCACHE_SUB_REGION1";
  char* subRegionName2 = (char*)"TESTCACHE_SUB_REGION2";
  char* subRegionName21 = (char*)"TESTCACHE_SUB_REGION21";
  CachePtr cptr;
  if (cptr != NULLPTR) {
    cout << "cptr is not null" << endl;
  }
  cout << "create Cache with name=" << host_name << " and unitialized system"
       << endl;
  CacheFactoryPtr cacheFactoryPtr = CacheFactory::createCacheFactory();
  cptr = cacheFactoryPtr->create();
  AttributesFactory attrFac;
  RegionAttributesPtr rAttr;
  cout << "create RegionAttributes" << endl;
  try {
    rAttr = attrFac.createRegionAttributes();
  } catch (Exception& ex) {
    cout << ex.getMessage() << endl;
    ASSERT(false, "attribute create failed");
  }
  if (rAttr == NULLPTR) {
    cout << "Warnning! : AttributesFactory returned NULL" << endl;
  }
  RegionPtr rptr;
  if (rptr != NULLPTR) {
    cout << "rptr is not null" << endl;
  }
  cout << "create Region with name=" << regionName << endl;
  try {
    CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cptr.ptr());
    cacheImpl->createRegion(regionName, rAttr, rptr);
  } catch (Exception& ex) {
    cout << ex.getMessage() << endl;
    ASSERT(false, (char*)"attribute create failed");
  }
  cout << "create Sub Region with name=" << subRegionName1 << endl;
  RegionPtr subRptr1;
  try {
    subRptr1 = rptr->createSubregion(subRegionName1, rAttr);
  } catch (Exception& ex) {
    cout << ex.getMessage() << endl;
    ASSERT(false, (char*)"subregion create failed");
  }
  cout << "create Sub Region with name=" << subRegionName2 << endl;
  RegionPtr subRptr2;
  try {
    subRptr2 = rptr->createSubregion(subRegionName2, rAttr);
  } catch (Exception& ex) {
    cout << ex.getMessage() << endl;
    ASSERT(false, (char*)"subregion create failed");
  }
  cout << "create Sub Region with name=" << subRegionName21
       << "inside region=" << subRegionName2 << endl;
  RegionPtr subRptr21;
  try {
    subRptr21 = subRptr2->createSubregion(subRegionName21, rAttr);
  } catch (Exception& ex) {
    cout << ex.getMessage() << endl;
    ASSERT(false, (char*)"subregion create failed");
  }
  VectorOfRegion vr;
  rptr->subregions(true, vr);
  cout << "  vr.size=" << vr.size() << endl;
  ASSERT(vr.size() == totalSubRegions, "Number of Subregions does not match");
  cout << "sub regions:" << endl;
  uint32_t i = 0;
  for (i = 0; i < static_cast<uint32_t>(vr.size()); i++) {
    cout << "vc[" << i << "]=" << vr.at(i)->getName() << endl;
  }
  vr.clear();
  VectorOfRegion vrp;
  cout << "get cache root regions" << endl;
  cptr->rootRegions(vrp);
  cout << "  vrp.size=" << vrp.size() << endl;
  cout << "root regions in Cache:" << endl;
  for (i = 0; i < static_cast<uint32_t>(vrp.size()); i++) {
    cout << "vc[" << i << "]=" << vrp.at(i)->getName() << endl;
  }
  vr.clear();
  std::string root(regionName);
  std::string subRegion2(subRegionName2);
  std::string subRegion1(subRegionName1);
  std::string subRegion21(subRegionName21);
  std::string sptor("/");
  subRegion2 = root + sptor + subRegion2;
  cout << "subRegion2=" << subRegion2.c_str() << endl;
  subRegion1 = root + sptor + subRegion1;
  cout << "subRegion1=" << subRegion1.c_str() << endl;
  subRegion21 = subRegion2 + sptor + subRegion21;
  cout << "subRegion21=" << subRegion21.c_str() << endl;
  RegionPtr region;
  cout << "find region:" << regionName << endl;
  try {
    region = cptr->getRegion(root.c_str());
  } catch (Exception& ex) {
    cout << ex.getMessage() << endl;
    ASSERT(false, (char*)"getRegion");
  }
  if (region == NULLPTR) {
    ASSERT(false, (char*)"did not find it");
  } else {
    cout << "found :" << region->getName() << endl;
  }
  cout << "find region:" << subRegionName1 << endl;
  try {
    region = cptr->getRegion(subRegion1.c_str());
  } catch (Exception& ex) {
    cout << ex.getMessage() << endl;
    ASSERT(false, (char*)"getRegion");
  }
  if (region == NULLPTR) {
    ASSERT(false, (char*)"did not find it");
  } else {
    cout << "found :" << region->getName() << endl;
  }
  cout << "find region:" << subRegionName21 << endl;
  try {
    region = cptr->getRegion(subRegion21.c_str());
  } catch (Exception& ex) {
    cout << ex.getMessage() << endl;
    ASSERT(false, (char*)"getRegion");
  }
  if (region == NULLPTR) {
    ASSERT(false, (char*)"did not find it");
  } else {
    cout << "found :" << region->getName() << endl;
  }
  subRegion21 = sptor + subRegion21;
  cout << "find region:" << subRegionName21 << endl;
  try {
    region = cptr->getRegion(subRegion21.c_str());
  } catch (Exception& ex) {
    cout << ex.getMessage() << endl;
    ASSERT(false, (char*)"getRegion");
  }
  if (region == NULLPTR) {
    ASSERT(false, (char*)"did not find it");
  } else {
    cout << "found :" << region->getName() << endl;
  }
  const char notExist[] = "/NotExistentRegion";
  cout << "find region:" << notExist << endl;
  try {
    region = cptr->getRegion(notExist);
  } catch (Exception& ex) {
    cout << ex.getMessage() << endl;
    ASSERT(false, (char*)"getRegion");
  }
  if (region == NULLPTR) {
    cout << "not found !" << endl;
  } else {
    ASSERT(false, (char*)"found it");
  }
END_TEST(CacheFunction)

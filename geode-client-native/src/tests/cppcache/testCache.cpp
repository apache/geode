/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/GemfireCppCache.hpp>
#include "fw_helper.hpp"

using namespace gemfire;


using namespace test;
BEGIN_TEST(CacheFunction)
   char* host_name = (char*)"TESTCACHE";
   char* host = NULL;
   uint16_t port=0;
   const uint32_t totalSubRegions = 3;
   char* regionName = (char*)"TESTCACHE_ROOT_REGION";
   char* subRegionName1 = (char*)"TESTCACHE_SUB_REGION1";
   char* subRegionName2 = (char*)"TESTCACHE_SUB_REGION2";
   char* subRegionName21 = (char*)"TESTCACHE_SUB_REGION21";
   DistributedSystemPtr dsys;
   bool exception_occured=false;
   cout <<"create DistributedSytem with null name (sock interface)"<< endl;
   try {
     dsys = DistributedSystem::connect( host );
   } catch (IllegalArgumentException& ex) {
     cout << "exception occured"<<endl;
     cout << ex.getMessage() <<endl;
     exception_occured=true;
   }
   ASSERT(exception_occured == true, (char*)"Exception Did not occur" );
   exception_occured=false;
   cout <<"create DistributedSytem with name="<< host_name <<endl;
   try {
     dsys = DistributedSystem::connect(host_name );
   } catch (Exception& ex) {
     cout << ex.getMessage() <<endl;
     ASSERT(false, "connect failed" );
   }
   if (dsys == NULLPTR) {
     cout <<"dynamic cast failed"<<endl;
   }
   else
   {
     cout <<"dynamic cast succeeded"<<endl;
   }
   CachePtr cptr;
   if (cptr != NULLPTR) {
      cout<<"cptr is not null"<<endl;
   }
   cout <<"create Cache with name="<< host_name <<" and unitialized system"<<endl;
   DistributedSystemPtr dsysUnitialized;
   try {
     cptr = CacheFactory::create(host_name, dsysUnitialized);
   } catch (Exception& ex) {
     cout << ex.getMessage() <<endl;
     exception_occured=true;
   }
   ASSERT(exception_occured == true, (char*)"Exception Did not occur" );
   cout <<"create Cache with name="<< host_name <<endl;
   try {
     cptr = CacheFactory::create(host_name, dsys);
   } catch (Exception& ex) {
     cout << ex.getMessage() <<endl;
     ASSERT(false, "cache create failed" );
   }
   cout <<"create DUPLICATEED Cache with name="<< host_name <<endl;
   CachePtr cptrDup;
   try {
     cptrDup = CacheFactory::create(host_name, dsys);
   } catch (Exception& ex) {
     cout << ex.getMessage() <<endl;
     exception_occured=true;
   }
   ASSERT(exception_occured == true, (char*)"Exception Did not occur" );
   AttributesFactory attrFac;
   attrFac.setScope(ScopeType::LOCAL);
   RegionAttributesPtr rAttr;
   cout <<"create RegionAttributes"<<endl;
   try {
     rAttr = attrFac.createRegionAttributes();
   } catch(Exception& ex){
     cout << ex.getMessage() <<endl;
     ASSERT(false, "attribute create failed" );
   }
   if (rAttr == NULLPTR) {
     cout <<"Warnning! : AttributesFactory returned NULL"<<endl;
   }
   RegionPtr rptr;
   if (rptr != NULLPTR) {
      cout<<"rptr is not null"<<endl;
   }
   cout <<"create Region with name="<< regionName<<endl;
   try {
     rptr = cptr->createRegion(regionName, rAttr);
   } catch (Exception& ex){
     cout << ex.getMessage() <<endl;
     ASSERT(false, (char*)"attribute create failed" );
   }
   cout <<"create Sub Region with name="<< subRegionName1<<endl;
   RegionPtr subRptr1;
   try {
     subRptr1 = rptr->createSubregion(subRegionName1, rAttr);
   } catch (Exception& ex){
     cout << ex.getMessage() <<endl;
     ASSERT(false, (char*)"subregion create failed" );
   }
   cout <<"create Sub Region with name="<< subRegionName2<<endl;
   RegionPtr subRptr2;
   try {
     subRptr2 = rptr->createSubregion(subRegionName2, rAttr);
   } catch (Exception& ex){
     cout << ex.getMessage() <<endl;
     ASSERT(false, (char*)"subregion create failed" );
   }
   cout <<"create Sub Region with name="<< subRegionName21<<"inside region="<<subRegionName2<<endl;
   RegionPtr subRptr21;
   try {
     subRptr21 = subRptr2->createSubregion(subRegionName21, rAttr);
   } catch (Exception& ex){
     cout << ex.getMessage() <<endl;
     ASSERT(false, (char*)"subregion create failed" );
   }
   VectorOfRegion vr;
   rptr->subregions(true, vr);
   cout << "  vr.size="<<vr.size()<<endl;
   ASSERT(vr.size()== totalSubRegions, "Number of Subregions does not match");
   cout<<"sub regions:"<<endl;
   uint32_t i=0;
   for(i=0; i< (uint32_t)vr.size(); i++)
   {
       cout <<"vc["<<i<<"]="<<vr.at(i)->getName()<<endl;
   }
   vr.clear();
   VectorOfRegion vrp;
   cout<<"get cache root regions"<<endl;
   cptr->rootRegions(vrp);
   cout << "  vrp.size="<<vrp.size()<<endl;
   cout<<"root regions in Cache:"<<endl;
   for(i=0; i< (uint32_t)vrp.size(); i++)
   {
       cout <<"vc["<<i<<"]="<<vrp.at(i)->getName()<<endl;
   }
   vr.clear();
   std::string root(regionName);
   std::string subRegion2(subRegionName2);
   std::string subRegion1(subRegionName1);
   std::string subRegion21(subRegionName21);
   std::string sptor("/");
   subRegion2 = root + sptor + subRegion2;
   cout<<"subRegion2="<<subRegion2.c_str()<<endl;
   subRegion1 = root + sptor + subRegion1;
   cout<<"subRegion1="<<subRegion1.c_str()<<endl;
   subRegion21 = subRegion2 + sptor + subRegion21;
   cout<<"subRegion21="<<subRegion21.c_str()<<endl;
   RegionPtr region;
   cout << "find region:"<< regionName<<endl;
   try {
      region = cptr->getRegion(root.c_str());
   } catch (Exception& ex){
     cout << ex.getMessage() <<endl;
     ASSERT(false, (char*)"getRegion");
   }
   if (region == NULLPTR) {
     ASSERT(false,  (char*)"did not find it");
   }
   else
   {
       cout << "found :"<<region->getName()<<endl;
   }
   cout << "find region:"<< subRegionName1<<endl;
   try {
      region = cptr->getRegion(subRegion1.c_str());
   } catch (Exception& ex){
     cout << ex.getMessage() <<endl;
     ASSERT(false, (char*)"getRegion");
   }
   if (region == NULLPTR) {
     ASSERT(false,  (char*)"did not find it");
   }
   else
   {
       cout << "found :"<<region->getName()<<endl;
   }
   cout << "find region:"<< subRegionName21<<endl;
   try {
      region = cptr->getRegion(subRegion21.c_str());
   } catch (Exception& ex){
     cout << ex.getMessage() <<endl;
     ASSERT(false, (char*)"getRegion");
   }
   if (region == NULLPTR) {
     ASSERT(false,  (char*)"did not find it");
   }
   else
   {
       cout << "found :"<<region->getName()<<endl;
   }
   subRegion21 = sptor + subRegion21;
   cout << "find region:"<< subRegionName21<<endl;
   try {
      region = cptr->getRegion(subRegion21.c_str());
   } catch (Exception& ex){
     cout << ex.getMessage() <<endl;
     ASSERT(false, (char*)"getRegion");
   }
   if (region == NULLPTR) {
     ASSERT(false,  (char*)"did not find it");
   }
   else
   {
       cout << "found :"<<region->getName()<<endl;
   }
   const char notExist[] = "/NotExistentRegion";
   cout << "find region:"<< notExist<<endl;
   try {
      region = cptr->getRegion(notExist);
   } catch (Exception& ex){
     cout << ex.getMessage() <<endl;
     ASSERT(false, (char*)"getRegion");
   }
   if (region == NULLPTR) {
       cout << "not found !"<<endl;
   }
   else
   {
     ASSERT(false,  (char*)"found it");
   }

   DistributedSystem::disconnect();
END_TEST(CacheFunction)

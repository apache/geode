/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * pdxTests.hpp
 *
 */

#ifndef PDXTESTS_HPP_
#define PDXTESTS_HPP_

#include "GemfireCppCache.hpp"

#include "CacheableString.hpp"
#include "Cache.hpp"
#include "Region.hpp"

#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/TestClient.hpp"
#include "fwklib/ClientTask.hpp"
#include "fwklib/FwkLog.hpp"
#include "testobject/PdxClassV1.hpp"
#include "testobject/PdxClassV2.hpp"
#include "testobject/PdxType.hpp"
#include "testobject/PdxVersioned1.hpp"
#include "testobject/PdxVersioned2.hpp"
#include "testobject/VariousPdxTypes.hpp"
#include "testobject/NestedPdxObject.hpp"
#include "testobject/PdxTypeWithAuto.hpp"
#include "pdxautoserializerclass/AutoPdxVersioned1.hpp"
#include "pdxautoserializerclass/AutoPdxVersioned2.hpp"
#include "testobject/PdxClassV1WithAuto.hpp"
#include "testobject/PdxClassV2WithAuto.hpp"

#include <stdlib.h>
#include <map>
#include <algorithm>
//extern PdxSerializable * createPdxType();
using namespace AutoPdxTests;
using namespace PdxTestsAuto;
namespace gemfire {
 namespace testframework {
   namespace pdxtests {

   class PutGetTask: public ClientTask {
   protected:
     RegionPtr m_region;
     uint32_t m_MaxKeys;
     FrameworkTest * m_test;
     AtomicInc m_Cntr;
     ACE_TSS<perf::Counter> m_count;
     ACE_TSS<perf::Counter> m_MyOffset;
     uint32_t m_iters;

   public:

     PutGetTask(RegionPtr reg, uint32_t max, FrameworkTest * test) :
       m_region(reg), m_MaxKeys(max), m_test(test),m_MyOffset(), m_iters(100) {
     }

     virtual bool doSetup(int32_t id) {
       // per thread iteration offset
       double max = m_MaxKeys;
       srand((++m_Cntr * id) + (unsigned int)time(0));
       m_MyOffset->add((int) (((max * rand()) / (RAND_MAX + 1.0))));
       if (m_Iterations > 0)
         m_Loop = m_Iterations;
       else
         m_Loop = -1;
       return true;
     }

     virtual void doCleanup(int32_t id) {
     }
     virtual ~PutGetTask() {
     }
   };
   class PdxEntryTask: public PutGetTask {
     AtomicInc m_cnt;
     AtomicInc m_create;
     AtomicInc m_update;
     AtomicInc m_putall;
     AtomicInc m_get;
     AtomicInc m_invalidate;
     AtomicInc m_localInvalidate;
     AtomicInc m_destroy;
     AtomicInc m_localDestroy;
     FrameworkTest * m_test;
     bool m_isSerialExecution;
     bool m_isEmptyClient;
     bool m_isThinClient;
     CacheableHashMapPtr MyMap;
     CacheableHashSetPtr m_destroyKey;
     std::string m_objectType;
     int32_t m_version;
    

     ACE_Recursive_Thread_Mutex m_lock;
   public:
       PdxEntryTask(RegionPtr reg, int32_t keyCnt,CacheableHashMapPtr MyMap1,CacheableHashSetPtr destKey,bool isSerial,std::string objType,int32_t verNo,FrameworkTest * test) :
       PutGetTask(reg, keyCnt,test),MyMap(MyMap1), m_destroyKey(destKey),m_isSerialExecution(isSerial),m_objectType(objType),m_version(verNo), m_cnt(0),m_create(0),m_update(0),m_putall(0),m_get(0),m_invalidate(0),m_localInvalidate(0),m_destroy(0),m_localDestroy(0),
       m_test(test)
       {}
   void validate(CacheableKeyPtr key,SerializablePtr pdxVal,int32_t beforeSize){
     if(m_isSerialExecution)
     {
       if(m_isEmptyClient)
       {
         verifySize(m_region,0);
       }
       else if(m_isThinClient)
       {
    	 if(m_test->gettxManager() != NULLPTR){
           verifyContainsKey(m_region,key,true);
           verifyContainsValueForKey(m_region, key, true);
    	 }
       }
       else
       { // region has all keys/values
         verifyContainsKey(m_region, key, true);
         verifyContainsValueForKey(m_region, key, true);
         verifySize(m_region, beforeSize);
       }
       //ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
       MyMap->update(key,pdxVal);
       int32_t destSize= (int32_t)m_destroyKey->size();
       if(destSize != 0){
    	   for (CacheableHashSet::Iterator it = m_destroyKey->begin(); it != m_destroyKey->end(); it++){
    		   CacheableKeyPtr key1 = *it;
    		   if(strcmp(key->toString()->asChar(), key1->toString()->asChar()) == 0){
    		   	 m_destroyKey->erase(key);
    		   	 break;
    		   }
    	   }
       }
     }
   }

   void addEntry(RegionPtr region,int32_t idx){
     //char buf[128];
    // sprintf(buf, "Key-%d", idx);
     CacheableKeyPtr key = CacheableKey::create(idx);
     PdxSerializablePtr pdxVal= GetValue(idx);
     int32_t beforeSize=0;
     if(m_isEmptyClient)
     {
       VectorOfCacheableKey keyVec;
       region->serverKeys(keyVec);
       beforeSize=keyVec.size();
     }
     else
       beforeSize=region->size();

     region->put(key,pdxVal);
     FWKINFO("In addEntry Key= " << key->toString()->asChar() );//<< " Value= " << pdxVal->toString());
     validate(key,pdxVal,beforeSize+1);
       //In order to determine value
          
  }

  void getKey(RegionPtr region,int32_t idx){
    CacheableKeyPtr key = GetExistingKey(m_isEmptyClient || m_isThinClient,idx);
    int beforeSize=0;
    beforeSize=region->size();
    bool beforeContainsValueForKey = region->containsValueForKey(key);
    CacheablePtr pdxVal;
    try
    {
      pdxVal=region->get(key);
    }
    catch(KeyNotFoundException e)
    {
      if(pdxVal != NULLPTR)
    	  throw new Exception(e.getMessage());
    }
	if(m_isSerialExecution)
	{
		if(m_isEmptyClient)
		{
		  verifySize(region,0);
		}
		else if(m_isThinClient)
		{
		  if(m_test->gettxManager() != NULLPTR)
		    verifyContainsKey(region,key,true);
		}
		else
		{ // we have all keys/values
		  verifyContainsKey(region, key, true);
		  verifyContainsValueForKey(region, key,(beforeContainsValueForKey));
		  verifySize(region, beforeSize);
		}

		MyMap->update(key,pdxVal);
		int32_t destSize= (int32_t)m_destroyKey->size();
		if(destSize != 0){
		for (CacheableHashSet::Iterator it = m_destroyKey->begin(); it != m_destroyKey->end(); it++){
				   CacheableKeyPtr key1 = *it;
				   if(strcmp(key->toString()->asChar(), key1->toString()->asChar()) == 0){
				     m_destroyKey->erase(key);
				     break;
				   }
			   }
		}
	}
  }
       
  void updateEntry(RegionPtr region,int32_t idx){
    CacheableKeyPtr key = GetExistingKey(m_isEmptyClient || m_isThinClient,idx);
    SerializablePtr pdxVal= GetValue(idx);
    /*std::string pdxValStr;
    if(typeid(pdxVal) == typeid(PdxSerializablePtr)){
    	PdxSerializablePtr pdxVal=dynCast<PdxSerializablePtr>(pdxVal);
        //pdxValStr = pdxVal->getClassName();
    }*/
    int32_t beforeSize=0;
    if(m_isEmptyClient)
    {
      VectorOfCacheableKey keyVec;
      region->serverKeys(keyVec);
      beforeSize=keyVec.size();
    }
    else
      beforeSize=region->size();
   // FWKINFO("upating key= " << key->toString()->asChar()<<" and value is= "<< pdxValStr);
    region->put(key,pdxVal);
    validate(key,pdxVal,beforeSize);
 }

 void invalidateEntry(RegionPtr region,bool isLocalInvalidate,int32_t idx){
   CacheableKeyPtr key=GetExistingKey(m_isEmptyClient || m_isThinClient,idx);
   CacheablePtr pdxVal=region->get(key);
   /*if(typeid(pdxVal) == typeid(PdxSerializablePtr)){
       	PdxSerializablePtr pdxVal=dynCast<PdxSerializablePtr>(pdxVal);
   }*/
  // PdxSerializablePtr pdxVal=dynCast<PdxSerializablePtr>(region->get(key));
   int32_t beforeSize=0;
   if(m_isEmptyClient){
     VectorOfCacheableKey keyBVec;
     region->serverKeys(keyBVec);
     beforeSize=keyBVec.size();
   }
   else
     beforeSize=region->size();
   //bool containKey = region->containsKey(key);
   //bool containValueForKey = region->containsValueForKey(key);
   try{
     if(isLocalInvalidate){
       region->localInvalidate(key);
     }
   else{
     region->invalidate(key);
   }
   if(m_isSerialExecution)
   {
     if(m_isEmptyClient){
       verifySize(region,0);
     }
     else if(m_isThinClient){
       if(m_test->gettxManager() != NULLPTR)
         verifySize(region,beforeSize);
     }
     else
     { // region has all keys/values
        verifyContainsKey(region, key, true);
        verifyContainsValueForKey(region, key, false);
        verifySize(region, beforeSize);
     }
       //ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
       MyMap->update(key,NULLPTR);
       int32_t destSize= (int32_t)m_destroyKey->size();
       if(destSize != 0){
    	   for (CacheableHashSet::Iterator it = m_destroyKey->begin(); it != m_destroyKey->end(); it++){
    	       		   CacheableKeyPtr key1 = *it;
    	       		if(strcmp(key->toString()->asChar(), key1->toString()->asChar()) == 0){
    	       			 m_destroyKey->erase(key);
    	       			 break;
    	       		   }
    	       	   }
       }
     }
   }
   catch(EntryNotFoundException e){

     if(m_isSerialExecution){
    	//validate(key,pdxVal,beforeSize+1);
       //throw new Exception(e.getMessage());
     }else
     {
       FWKINFO("Caught this "<< e.getMessage()<<" Expected with concurrent execution,hence continuing with the test");
       return;
     }
   }

 }

 void destroyEntry(RegionPtr region,bool isLocalDestroy,int32_t idx){
   CacheableKeyPtr key=GetExistingKey(m_isEmptyClient || m_isThinClient,idx);
   int beforeSize=0;
   if(m_isEmptyClient){
     VectorOfCacheableKey keyBVec;
     region->serverKeys(keyBVec);
     beforeSize=keyBVec.size();
   }
   else
     beforeSize=region->size();
   try{
     if(isLocalDestroy){
       region->localDestroy(key);
     }
     else{
       region->destroy(key);
     }
     if(m_isSerialExecution){
       if(m_isEmptyClient){
    	 verifySize(region,0);
       }
       else if(m_isThinClient){
    	   if(m_test->gettxManager() != NULLPTR){
			 verifyContainsKey(region,key,false);
			 verifyContainsValueForKey(region, key, false);
			 VectorOfCacheableKey keyAVec;
			 region->serverKeys(keyAVec);
			 int afterSize=keyAVec.size();
			 if((afterSize != beforeSize) && (afterSize != beforeSize - 1)){
			   FWKEXCEPTION("Expected region size "<< afterSize<<" to be either "<<beforeSize << " or "<< (beforeSize-1));
			 }
    	   }
       }
       else
       { // region has all keys/values
         verifyContainsKey(m_region, key, false);
         verifyContainsValueForKey(m_region, key, false);
         verifySize(m_region, beforeSize - 1);
       }
       //ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
       MyMap->erase(key);
       m_destroyKey->insert(key);
     }
   }
   catch(EntryNotFoundException e){
     if(m_isSerialExecution){
       //throw new Exception(e.getMessage());
     }else
     {
       FWKINFO("Caught this "<< e.getMessage()<<" Expected with concurrent execution,hence continuing with the test");
       return;
     }
   }
 }

 /*bool checkKeyDestroyed(int32_t index)
 {
     bool isDestroyed = false;
	 int32_t destSize= (int32_t)m_destroyKey->size();
	 if(destSize != 0){
	   for (CacheableHashSet::Iterator it = m_destroyKey->begin(); it != m_destroyKey->end(); it++){
	   	  CacheableKeyPtr key1 = *it;
	      if(key1 != NULLPTR){
	    	  isDestroyed = true;
	    	  break;
	      }
	   }
	 }
	  return  isDestroyed;
 }*/
 CacheableKeyPtr GetExistingKey(bool useServerKeys,int32_t index)
 {
   RegionPtr region=m_region;
   CacheableKeyPtr key=NULLPTR;
   VectorOfCacheableKey Exkeys;
   /*bool isdestroy = checkKeyDestroyed(index);
   if(isdestroy){
	   key = CacheableKey::create(index);
	   return key;
   }*/
   if(useServerKeys)
   {
    try{
     m_region->serverKeys(Exkeys);
     key=Exkeys.at(index);
    }
   catch(Exception e){FWKEXCEPTION("Caught during GetExistingKey "<< e.getMessage());}
   }
   else {
	   m_region->keys(Exkeys);
	   key=Exkeys.at(index);
   }
   return key;
 }

 PdxInstancePtr createpdxInstance()
 {
   PdxTests::PdxTypePtr pdxobj(new PdxTests::PdxType());
   PdxInstanceFactoryPtr pdxIF = m_region->getCache()->createPdxInstanceFactory("PdxTests.PdxType");
   pdxIF->writeInt("m_int32", pdxobj->getInt());
   pdxIF->writeLong("m_long", pdxobj->getLong());
   pdxIF->writeFloat("m_float", pdxobj->getFloat());
   pdxIF->writeDouble("m_double", pdxobj->getDouble());
   pdxIF->writeString("m_string", pdxobj->getString());
   pdxIF->writeByte("m_byte", pdxobj->getByte());
   pdxIF->writeShort("m_int16", pdxobj->getShort());
   pdxIF->writeChar("m_char", (char)pdxobj->getChar());
   pdxIF->writeInt("m_uint32", pdxobj->getUInt());
   pdxIF->writeBoolean("m_bool", pdxobj->getBool());
   pdxIF->writeLong("m_ulong", pdxobj->getULong());
   pdxIF->writeShort("m_uint16", pdxobj->getUint16());
   pdxIF->writeByte("m_sbyte", pdxobj->getSByte());
   pdxIF->writeDate("m_dateTime", pdxobj->getDate());
   pdxIF->writeObject("m_map", pdxobj->getHashMap());
   pdxIF->writeObject("m_pdxEnum", pdxobj->getEnum());
   pdxIF->writeObject("m_arraylist", pdxobj->getArrayList());
   pdxIF->writeObject("m_hashtable", pdxobj->getHashTable());
   pdxIF->writeObject("m_vector", pdxobj->getVector());
   pdxIF->writeObject("m_chs", pdxobj->getHashSet());
   pdxIF->writeObject("m_clhs", pdxobj->getLinkedHashSet());
   pdxIF->writeObject("m_address", pdxobj->getCacheableObjectArray());
   pdxIF->writeBooleanArray("m_boolArray", pdxobj->getBoolArray(), 3);
   pdxIF->writeByteArray("m_byteArray", pdxobj->getByteArray(), 2);
   pdxIF->writeShortArray("m_int16Array", pdxobj->getShortArray(), 2);
   pdxIF->writeIntArray("m_int32Array", pdxobj->getIntArray(), 4);
   pdxIF->writeLongArray("m_longArray", pdxobj->getLongArray(), 2);
   pdxIF->writeFloatArray("m_floatArray", pdxobj->getFloatArray(), 2);
   pdxIF->writeDoubleArray("m_doubleArray", pdxobj->getDoubleArray(), 2);
   pdxIF->writeIntArray("m_uint32Array", pdxobj->getUIntArray(), 4);
   pdxIF->writeLongArray("m_ulongArray", pdxobj->getULongArray(), 2);
   pdxIF->writeByteArray("m_byte252",pdxobj->getByte252(), 252);
   pdxIF->writeByteArray("m_byte253",pdxobj->getByte253(), 253);
   pdxIF->writeByteArray("m_byte65535",pdxobj->getByte65535(), 65535);
   pdxIF->writeByteArray("m_byte65536",pdxobj->getByte65536(), 65536);
   pdxIF->writeStringArray("m_stringArray", pdxobj->getStringArray(), 2);
   pdxIF->writeByteArray("m_sbyteArray", pdxobj->getSByteArray(), 2);
   pdxIF->writeShortArray("m_uint16Array", pdxobj->getUInt16Array(), 2);
   pdxIF->writeObjectArray("m_objectArray", pdxobj->getCacheableObjectArray());

   PdxInstancePtr pi=pdxIF->create();
   return pi;
 }
 PdxInstancePtr createAutopdxInstance()
 {
   PdxTestsAuto::PdxTypePtr pdxobj(new PdxTestsAuto::PdxType());
   PdxInstanceFactoryPtr pdxIF = m_region->getCache()->createPdxInstanceFactory("PdxTestsAuto.PdxType");
   pdxIF->writeInt("m_int32", pdxobj->getInt());
   pdxIF->writeLong("m_long", pdxobj->getLong());
   pdxIF->writeFloat("m_float", pdxobj->getFloat());
   pdxIF->writeDouble("m_double", pdxobj->getDouble());
   pdxIF->writeString("m_string", pdxobj->getString());
   pdxIF->writeByte("m_byte", pdxobj->getByte());
   pdxIF->writeShort("m_int16", pdxobj->getShort());
   pdxIF->writeChar("m_char", (char)pdxobj->getChar());
   pdxIF->writeInt("m_uint32", pdxobj->getUInt());
   pdxIF->writeBoolean("m_bool", pdxobj->getBool());
   pdxIF->writeLong("m_ulong", pdxobj->getULong());
   pdxIF->writeShort("m_uint16", pdxobj->getUint16());
   pdxIF->writeByte("m_sbyte", pdxobj->getSByte());
   pdxIF->writeDate("m_dateTime", pdxobj->getDate());
   pdxIF->writeObject("m_map", pdxobj->getHashMap());
   pdxIF->writeObject("m_pdxEnum", pdxobj->getEnum());
   pdxIF->writeObject("m_arraylist", pdxobj->getArrayList());
   pdxIF->writeObject("m_hashtable", pdxobj->getHashTable());
   pdxIF->writeObject("m_vector", pdxobj->getVector());
   pdxIF->writeObject("m_chs", pdxobj->getHashSet());
   pdxIF->writeObject("m_clhs", pdxobj->getLinkedHashSet());
   pdxIF->writeObject("m_address", pdxobj->getCacheableObjectArray());
   pdxIF->writeBooleanArray("m_boolArray", pdxobj->getBoolArray(), 3);
   pdxIF->writeByteArray("m_byteArray", pdxobj->getByteArray(), 2);
   pdxIF->writeShortArray("m_int16Array", pdxobj->getShortArray(), 2);
   pdxIF->writeIntArray("m_int32Array", pdxobj->getIntArray(), 4);
   pdxIF->writeLongArray("m_longArray", pdxobj->getLongArray(), 2);
   pdxIF->writeFloatArray("m_floatArray", pdxobj->getFloatArray(), 2);
   pdxIF->writeDoubleArray("m_doubleArray", pdxobj->getDoubleArray(), 2);
   pdxIF->writeIntArray("m_uint32Array", pdxobj->getUIntArray(), 4);
   pdxIF->writeLongArray("m_ulongArray", pdxobj->getULongArray(), 2);
   pdxIF->writeByteArray("m_byte252",pdxobj->getByte252(), 252);
   pdxIF->writeByteArray("m_byte253",pdxobj->getByte253(), 253);
   pdxIF->writeByteArray("m_byte65535",pdxobj->getByte65535(), 65535);
   pdxIF->writeByteArray("m_byte65536",pdxobj->getByte65536(), 65536);
   pdxIF->writeStringArray("m_stringArray", pdxobj->getStringArray(), 2);
   pdxIF->writeByteArray("m_sbyteArray", pdxobj->getSByteArray(), 2);
   pdxIF->writeShortArray("m_uint16Array", pdxobj->getUInt16Array(), 2);
   pdxIF->writeObjectArray("m_objectArray", pdxobj->getCacheableObjectArray());
 
   PdxInstancePtr pi=pdxIF->create();
   return pi;
 }
SerializablePtr GetValue(int32_t value)
 {
	 SerializablePtr tempVal;
   char buf[252];
   sprintf(buf,"%d",value);
   if(m_objectType != ""){
	   if(m_objectType == "PdxVersioned" && m_version == 1)
	   {
		 PdxTests::PdxVersioned1Ptr pdxV1(new PdxTests::PdxVersioned1(buf));
		 tempVal = dynCast<PdxSerializablePtr>(pdxV1);
	   }
	   else if(m_objectType == "PdxVersioned" && m_version == 2)
	   {
		   PdxTests::PdxVersioned2Ptr pdxV2(new PdxTests::PdxVersioned2(buf));
		   tempVal = dynCast<PdxSerializablePtr>(pdxV2);
	   }
	   else if(m_objectType == "AutoPdxVersioned" && m_version == 1)
	   {
	      AutoPdxTests::AutoPdxVersioned1Ptr pdxV1(new AutoPdxTests::AutoPdxVersioned1(buf));
	      tempVal = dynCast<PdxSerializablePtr>(pdxV1);
	      FWKINFO("returning AutoPdxVersioned1 object");
	   }
	   else if(m_objectType == "AutoPdxVersioned" && m_version == 2)
	   {
	      AutoPdxTests::AutoPdxVersioned2Ptr pdxV2(new AutoPdxTests::AutoPdxVersioned2(buf));
	      tempVal = dynCast<PdxSerializablePtr>(pdxV2);
	      FWKINFO("returning AutoPdxVersioned2 object");
	   }
	   else if(m_objectType == "PdxType")
	   {
		 PdxTests::PdxTypePtr pdxTp(new PdxTests::PdxType());
		 tempVal = dynCast<PdxSerializablePtr>(pdxTp);
	   }
	   else if(m_objectType == "AutoPdxType")
	   {
		   PdxTestsAuto::PdxTypePtr pdxTp(new PdxTestsAuto::PdxType());
	   		 tempVal = dynCast<PdxSerializablePtr>(pdxTp);
	   		FWKINFO("returning AutoPdxType object");
	   }
	   else if(m_objectType == "Nested")
	   {
		 PdxTests::NestedPdxPtr nestedPtr(new PdxTests::NestedPdx(buf));
		 tempVal = dynCast<PdxSerializablePtr>(nestedPtr);
	   }
	   else if(m_objectType == "AutoNested")
	   {
	     AutoPdxTests::NestedPdxPtr nestedPtr(new AutoPdxTests::NestedPdx(buf));
	  	 tempVal = dynCast<PdxSerializablePtr>(nestedPtr);
	   }
	   else if(m_objectType == "PdxInstanceFactory")
	   {
		 PdxInstancePtr pdxInstVal=createpdxInstance();
		 tempVal = dynCast<PdxSerializablePtr>(pdxInstVal);
	   }
	   else if(m_objectType == "AutoPdxInstanceFactory")
	   {
	   	  PdxInstancePtr pdxInstVal=createAutopdxInstance();
	   	  tempVal = dynCast<PdxSerializablePtr>(pdxInstVal);
	   }
   }
   else
   {
	 int32_t valSize = m_test->getIntValue( "valueSizes" );
	 valSize = ( ( valSize < 0) ? 32 : valSize );
	 std::string val = GsRandom::getAlphanumericString( valSize - 1 );
	 tempVal = CacheableString::create( val.c_str() );
   }

   //std::string TypeName = tempVal->getClassName();
   return tempVal;
 } 
 
 void verifySize(RegionPtr region,int32_t expectedRegSize){
   int32_t size=0;
   size=region->size();
   if(size != expectedRegSize)
   {
     FWKEXCEPTION("Size = "<< size << "and expectedRegSize = "<<expectedRegSize);
   }
 }

 void verifyContainsKey(RegionPtr region,CacheableKeyPtr key,bool expected){
   FWKINFO("inside verifyContainsKey");
   bool containsKey=false;
   if(m_isEmptyClient){
     containsKey=region->containsKeyOnServer(key);
   }
   else
   {
	 containsKey=region->containsKey(key);
   }
  std::string containsKeyVal = "false";
  std::string expectedVal = "false";
  if(containsKey)
    containsKeyVal = "true";
  if(expected)
    expectedVal = "true"; 
  if(containsKey != expected)
   {
     FWKEXCEPTION("Expected ContainsKeys for key "<<key->toString()->asChar()<< "to be " << expectedVal << "but it is " << containsKey);
   }

 }
      
 void verifyContainsValueForKey(RegionPtr region,CacheableKeyPtr key,bool expected){
   FWKINFO("inside verifyContainsValueForKey");
   bool containsValueForKey = false;
   if (m_isEmptyClient)
     containsValueForKey = region->containsValueForKey(key);
   else
     containsValueForKey = region->containsValueForKey(key);
  std::string containsVal = "false";
  std::string expectedVal = "false";
  if(containsValueForKey)
    containsVal = "true";
  if(expected)
    expectedVal = "true"; 
   if (containsValueForKey != expected)
   {
     FWKEXCEPTION("Expected ContainsValueForKey() for "<< key->toString()->asChar() <<" to be " << expectedVal << ", but it is " << containsVal);
   }
 }

 virtual uint32_t doTask(int32_t id) {
  int32_t count = m_MyOffset->value();
   uint32_t loop = m_Loop;
   uint32_t idx;
   int32_t creates=0;
   int32_t updates=0;
   int32_t gets=0;
   RegionAttributesPtr atts = m_region->getAttributes();
   m_isEmptyClient = !(atts->getCachingEnabled());
   m_isThinClient = atts->getCachingEnabled();
   while (m_Run && loop--) {
     idx = count % m_MaxKeys;
     std::string opcode = m_test->getStringValue("entryOps");
     FWKINFO("Inside doTask() opcode is "<< opcode);
     ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
     if(opcode == "create")
     {
       try{
         addEntry(m_region, idx+1);
    	 creates=++m_create;
       }
       catch(EntryExistsException)
       {}
     }
     else if(opcode == "update")
     {    
       updateEntry(m_region, idx);
       updates=++m_update;
     }
     else if(opcode == "putall"){
       m_putall++;
     }
     else if(opcode == "get")
     {
       getKey(m_region,idx);
       gets=m_get++;
     }
     else if(opcode == "destroy")
     {
       destroyEntry(m_region,false,idx);
       m_destroy++;
     }
     else if(opcode == "localDestroy")
     {
       destroyEntry(m_region,true,idx);
       m_localDestroy++;
     }
     else if(opcode == "invalidate")
     {     
       invalidateEntry(m_region,false,idx);
       m_invalidate++;
     }
     else if(opcode == "localInvalidate")
     {
       invalidateEntry(m_region,true,idx);
       m_localInvalidate++;
     }
     else
     {
       FWKINFO("Invalid operation specified " << opcode);
     }
     count++;
   }
   FWKINFO("DoEntryOps Completed creates = " << creates <<" updates= "<< updates<<" gets= "<<gets );
   return (count - m_MyOffset->value());
  }
};

class Pdxtests: public FrameworkTest
{
public:
    Pdxtests( const char * initArgs ) :
    FrameworkTest( initArgs )
 {}

  virtual ~Pdxtests( void ) {}

  int32_t createRegion();
  int32_t createPools();

  void checkTest(const char * taskId,bool ispool=false);
  void verifyServerKeysFromSnapshot();
  int32_t verifyFromSnapshotOnly();
  void verifyContainsValueForKey(RegionPtr region,CacheableKeyPtr key,bool expected);
  void verifyContainsKey(RegionPtr region,CacheableKeyPtr key,bool expected);
  void GetPdxVersionedVal(CacheableKeyPtr key,SerializablePtr myPdxVal,int32_t versionNo,bool expectedVal,const char* pdxTostring);
  int32_t doFeed();
  int32_t puts();
  int32_t gets();
  int32_t populateRegion();
  int32_t randomEntryOperation();
  int32_t doVerifyModifyPdxInstance();
  int32_t doVerifyModifyAutoPdxInstance();
  int32_t doAccessPdxInstanceAndVerify();
  int32_t doAccessAutoPdxInstanceAndVerify();
  int32_t doModifyPdxInstance();
  int32_t registerAllKeys();
  void waitForSilenceListenerComplete(int32_t desiredSilenceSec, int32_t sleepMS);
  int32_t DumpToBB();
  std::map<std::string,std::string> getBBStrMap(std::string BBString);
  void setBBStrMap(CacheableHashMapPtr tempMap,std::string objectype,int32_t versionno);
  void setBBStrVec(CacheableHashSetPtr tempVec);
  std::vector<std::string> getBBStrVec(std::string BBString);

private:
  int32_t initKeys(bool useDefault = true, bool useAllClientID = false);
  RegionPtr getRegionPtr( const char * reg = NULL );
  bool checkReady(int32_t numClients);
  CacheableKeyPtr * m_KeysA;
  int32_t m_MaxKeys;
  int32_t m_KeyIndexBegin;
  int32_t m_MaxValues;
};
   };
    };
     };


#endif /* PDXTESTS_HPP_ */

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxTypeRegistry.cpp
 *
 *  Created on: Dec 9, 2011
 *      Author: npatel
 */

#include "PdxTypeRegistry.hpp"
#include "SerializationRegistry.hpp"

namespace gemfire {

TypeIdVsPdxType* PdxTypeRegistry::typeIdToPdxType = NULL;

TypeIdVsPdxType* PdxTypeRegistry::remoteTypeIdToMergedPdxType = NULL;

TypeNameVsPdxType* PdxTypeRegistry::localTypeToPdxType = NULL;

// TODO::Add support for weakhashmap
// std::map<PdxSerializablePtr, PdxRemotePreservedDataPtr>
// *PdxTypeRegistry::preserveData = NULL;
PreservedHashMap PdxTypeRegistry::preserveData;

CacheableHashMapPtr PdxTypeRegistry::enumToInt = NULLPTR;

CacheableHashMapPtr PdxTypeRegistry::intToEnum = NULLPTR;

ACE_RW_Thread_Mutex PdxTypeRegistry::g_readerWriterLock;

ACE_RW_Thread_Mutex PdxTypeRegistry::g_preservedDataLock;

PdxTypeToTypeIdMap* PdxTypeRegistry::pdxTypeToTypeIdMap = NULL;
bool PdxTypeRegistry::pdxReadSerialized;
bool PdxTypeRegistry::pdxIgnoreUnreadFields;

PdxTypeRegistry::PdxTypeRegistry() /*:pdxIgnoreUnreadFields (false),
                                      pdxReadSerialized(false)*/
{}

PdxTypeRegistry::~PdxTypeRegistry() {}

void PdxTypeRegistry::init() {
  // try{
  typeIdToPdxType = new TypeIdVsPdxType();
  remoteTypeIdToMergedPdxType = new TypeIdVsPdxType();
  localTypeToPdxType = new TypeNameVsPdxType();
  // preserveData = CacheableHashMap::create();
  // preserveData = PreservedHashMapPtr(new PreservedHashMap());
  enumToInt = CacheableHashMap::create();
  intToEnum = CacheableHashMap::create();
  pdxTypeToTypeIdMap = new PdxTypeToTypeIdMap();
  /*}catch(const std::bad_alloc&){
  throw gemfire::OutOfMemoryException( "Out of Memory while executing new in
  PdxTypeRegistry::init()");
  }*/
}

void PdxTypeRegistry::cleanup() {
  clear();
  GF_SAFE_DELETE(typeIdToPdxType);
  GF_SAFE_DELETE(remoteTypeIdToMergedPdxType);
  GF_SAFE_DELETE(localTypeToPdxType);
  GF_SAFE_DELETE(pdxTypeToTypeIdMap);
  // GF_SAFE_DELETE(preserveData);
}

int PdxTypeRegistry::testGetNumberOfPdxIds() {
  return static_cast<int>(typeIdToPdxType->size());
}

int PdxTypeRegistry::testNumberOfPreservedData() { return preserveData.size(); }

int32 PdxTypeRegistry::getPDXIdForType(const char* type, const char* poolname,
                                       PdxTypePtr nType, bool checkIfThere) {
  // WriteGuard guard(g_readerWriterLock);
  if (checkIfThere) {
    PdxTypePtr lpdx = getLocalPdxType(type);
    if (lpdx != NULLPTR) {
      int id = lpdx->getTypeId();
      if (id != 0) {
        return id;
      }
    }
  }

  int typeId = SerializationRegistry::GetPDXIdForType(poolname, nType);
  nType->setTypeId(typeId);

  PdxTypeRegistry::addPdxType(typeId, nType);
  return typeId;
}

int32 PdxTypeRegistry::getPDXIdForType(PdxTypePtr nType, const char* poolname) {
  PdxTypeToTypeIdMap* tmp = pdxTypeToTypeIdMap;
  int32_t typeId = 0;
  PdxTypeToTypeIdMap::iterator iter = tmp->find(nType);
  if (iter != tmp->end()) {
    typeId = iter->second;
    if (typeId != 0) {
      return typeId;
    }
  }
  /*WriteGuard guard(g_readerWriterLock);
  tmp = pdxTypeToTypeIdMap;
  if(tmp->find(nType) != tmp->end()) {
    typeId = tmp->operator[](nType);
    if (typeId != 0) {
      return typeId;
    }
  }*/
  typeId = SerializationRegistry::GetPDXIdForType(poolname, nType);
  nType->setTypeId(typeId);
  tmp = pdxTypeToTypeIdMap;
  tmp->insert(std::make_pair(nType, typeId));
  pdxTypeToTypeIdMap = tmp;
  PdxTypeRegistry::addPdxType(typeId, nType);
  return typeId;
}

void PdxTypeRegistry::clear() {
  {
    WriteGuard guard(g_readerWriterLock);
    if (typeIdToPdxType != NULL) typeIdToPdxType->clear();

    if (remoteTypeIdToMergedPdxType != NULL) {
      remoteTypeIdToMergedPdxType->clear();
    }

    if (localTypeToPdxType != NULL) localTypeToPdxType->clear();

    if (intToEnum != NULLPTR) intToEnum->clear();

    if (enumToInt != NULLPTR) enumToInt->clear();

    if (pdxTypeToTypeIdMap != NULL) pdxTypeToTypeIdMap->clear();
  }
  {
    WriteGuard guard(getPreservedDataLock());
    preserveData.clear();
  }
}

void PdxTypeRegistry::addPdxType(int32 typeId, PdxTypePtr pdxType) {
  WriteGuard guard(g_readerWriterLock);
  std::pair<int32, PdxTypePtr> pc(typeId, pdxType);
  typeIdToPdxType->insert(pc);
}

PdxTypePtr PdxTypeRegistry::getPdxType(int32 typeId) {
  ReadGuard guard(g_readerWriterLock);
  PdxTypePtr retValue = NULLPTR;
  TypeIdVsPdxType::iterator iter;
  iter = typeIdToPdxType->find(typeId);
  if (iter != typeIdToPdxType->end()) {
    retValue = (*iter).second;
    return retValue;
  }
  return NULLPTR;
}

void PdxTypeRegistry::addLocalPdxType(const char* localType,
                                      PdxTypePtr pdxType) {
  WriteGuard guard(g_readerWriterLock);
  localTypeToPdxType->insert(
      std::pair<std::string, PdxTypePtr>(localType, pdxType));
}

PdxTypePtr PdxTypeRegistry::getLocalPdxType(const char* localType) {
  ReadGuard guard(g_readerWriterLock);
  PdxTypePtr localTypePtr = NULLPTR;
  TypeNameVsPdxType::iterator it;
  it = localTypeToPdxType->find(localType);
  if (it != localTypeToPdxType->end()) {
    localTypePtr = (*it).second;
    return localTypePtr;
  }
  return NULLPTR;
}

void PdxTypeRegistry::setMergedType(int32 remoteTypeId, PdxTypePtr mergedType) {
  WriteGuard guard(g_readerWriterLock);
  std::pair<int32, PdxTypePtr> mergedTypePair(remoteTypeId, mergedType);
  remoteTypeIdToMergedPdxType->insert(mergedTypePair);
}

PdxTypePtr PdxTypeRegistry::getMergedType(int32 remoteTypeId) {
  PdxTypePtr retVal = NULLPTR;
  TypeIdVsPdxType::iterator it;
  it = remoteTypeIdToMergedPdxType->find(remoteTypeId);
  if (it != remoteTypeIdToMergedPdxType->end()) {
    retVal = (*it).second;
    return retVal;
  }
  return retVal;
}

void PdxTypeRegistry::setPreserveData(PdxSerializablePtr obj,
                                      PdxRemotePreservedDataPtr pData) {
  WriteGuard guard(getPreservedDataLock());
  pData->setOwner(obj);
  if (preserveData.contains(obj)) {
    // reset expiry task
    // TODO: check value for NULL
    long expTaskId = preserveData[obj]->getPreservedDataExpiryTaskId();
    CacheImpl::expiryTaskManager->resetTask(expTaskId, 5);
    LOGDEBUG("PdxTypeRegistry::setPreserveData Reset expiry task Done");
    pData->setPreservedDataExpiryTaskId(expTaskId);
    preserveData[obj] = pData;
  } else {
    // schedule new expiry task
    PreservedDataExpiryHandler* handler =
        new PreservedDataExpiryHandler(obj, 20);
    long id =
        CacheImpl::expiryTaskManager->scheduleExpiryTask(handler, 20, 0, false);
    pData->setPreservedDataExpiryTaskId(id);
    LOGDEBUG(
        "PdxTypeRegistry::setPreserveData Schedule new expirt task with id=%ld",
        id);
    preserveData.insert(obj, pData);
  }

  LOGDEBUG(
      "PdxTypeRegistry::setPreserveData Successfully inserted new entry in "
      "preservedData");
}

PdxRemotePreservedDataPtr PdxTypeRegistry::getPreserveData(
    PdxSerializablePtr pdxobj) {
  ReadGuard guard(getPreservedDataLock());
  PreservedHashMap::Iterator iter = preserveData.find((pdxobj));
  if (iter != preserveData.end()) {
    PdxRemotePreservedDataPtr retValPtr = iter.second();
    return retValPtr;
  }
  return NULLPTR;
}

int32_t PdxTypeRegistry::getEnumValue(EnumInfoPtr ei) {
  CacheableHashMapPtr tmp;
  tmp = enumToInt;
  if (tmp->contains(ei)) {
    CacheableInt32Ptr val = tmp->operator[](ei);
    return val->value();
  }
  WriteGuard guard(g_readerWriterLock);
  tmp = enumToInt;
  if (tmp->contains(ei)) {
    CacheableInt32Ptr val = tmp->operator[](ei);
    return val->value();
  }
  int val = SerializationRegistry::GetEnumValue(ei);
  tmp = enumToInt;
  tmp->update(ei, CacheableInt32::create(val));
  enumToInt = tmp;
  return val;
}

EnumInfoPtr PdxTypeRegistry::getEnum(int32_t enumVal) {
  EnumInfoPtr ret;
  CacheableHashMapPtr tmp;
  tmp = intToEnum;
  if (tmp->contains(CacheableInt32::create(enumVal))) {
    ret = tmp->operator[](CacheableInt32::create(enumVal));
  }

  if (ret != NULLPTR) {
    return ret;
  }

  WriteGuard guard(g_readerWriterLock);
  tmp = intToEnum;
  if (tmp->contains(CacheableInt32::create(enumVal))) {
    ret = tmp->operator[](CacheableInt32::create(enumVal));
  }

  if (ret != NULLPTR) {
    return ret;
  }

  ret = SerializationRegistry::GetEnum(enumVal);
  tmp = intToEnum;
  tmp->update(CacheableInt32::create(enumVal), ret);
  intToEnum = tmp;
  return ret;
}
}  // namespace gemfire

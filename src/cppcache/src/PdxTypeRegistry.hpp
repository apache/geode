/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxTypeRegistry.hpp
 *
 *  Created on: Dec 9, 2011
 *      Author: npatel
 */

#ifndef _GEMFIRE_IMPL_PDXTYPEREGISTRY_HPP_
#define _GEMFIRE_IMPL_PDXTYPEREGISTRY_HPP_

#include <gfcpp/PdxSerializable.hpp>
#include "PdxRemotePreservedData.hpp"
#include "ReadWriteLock.hpp"
#include <map>
#include <ace/ACE.h>
#include <ace/Recursive_Thread_Mutex.h>
#include "PdxType.hpp"
#include "EnumInfo.hpp"
#include "PreservedDataExpiryHandler.hpp"

namespace gemfire {

struct PdxTypeLessThan {
  bool operator()(PdxTypePtr const& n1, PdxTypePtr const& n2) const {
    // call to PdxType::operator <()
    return *n1 < *n2;
  }
};

typedef std::map<int32, PdxTypePtr> TypeIdVsPdxType;
typedef std::map</*char**/ std::string, PdxTypePtr> TypeNameVsPdxType;
typedef HashMapT<PdxSerializablePtr, PdxRemotePreservedDataPtr>
    PreservedHashMap;
typedef std::map<PdxTypePtr, int32_t, PdxTypeLessThan> PdxTypeToTypeIdMap;

class CPPCACHE_EXPORT PdxTypeRegistry {
 private:
  static TypeIdVsPdxType* typeIdToPdxType;

  static TypeIdVsPdxType* remoteTypeIdToMergedPdxType;

  static TypeNameVsPdxType* localTypeToPdxType;

  // TODO:: preserveData need to be of type WeakHashMap
  // static std::map<PdxSerializablePtr , PdxRemotePreservedDataPtr>
  // *preserveData;
  // static CacheableHashMapPtr preserveData;
  static PreservedHashMap preserveData;

  static ACE_RW_Thread_Mutex g_readerWriterLock;

  static ACE_RW_Thread_Mutex g_preservedDataLock;

  static bool pdxIgnoreUnreadFields;

  static bool pdxReadSerialized;

  static CacheableHashMapPtr enumToInt;

  static CacheableHashMapPtr intToEnum;

 public:
  PdxTypeRegistry();

  virtual ~PdxTypeRegistry();

  static void init();

  static void cleanup();

  // test hook;
  static int testGetNumberOfPdxIds();

  // test hook
  static int testNumberOfPreservedData();

  static void addPdxType(int32 typeId, PdxTypePtr pdxType);

  static PdxTypePtr getPdxType(int32 typeId);

  static void addLocalPdxType(const char* localType, PdxTypePtr pdxType);

  // newly added
  static PdxTypePtr getLocalPdxType(const char* localType);

  static void setMergedType(int32 remoteTypeId, PdxTypePtr mergedType);

  static PdxTypePtr getMergedType(int32 remoteTypeId);

  static void setPreserveData(PdxSerializablePtr obj,
                              PdxRemotePreservedDataPtr preserveDataPtr);

  static PdxRemotePreservedDataPtr getPreserveData(PdxSerializablePtr obj);

  static void clear();

  static int32 getPDXIdForType(const char* type, const char* poolname,
                               PdxTypePtr nType, bool checkIfThere);

  static bool getPdxIgnoreUnreadFields() { return pdxIgnoreUnreadFields; }

  static void setPdxIgnoreUnreadFields(bool value) {
    pdxIgnoreUnreadFields = value;
  }

  static void setPdxReadSerialized(bool value) { pdxReadSerialized = value; }

  static bool getPdxReadSerialized() { return pdxReadSerialized; }

  static inline PreservedHashMap& getPreserveDataMap() { return preserveData; };

  static int32_t getEnumValue(EnumInfoPtr ei);

  static EnumInfoPtr getEnum(int32_t enumVal);

  static int32 getPDXIdForType(PdxTypePtr nType, const char* poolname);

  static ACE_RW_Thread_Mutex& getPreservedDataLock() {
    return g_preservedDataLock;
  }

 private:
  static PdxTypeToTypeIdMap* pdxTypeToTypeIdMap;
};
}
#endif /* PDXTYPEREGISTRY_HPP_ */

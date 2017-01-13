/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef PDX_AUTO_MEGA_TYPE_HPP_
#define PDX_AUTO_MEGA_TYPE_HPP_

#include <gfcpp/PdxSerializable.hpp>
#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/PdxWriter.hpp>
#include <gfcpp/PdxReader.hpp>

#ifdef _WIN32
#ifdef BUILD_TESTOBJECT
#define TESTOBJECT_EXPORT LIBEXP
#else
#define TESTOBJECT_EXPORT LIBIMP
#endif
#else
#define TESTOBJECT_EXPORT
#endif

using namespace gemfire;

#define GFIGNORE(X) X
#define GFID
#define GFARRAYSIZE(X)

namespace PdxAutoTests {
class GFIGNORE(TESTOBJECT_EXPORT) PdxAutoMegaType : public PdxSerializable {
 private:
  GFID char pdxType_Char;
  GFID wchar_t pdxType_Wchar;
  GFID bool pdxType_Boolean;
  GFID int8_t pdxType_Byte;
  GFID int16_t pdxType_Short;
  GFID int32_t pdxType_Int;
  GFID int64_t pdxType_Long;
  GFID float pdxType_Float;
  GFID double pdxType_Double;
  GFID CacheableDatePtr pdxType_Date;
  GFID char* pdxType_String;
  GFID wchar_t* pdxType_WideString;

  GFID bool* pdxType_BoolArray;
  GFARRAYSIZE(pdxType_BoolArray) int32_t pdxType_BoolArray_Size;

  GFID wchar_t* pdxType_WideCharArray;
  GFARRAYSIZE(pdxType_WideCharArray) int32_t pdxType_WideCharArray_Size;

  GFID char** pdxType_CharArray;
  GFARRAYSIZE(pdxType_CharArray) int32_t pdxType_CharArray_Size;

  GFID int8_t* pdxType_ByteArray;
  GFARRAYSIZE(pdxType_ByteArray) int32_t pdxType_ByteArray_Size;

  GFID int16_t* pdxType_ShortArray;
  GFARRAYSIZE(pdxType_ShortArray) int32_t pdxType_ShortArray_Size;

  GFID int32_t* pdxType_Int32Array;
  GFARRAYSIZE(pdxType_Int32Array) int32_t pdxType_Int32Array_Size;

  GFID int64_t* pdxType_LongArray;
  GFARRAYSIZE(pdxType_LongArray) int32_t pdxType_LongArray_Size;

  GFID float* pdxType_FloatArray;
  GFARRAYSIZE(pdxType_FloatArray) int32_t pdxType_FloatArray_Size;

  GFID double* pdxType_DoubleArray;
  GFARRAYSIZE(pdxType_DoubleArray) int32_t pdxType_DoubleArray_Size;

  GFID wchar_t** pdxType_WideStringArray;
  GFARRAYSIZE(pdxType_WideStringArray) int32_t pdxType_WideStringArray_Size;

  CacheableArrayListPtr pdxType_CacheableArrayListPtr;
  CacheableHashMapPtr pdxType_CacheableHashMapPtr;
  CacheableHashTablePtr PdxType_CacheableHashTablePtr;
  CacheableVectorPtr pdxType_CacheableVectorPtr;
  CacheableHashSetPtr pdxType_CacheableHashSetPtr;
  CacheableLinkedHashSetPtr pdxType_CacheableLinkedHashSetPtr;

 public:
  PdxAutoMegaType();
  ~PdxAutoMegaType();
  bool equals(PdxSerializablePtr obj);

  // Decleare following methods.
  // Do not write any implementation for the same.
  // These will be generated in a file
  // <ClassName>Serializer.cpp
  const char* getClassName() const;
  using PdxSerializable::toData;
  using PdxSerializable::fromData;
  virtual void toData(PdxWriterPtr pw);
  virtual void fromData(PdxReaderPtr pr);
  static PdxSerializable* createDeserializable();

  void initPdxAutoMegaType();

 private:
  void populatePrimitives();
  bool verifyPrimitives(PdxSerializablePtr);

  void populateArrays();
  bool verifyArrays(PdxSerializablePtr);

  void populateCacheableContainers();
  bool verifyCacheableContainers(PdxSerializablePtr);
};

typedef SharedPtr<PdxAutoMegaType> PdxAutoMegaTypePtr;
}

#endif

#ifndef PDX_AUTO_SERIALIZER_HPP
#define PDX_AUTO_SERIALIZER_HPP

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"
#include "PdxWriter.hpp"
#include "PdxReader.hpp"
#include "VectorT.hpp"
#include "HashMapT.hpp"
#include "HashSetT.hpp"
#include "GemfireTypeIds.hpp"
#include "TypeHelper.hpp"

namespace gemfire {
namespace PdxAutoSerializable {
// Read and write methods for various types
// uint8_t
//------------------------------------------------------------------------------------------------
#ifdef _SOLARIS
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           int8_t value) {
  pw->writeByte(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          int8_t& value) {
  value = pr->readByte(fieldName);
}
#else
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           char value) {
  pw->writeChar(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          char& value) {
  value = pr->readChar(fieldName);
}
#endif
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           wchar_t value) {
  pw->writeWideChar(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          wchar_t& value) {
  value = pr->readWideChar(fieldName);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           bool value) {
  pw->writeBoolean(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          bool& value) {
  value = pr->readBoolean(fieldName);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           signed char value) {
  pw->writeByte(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          signed char& value) {
  value = pr->readByte(fieldName);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           int16_t value) {
  pw->writeShort(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          int16_t& value) {
  value = pr->readShort(fieldName);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           int32_t value) {
  pw->writeInt(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          int32_t& value) {
  value = pr->readInt(fieldName);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           int64_t value) {
  pw->writeLong(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          int64_t& value) {
  value = pr->readLong(fieldName);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           float value) {
  pw->writeFloat(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          float& value) {
  value = pr->readFloat(fieldName);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           double value) {
  pw->writeDouble(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          double& value) {
  value = pr->readDouble(fieldName);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           CacheableDatePtr value) {
  pw->writeDate(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          CacheableDatePtr& value) {
  value = pr->readDate(fieldName);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           const char* value) {
  pw->writeString(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          char*& value) {
  value = pr->readString(fieldName);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           const wchar_t* value) {
  pw->writeWideString(fieldName, value);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          wchar_t*& value) {
  value = pr->readWideString(fieldName);
}
// ---- helper methods for bytearrayofarray -------
#ifdef _SOLARIS
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           char** value, int32_t arraySize,
                           int32_t* elemArraySize) {
  pw->writeArrayOfByteArrays(fieldName, (char**)value, arraySize,
                             elemArraySize);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          char**& value, int32_t& len, int32_t*& Lengtharr) {
  GF_NEW(Lengtharr, int32_t[len]);
  value = (char**)pr->readArrayOfByteArrays(fieldName, len, &Lengtharr);
}
#else
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           int8_t** value, int32_t arraySize,
                           int32_t* elemArraySize) {
  pw->writeArrayOfByteArrays(fieldName, value, arraySize, elemArraySize);
}
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          int8_t**& value, int32_t& len, int32_t*& Lengtharr) {
  GF_NEW(Lengtharr, int32_t[len]);
  value = (signed char**)pr->readArrayOfByteArrays(fieldName, len, &Lengtharr);
}
#endif

//------------------------------------------------------------------------------------------------
// Base Serializable types
template <typename TObj>
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           const gemfire::SharedPtr<TObj>& value,
                           gemfire::TypeHelper::yes_type isSerializable) {
  pw->writeObject(fieldName, value);
}

template <typename TObj>
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           const gemfire::SharedPtr<TObj>& value) {
  writePdxObject(pw, fieldName, value, GF_TYPE_IS_SERIALIZABLE_TYPE(TObj));
}

template <typename TObj>
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          gemfire::SharedPtr<TObj>& value,
                          gemfire::TypeHelper::yes_type isSerializable) {
  value = dynCast<gemfire::SharedPtr<TObj> >(pr->readObject(fieldName));
}

template <typename TObj>
inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          gemfire::SharedPtr<TObj>& value) {
  readPdxObject(pr, fieldName, value, GF_TYPE_IS_SERIALIZABLE_TYPE(TObj));
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           bool* value, int32_t len) {
  pw->writeBooleanArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          bool*& value, int32_t& len) {
  value = pr->readBooleanArray(fieldName, len);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           wchar_t* value, int32_t len) {
  pw->writeWideCharArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          wchar_t*& value, int32_t& len) {
  value = pr->readWideCharArray(fieldName, len);
}
//------------------------------------------------------------------------------------------------
#ifdef _SOLARIS
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           int8_t* value, int32_t len) {
  pw->writeByteArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          int8_t*& value, int32_t& len) {
  value = (int8_t*)pr->readByteArray(fieldName, len);
}
#else
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           char* value, int32_t len) {
  pw->writeCharArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          char*& value, int32_t& len) {
  value = pr->readCharArray(fieldName, len);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           signed char* value, int32_t len) {
  pw->writeByteArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          signed char*& value, int32_t& len) {
  value = (signed char*)pr->readByteArray(fieldName, len);
}
#endif
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           int16_t* value, int32_t len) {
  pw->writeShortArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          int16_t*& value, int32_t& len) {
  value = pr->readShortArray(fieldName, len);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           int32_t* value, int32_t len) {
  pw->writeIntArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          int32_t*& value, int32_t& len) {
  value = pr->readIntArray(fieldName, len);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           int64_t* value, int32_t len) {
  pw->writeLongArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          int64_t*& value, int32_t& len) {
  value = pr->readLongArray(fieldName, len);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           float* value, int32_t len) {
  pw->writeFloatArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          float*& value, int32_t& len) {
  value = pr->readFloatArray(fieldName, len);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           double* value, int32_t len) {
  pw->writeDoubleArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          double*& value, int32_t& len) {
  value = pr->readDoubleArray(fieldName, len);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           char** value, int32_t len) {
  pw->writeStringArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          char**& value, int32_t& len) {
  value = pr->readStringArray(fieldName, len);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           wchar_t** value, int32_t len) {
  pw->writeWideStringArray(fieldName, value, len);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          wchar_t**& value, int32_t& len) {
  value = pr->readWideStringArray(fieldName, len);
}
//------------------------------------------------------------------------------------------------
inline void writePdxObject(gemfire::PdxWriterPtr& pw, const char* fieldName,
                           CacheableObjectArrayPtr value) {
  pw->writeObjectArray(fieldName, value);
}

inline void readPdxObject(gemfire::PdxReaderPtr& pr, const char* fieldName,
                          CacheableObjectArrayPtr& value) {
  value = pr->readObjectArray(fieldName);
}
//------------------------------------------------------------------------------------------------
// For containers vector/hashmap/hashset
// template <typename TObj>
// inline void writePdxObject( gemfire::PdxWriterPtr& pw,  const char*
// fieldName,
//    const VectorT< TObj >& value )
//{
//  int32_t len = (int32_t)value.size();
//  pw->writeArrayLen( len );
//  for ( typename VectorT< TObj >::Iterator iter = value.begin( );
//      iter != value.end( ); ++iter ) {
//    writePdxObject( output, *iter );
//  }
//}

// inline uint32_t objectSize( const _VectorOfCacheable& value )
//{
//  uint32_t objectSize = 0;
//  for ( _VectorOfCacheable::Iterator iter = value.begin( );
//      iter != value.end( ); ++iter ) {
//    if (*iter != NULLPTR) {
//      objectSize += (*iter)->objectSize( );
//    }
//  }
//  objectSize += (sizeof(CacheablePtr) * (uint32_t)value.size());
//  return objectSize;
//}

// template <typename TObj>
// inline void readPdxObject( gemfire::PdxReaderPtr& pr,  const char* fieldName,
//    VectorT< TObj >& value )
//{
//  int32_t len;
//  pr->readArrayLen( &len );
//  if ( len >= 0 ) {
//    TObj obj;
//    for ( int32_t index = 0; index < len; index++ ) {
//      readPdxObject( input, obj );
//      value.push_back( obj );
//    }
//  }
//}

// template <typename TKey, typename TValue>
// inline void writePdxObject( gemfire::PdxWriterPtr& pw,  const char*
// fieldName,
//    const HashMapT< TKey, TValue >& value )
//{
//  int32_t len = (int32_t)value.size();
//  pw->writeArrayLen( len );
//  if ( len > 0 ) {
//    for ( typename HashMapT< TKey, TValue >::Iterator iter = value.begin( );
//        iter != value.end( ); ++iter ) {
//      writePdxObject( output, iter.first( ) );
//      writePdxObject( output, iter.second( ) );
//    }
//  }
//}

// inline uint32_t objectSize( const _HashMapOfCacheable& value )
//{
//  uint32_t objectSize = 0;
//  for ( _HashMapOfCacheable::Iterator iter = value.begin( );
//      iter != value.end( ); ++iter ) {
//    objectSize += iter.first( )->objectSize( );
//    if (iter.second( ) != NULLPTR) {
//      objectSize += iter.second( )->objectSize( );
//    }
//  }
//  objectSize += ( ( sizeof( CacheableKeyPtr ) + sizeof( CacheablePtr ) )
//      * (uint32_t)value.size());
//  return objectSize;
//}

// template <typename TKey, typename TValue>
// inline void readPdxObject( gemfire::PdxReaderPtr& pr,  const char* fieldName,
//    HashMapT< TKey, TValue >& value )
//{
//  int32_t len;
//  pr->readArrayLen( &len );
//  if ( len > 0 ) {
//    TKey key;
//    TValue val;
//    for( int32_t index = 0; index < len; index++ ) {
//      readPdxObject( input, key );
//      readPdxObject( input, val );
//      value.insert( key, val );
//    }
//  }
//}

// template <typename TKey>
// inline void writePdxObject( gemfire::PdxWriterPtr& pw,  const char*
// fieldName,
//  const HashSetT< TKey >& value )
//{
//  int32_t len = (int32_t)value.size();
//  pw->writeArrayLen( len );
//  for ( typename HashSetT< TKey >::Iterator iter = value.begin( );
//    iter != value.end( ); ++iter ) {
//    writePdxObject( output, *iter );
//  }
//}

// inline uint32_t objectSize( const _HashSetOfCacheableKey& value )
//{
//  uint32_t objectSize = 0;
//  for ( _HashSetOfCacheableKey::Iterator iter = value.begin( );
//    iter != value.end( ); ++iter ) {
//    if (*iter != NULLPTR) {
//      objectSize += (*iter)->objectSize( );
//    }
//  }
//  objectSize += (sizeof(CacheableKeyPtr) * (uint32_t)value.size());
//  return objectSize;
//}

// template <typename TKey>
// inline void readPdxObject( gemfire::PdxReaderPtr& pr,  const char* fieldName,
//    HashSetT< TKey >& value )
//{
//  int32_t len;
//  pr->readArrayLen( &len );
//  if ( len > 0 ) {
//    TKey key;
//    for( int32_t index = 0; index < len; index++ ) {
//      readPdxObject( input, key );
//      value.insert( key );
//    }
//  }
//}

//// Default value for builtin types

// template <typename TObj>
// inline TObj zeroObject( )
//{
//  return 0;
//}

// template <>
// inline bool zeroObject<bool>( )
//{
//  return false;
//}

// template <>
// inline double zeroObject<double>( )
//{
//  return 0.0;
//}

// template <>
// inline float zeroObject<float>( )
//{
//  return 0.0F;
//}

}  // namespace PdxAutoSerializable
}

#endif  // _GEMFIRE_SERIALIZER_HPP_

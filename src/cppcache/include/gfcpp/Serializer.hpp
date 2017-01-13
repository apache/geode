#ifndef _GEMFIRE_SERIALIZER_HPP_
#define _GEMFIRE_SERIALIZER_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"
#include "DataOutput.hpp"
#include "DataInput.hpp"
#include "VectorT.hpp"
#include "HashMapT.hpp"
#include "HashSetT.hpp"
#include "GemfireTypeIds.hpp"
#include "TypeHelper.hpp"

namespace gemfire {
namespace serializer {

// Read and write methods for various types

inline void writeObject(gemfire::DataOutput& output, uint8_t value) {
  output.write(value);
}

inline void readObject(gemfire::DataInput& input, uint8_t& value) {
  input.read(&value);
}

inline void writeObject(gemfire::DataOutput& output, int8_t value) {
  output.write(value);
}

inline void readObject(gemfire::DataInput& input, int8_t& value) {
  input.read(&value);
}

inline void writeObject(gemfire::DataOutput& output, const uint8_t* bytes,
                        int32_t len) {
  output.writeBytes(bytes, len);
}

inline void readObject(gemfire::DataInput& input, uint8_t*& bytes,
                       int32_t& len) {
  input.readBytes(&bytes, &len);
}

inline void writeObject(gemfire::DataOutput& output, const int8_t* bytes,
                        int32_t len) {
  output.writeBytes(bytes, len);
}

inline void readObject(gemfire::DataInput& input, int8_t*& bytes,
                       int32_t& len) {
  input.readBytes(&bytes, &len);
}

inline void writeObject(gemfire::DataOutput& output, int16_t value) {
  output.writeInt(value);
}

inline void readObject(gemfire::DataInput& input, int16_t& value) {
  input.readInt(&value);
}

inline void writeObject(gemfire::DataOutput& output, int32_t value) {
  output.writeInt(value);
}

inline void readObject(gemfire::DataInput& input, int32_t& value) {
  input.readInt(&value);
}

inline void writeObject(gemfire::DataOutput& output, int64_t value) {
  output.writeInt(value);
}

inline void readObject(gemfire::DataInput& input, int64_t& value) {
  input.readInt(&value);
}

inline void writeObject(gemfire::DataOutput& output, uint16_t value) {
  output.writeInt(value);
}

inline void readObject(gemfire::DataInput& input, uint16_t& value) {
  input.readInt(&value);
}

inline void writeObject(gemfire::DataOutput& output, uint32_t value) {
  output.writeInt(value);
}

inline void readObject(gemfire::DataInput& input, uint32_t& value) {
  input.readInt(&value);
}

inline void writeObject(gemfire::DataOutput& output, uint64_t value) {
  output.writeInt(value);
}

inline void readObject(gemfire::DataInput& input, uint64_t& value) {
  input.readInt(&value);
}

inline void writeObject(gemfire::DataOutput& output, bool value) {
  output.writeBoolean(value);
}

inline void readObject(gemfire::DataInput& input, bool& value) {
  input.readBoolean(&value);
}

inline void writeObject(gemfire::DataOutput& output, double value) {
  output.writeDouble(value);
}

inline void readObject(gemfire::DataInput& input, double& value) {
  input.readDouble(&value);
}

inline void writeObject(gemfire::DataOutput& output, float value) {
  output.writeFloat(value);
}

inline void readObject(gemfire::DataInput& input, float& value) {
  input.readFloat(&value);
}

inline void writeObject(gemfire::DataOutput& output, wchar_t value) {
  output.writeInt((int16_t)value);
}

inline void readObject(gemfire::DataInput& input, wchar_t& value) {
  int16_t val;
  input.readInt(&val);
  value = val;
}

inline void writeObject(gemfire::DataOutput& output, const char* value,
                        uint32_t length) {
  output.writeASCII(value, length);
}

template <typename TLen>
inline void readObject(gemfire::DataInput& input, char*& value, TLen& length) {
  uint16_t len;
  input.readASCII(&value, &len);
  length = len;
}

inline void writeObject(gemfire::DataOutput& output, const char* value) {
  output.writeASCII(value);
}

inline void readObject(gemfire::DataInput& input, char*& value) {
  input.readASCII(&value);
}

inline void writeObject(gemfire::DataOutput& output, const wchar_t* value,
                        uint32_t length) {
  output.writeUTF(value, length);
}

template <typename TLen>
inline void readObject(gemfire::DataInput& input, wchar_t*& value,
                       TLen& length) {
  uint16_t len;
  input.readUTF(&value, &len);
  length = len;
}

inline void writeObject(gemfire::DataOutput& output, const wchar_t* value) {
  output.writeUTF(value);
}

inline void readObject(gemfire::DataInput& input, wchar_t*& value) {
  input.readUTF(&value);
}

// Base Serializable types

template <typename TObj>
inline void writeObject(gemfire::DataOutput& output,
                        const gemfire::SharedPtr<TObj>& value,
                        gemfire::TypeHelper::yes_type isSerializable) {
  output.writeObject(value);
}

template <typename TObj>
inline void writeObject(gemfire::DataOutput& output,
                        const gemfire::SharedPtr<TObj>& value) {
  writeObject(output, value, GF_TYPE_IS_SERIALIZABLE_TYPE(TObj));
}

template <typename TObj>
inline void readObject(gemfire::DataInput& input,
                       gemfire::SharedPtr<TObj>& value,
                       gemfire::TypeHelper::yes_type isSerializable) {
  input.readObject(value, true);
}

template <typename TObj>
inline void readObject(gemfire::DataInput& input,
                       gemfire::SharedPtr<TObj>& value) {
  readObject(input, value, GF_TYPE_IS_SERIALIZABLE_TYPE(TObj));
}

// For arrays

template <typename TObj, typename TLen>
inline void writeObject(gemfire::DataOutput& output, const TObj* array,
                        TLen len) {
  if (array == NULL) {
    output.write((int8_t)-1);
  } else {
    output.writeArrayLen(len);
    const TObj* endArray = array + len;
    while (array < endArray) {
      writeObject(output, *array++);
    }
  }
}

template <typename TObj, typename TLen>
inline void readObject(gemfire::DataInput& input, TObj*& array, TLen& len) {
  input.readArrayLen(&len);
  if (len > 0) {
    GF_NEW(array, TObj[len]);
    TObj* startArray = array;
    TObj* endArray = array + len;
    while (startArray < endArray) {
      readObject(input, *startArray++);
    }
  } else {
    array = NULL;
  }
}

template <typename TObj, typename TLen>
inline uint32_t objectSize(const TObj* array, TLen len,
                           gemfire::TypeHelper::yes_type isSerializable) {
  uint32_t size = 0;
  const TObj* endArray = array + len;
  while (array < endArray) {
    if (*array != NULL) {
      size += (*array)->objectSize();
    }
    array++;
  }
  size += (uint32_t)(sizeof(TObj) * len);
  return size;
}

template <typename TObj, typename TLen>
inline uint32_t objectSize(const TObj* array, TLen len,
                           gemfire::TypeHelper::no_type isNotSerializable) {
  return (uint32_t)(sizeof(TObj) * len);
}

template <typename TObj, typename TLen>
inline uint32_t objectSize(const TObj* array, TLen len) {
  return objectSize(array, len, GF_TYPE_IS_SERIALIZABLE_TYPE(TObj));
}

// For containers vector/hashmap/hashset

template <typename TObj>
inline void writeObject(gemfire::DataOutput& output,
                        const VectorT<TObj>& value) {
  int32_t len = (int32_t)value.size();
  output.writeArrayLen(len);
  for (typename VectorT<TObj>::Iterator iter = value.begin();
       iter != value.end(); ++iter) {
    writeObject(output, *iter);
  }
}

inline uint32_t objectSize(const _VectorOfCacheable& value) {
  uint32_t objectSize = 0;
  for (_VectorOfCacheable::Iterator iter = value.begin(); iter != value.end();
       ++iter) {
    if (*iter != NULLPTR) {
      objectSize += (*iter)->objectSize();
    }
  }
  objectSize += (uint32_t)(sizeof(CacheablePtr) * value.size());
  return objectSize;
}

template <typename TObj>
inline void readObject(gemfire::DataInput& input, VectorT<TObj>& value) {
  int32_t len;
  input.readArrayLen(&len);
  if (len >= 0) {
    TObj obj;
    for (int32_t index = 0; index < len; index++) {
      readObject(input, obj);
      value.push_back(obj);
    }
  }
}

template <typename TKey, typename TValue>
inline void writeObject(gemfire::DataOutput& output,
                        const HashMapT<TKey, TValue>& value) {
  int32_t len = (int32_t)value.size();
  output.writeArrayLen(len);
  if (len > 0) {
    for (typename HashMapT<TKey, TValue>::Iterator iter = value.begin();
         iter != value.end(); ++iter) {
      writeObject(output, iter.first());
      writeObject(output, iter.second());
    }
  }
}

inline uint32_t objectSize(const _HashMapOfCacheable& value) {
  uint32_t objectSize = 0;
  for (_HashMapOfCacheable::Iterator iter = value.begin(); iter != value.end();
       ++iter) {
    objectSize += iter.first()->objectSize();
    if (iter.second() != NULLPTR) {
      objectSize += iter.second()->objectSize();
    }
  }
  objectSize += (uint32_t)((sizeof(CacheableKeyPtr) + sizeof(CacheablePtr)) *
                           value.size());
  return objectSize;
}

template <typename TKey, typename TValue>
inline void readObject(gemfire::DataInput& input,
                       HashMapT<TKey, TValue>& value) {
  int32_t len;
  input.readArrayLen(&len);
  if (len > 0) {
    TKey key;
    TValue val;
    for (int32_t index = 0; index < len; index++) {
      readObject(input, key);
      readObject(input, val);
      value.insert(key, val);
    }
  }
}

template <typename TKey>
inline void writeObject(gemfire::DataOutput& output,
                        const HashSetT<TKey>& value) {
  int32_t len = (int32_t)value.size();
  output.writeArrayLen(len);
  for (typename HashSetT<TKey>::Iterator iter = value.begin();
       iter != value.end(); ++iter) {
    writeObject(output, *iter);
  }
}

inline uint32_t objectSize(const _HashSetOfCacheableKey& value) {
  uint32_t objectSize = 0;
  for (_HashSetOfCacheableKey::Iterator iter = value.begin();
       iter != value.end(); ++iter) {
    if (*iter != NULLPTR) {
      objectSize += (*iter)->objectSize();
    }
  }
  objectSize += (uint32_t)(sizeof(CacheableKeyPtr) * value.size());
  return objectSize;
}

template <typename TKey>
inline void readObject(gemfire::DataInput& input, HashSetT<TKey>& value) {
  int32_t len;
  input.readArrayLen(&len);
  if (len > 0) {
    TKey key;
    for (int32_t index = 0; index < len; index++) {
      readObject(input, key);
      value.insert(key);
    }
  }
}

// Default value for builtin types

template <typename TObj>
inline TObj zeroObject() {
  return 0;
}

template <>
inline bool zeroObject<bool>() {
  return false;
}

template <>
inline double zeroObject<double>() {
  return 0.0;
}

template <>
inline float zeroObject<float>() {
  return 0.0F;
}
}
}

#endif  // _GEMFIRE_SERIALIZER_HPP_

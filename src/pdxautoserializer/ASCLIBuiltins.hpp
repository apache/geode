/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_ASCLIBUILTINS_HPP_
#define _GFAS_ASCLIBUILTINS_HPP_

#ifdef GEMFIRE_CLR

#include "ASBuiltins.hpp"
#include <vcclr.h>

using namespace System;
using namespace System::Collections::Generic;

namespace GemStone {
namespace GemFire {
namespace Cache {
namespace Serializer {

/**
* @brief Helper type traits and other structs/classes do determine type
*        information at compile time using typename.
*        Useful for templates in particular.
*/
namespace TypeHelper {
typedef uint8_t yes_type;
typedef uint32_t no_type;

template <typename TBase, typename TDerived>
struct BDHelper {
  template <typename T>
  static yes_type check_sig(TDerived const volatile ^, T);
  static no_type check_sig(TBase const volatile ^, int);
};

/**
* @brief This struct helps us determine whether or not a class is a
*        subclass of another at compile time, so that it can be used
*        in templates. Adapted and extended from
*        {@link
* http://groups.google.com/group/comp.lang.c++.moderated/msg/dd6c4e4d5160bd83}
*/
template <typename TBase, typename TDerived>
struct SuperSubclass {
 private:
  struct Host {
    operator TBase const volatile ^() const;
    operator TDerived const volatile ^();
  };

 public:
  static const bool result = sizeof(BDHelper<TBase, TDerived>::check_sig(
                                 Host(), 0)) == sizeof(yes_type);
};

template <typename TBase>
struct SuperSubclass<TBase, TBase> {
  static const bool result = true;
};

template <bool getType = true>
struct YesNoType {
  static const yes_type value = 0;
};

template <>
struct YesNoType<false> {
  static const no_type value = 0;
};
}

#define TYPE_IS_IGFSERIALIZABLE(T) \
  TypeHelper::SuperSubclass<IGFSerializable, T>::result
#define TYPE_IS_IGFSERIALIZABLE_TYPE(T) \
  TypeHelper::YesNoType<TYPE_IS_SERIALIZABLE(T)>::value

inline void WriteObject(gemfire::DataOutput& output, Byte value) {
  output.write((uint8_t)value);
}

inline void ReadObject(gemfire::DataInput& input, Byte % value) {
  uint8_t val;
  input.read(&val);
  value = val;
}

inline void WriteObject(gemfire::DataOutput& output, SByte value) {
  output.write((int8_t)value);
}

inline void ReadObject(gemfire::DataInput& input, SByte % value) {
  int8_t val;
  input.read(&val);
  value = val;
}

inline void WriteObject(gemfire::DataOutput& output, array<Byte> ^ bytes) {
  if (bytes != nullptr) {
    // We can safely use the pinning technique here since the pointer
    // is not required after the end of native 'writeBytes' function.
    pin_ptr<Byte> pin_bytes = &bytes[0];
    output.writeBytes((uint8_t*)pin_bytes, bytes->Length);
  } else {
    output.writeBytes((uint8_t*)NULL, 0);
  }
}

inline void ReadObject(gemfire::DataInput& input, array<Byte> ^ % bytes) {
  int32_t length;
  input.readInt(&length);
  if (length > 0) {
    bytes = gcnew array<Byte>(length);
    pin_ptr<Byte> pin_bytes = &bytes[0];
    input.readBytes((uint8_t*)pin_bytes, length);
  } else {
    bytes = nullptr;
  }
}

inline void WriteObject(gemfire::DataOutput& output, array<SByte> ^ bytes) {
  if (bytes != nullptr) {
    pin_ptr<SByte> pin_bytes = &bytes[0];
    output.writeBytes((int8_t*)pin_bytes, bytes->Length);
  } else {
    output.writeBytes((int8_t*)NULL, 0);
  }
}

inline void ReadObject(gemfire::DataInput& input, array<SByte> ^ % bytes) {
  int32_t length;
  input.readInt(&length);
  if (length > 0) {
    bytes = gcnew array<SByte>(length);
    pin_ptr<SByte> pin_bytes = &bytes[0];
    input.readBytes((int8_t*)pin_bytes, length);
  } else {
    bytes = nullptr;
  }
}

inline void WriteObject(gemfire::DataOutput& output, Int16 value) {
  output.writeInt((int16_t)value);
}

inline void ReadObject(gemfire::DataInput& input, Int16 % value) {
  int16_t val;
  input.readInt(&val);
  value = val;
}

inline void WriteObject(gemfire::DataOutput& output, Int32 value) {
  output.writeInt(value);
}

inline void ReadObject(gemfire::DataInput& input, Int32 % value) {
  int32_t val;
  input.readInt(&val);
  value = val;
}

inline void WriteObject(gemfire::DataOutput& output, Int64 value) {
  output.writeInt(value);
}

inline void ReadObject(gemfire::DataInput& input, Int64 % value) {
  int64_t val;
  input.readInt(&val);
  value = val;
}

inline void WriteObject(gemfire::DataOutput& output, UInt16 value) {
  output.writeInt(value);
}

inline void ReadObject(gemfire::DataInput& input, UInt16 % value) {
  uint16_t val;
  input.readInt(&val);
  value = val;
}

inline void WriteObject(gemfire::DataOutput& output, UInt32 value) {
  output.writeInt(value);
}

inline void ReadObject(gemfire::DataInput& input, UInt32 % value) {
  uint32_t val;
  input.readInt(&val);
  value = val;
}

inline void WriteObject(gemfire::DataOutput& output, UInt64 value) {
  output.writeInt(value);
}

inline void ReadObject(gemfire::DataInput& input, UInt64 % value) {
  uint64_t val;
  input.readInt(&val);
  value = val;
}

inline void WriteObject(gemfire::DataOutput& output, Boolean value) {
  output.write((uint8_t)(value ? 1 : 0));
}

inline void ReadObject(gemfire::DataInput& input, Boolean % value) {
  uint8_t byte;
  input.read(&byte);
  value = (byte == 1 ? true : false);
}

inline void WriteObject(gemfire::DataOutput& output, Double value) {
  output.writeDouble(value);
}

inline void ReadObject(gemfire::DataInput& input, Double % value) {
  double val;
  input.readDouble(&val);
  value = val;
}

inline void WriteObject(gemfire::DataOutput& output, Single value) {
  output.writeFloat(value);
}

inline void ReadObject(gemfire::DataInput& input, Single % value) {
  float val;
  input.readFloat(&val);
  value = val;
}

inline void WriteObject(gemfire::DataOutput& output, String ^ str) {
  if (str != nullptr) {
    pin_ptr<const wchar_t> pin_str = PtrToStringChars(str);
    output.writeUTF((wchar_t*)pin_str, str->Length);
  } else {
    output.writeUTF((wchar_t*)NULL, 0);
  }
}

inline void ReadObject(gemfire::DataInput& input, String ^ % str) {
  wchar_t* n_buffer = nullptr;
  uint16_t len;
  input.readUTF(&n_buffer, &len);
  if (len > 0) {
    str = gcnew String(n_buffer, 0, len);
    input.freeUTFMemory(n_buffer);
  } else {
    str = nullptr;
  }
}

template <typename TObj>
inline void WriteObject(gemfire::DataOutput& output, DataOutput ^ mg_output,
                        TObj ^ value, TypeHelper::yes_type isSerializable) {
  mg_output->WriteObject(value);
}

template <typename TObj>
inline void WriteObject(gemfire::DataOutput& output, DataOutput ^ mg_output,
                        TObj ^ value, TypeHelper::no_type isNotSerializable) {
  WriteObject(output, value);
}

template <typename TObj>
inline void WriteObject(gemfire::DataOutput& output, DataOutput ^ mg_output,
                        TObj ^ value) {
  WriteObject(output, mg_output, value, TYPE_IS_IGFSERIALIZABLE_TYPE(TObj));
}

template <typename TObj>
inline void WriteObject(gemfire::DataOutput& output, DataOutput ^ mg_output,
                        TObj value) {
  WriteObject(output, value);
}

template <typename TObj>
inline void ReadObject(gemfire::DataInput& input, DataInput ^ mg_input,
                       TObj ^ % value, TypeHelper::yes_type isSerializable) {
  value = static_cast<TObj ^>(mg_input->ReadObject());
}

template <typename TObj>
inline void ReadObject(gemfire::DataInput& input, DataInput ^ mg_input,
                       TObj ^ % value, TypeHelper::no_type isNotSerializable) {
  ReadObject(input, value);
}

template <typename TObj>
inline void ReadObject(gemfire::DataInput& input, DataInput ^ mg_input,
                       TObj ^ % value) {
  ReadObject(input, mg_input, value, TYPE_IS_IGFSERIALIZABLE_TYPE(TObj));
}

template <typename TObj>
inline void ReadObject(gemfire::DataInput& input, DataInput ^ mg_input,
                       TObj % value) {
  ReadObject(input, value);
}

template <typename TObj>
inline void WriteObject(gemfire::DataOutput& output, array<TObj> ^ arr) {
  if (arr != nullptr) {
    pin_ptr<TObj> pin_arr = &arr[0];
    gemfire::serializer::writeObject(output, (TObj*)pin_arr, arr->Length);
  } else {
    output.writeInt(0);
  }
}

template <typename TObj>
inline void ReadObject(gemfire::DataInput& input, array<TObj> ^ % arr) {
  int32_t size;
  input.readInt(&size);
  if (size > 0) {
    arr = gcnew array<TObj>(size);
    pin_ptr<TObj> pin_arr = &arr[0];
    gemfire::serializer::readArray(input, (TObj*)pin_arr, size);
  } else {
    arr = nullptr;
  }
}

template <typename TObj>
inline void WriteObject(gemfire::DataOutput& output, DataOutput ^ mg_output,
                        array<TObj ^> ^ arr) {
  if (arr != nullptr) {
    output.writeInt(arr->Length);
            for
              each(TObj ^ obj in arr) { WriteObject(output, mg_output, obj); }
  } else {
    output.writeInt(-1);
  }
}

template <typename TObj>
inline void ReadObject(gemfire::DataInput& input, DataInput ^ mg_input,
                       array<TObj ^> ^ % arr) {
  int32_t size;
  input.readInt(&size);
  if (size > 0) {
    arr = gcnew array<TObj ^>(size);
            for
              each(TObj ^ obj in arr) { ReadObject(input, mg_input, obj); }
  } else {
    arr = nullptr;
  }
}

template <typename TObj>
inline void WriteObject(gemfire::DataOutput& output, DataOutput ^ mg_output,
                        List<TObj> ^ value) {
  if (value != nullptr) {
    output.writeInt(value->Count);
            for
              each(TObj obj in value) { WriteObject(output, mg_output, obj); }
  } else {
    output.writeInt(-1);
  }
}

template <typename TObj>
inline void ReadObject(gemfire::DataInput& input, DataInput ^ mg_input,
                       List<TObj> ^ % value) {
  int32_t len;
  input.readInt(&len);
  if (len > 0) {
    value = gcnew List<TObj>(len);
    for (int32_t listIndex = 0; listIndex < len; listIndex++) {
      ReadObject(input, mg_input, value[listIndex]);
    }
  } else {
    value = nullptr;
  }
}

template <typename TKey, typename TVal>
inline void WriteObject(gemfire::DataOutput& output, DataOutput ^ mg_output,
                        Dictionary<TKey, TVal> ^ value) {
  if (value != nullptr) {
    output.writeInt(value->Count);
            for
              each(KeyValuePair<TKey, TVal> ^ pair in value) {
                WriteObject(output, mg_output, pair->Key);
                WriteObject(output, mg_output, pair->Value);
              }
  } else {
    output.writeInt(-1);
  }
}

template <typename TKey, typename TVal>
inline void ReadObject(gemfire::DataInput& input, DataInput ^ mg_input,
                       Dictionary<TKey, TVal> ^ % value) {
  int32_t len;
  input.readInt(&len);
  if (len > 0) {
    TKey ^ key;
    TVal ^ val;
    value = gcnew Dictionary<TKey, TVal>(len);
    for (int32_t listIndex = 0; listIndex < len; listIndex++) {
      ReadObject(input, mg_input, key);
      ReadObject(input, mg_input, val);
      value[key] = val;
    }
  } else {
    value = nullptr;
  }
}
}
}
}
}

#endif  // GEMFIRE_CLR

#endif  // _GFAS_ASCLIBUILTINS_HPP_

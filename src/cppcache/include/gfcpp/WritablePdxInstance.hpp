#ifndef __WRITABLE_PDXINSTANCE_HPP_
#define __WRITABLE_PDXINSTANCE_HPP_

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "SharedPtr.hpp"
#include "Cacheable.hpp"

#include "PdxInstance.hpp"

namespace gemfire {

/**
* WritablePdxInstance is a {@link PdxInstance} that also supports field
* modification
* using the {@link #setField} method.
* To get a WritablePdxInstance call {@link PdxInstance#createWriter}.
*/
class CPPCACHE_EXPORT WritablePdxInstance : public PdxInstance {
  /**
  * @brief public methods
  */

 public:
  /**
  * @brief destructor
  */
  virtual ~WritablePdxInstance() {}

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * CacheablePtr type is corresponding to java object type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type CacheablePtr
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, CacheablePtr value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * bool type is corresponding to java boolean type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type bool
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, bool value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * signed char type is corresponding to java byte type.
  * For C++ on Windows and Linux, signed char type is corresponding to int8_t
  * type.
  * However C++ users on Solaris should always use this api after casting int8_t
  * to signed char.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type signed char
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, signed char value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * unsigned char type is corresponding to java byte type.
  * For C++ on Windows and Linux, unsigned char type is corresponding to int8_t
  * type.
  * However C++ users on Solaris should always use this api after casting int8_t
  * to unsigned char.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type unsigned char
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, unsigned char value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int16_t type is corresponding to java short type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int16_t
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int16_t value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int32_t type is corresponding to java int type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int32_t
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int32_t value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int64_t type is corresponding to java long type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int64_t
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int64_t value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * float type is corresponding to java float type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type float
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, float value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * double type is corresponding to java double type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type double
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, double value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * wchar_t type is corresponding to java char type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type wchar_t
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, wchar_t value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * char type is corresponding to java char type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type char
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, char value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * CacheableDatePtr type is corresponding to java Java.util.date type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type CacheableDatePtr
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, CacheableDatePtr value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * bool* type is corresponding to java boolean[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type bool array
  * @param length
  *          The number of elements in bool array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, bool* value, int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * signed char* type is corresponding to java byte[] type.
  * For C++ on Windows and Linux, signed char* type is corresponding to int8_t*
  * type.
  * However C++ users on Solaris should always use this api after casting
  * int8_t* to signed char*.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type signed char array
  * @param length
  *          The number of elements in signed char array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, signed char* value,
                        int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * unsigned char* type is corresponding to java byte[] type.
  * For C++ on Windows and Linux, unsigned char* type is corresponding to
  * int8_t* type.
  * However C++ users on Solaris should always use this api after casting
  * int8_t* to unsigned char*.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type unsigned char array
  * @param length
  *          The number of elements in unsigned char array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, unsigned char* value,
                        int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int16_t* type is corresponding to java short[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int16_t array
  * @param length
  *          The number of elements in int16_t array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int16_t* value,
                        int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int32_t* type is corresponding to java int[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int32_t array
  * @param length
  *          The number of elements in int32_t array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int32_t* value,
                        int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int64_t* type is corresponding to java long[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type int64_t array
  * @param length
  *          The number of elements in int64_t array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int64_t* value,
                        int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * float* type is corresponding to java float[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type float array
  * @param length
  *          The number of elements in float array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, float* value,
                        int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * double* type is corresponding to java double[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type double array
  * @param length
  *          The number of elements in double array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, double* value,
                        int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * wchar_t* type is corresponding to java String type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type wchar_t*
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, const wchar_t* value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * char* type is corresponding to java String type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type char*
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, const char* value) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * wchar_t* type is corresponding to java char[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type wchar_t array
  * @param length
  *          The number of elements in wchar_t array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, wchar_t* value,
                        int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * char* type is corresponding to java char[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type char array
  * @param length
  *          The number of elements in char array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, char* value, int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * wchar_t** type is corresponding to java String[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type wchar_t* array
  * @param length
  *          The number of elements in WCString array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, wchar_t** value,
                        int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * char** type is corresponding to java String[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type char* array
  * @param length
  *          The number of elements in CString array type.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, char** value,
                        int32_t length) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * int8_t** type is corresponding to java byte[][] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type array of byte arrays
  * @param arrayLength
  *          The number of byte arrays.
  * @param elementLength
  *          The lengths of individual byte arrays.
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName, int8_t** value,
                        int32_t arrayLength, int32_t* elementLength) = 0;

  /**
  * Set the existing named field to the given value.
  * The setField method has copy-on-write semantics.
  * So for the modifications to be stored in the cache the WritablePdxInstance
  * must be put into a region after setField has been called one or more times.
  * CacheableObjectArrayPtr type is corresponding to java Object[] type.
  * @param fieldName
  *          name of the field whose value will be set
  * @param value
  *          value that will be set to the field of type CacheableObjectArrayPtr
  * @throws IllegalStateException if the named field does not exist
  * or if the type of the value is not compatible with the field.
  */
  virtual void setField(const char* fieldName,
                        CacheableObjectArrayPtr value) = 0;

 protected:
  /**
  * @brief constructors
  */
  WritablePdxInstance(){};

 private:
  // never implemented.
  WritablePdxInstance(const WritablePdxInstance& other);
  void operator=(const WritablePdxInstance& other);
};
}

#endif /* __WRITABLE_PDXINSTANCE_HPP_ */

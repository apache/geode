/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/*
 * @brief ExampleObject class for testing the put functionality for object. 
 */


#ifndef __EXAMPLE_OBJECT_HPP__
#define __EXAMPLE_OBJECT_HPP__

#ifdef _EXAMPLE
#include <gfcpp/GemfireCppCache.hpp>
#else
#include <../GemfireCppCache.hpp>
#endif
#include <string>
#include <vector>
#include <stdlib.h>

class ExampleObject
: public CacheableKey
// : public Serializable
{
  private:
    double double_field;
    float float_field;
    long long_field;
    int  int_field;
    short short_field;
    std::string string_field;
    std::vector<std::string> string_vector;
  public:
    ExampleObject() {
      double_field = 0.0;
      float_field = 0.0;
      long_field = 0;
      int_field = 0;
      short_field = 0;
      string_field = "";
      string_vector.clear();
    }
    ~ExampleObject() {
    }
    ExampleObject(int id) {
      char buf[64];
      sprintf(buf, "%d", id);
      std::string sValue(buf);
      int_field = id;
      long_field = int_field;
      short_field = int_field;
      double_field = (double)int_field;
      float_field = (float)int_field;
      string_field = sValue;
      string_vector.clear();
      for (int i=0; i<3; i++) {
        string_vector.push_back(sValue);
      }
    }
    ExampleObject(std::string sValue) {
      int_field = atoi(sValue.c_str());
      long_field = int_field;
      short_field = int_field;
      double_field = (double)int_field;
      float_field = (float)int_field;
      string_field = sValue;
      string_vector.clear();
      for (int i=0; i<3; i++) {
        string_vector.push_back(sValue);
      }
    }
    CacheableStringPtr toString() const {
      char buf[1024];
      std::string sValue = "ExampleObject: ";
      sprintf(buf,"%f(double),%f(double),%ld(long),%d(int),%d(short),", double_field,float_field,long_field,int_field,short_field);
      sValue += std::string(buf) + string_field + "(string),";
      if (string_vector.size() >0) {
        sValue += "[";
        for (unsigned int i=0; i<string_vector.size(); i++) {
          sValue += string_vector[i];
          if (i != string_vector.size()-1) {
            sValue += ",";
          }
        }
        sValue += "](string vector)";
      }
      return CacheableString::create( sValue.c_str() );
    }
    double getDouble_field() {
      return double_field;
    }
    float getFloat_field() {
      return float_field;
    }
    long getLong_field() {
      return long_field;
    }
    int getInt_field() {
      return int_field;
    }
    short getShort_field() {
      return short_field;
    }
    std::string & getString_field() {
      return string_field;
    }
    std::vector<std::string> & getString_vector( ) {
      return string_vector;
    }

// Add the following for the Serializable interface
// Our TypeFactoryMethod
    
    static Serializable* createInstance( ) {
      return new ExampleObject();
    } 
    int32_t classId( ) const
    { 
      return 0x2e; // 46
    }
    bool operator == ( const CacheableKey& other ) const {
      const ExampleObject& otherEO = static_cast<const ExampleObject&>( other );
      return ( 0 == strcmp( otherEO.toString()->asChar(), toString()->asChar() ) ); 
    }
    uint32_t hashcode( ) const {
      return int_field;
    }

    uint32_t objectSize( ) const
    {
      uint32_t objectSize = sizeof(ExampleObject);
      objectSize += sizeof(char) * ( string_field.size() + 1 );
      size_t itemCount = string_vector.size();
      for( size_t idx = 0; idx < itemCount; idx++ ) {
        // copy each string to the serialization buffer, including the null
        // terminating character at the end of the string.
        objectSize += sizeof(char) * ( string_vector[idx].size() + 1 );
      }
      return objectSize;
    }
    
    void toData( DataOutput& output ) const {
      output.writeDouble( double_field );
      output.writeFloat( float_field );
      output.writeInt( (int64_t)long_field );
      output.writeInt( (int32_t)int_field );
      output.writeInt( (int16_t)short_field );
      output.writeASCII( string_field.c_str(), string_field.size());
      size_t itemCount = string_vector.size();
      output.writeInt( (int32_t) itemCount );
      for( size_t idx = 0; idx < itemCount; idx++ ) {
        // copy each string to the serialization buffer, including the null
        // terminating character at the end of the string.
        output.writeASCII( string_vector[idx].c_str(), string_vector[idx].size() );
      }
    }

    Serializable* fromData( DataInput& input )
    {
      char *readbuf;
      input.readDouble( &double_field );
      input.readFloat( &float_field );
      input.readInt( (int64_t *)(void *)&long_field );
      input.readInt( (int32_t *)(void *)&int_field );
      input.readInt( (int16_t *)(void *)&short_field );

      int32_t itemCount = 0;
      input.readASCII( &readbuf );
      string_field = std::string(readbuf);
      input.freeUTFMemory( readbuf );

      string_vector.clear();
      input.readInt( (int32_t*) &itemCount );
      for( int32_t idx = 0; idx < itemCount; idx++ ) {
        // read from serialization buffer into a character array
        input.readASCII( &readbuf );
        // and store in the history list of strings.
        string_vector.push_back( readbuf );
        input.freeUTFMemory( readbuf );
      }
      return this;
    }

};

typedef SharedPtr<ExampleObject> ExampleObjectPtr;

#endif


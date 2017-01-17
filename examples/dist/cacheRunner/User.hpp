/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * @brief User class for testing the put functionality for object. 
 */


#ifndef __USER_HPP__
#define __USER_HPP__

#ifdef _EXAMPLE
#include <gfcpp/GemfireCppCache.hpp>
#else
#include <../GemfireCppCache.hpp>
#endif
#include "ExampleObject.hpp"
#include <string>

class User
: public Serializable
{
  private:
    std::string name;
    int32_t userId;
    ExampleObject *eo;
  public:
    User( std::string name, int32_t userId )
    : name( name ),userId( userId )
    {
      eo = new ExampleObject(this->userId);
    }

    ~User() {
      if (eo != NULL) delete eo;
        eo = NULL;
    }

    User ()
    {
      name = "";
      userId = 0;
      eo = new ExampleObject(userId);
    }

    User( const char *strfmt, char delimeter )
    {
      std::string userId_str;
      std::string sValue(strfmt);
      std::string::size_type pos1 = sValue.find_first_of(delimeter);
      std::string::size_type pos2;
      if (pos1 == std::string::npos) {
        userId_str = sValue;
        name = sValue; 
      } 
      else {
        userId_str = sValue.substr(0, pos1);
        pos2 = sValue.find(delimeter, pos1+1);
        int len;
        if (pos2==std::string::npos) {
          len = sValue.length()-pos1;
        } 
        else {
          len = pos2-pos1;
        }
        name = sValue.substr(pos1+1, len);
      }
      userId = (int32_t)atoi(userId_str.c_str());
      eo = new ExampleObject(userId_str);
    }

    CacheableStringPtr toString() const
    {
      CacheableStringPtr eo_str = eo->toString();
      char userId_str[128];
      sprintf(userId_str,"User: %d", userId);
      std::string sValue = std::string(userId_str) + "," + name + "\n";
      sValue += std::string(eo_str->asChar());
      return CacheableString::create( sValue.c_str() );
    }

    int32_t getUserId( )
    {
      return userId;
    }

    std::string getName( )
    {
      return name;
    }

    ExampleObject *getEO()
    {
      return eo;
    }

    void setEO(ExampleObject *eObject)
    {
      eo = eObject;
    }

    // Add the following for the Serializable interface
    // Our TypeFactoryMethod

    static Serializable* createInstance( )
    {
      return new User(std::string("gester"), 123);
    }

    int32_t classId( ) const
    {
      return 0x2d; // 45
    }

    void toData( DataOutput& output ) const
    {
      output.writeASCII( name.c_str(), name.size() );
      output.writeInt( userId );
      eo->toData(output);
    }

    uint32_t objectSize( ) const
    {
      return ( sizeof(char) * ( name.size() + 1 ) ) +
        sizeof(User) + eo->objectSize();
    }

    Serializable* fromData( DataInput& input )
    {
      char *readbuf;
      input.readASCII( &readbuf );
      name = std::string(readbuf);
      input.freeUTFMemory( readbuf );
      input.readInt( &userId );
      eo->fromData(input);
      return this;
    }
};

typedef SharedPtr<User> UserPtr;
#endif

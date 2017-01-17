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

#pragma once



#include "../gf_defs.hpp"

#ifdef _WIN32
#define snprintf _snprintf
#endif


using namespace System;

namespace GemStone
{
  namespace GemFire
	{namespace Cache { namespace Generic{

    ref class ManagedString sealed
    {
    private:

      IntPtr m_str;


    public:

      // Constructors

      inline ManagedString( String^ str )
      {
        m_str = (str == nullptr) ? IntPtr::Zero :
          System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi( str );
      }

      // Destructor

      inline ~ManagedString( )
      {
        if (m_str != IntPtr::Zero)
        {
          System::Runtime::InteropServices::Marshal::FreeHGlobal( m_str );
        }
      }

      // The finalizer should normally never be called; either use non-pointer object
      // or call delete explicitly.
      !ManagedString( )
      {
        if (m_str != IntPtr::Zero)
        {
          System::Runtime::InteropServices::Marshal::FreeHGlobal( m_str );
        }
#if GF_DEVEL_ASSERTS == 1
        throw gcnew System::ApplicationException(
          "Finalizer for ManagedString should not have been called!!" );
#endif
      }

      inline static String^ Get( const char* str )
      {
        return ((str == nullptr) ? nullptr : gcnew String( str ));
      }

      inline static String^ Get( const wchar_t* str )
      {
        return ((str == nullptr) ? nullptr : gcnew String( str ));
      }

      // Properties

      property const char* CharPtr
      {
        inline const char* get( )
        {
          return ((m_str == IntPtr::Zero) ? nullptr :
            static_cast<const char*>( m_str.ToPointer( ) ));
        }
      }
    };
	}}
  }
}

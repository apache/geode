/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#pragma once

#include "gf_defs.hpp"
#include "gfcpp/UserFunctionExecutionException.hpp"
#include "IGFSerializable.hpp"
#include "DataInput.hpp"
#include "DataOutput.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
        /// <summary>
        /// UserFunctionExecutionException class is used to encapsulate gemfire sendException in case of Function execution. 
        /// </summary>
        public ref class UserFunctionExecutionException sealed
          : public Internal::SBWrap<gemfire::UserFunctionExecutionException>, public IGFSerializable
        {
        public:
          // IGFSerializable members

          /// <summary>
          /// Serializes this object.
          /// Users should not implement/call this api as it is only intended for internal use.
          /// </summary>
          /// <param name="output">
          /// the DataOutput stream to use for serialization
          /// </param>
          /// <exception cref="IllegalStateException">
          /// If this api is called from User code.
          /// </exception>
          virtual void ToData( DataOutput^ output );

          /// <summary>
          /// Deserializes this object.
          /// Users should not implement/call this api as it is only intended for internal use.
          /// </summary>
          /// <param name="input">
          /// the DataInput stream to use for reading data
          /// </param>
          /// <exception cref="IllegalStateException">
          /// If this api is called from User code.
          /// </exception>
          /// <returns>the deserialized object</returns>
          virtual IGFSerializable^ FromData( DataInput^ input );        

          /// <summary>
          /// Returns the classId of this class for serialization.
          /// Users should not implement/call this api as it is only intended for internal use.
          /// </summary>
          /// <exception cref="IllegalStateException">
          /// If this api is called from User code.
          /// </exception>
          /// <returns>classId of this class</returns>
          /// <seealso cref="IGFSerializable.ClassId" />
          virtual property uint32_t ClassId
          {
            inline virtual uint32_t get( )
            {
              throw gcnew IllegalStateException("UserFunctionExecutionException::ClassId is not intended for use.");
              return 0;
            }
          }

          /// <summary>
          /// return the size of this object in bytes
          /// Users should not implement/call this api as it is only intended for internal use.
          /// </summary>
          /// <exception cref="IllegalStateException">
          /// If this api is called from User code.
          /// </exception>
          virtual property uint32_t ObjectSize
          {
            virtual uint32_t get( ); 
          }

          // End: IGFSerializable members   

          /// <summary>
          /// return as String the Exception message returned from gemfire sendException api.          
          /// </summary>
          /// <returns>the String Exception Message</returns>
          property String^ Message
          {
            String^ get();          
          }          

          /// <summary>
          /// return as String the Exception name returned from gemfire sendException api.          
          /// </summary>
          /// <returns>the String Exception Name</returns>
          property String^ Name
          {
            String^ get();          
          } 

        internal:

          /// <summary>
          /// Private constructor to wrap a native object pointer.
          /// </summary>
          /// <param name="nativeptr">The native object pointer</param>
          inline UserFunctionExecutionException( gemfire::UserFunctionExecutionException* nativeptr )
            : SBWrap( nativeptr ) { }
        };

      }
    }
  }
}


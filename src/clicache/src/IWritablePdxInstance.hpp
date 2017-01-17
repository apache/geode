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
	      /// WritablePdxInstance is a <see cref="IPdxInstance" /> that also supports field modification 
        /// using the <see cref="SetField" />method. 
        /// To get a WritablePdxInstance call <see cref="IPdxInstance.CreateWriter" />.
 				/// </summary>
				public interface class IWritablePdxInstance
				{
        public:
          /// <summary>
          ///Set the existing named field to the given value.
          ///The setField method has copy-on-write semantics.
          /// So for the modifications to be stored in the cache the WritablePdxInstance 
          ///must be put into a region after setField has been called one or more times.
          /// </summary>
          ///
          ///<param name="fieldName"> name of the field whose value will be set </param>
          ///<param name="value"> value that will be assigned to the field </param>
          ///<exception cref="IllegalStateException"/> if the named field does not exist
          ///or if the type of the value is not compatible with the field </exception>
          
          void SetField(String^ fieldName, Object^ value);
				};
			}
    }
  }
}

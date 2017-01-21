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

#include "gf_defs.hpp"
#include "IRegion.hpp"
#include "Properties.hpp"
using namespace System;
namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

         /// <summary>
        /// IPersistenceManager interface for persistence and overflow. 
        /// This class abstracts the disk-related operations in case of persistence or overflow to disk.
        /// A specific disk storage implementation will implement all the methods described here.
        /// </summary>
        generic<class TKey, class TValue>
        public interface class IPersistenceManager
        {
        public:
          /// <summary>
          /// Called after an implementation object is created. Initializes all the implementation
          /// specific environments needed.
          /// </summary>
          /// <param name="region">
          /// Region for which this PersistenceManager is initialized.
          /// </param>
          /// <param name="diskProperties">
          /// Configuration Properties used by PersistenceManager implementation.
          /// </param>
          void Init(IRegion<TKey, TValue>^ region, Properties<String^, String^>^ diskProperties);
          
          /// <summary>
          /// Writes a key, value pair of region to the disk. The actual file or database related write operations should be implemented 
          /// in this method by the class implementing this method.
          /// </summary>
          /// <param name="key">
          /// the key to write.
          /// </param>
          /// <param name="value">
          /// the value to write.
          /// </param>
          void Write(TKey key, TValue value);

          /// <summary>
          /// Writes all the entries for a region. Refer persistance requirement doc for the use case.
          /// </summary>
          /// <returns>
          /// true if WriteAll is successful.
          /// </returns>
          bool WriteAll();

          /// <summary>
          /// Reads the value for the key from the disk.
          /// </summary>
          /// <param name="key">
          /// key for which the value has to be read.
          /// </param>
          TValue Read(TKey key);

          /// <summary>
          /// Reads all the values from the region.
          /// </summary>
          /// <returns>
          /// true if ReadAll is successful.
          /// </returns>
          bool ReadAll();

          /// <summary>
          /// Destroys the entry specified by the key in the argument.
          /// </summary>
          /// <param name="key">
          /// key of the entry which is being destroyed.
          /// </param>
          void Destroy(TKey key);

          /// <summary>
          /// Closes the persistence manager instance.
          /// </summary>
          void Close();

        };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache


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
#include "IPartitionResolver.hpp"

//using System::Collections::Generics;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      interface class IGFSerializable;
      /// <summary>
      /// Implementers of interface <code>FixedPartitionResolver</code> helps to
      /// achieve explicit mapping of a "user defined" partition to a data member node.
      /// </summary>
      /// <remarks>
      /// <p>
      /// Geode uses the partition name returned by 
      /// {@link FixedPartitionResolver#getPartitionName(EntryEvent, CacheableHashSet)}
      /// to determine on which member the data is being managed. Say, for example, you want to
      /// partition all Trades according to quarters. You can implement
      /// FixedPartitionResolver to get the name of the quarter based on the date given
      /// as part of {@link EntryEvent}.
      /// </p>
      ///  
      /// public class QuarterPartitionResolver implements FixedPartitionResolver{<br>
      /// &nbsp &nbsp public String getPartitionName(EntryOperation opDetails, CacheableHashSet
      /// allAvailablePartitions) {<br>
      /// &nbsp &nbsp Date date = sdf.parse((String)opDetails.getKey());<br>
      /// &nbsp &nbsp Calendar cal = Calendar.getInstance();<br>
      /// &nbsp &nbsp cal.setTime(date);<br>
      /// &nbsp &nbsp int month = cal.get(Calendar.MONTH);<br>
      /// &nbsp &nbsp if (month == 0 || month == 1 || month == 2) {<br>
      /// &nbsp &nbsp &nbsp return "Quarter1";<br>
      /// &nbsp &nbsp }<br>
      /// &nbsp &nbsp else if (month == 3 || month == 4 || month == 5) {<br>
      /// &nbsp &nbsp &nbsp return "Quarter2";<br>
      /// &nbsp &nbsp }<br>
      /// &nbsp &nbsp else if (month == 6 || month == 7 || month == 8) {<br>
      /// &nbsp &nbsp &nbsp return "Quarter3";<br>
      /// &nbsp &nbsp }<br>
      /// &nbsp &nbsp else if (month == 9 || month == 10 || month == 11) {<br>
      /// &nbsp &nbsp &nbsp return "Quarter4";<br>
      /// &nbsp &nbsp }<br>
      /// &nbsp &nbsp else {<br>
      /// &nbsp &nbsp &nbsp return "Invalid Quarter";<br>
      /// &nbsp &nbsp }<br>
      /// &nbsp }<br>
      ///
      /// @see PartitionResolver
      ///
      /// </remarks>
      /// <seealso cref="AttributesFactory.SetPartitionResolver" />
      /// <seealso cref="RegionAttributes.PartitionResolver" />
      generic<class TKey, class TValue>
      public interface class IFixedPartitionResolver : public IPartitionResolver<TKey, TValue>
      {
      public:

        /// <summary>
        /// This method is used to get the name of the partition for the given entry
        /// operation.
        /// </summary> 
        /// <param name="opDetails"> 
        /// the details of the entry event e.g. {@link Region#get(Object)}
        /// </param>
        /// <return> partition-name associated with node which allows mapping of given
        /// data to user defined partition
        /// </return>         
        String^ GetPartitionName(EntryEvent<TKey, TValue>^ opDetails);
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache


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



//#include "gf_includes.hpp"
#include "StatisticsType.hpp"
#include "StatisticDescriptor.hpp"

#include "impl/ManagedString.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      String^ StatisticsType::Name::get()
      {
        return ManagedString::Get( NativePtr->getName() );
      }

      String^ StatisticsType::Description::get()
      {
        return ManagedString::Get( NativePtr->getDescription() );
      }

      array<StatisticDescriptor^>^ StatisticsType::Statistics::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::statistics::StatisticDescriptor ** nativedescriptors = NativePtr->getStatistics();
          array<StatisticDescriptor^>^ descriptors = gcnew array<StatisticDescriptor^>(NativePtr->getDescriptorsCount());
          for (int item = 0; item < NativePtr->getDescriptorsCount(); item++)
          {
            descriptors[item] = StatisticDescriptor::Create(nativedescriptors[item]);
          }
          return descriptors;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int32_t StatisticsType::NameToId( String^ name )
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->nameToId(mg_name.CharPtr);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      StatisticDescriptor^ StatisticsType::NameToDescriptor( String^ name )
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return StatisticDescriptor::Create(NativePtr->nameToDescriptor(mg_name.CharPtr));
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int32_t StatisticsType::DescriptorsCount::get()
      {
        return NativePtr->getDescriptorsCount();
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

 } //namespace 


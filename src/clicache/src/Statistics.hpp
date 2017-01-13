/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



#pragma once

#include "gf_defs.hpp"
#include "impl/NativeWrapper.hpp"
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/statistics/StatisticDescriptor.hpp>
#include <gfcpp/statistics/StatisticsType.hpp>

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      ref class StatisticDescriptor;
      ref class StatisticsType;

      /// <summary>
      /// An instantiation of an existing <c>StatisticsType</c> object with methods for
      /// setting, incrementing and getting individual <c>StatisticDescriptor</c> values.
      /// </summary>
      /// <para>
      /// The class is purposefully inherited from UMWrapN and not UMWrap as the destructor
      /// of the class is protected, and so it is now not called from inside the InternalCleanup
      /// method.
      /// </para>
      public ref class Statistics sealed
        : public Internal::UMWrapN<gemfire_statistics::Statistics>
       {
       public:

         /// <summary>
         /// Closes these statistics.  After statistics have been closed, they
         /// are no longer archived.
         /// A value access on a closed statistics always results in zero.
         /// A value modification on a closed statistics is ignored.
         /// </summary>
         virtual void Close();

         /// <summary>
         /// Returns the id of the statistic with the given name in this
         /// statistics instance.
         /// </summary>
         /// <param name="name">the statistic name</param>
         /// <returns>the id of the statistic with the given name</returns>
         /// <exception cref="IllegalArgumentException">
         /// if no statistic named <c>name</c> exists in this
         /// statistics instance.
         /// </exception>
         /// <see cref="StatisticsType#nameToDescriptor" />        
         virtual int32_t NameToId(String^ name);

         /// <summary>
         /// Returns the descriptor of the statistic with the given name in this
         /// statistics instance.
         /// </summary>
         /// <param name="name">the statistic name</param>
         /// <returns>the descriptor of the statistic with the given name</returns>
         /// <exception cref="IllegalArgumentException">
         /// if no statistic named <c>name</c> exists in this
         /// statistics instance.
         /// </exception>
         /// <see cref="StatisticsType#nameToId" />
         virtual StatisticDescriptor^ NameToDescriptor(String^ name);

         /// <summary>
         /// Gets a value that uniquely identifies this statistics.
         /// </summary>
         virtual property int64_t UniqueId
         {
           virtual int64_t get( );
         }

         /// <summary>
         /// Gets the <see cref="StatisticsType" /> of this instance.
         /// </summary>
         virtual property StatisticsType^ Type
         {
           virtual StatisticsType^ get( );
         }

         /// <summary>
         /// Gets the text associated with this instance that helps identify it.
         /// </summary>
         virtual property String^ TextId
         {
           virtual String^ get( );
         }

         /// <summary>
         /// Gets the number associated with this instance that helps identify it.
         /// </summary>
         virtual property int64_t NumericId 
         {
           virtual int64_t get( );
         }

         /// <summary>
         /// Returns true if modifications are atomic. This means that multiple threads
         /// can safely modify this instance without additional synchronization.
         /// </summary>
         /// <para>
         /// Returns false if modifications are not atomic. This means that modifications
         /// to this instance are cheaper but not thread safe.
         /// </para>
         /// <para>
         /// Note that all instances that are <see cref="#isShared" /> shared are also atomic.
         /// </para>
         virtual property bool IsAtomic
         {
           virtual bool get( );
         }

         /// <summary>
         /// Returns true if the data for this instance is stored in shared memory.
         /// Returns false if the data is store in local memory.
         /// </summary>
         /// <para>
         /// Note that all instances that are <see cref="#isShared" /> shared are also atomic.
         /// </para>
         virtual property bool IsShared
         {
           virtual bool get( );
         }

         /// <summary>
         /// Returns true if the instance has been <see cref="#close" /> closed.
         /// </summary>
         virtual property bool IsClosed
         {
           virtual bool get( );
         }

         /// <summary>
         /// Sets the value of a statistic with the given <c>id</c>
         /// whose type is <c>int</c>.
         /// </summary>
         /// <param name="id">a statistic id obtained with <see cref="#nameToId" />
         /// or <see cref="#StatisticsType#nameToId" /> </param>
         /// <param name="value">value to set</param>
         /// <exception cref="IllegalArgumentException">
         /// If the id is invalid.
         /// </exception>
         virtual void SetInt(int32_t id, int32_t value);

         /// <summary>
         /// Sets the value of a named statistic of type <c>int</c>
         /// </summary>
         /// <param name="name">statistic name</param>
         /// <param name="value">value to set</param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists named <c>name</c> or
         /// if the statistic with name <c>name</c> is not of
         /// type <c>int</c>.
         /// </exception>
         virtual void SetInt(String^ name, int32_t value);

         /// <summary>
         /// Sets the value of a described statistic of type <c>int</c>
         /// </summary>
         /// <param name="descriptor">a statistic descriptor obtained with <see cref="#nameToDescriptor" />
         /// or <see cref="#StatisticsType#nameToDescriptor" /> </param>
         /// <param name="value">value to set</param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists for the given <c>descriptor</c> or
         /// if the described statistic is not of
         /// type <c>int</c>.
         /// </exception>
         virtual void SetInt(StatisticDescriptor^ descriptor, int32_t value);

         /// <summary>
         /// Sets the value of a statistic with the given <c>id</c>
         /// whose type is <c>long</c>.
         /// </summary>
         /// <param name="id">a statistic id obtained with <see cref="#nameToId" /> 
         /// or <see cref="#StatisticsType#nameToId" />. </param>
         /// <param name="value">value to set</param>
         /// <exception cref="IllegalArgumentException">
         /// If the id is invalid.
         /// </exception>
         virtual void SetLong(int32_t id, int64_t value); 

         /// <summary>
         /// Sets the value of a described statistic of type <c>long</c>
         /// </summary>
         /// <param name="descriptor">a statistic descriptor obtained with <see cref="#nameToDescriptor" />
         /// or <see cref="StatisticsType#nameToDescriptor" /> </param>
         /// <param name="value">value to set</param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists for the given <c>descriptor</c> or
         /// if the described statistic is not of
         /// type <c>long</c>.
         /// </exception>
         virtual void SetLong(StatisticDescriptor^ descriptor, int64_t value);

         /// <summary>
         /// Sets the value of a named statistic of type <c>long</c>.
         /// </summary>
         /// <param name="name">statistic name</param>
         /// <param name="value">value to set</param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists named <c>name</c> or
         /// if the statistic with name <c>name</c> is not of
         /// type <c>long</c>.
         /// </exception>
         virtual void SetLong(String^ name, int64_t value);


         /// <summary>
         /// Sets the value of a statistic with the given <c>id</c>
         /// whose type is <c>double</c>.
         /// </summary>
         /// <param name="id">a statistic id obtained with <see cref="#nameToId" />
         /// or <see cref="#StatisticsType#nameToId" /> </param>
         /// <param name="value">value to set</param>
         /// <exception cref="IllegalArgumentException">
         /// If the id is invalid.
         /// </exception>
         virtual void SetDouble(int32_t id, double value);

         /// <summary>
         /// Sets the value of a named statistic of type <c>double</c>
         /// </summary>
         /// <param name="name">statistic name</param>
         /// <param name="value">value to set</param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists named <c>name</c> or
         /// if the statistic with name <c>name</c> is not of
         /// type <c>double</c>.
         /// </exception>
         virtual void SetDouble(String^ name, double value);

         /// <summary>
         /// Sets the value of a described statistic of type <c>double</c>
         /// </summary>
         /// <param name="descriptor">a statistic descriptor obtained with <see cref="#nameToDescriptor" />
         /// or <see cref="StatisticsType#nameToDescriptor" /> </param>
         /// <param name="value">value to set</param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists for the given <c>descriptor</c> or
         /// if the described statistic is not of
         /// type <c>double</c>.
         /// </exception>
         virtual void SetDouble(StatisticDescriptor^ descriptor, double value);

         /// <summary>
         /// Returns the value of the identified statistic of type <c>int</c>.
         /// whose type is <c>double</c>.
         /// </summary>
         /// <param name="id">a statistic id obtained with <see cref="#nameToId" />
         /// or <see cref="#StatisticsType#nameToId" /> </param>
         /// <exception cref="IllegalArgumentException">
         /// If the id is invalid.
         /// </exception>
         virtual int32_t GetInt(int32_t id);

         /// <summary>
         /// Returns the value of the described statistic of type <code>int</code>.
         /// </summary>
         /// <param name="descriptor">a statistic descriptor obtained with <see cref="#nameToDescriptor" />
         /// or <see cref="StatisticsType#nameToDescriptor" /> </param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists with the specified <c>descriptor</c> or
         /// if the described statistic is not of
         /// type <c>int</c>.
         /// </exception>
         virtual int32_t GetInt(StatisticDescriptor^ descriptor);


         /// <summary>
         /// Returns the value of the statistic of type <code>int</code> at
         /// the given name.
         /// </summary>
         /// <param name="name">statistic name</param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists named <c>name</c> or
         /// if the statistic with name <c>name</c> is not of
         /// type <c>int</c>.
         /// </exception>
         virtual int32_t GetInt(String^ name);

         /// <summary>
         /// Returns the value of the identified statistic of type <c>long</c>.
         /// </summary>
         /// <param name="id">a statistic id obtained with <see cref="#nameToId" />
         /// or <see cref="#StatisticsType#nameToId" /> </param>
         /// <exception cref="IllegalArgumentException">
         /// If the id is invalid.
         /// </exception>
         virtual int64_t GetLong(int32_t id);

         
         /// <summary>
         /// Returns the value of the described statistic of type <c>long</c>.
         /// </summary>
         /// <param name="descriptor">a statistic descriptor obtained with <see cref="#nameToDescriptor" />
         /// or <see cref="StatisticsType#nameToDescriptor" /> </param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists with the specified <c>descriptor</c> or
         /// if the described statistic is not of
         /// type <c>long</c>.
         /// </exception>
         virtual int64_t GetLong(StatisticDescriptor^ descriptor);

         
         /// <summary>
         /// Returns the value of the statistic of type <c>long</c> at
         /// the given name.
         /// </summary>
         /// <param name="name">statistic name</param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists named <c>name</c> or
         /// if the statistic with name <c>name</c> is not of
         /// type <c>long</c>.
         /// </exception>
         virtual int64_t GetLong(String^ name);


         /// <summary>
         /// Returns the value of the identified statistic of type <c>double</c>.
         /// </summary>
         /// <param name="id">a statistic id obtained with <see cref="#nameToId" />
         /// or <see cref="#StatisticsType#nameToId" /> </param>
         /// <exception cref="IllegalArgumentException">
         /// If the id is invalid.
         /// </exception>
         virtual double GetDouble(int32_t id);
                  
         /// <summary>
         /// Returns the value of the described statistic of type <c>double</c>.
         /// </summary>
         /// <param name="descriptor">a statistic descriptor obtained with <see cref="#nameToDescriptor" />
         /// or <see cref="StatisticsType#nameToDescriptor" /> </param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists with the specified <c>descriptor</c> or
         /// if the described statistic is not of
         /// type <c>double</c>.
         /// </exception>
         virtual double GetDouble(StatisticDescriptor^ descriptor);

         /// <summary>
         /// Returns the value of the statistic of type <c>double</c> at
         /// the given name.
         /// </summary>
         /// <param name="name">statistic name</param>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists named <c>name</c> or
         /// if the statistic with name <c>name</c> is not of
         /// type <c>double</c>.
         /// </exception>
         virtual double GetDouble(String^ name);

         /// <summary>
         /// Returns the bits that represent the raw value of the described statistic.
         /// </summary>
         /// <param name="descriptor">a statistic descriptor obtained with <see cref="#nameToDescriptor" />
         /// or <see cref="StatisticsType#nameToDescriptor" /> </param>
         /// <exception cref="IllegalArgumentException">
         /// If the described statistic does not exist
         /// </exception>
         virtual int64_t GetRawBits(StatisticDescriptor^ descriptor);

         /// <summary>
         /// Increments the value of the identified statistic of type <c>int</c>
         /// by the given amount.
         /// </summary>
         /// <param name="id">a statistic id obtained with <see cref="#nameToId" />
         /// or <see cref="#StatisticsType#nameToId" /> </param>
         /// <param name="delta">the value of the statistic after it has been incremented</param>
         /// <returns>the value of the statistic after it has been incremented</returns>
         /// <exception cref="IllegalArgumentException">
         /// If the id is invalid.
         /// </exception>
         virtual int32_t IncInt(int32_t id, int32_t delta);

         /// <summary>
         /// Increments the value of the described statistic of type <c>int</c>
         /// by the given amount.
         /// </summary>
         /// <param name="descriptor">a statistic descriptor obtained with <see cref="#nameToDescriptor" />
         /// or <see cref="StatisticsType#nameToDescriptor" /> </param>
         /// <param name="delta">change value to be added</param>
         /// <returns>the value of the statistic after it has been incremented</returns>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists for the given <c>descriptor</c> or
         /// if the described statistic is not of
         /// type <c>int</c>.
         /// </exception>
         virtual int32_t IncInt(StatisticDescriptor^ descriptor, int32_t delta);

         /// <summary>
         /// Increments the value of the statistic of type <c>int</c> with
         /// the given name by a given amount.
         /// </summary>
         /// <param name="name">statistic name</param>
         /// <param name="delta">change value to be added</param>
         /// <returns>the value of the statistic after it has been incremented</returns>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists named <c>name</c> or
         /// if the statistic with name <c>name</c> is not of
         /// type <c>int</c>.
         /// </exception>
         virtual int32_t IncInt(String^ name, int32_t delta);

         /// <summary>
         /// Increments the value of the identified statistic of type <c>long</c>
         /// by the given amount.
         /// </summary>
         /// <param name="id">a statistic id obtained with <see cref="#nameToId" />
         /// or <see cref="#StatisticsType#nameToId" /> </param>
         /// <param name="delta">the value of the statistic after it has been incremented</param>
         /// <returns>the value of the statistic after it has been incremented</returns>
         /// <exception cref="IllegalArgumentException">
         /// If the id is invalid.
         /// </exception>
         virtual int64_t IncLong(int32_t id, int64_t delta);


         /// <summary>
         /// Increments the value of the described statistic of type <c>long</c>
         /// by the given amount.
         /// </summary>
         /// <param name="descriptor">a statistic descriptor obtained with <see cref="#nameToDescriptor" />
         /// or <see cref="StatisticsType#nameToDescriptor" /> </param>
         /// <param name="delta">change value to be added</param>
         /// <returns>the value of the statistic after it has been incremented</returns>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists for the given <c>descriptor</c> or
         /// if the described statistic is not of
         /// type <c>long</c>.
         /// </exception>
         virtual int64_t IncLong(StatisticDescriptor^ descriptor, int64_t delta);

         /// <summary>
         /// Increments the value of the statistic of type <c>long</c> with
         /// the given name by a given amount.
         /// </summary>
         /// <param name="name">statistic name</param>
         /// <param name="delta">change value to be added</param>
         /// <returns>the value of the statistic after it has been incremented</returns>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists named <c>name</c> or
         /// if the statistic with name <c>name</c> is not of
         /// type <c>long</c>.
         /// </exception>
         virtual int64_t IncLong(String^ name, int64_t delta);


         /// <summary>
         /// Increments the value of the identified statistic of type <c>double</c>
         /// by the given amount.
         /// </summary>
         /// <param name="id">a statistic id obtained with <see cref="#nameToId" />
         /// or <see cref="#StatisticsType#nameToId" /> </param>
         /// <param name="delta">the value of the statistic after it has been incremented</param>
         /// <returns>the value of the statistic after it has been incremented</returns>
         /// <exception cref="IllegalArgumentException">
         /// If the id is invalid.
         /// </exception>
         virtual double IncDouble(int32_t id, double delta);

         /// <summary>
         /// Increments the value of the described statistic of type <c>double</c>
         /// by the given amount.
         /// </summary>
         /// <param name="descriptor">a statistic descriptor obtained with <see cref="#nameToDescriptor" />
         /// or <see cref="StatisticsType#nameToDescriptor" /> </param>
         /// <param name="delta">change value to be added</param>
         /// <returns>the value of the statistic after it has been incremented</returns>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists for the given <c>descriptor</c> or
         /// if the described statistic is not of
         /// type <c>double</c>.
         /// </exception>
         virtual double IncDouble(StatisticDescriptor^ descriptor, double delta);

         /// <summary>
         /// Increments the value of the statistic of type <c>double</c> with
         /// the given name by a given amount.
         /// </summary>
         /// <param name="name">statistic name</param>
         /// <param name="delta">change value to be added</param>
         /// <returns>the value of the statistic after it has been incremented</returns>
         /// <exception cref="IllegalArgumentException">
         /// If no statistic exists named <c>name</c> or
         /// if the statistic with name <c>name</c> is not of
         /// type <c>double</c>.
         /// </exception>
         virtual double IncDouble(String^ name, double delta);

         internal:
           /// <summary>
           /// Internal factory function to wrap a native object pointer inside
           /// this managed class, with null pointer check.
           /// </summary>
           /// <param name="nativeptr">native object pointer</param>
           /// <returns>
           /// the managed wrapper object, or null if the native pointer is null.
           /// </returns>
          inline static Statistics^ Create(
          gemfire_statistics::Statistics* nativeptr )
          {
            return ( nativeptr != nullptr ?
            gcnew Statistics( nativeptr ) : nullptr );
          }

         private:
           /// <summary>
           /// Private constructor to wrap a native object pointer
           /// </summary>
           /// <param name="nativeptr">The native object pointer</param>
          inline Statistics( gemfire_statistics::Statistics* nativeptr )
          : UMWrapN( nativeptr, false ) { }

      };
    }
  }
}




 } //namespace 


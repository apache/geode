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
#include <vcclr.h>
#include <gfcpp/Properties.hpp>
#include "../Properties.hpp"

//using namespace apache::geode::client;
namespace apache
{
  namespace geode
  {
    namespace client
    {

      /// <summary>
      /// Wraps the managed <see cref="Apache.Geode.Client.PropertyVisitor" />
      /// delegate and implements the native <c>apache::geode::client::Properties::Visitor</c> interface.
      /// </summary>
      class ManagedVisitorGeneric
        : public apache::geode::client::Properties::Visitor
      {
      public:

        /// <summary>
        /// Create a <c>apache::geode::client::Properties::Visitor</c> from the given managed
        /// <c>PropertyVisitor</c> delegate.
        /// </summary>
        inline ManagedVisitorGeneric(Object^ visitorFunc) : m_managedptr(visitorFunc) { }

        /// <summary>
        /// Invokes the managed <c>PropertyVisitor</c> delegate for the given
        /// <c>Property</c> key and value.
        /// </summary>
        virtual void visit(CacheableKeyPtr& key, CacheablePtr& value);

        /// <summary>
        /// Destructor -- does nothing.
        /// </summary>
        virtual ~ManagedVisitorGeneric() { }

        void setptr(Apache::Geode::Client::PropertyVisitor^ visitor)
        {
          m_visitor = visitor;
        }

      private:

        // Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
        // Note: not using auto_gcroot since it will result in 'Dispose' of the PropertyVisitor
        // to be called which is not what is desired when this object is destroyed. Normally this
        // managed object may be created by the user and will be handled automatically by the GC.
        gcroot<Object^> m_managedptr;

        gcroot<Apache::Geode::Client::PropertyVisitor^> m_visitor;

        // Disable the copy and assignment constructors
        ManagedVisitorGeneric();
        ManagedVisitorGeneric(const ManagedVisitorGeneric&);
      };

    }  // namespace client
  }  // namespace geode
}  // namespace apache

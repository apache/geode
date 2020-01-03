/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
/**
 * The membership package implements cluster membership services. It is used to build a
 * Membership instance using a MembershipBuilder. The builder allows plugging in listeners
 * for various kinds of events (new member, departed member, unexpected shutdown, etc).
 * <p>
 * Once a Membership is created it must be started using its start() method.
 * <p>
 * The "api" package contains builders and interfaces for Membership.
 * <p>
 * The "gms" package contains the implementation of membership services. Class GMSMembership
 * is the primary class in that package. It encapsulates a Services object that handles
 * startup and shutdown of the various services (messaging, cluster membership, failure
 * detection).
 */
package org.apache.geode.distributed.internal.membership;

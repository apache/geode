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
package org.apache.geode.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.admin.internal.SystemMemberCacheEventProcessor;
import org.apache.geode.admin.jmx.internal.StatAlertNotification;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.client.internal.CacheServerLoadMessage;
import org.apache.geode.cache.client.internal.locator.ClientConnectionRequest;
import org.apache.geode.cache.client.internal.locator.ClientConnectionResponse;
import org.apache.geode.cache.client.internal.locator.ClientReplacementRequest;
import org.apache.geode.cache.client.internal.locator.GetAllServersRequest;
import org.apache.geode.cache.client.internal.locator.GetAllServersResponse;
import org.apache.geode.cache.client.internal.locator.LocatorListRequest;
import org.apache.geode.cache.client.internal.locator.LocatorListResponse;
import org.apache.geode.cache.client.internal.locator.LocatorStatusRequest;
import org.apache.geode.cache.client.internal.locator.LocatorStatusResponse;
import org.apache.geode.cache.client.internal.locator.QueueConnectionRequest;
import org.apache.geode.cache.client.internal.locator.QueueConnectionResponse;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.CqEntry;
import org.apache.geode.cache.query.internal.CumulativeNonDistinctResults;
import org.apache.geode.cache.query.internal.LinkedResultSet;
import org.apache.geode.cache.query.internal.LinkedStructSet;
import org.apache.geode.cache.query.internal.NWayMergeResults;
import org.apache.geode.cache.query.internal.NullToken;
import org.apache.geode.cache.query.internal.PRQueryTraceInfo;
import org.apache.geode.cache.query.internal.ResultsBag;
import org.apache.geode.cache.query.internal.ResultsCollectionWrapper;
import org.apache.geode.cache.query.internal.ResultsSet;
import org.apache.geode.cache.query.internal.SortedResultSet;
import org.apache.geode.cache.query.internal.SortedStructSet;
import org.apache.geode.cache.query.internal.StructBag;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.StructSet;
import org.apache.geode.cache.query.internal.Undefined;
import org.apache.geode.cache.query.internal.index.IndexCreationData;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.cache.query.internal.types.MapTypeImpl;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.HighPriorityAckedMessage;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.SerialAckedMessage;
import org.apache.geode.distributed.internal.ShutdownMessage;
import org.apache.geode.distributed.internal.StartupMessage;
import org.apache.geode.distributed.internal.StartupResponseMessage;
import org.apache.geode.distributed.internal.StartupResponseWithVersionMessage;
import org.apache.geode.distributed.internal.WaitForViewInstallation;
import org.apache.geode.distributed.internal.locks.DLockQueryProcessor;
import org.apache.geode.distributed.internal.locks.DLockRecoverGrantorProcessor.DLockRecoverGrantorMessage;
import org.apache.geode.distributed.internal.locks.DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage;
import org.apache.geode.distributed.internal.locks.DLockReleaseProcessor;
import org.apache.geode.distributed.internal.locks.DLockRemoteToken;
import org.apache.geode.distributed.internal.locks.DLockRequestProcessor;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.DeposeGrantorProcessor;
import org.apache.geode.distributed.internal.locks.ElderInitProcessor;
import org.apache.geode.distributed.internal.locks.GrantorRequestProcessor;
import org.apache.geode.distributed.internal.locks.NonGrantorDestroyedProcessor;
import org.apache.geode.distributed.internal.locks.NonGrantorDestroyedProcessor.NonGrantorDestroyedReplyMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.locator.GetViewRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.GetViewResponse;
import org.apache.geode.distributed.internal.membership.gms.messages.FinalCheckPassedMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.HeartbeatRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.NetworkPartitionMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.SuspectMembersMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.ViewAckMessage;
import org.apache.geode.distributed.internal.streaming.StreamingOperation.StreamingReplyMessage;
import org.apache.geode.internal.admin.ClientMembershipMessage;
import org.apache.geode.internal.admin.remote.AddHealthListenerRequest;
import org.apache.geode.internal.admin.remote.AddHealthListenerResponse;
import org.apache.geode.internal.admin.remote.AddStatListenerRequest;
import org.apache.geode.internal.admin.remote.AddStatListenerResponse;
import org.apache.geode.internal.admin.remote.AdminConsoleDisconnectMessage;
import org.apache.geode.internal.admin.remote.AdminConsoleMessage;
import org.apache.geode.internal.admin.remote.AdminFailureResponse;
import org.apache.geode.internal.admin.remote.AlertLevelChangeMessage;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.internal.admin.remote.AlertsNotificationMessage;
import org.apache.geode.internal.admin.remote.AppCacheSnapshotMessage;
import org.apache.geode.internal.admin.remote.BridgeServerRequest;
import org.apache.geode.internal.admin.remote.BridgeServerResponse;
import org.apache.geode.internal.admin.remote.CacheConfigRequest;
import org.apache.geode.internal.admin.remote.CacheConfigResponse;
import org.apache.geode.internal.admin.remote.CacheInfoRequest;
import org.apache.geode.internal.admin.remote.CacheInfoResponse;
import org.apache.geode.internal.admin.remote.CancelStatListenerRequest;
import org.apache.geode.internal.admin.remote.CancelStatListenerResponse;
import org.apache.geode.internal.admin.remote.CancellationMessage;
import org.apache.geode.internal.admin.remote.ChangeRefreshIntervalMessage;
import org.apache.geode.internal.admin.remote.ClientHealthStats;
import org.apache.geode.internal.admin.remote.CompactRequest;
import org.apache.geode.internal.admin.remote.CompactResponse;
import org.apache.geode.internal.admin.remote.DestroyEntryMessage;
import org.apache.geode.internal.admin.remote.DestroyRegionMessage;
import org.apache.geode.internal.admin.remote.DurableClientInfoRequest;
import org.apache.geode.internal.admin.remote.DurableClientInfoResponse;
import org.apache.geode.internal.admin.remote.FetchDistLockInfoRequest;
import org.apache.geode.internal.admin.remote.FetchDistLockInfoResponse;
import org.apache.geode.internal.admin.remote.FetchHealthDiagnosisRequest;
import org.apache.geode.internal.admin.remote.FetchHealthDiagnosisResponse;
import org.apache.geode.internal.admin.remote.FetchHostRequest;
import org.apache.geode.internal.admin.remote.FetchHostResponse;
import org.apache.geode.internal.admin.remote.FetchResourceAttributesRequest;
import org.apache.geode.internal.admin.remote.FetchResourceAttributesResponse;
import org.apache.geode.internal.admin.remote.FetchStatsRequest;
import org.apache.geode.internal.admin.remote.FetchStatsResponse;
import org.apache.geode.internal.admin.remote.FetchSysCfgRequest;
import org.apache.geode.internal.admin.remote.FetchSysCfgResponse;
import org.apache.geode.internal.admin.remote.FlushAppCacheSnapshotMessage;
import org.apache.geode.internal.admin.remote.HealthListenerMessage;
import org.apache.geode.internal.admin.remote.LicenseInfoRequest;
import org.apache.geode.internal.admin.remote.LicenseInfoResponse;
import org.apache.geode.internal.admin.remote.MissingPersistentIDsRequest;
import org.apache.geode.internal.admin.remote.MissingPersistentIDsResponse;
import org.apache.geode.internal.admin.remote.ObjectDetailsRequest;
import org.apache.geode.internal.admin.remote.ObjectDetailsResponse;
import org.apache.geode.internal.admin.remote.ObjectNamesRequest;
import org.apache.geode.internal.admin.remote.ObjectNamesResponse;
import org.apache.geode.internal.admin.remote.PrepareRevokePersistentIDRequest;
import org.apache.geode.internal.admin.remote.RefreshMemberSnapshotRequest;
import org.apache.geode.internal.admin.remote.RefreshMemberSnapshotResponse;
import org.apache.geode.internal.admin.remote.RegionAttributesRequest;
import org.apache.geode.internal.admin.remote.RegionAttributesResponse;
import org.apache.geode.internal.admin.remote.RegionRequest;
import org.apache.geode.internal.admin.remote.RegionResponse;
import org.apache.geode.internal.admin.remote.RegionSizeRequest;
import org.apache.geode.internal.admin.remote.RegionSizeResponse;
import org.apache.geode.internal.admin.remote.RegionStatisticsRequest;
import org.apache.geode.internal.admin.remote.RegionStatisticsResponse;
import org.apache.geode.internal.admin.remote.RegionSubRegionSizeRequest;
import org.apache.geode.internal.admin.remote.RegionSubRegionsSizeResponse;
import org.apache.geode.internal.admin.remote.RemoveHealthListenerRequest;
import org.apache.geode.internal.admin.remote.RemoveHealthListenerResponse;
import org.apache.geode.internal.admin.remote.ResetHealthStatusRequest;
import org.apache.geode.internal.admin.remote.ResetHealthStatusResponse;
import org.apache.geode.internal.admin.remote.RevokePersistentIDRequest;
import org.apache.geode.internal.admin.remote.RevokePersistentIDResponse;
import org.apache.geode.internal.admin.remote.RootRegionRequest;
import org.apache.geode.internal.admin.remote.RootRegionResponse;
import org.apache.geode.internal.admin.remote.ShutdownAllGatewayHubsRequest;
import org.apache.geode.internal.admin.remote.ShutdownAllRequest;
import org.apache.geode.internal.admin.remote.ShutdownAllResponse;
import org.apache.geode.internal.admin.remote.SnapshotResultMessage;
import org.apache.geode.internal.admin.remote.StatAlertsManagerAssignMessage;
import org.apache.geode.internal.admin.remote.StatListenerMessage;
import org.apache.geode.internal.admin.remote.StoreSysCfgRequest;
import org.apache.geode.internal.admin.remote.StoreSysCfgResponse;
import org.apache.geode.internal.admin.remote.SubRegionRequest;
import org.apache.geode.internal.admin.remote.SubRegionResponse;
import org.apache.geode.internal.admin.remote.TailLogRequest;
import org.apache.geode.internal.admin.remote.TailLogResponse;
import org.apache.geode.internal.admin.remote.UpdateAlertDefinitionMessage;
import org.apache.geode.internal.admin.remote.VersionInfoRequest;
import org.apache.geode.internal.admin.remote.VersionInfoResponse;
import org.apache.geode.internal.admin.statalerts.GaugeThresholdDecoratorImpl;
import org.apache.geode.internal.admin.statalerts.NumberThresholdDecoratorImpl;
import org.apache.geode.internal.cache.AddCacheServerProfileMessage;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CacheServerAdvisor.CacheServerProfile;
import org.apache.geode.internal.cache.ClientRegionEventImpl;
import org.apache.geode.internal.cache.CloseCacheMessage;
import org.apache.geode.internal.cache.ControllerAdvisor.ControllerProfile;
import org.apache.geode.internal.cache.CreateRegionProcessor;
import org.apache.geode.internal.cache.DestroyOperation;
import org.apache.geode.internal.cache.DestroyPartitionedRegionMessage;
import org.apache.geode.internal.cache.DestroyRegionOperation;
import org.apache.geode.internal.cache.DistTXCommitMessage;
import org.apache.geode.internal.cache.DistTXPrecommitMessage;
import org.apache.geode.internal.cache.DistTXPrecommitMessage.DistTxPrecommitResponse;
import org.apache.geode.internal.cache.DistTXRollbackMessage;
import org.apache.geode.internal.cache.DistributedClearOperation.ClearRegionMessage;
import org.apache.geode.internal.cache.DistributedClearOperation.ClearRegionWithContextMessage;
import org.apache.geode.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import org.apache.geode.internal.cache.DistributedPutAllOperation.PutAllMessage;
import org.apache.geode.internal.cache.DistributedRegionFunctionStreamingMessage;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation.RemoveAllMessage;
import org.apache.geode.internal.cache.DistributedTombstoneOperation.TombstoneMessage;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.ExpireDisconnectedClientTransactionsMessage;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.FindDurableQueueProcessor.FindDurableQueueMessage;
import org.apache.geode.internal.cache.FindDurableQueueProcessor.FindDurableQueueReply;
import org.apache.geode.internal.cache.FindRemoteTXMessage;
import org.apache.geode.internal.cache.FindRemoteTXMessage.FindRemoteTXMessageReply;
import org.apache.geode.internal.cache.FindVersionTagOperation.FindVersionTagMessage;
import org.apache.geode.internal.cache.FindVersionTagOperation.VersionTagReply;
import org.apache.geode.internal.cache.FunctionStreamingOrderedReplyMessage;
import org.apache.geode.internal.cache.FunctionStreamingReplyMessage;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InitialImageFlowControl.FlowControlPermitMessage;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InitialImageOperation.InitialImageVersionedEntryList;
import org.apache.geode.internal.cache.InvalidateOperation;
import org.apache.geode.internal.cache.InvalidatePartitionedRegionMessage;
import org.apache.geode.internal.cache.InvalidateRegionOperation.InvalidateRegionMessage;
import org.apache.geode.internal.cache.JtaAfterCompletionMessage;
import org.apache.geode.internal.cache.JtaBeforeCompletionMessage;
import org.apache.geode.internal.cache.LatestLastAccessTimeMessage;
import org.apache.geode.internal.cache.MemberFunctionStreamingMessage;
import org.apache.geode.internal.cache.Node;
import org.apache.geode.internal.cache.PRQueryProcessor;
import org.apache.geode.internal.cache.PartitionRegionConfig;
import org.apache.geode.internal.cache.PreferBytesCachedDeserializable;
import org.apache.geode.internal.cache.RegionEventImpl;
import org.apache.geode.internal.cache.ReleaseClearLockMessage;
import org.apache.geode.internal.cache.RemoveCacheServerProfileMessage;
import org.apache.geode.internal.cache.RoleEventImpl;
import org.apache.geode.internal.cache.SearchLoadAndWriteProcessor;
import org.apache.geode.internal.cache.ServerPingMessage;
import org.apache.geode.internal.cache.StateFlushOperation.StateMarkerMessage;
import org.apache.geode.internal.cache.StateFlushOperation.StateStabilizationMessage;
import org.apache.geode.internal.cache.StateFlushOperation.StateStabilizedMessage;
import org.apache.geode.internal.cache.StoreAllCachedDeserializable;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.TXCommitMessage.CommitProcessForLockIdMessage;
import org.apache.geode.internal.cache.TXCommitMessage.CommitProcessForTXIdMessage;
import org.apache.geode.internal.cache.TXCommitMessage.CommitProcessQueryMessage;
import org.apache.geode.internal.cache.TXCommitMessage.CommitProcessQueryReplyMessage;
import org.apache.geode.internal.cache.TXEntryState;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXRemoteCommitMessage;
import org.apache.geode.internal.cache.TXRemoteCommitMessage.TXRemoteCommitReplyMessage;
import org.apache.geode.internal.cache.TXRemoteRollbackMessage;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.UpdateAttributesProcessor;
import org.apache.geode.internal.cache.UpdateEntryVersionOperation.UpdateEntryVersionMessage;
import org.apache.geode.internal.cache.UpdateOperation;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.backup.AbortBackupRequest;
import org.apache.geode.internal.cache.backup.BackupResponse;
import org.apache.geode.internal.cache.backup.FinishBackupRequest;
import org.apache.geode.internal.cache.backup.FlushToDiskRequest;
import org.apache.geode.internal.cache.backup.FlushToDiskResponse;
import org.apache.geode.internal.cache.backup.PrepareBackupRequest;
import org.apache.geode.internal.cache.compression.SnappyCompressedCachedDeserializable;
import org.apache.geode.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;
import org.apache.geode.internal.cache.control.ResourceAdvisor.ResourceProfileMessage;
import org.apache.geode.internal.cache.ha.HARegionQueue.DispatchedAndCurrentEvents;
import org.apache.geode.internal.cache.ha.QueueRemovalMessage;
import org.apache.geode.internal.cache.locks.TXLockBatch;
import org.apache.geode.internal.cache.locks.TXLockIdImpl;
import org.apache.geode.internal.cache.locks.TXLockUpdateParticipantsMessage;
import org.apache.geode.internal.cache.locks.TXLockUpdateParticipantsMessage.TXLockUpdateParticipantsReplyMessage;
import org.apache.geode.internal.cache.locks.TXOriginatorRecoveryProcessor.TXOriginatorRecoveryMessage;
import org.apache.geode.internal.cache.locks.TXOriginatorRecoveryProcessor.TXOriginatorRecoveryReplyMessage;
import org.apache.geode.internal.cache.partitioned.AllBucketProfilesUpdateMessage;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage.BecomePrimaryBucketReplyMessage;
import org.apache.geode.internal.cache.partitioned.BucketBackupMessage;
import org.apache.geode.internal.cache.partitioned.BucketCountLoadProbe;
import org.apache.geode.internal.cache.partitioned.BucketProfileUpdateMessage;
import org.apache.geode.internal.cache.partitioned.BucketSizeMessage;
import org.apache.geode.internal.cache.partitioned.BucketSizeMessage.BucketSizeReplyMessage;
import org.apache.geode.internal.cache.partitioned.ContainsKeyValueMessage;
import org.apache.geode.internal.cache.partitioned.ContainsKeyValueMessage.ContainsKeyValueReplyMessage;
import org.apache.geode.internal.cache.partitioned.CreateBucketMessage;
import org.apache.geode.internal.cache.partitioned.CreateBucketMessage.CreateBucketReplyMessage;
import org.apache.geode.internal.cache.partitioned.DeposePrimaryBucketMessage;
import org.apache.geode.internal.cache.partitioned.DeposePrimaryBucketMessage.DeposePrimaryBucketReplyMessage;
import org.apache.geode.internal.cache.partitioned.DestroyMessage;
import org.apache.geode.internal.cache.partitioned.DestroyRegionOnDataStoreMessage;
import org.apache.geode.internal.cache.partitioned.DumpAllPRConfigMessage;
import org.apache.geode.internal.cache.partitioned.DumpB2NRegion;
import org.apache.geode.internal.cache.partitioned.DumpB2NRegion.DumpB2NReplyMessage;
import org.apache.geode.internal.cache.partitioned.DumpBucketsMessage;
import org.apache.geode.internal.cache.partitioned.EndBucketCreationMessage;
import org.apache.geode.internal.cache.partitioned.FetchBulkEntriesMessage;
import org.apache.geode.internal.cache.partitioned.FetchBulkEntriesMessage.FetchBulkEntriesReplyMessage;
import org.apache.geode.internal.cache.partitioned.FetchEntriesMessage;
import org.apache.geode.internal.cache.partitioned.FetchEntriesMessage.FetchEntriesReplyMessage;
import org.apache.geode.internal.cache.partitioned.FetchEntryMessage;
import org.apache.geode.internal.cache.partitioned.FetchEntryMessage.FetchEntryReplyMessage;
import org.apache.geode.internal.cache.partitioned.FetchKeysMessage;
import org.apache.geode.internal.cache.partitioned.FetchKeysMessage.FetchKeysReplyMessage;
import org.apache.geode.internal.cache.partitioned.FetchPartitionDetailsMessage;
import org.apache.geode.internal.cache.partitioned.FetchPartitionDetailsMessage.FetchPartitionDetailsReplyMessage;
import org.apache.geode.internal.cache.partitioned.FlushMessage;
import org.apache.geode.internal.cache.partitioned.GetMessage;
import org.apache.geode.internal.cache.partitioned.GetMessage.GetReplyMessage;
import org.apache.geode.internal.cache.partitioned.IdentityRequestMessage;
import org.apache.geode.internal.cache.partitioned.IdentityRequestMessage.IdentityReplyMessage;
import org.apache.geode.internal.cache.partitioned.IdentityUpdateMessage;
import org.apache.geode.internal.cache.partitioned.IndexCreationMsg;
import org.apache.geode.internal.cache.partitioned.IndexCreationMsg.IndexCreationReplyMsg;
import org.apache.geode.internal.cache.partitioned.InterestEventMessage;
import org.apache.geode.internal.cache.partitioned.InterestEventMessage.InterestEventReplyMessage;
import org.apache.geode.internal.cache.partitioned.InvalidateMessage;
import org.apache.geode.internal.cache.partitioned.ManageBackupBucketMessage;
import org.apache.geode.internal.cache.partitioned.ManageBackupBucketMessage.ManageBackupBucketReplyMessage;
import org.apache.geode.internal.cache.partitioned.ManageBucketMessage;
import org.apache.geode.internal.cache.partitioned.ManageBucketMessage.ManageBucketReplyMessage;
import org.apache.geode.internal.cache.partitioned.MoveBucketMessage;
import org.apache.geode.internal.cache.partitioned.MoveBucketMessage.MoveBucketReplyMessage;
import org.apache.geode.internal.cache.partitioned.PRSanityCheckMessage;
import org.apache.geode.internal.cache.partitioned.PRTombstoneMessage;
import org.apache.geode.internal.cache.partitioned.PRUpdateEntryVersionMessage;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionFunctionStreamingMessage;
import org.apache.geode.internal.cache.partitioned.PrimaryRequestMessage;
import org.apache.geode.internal.cache.partitioned.PrimaryRequestMessage.PrimaryRequestReplyMessage;
import org.apache.geode.internal.cache.partitioned.PutAllPRMessage;
import org.apache.geode.internal.cache.partitioned.PutAllPRMessage.PutAllReplyMessage;
import org.apache.geode.internal.cache.partitioned.PutMessage;
import org.apache.geode.internal.cache.partitioned.PutMessage.PutReplyMessage;
import org.apache.geode.internal.cache.partitioned.QueryMessage;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.RemoveAllPRMessage;
import org.apache.geode.internal.cache.partitioned.RemoveBucketMessage;
import org.apache.geode.internal.cache.partitioned.RemoveBucketMessage.RemoveBucketReplyMessage;
import org.apache.geode.internal.cache.partitioned.RemoveIndexesMessage;
import org.apache.geode.internal.cache.partitioned.RemoveIndexesMessage.RemoveIndexesReplyMessage;
import org.apache.geode.internal.cache.partitioned.SizeMessage;
import org.apache.geode.internal.cache.partitioned.SizeMessage.SizeReplyMessage;
import org.apache.geode.internal.cache.partitioned.SizedBasedLoadProbe;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.persistence.MembershipFlushRequest;
import org.apache.geode.internal.cache.persistence.MembershipViewRequest;
import org.apache.geode.internal.cache.persistence.MembershipViewRequest.MembershipViewReplyMessage;
import org.apache.geode.internal.cache.persistence.PersistentStateQueryMessage;
import org.apache.geode.internal.cache.persistence.PersistentStateQueryMessage.PersistentStateQueryReplyMessage;
import org.apache.geode.internal.cache.persistence.PrepareNewPersistentMemberMessage;
import org.apache.geode.internal.cache.persistence.RemovePersistentMemberMessage;
import org.apache.geode.internal.cache.snapshot.FlowController.FlowControlAbortMessage;
import org.apache.geode.internal.cache.snapshot.FlowController.FlowControlAckMessage;
import org.apache.geode.internal.cache.snapshot.SnapshotPacket;
import org.apache.geode.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;
import org.apache.geode.internal.cache.tier.sockets.ClientDataSerializerMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientDenylistProcessor.ClientDenylistMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientInstantiatorMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientInterestMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientMarkerMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientPingMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientTombstoneMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.internal.cache.tier.sockets.InterestResultPolicyImpl;
import org.apache.geode.internal.cache.tier.sockets.ObjectPartList;
import org.apache.geode.internal.cache.tier.sockets.ObjectPartList651;
import org.apache.geode.internal.cache.tier.sockets.RemoveClientFromDenylistMessage;
import org.apache.geode.internal.cache.tier.sockets.SerializedObjectPartList;
import org.apache.geode.internal.cache.tier.sockets.ServerInterestRegistrationMessage;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.DistTxEntryEvent;
import org.apache.geode.internal.cache.tx.RemoteClearMessage;
import org.apache.geode.internal.cache.tx.RemoteClearMessage.RemoteClearReplyMessage;
import org.apache.geode.internal.cache.tx.RemoteContainsKeyValueMessage;
import org.apache.geode.internal.cache.tx.RemoteDestroyMessage;
import org.apache.geode.internal.cache.tx.RemoteFetchEntryMessage;
import org.apache.geode.internal.cache.tx.RemoteFetchKeysMessage;
import org.apache.geode.internal.cache.tx.RemoteFetchVersionMessage;
import org.apache.geode.internal.cache.tx.RemoteGetMessage;
import org.apache.geode.internal.cache.tx.RemoteInvalidateMessage;
import org.apache.geode.internal.cache.tx.RemotePutAllMessage;
import org.apache.geode.internal.cache.tx.RemotePutMessage;
import org.apache.geode.internal.cache.tx.RemoteRemoveAllMessage;
import org.apache.geode.internal.cache.tx.RemoteSizeMessage;
import org.apache.geode.internal.cache.versions.DiskRegionVersionVector;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.VMRegionVersionVector;
import org.apache.geode.internal.cache.versions.VMVersionTag;
import org.apache.geode.internal.cache.wan.GatewaySenderAdvisor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventCallbackArgument;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderQueueEntrySynchronizationOperation;
import org.apache.geode.internal.cache.wan.parallel.ParallelQueueRemovalMessage;
import org.apache.geode.internal.cache.wan.serial.BatchDestroyOperation;
import org.apache.geode.management.internal.JmxManagerAdvisor.JmxManagerProfile;
import org.apache.geode.management.internal.JmxManagerAdvisor.JmxManagerProfileMessage;
import org.apache.geode.management.internal.JmxManagerLocatorRequest;
import org.apache.geode.management.internal.JmxManagerLocatorResponse;
import org.apache.geode.management.internal.ManagerStartupMessage;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.pdx.internal.CheckTypeRegistryState;
import org.apache.geode.pdx.internal.EnumId;
import org.apache.geode.pdx.internal.EnumInfo;

/**
 * Factory for instances of DataSerializableFixedID instances. Note that this class implements
 * DataSerializableFixedID to inherit constants but is not actually an instance of this interface.
 *
 * @since GemFire 5.7
 */
public class DSFIDFactory implements DataSerializableFixedID {

  private DSFIDFactory() {
    // no instances allowed
    throw new UnsupportedOperationException();
  }

  public int getDSFID() {
    throw new UnsupportedOperationException();
  }

  public void toData(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException();
  }

  public Version[] getSerializationVersions() {
    throw new UnsupportedOperationException();
  }

  private static final Constructor<?>[] dsfidMap = new Constructor<?>[256];

  private static final Int2ObjectOpenHashMap dsfidMap2 = new Int2ObjectOpenHashMap(800);

  static {
    registerDSFIDTypes();
  }

  /** Register the constructor for a fixed ID class. */
  public static void registerDSFID(int dsfid, Class dsfidClass) {
    try {
      Constructor<?> cons = dsfidClass.getConstructor((Class[]) null);
      cons.setAccessible(true);
      if (!cons.isAccessible()) {
        throw new InternalGemFireError(
            "default constructor not accessible " + "for DSFID=" + dsfid + ": " + dsfidClass);
      }
      if (dsfid >= Byte.MIN_VALUE && dsfid <= Byte.MAX_VALUE) {
        dsfidMap[dsfid + Byte.MAX_VALUE + 1] = cons;
      } else {
        dsfidMap2.put(dsfid, cons);
      }
    } catch (NoSuchMethodException nsme) {
      throw new InternalGemFireError(nsme);
    }
  }

  public static void registerTypes() {
    // nothing to do; static initializer will take care of the type registration
  }

  private static void registerDSFIDTypes() {
    registerDSFID(FINAL_CHECK_PASSED_MESSAGE, FinalCheckPassedMessage.class);
    registerDSFID(NETWORK_PARTITION_MESSAGE, NetworkPartitionMessage.class);
    registerDSFID(REMOVE_MEMBER_REQUEST, RemoveMemberMessage.class);
    registerDSFID(HEARTBEAT_REQUEST, HeartbeatRequestMessage.class);
    registerDSFID(HEARTBEAT_RESPONSE, HeartbeatMessage.class);
    registerDSFID(SUSPECT_MEMBERS_MESSAGE, SuspectMembersMessage.class);
    registerDSFID(LEAVE_REQUEST_MESSAGE, LeaveRequestMessage.class);
    registerDSFID(VIEW_ACK_MESSAGE, ViewAckMessage.class);
    registerDSFID(INSTALL_VIEW_MESSAGE, InstallViewMessage.class);
    registerDSFID(GMSMEMBER, GMSMember.class);
    registerDSFID(NETVIEW, NetView.class);
    registerDSFID(GET_VIEW_REQ, GetViewRequest.class);
    registerDSFID(GET_VIEW_RESP, GetViewResponse.class);
    registerDSFID(FIND_COORDINATOR_REQ, FindCoordinatorRequest.class);
    registerDSFID(FIND_COORDINATOR_RESP, FindCoordinatorResponse.class);
    registerDSFID(JOIN_RESPONSE, JoinResponseMessage.class);
    registerDSFID(JOIN_REQUEST, JoinRequestMessage.class);
    registerDSFID(CLIENT_TOMBSTONE_MESSAGE, ClientTombstoneMessage.class);
    registerDSFID(R_CLEAR_MSG, RemoteClearMessage.class);
    registerDSFID(R_CLEAR_MSG_REPLY, RemoteClearReplyMessage.class);
    registerDSFID(WAIT_FOR_VIEW_INSTALLATION, WaitForViewInstallation.class);
    registerDSFID(DISPATCHED_AND_CURRENT_EVENTS, DispatchedAndCurrentEvents.class);
    registerDSFID(DISTRIBUTED_MEMBER, InternalDistributedMember.class);
    registerDSFID(UPDATE_MESSAGE, UpdateOperation.UpdateMessage.class);
    registerDSFID(REPLY_MESSAGE, ReplyMessage.class);
    registerDSFID(PR_DESTROY, DestroyMessage.class);
    registerDSFID(CREATE_REGION_MESSAGE, CreateRegionProcessor.CreateRegionMessage.class);
    registerDSFID(CREATE_REGION_REPLY_MESSAGE,
        CreateRegionProcessor.CreateRegionReplyMessage.class);
    registerDSFID(REGION_STATE_MESSAGE, InitialImageOperation.RegionStateMessage.class);
    registerDSFID(QUERY_MESSAGE, SearchLoadAndWriteProcessor.QueryMessage.class);
    registerDSFID(RESPONSE_MESSAGE, SearchLoadAndWriteProcessor.ResponseMessage.class);
    registerDSFID(NET_SEARCH_REQUEST_MESSAGE,
        SearchLoadAndWriteProcessor.NetSearchRequestMessage.class);
    registerDSFID(NET_SEARCH_REPLY_MESSAGE,
        SearchLoadAndWriteProcessor.NetSearchReplyMessage.class);
    registerDSFID(NET_LOAD_REQUEST_MESSAGE,
        SearchLoadAndWriteProcessor.NetLoadRequestMessage.class);
    registerDSFID(NET_LOAD_REPLY_MESSAGE, SearchLoadAndWriteProcessor.NetLoadReplyMessage.class);
    registerDSFID(NET_WRITE_REQUEST_MESSAGE,
        SearchLoadAndWriteProcessor.NetWriteRequestMessage.class);
    registerDSFID(NET_WRITE_REPLY_MESSAGE, SearchLoadAndWriteProcessor.NetWriteReplyMessage.class);
    registerDSFID(DLOCK_REQUEST_MESSAGE, DLockRequestProcessor.DLockRequestMessage.class);
    registerDSFID(DLOCK_RESPONSE_MESSAGE, DLockRequestProcessor.DLockResponseMessage.class);
    registerDSFID(DLOCK_RELEASE_MESSAGE, DLockReleaseProcessor.DLockReleaseMessage.class);
    registerDSFID(ADMIN_CACHE_EVENT_MESSAGE,
        SystemMemberCacheEventProcessor.SystemMemberCacheMessage.class);
    registerDSFID(CQ_ENTRY_EVENT, CqEntry.class);
    registerDSFID(REQUEST_IMAGE_MESSAGE, InitialImageOperation.RequestImageMessage.class);
    registerDSFID(IMAGE_REPLY_MESSAGE, InitialImageOperation.ImageReplyMessage.class);
    registerDSFID(IMAGE_ENTRY, InitialImageOperation.Entry.class);
    registerDSFID(CLOSE_CACHE_MESSAGE, CloseCacheMessage.class);
    registerDSFID(NON_GRANTOR_DESTROYED_MESSAGE,
        NonGrantorDestroyedProcessor.NonGrantorDestroyedMessage.class);
    registerDSFID(DLOCK_RELEASE_REPLY, DLockReleaseProcessor.DLockReleaseReplyMessage.class);
    registerDSFID(GRANTOR_REQUEST_MESSAGE, GrantorRequestProcessor.GrantorRequestMessage.class);
    registerDSFID(GRANTOR_INFO_REPLY_MESSAGE,
        GrantorRequestProcessor.GrantorInfoReplyMessage.class);
    registerDSFID(ELDER_INIT_MESSAGE, ElderInitProcessor.ElderInitMessage.class);
    registerDSFID(ELDER_INIT_REPLY_MESSAGE, ElderInitProcessor.ElderInitReplyMessage.class);
    registerDSFID(DEPOSE_GRANTOR_MESSAGE, DeposeGrantorProcessor.DeposeGrantorMessage.class);
    registerDSFID(STARTUP_MESSAGE, StartupMessage.class);
    registerDSFID(STARTUP_RESPONSE_MESSAGE, StartupResponseMessage.class);
    registerDSFID(STARTUP_RESPONSE_WITHVERSION_MESSAGE, StartupResponseWithVersionMessage.class);
    registerDSFID(SHUTDOWN_MESSAGE, ShutdownMessage.class);
    registerDSFID(DESTROY_REGION_MESSAGE, DestroyRegionOperation.DestroyRegionMessage.class);
    registerDSFID(PR_PUTALL_MESSAGE, PutAllPRMessage.class);
    registerDSFID(PR_REMOVE_ALL_MESSAGE, RemoveAllPRMessage.class);
    registerDSFID(PR_REMOVE_ALL_REPLY_MESSAGE, RemoveAllPRMessage.RemoveAllReplyMessage.class);
    registerDSFID(REMOTE_REMOVE_ALL_MESSAGE, RemoteRemoveAllMessage.class);
    registerDSFID(REMOTE_REMOVE_ALL_REPLY_MESSAGE,
        RemoteRemoveAllMessage.RemoveAllReplyMessage.class);
    registerDSFID(DISTTX_ROLLBACK_MESSAGE, DistTXRollbackMessage.class);
    registerDSFID(DISTTX_COMMIT_MESSAGE, DistTXCommitMessage.class);
    registerDSFID(DISTTX_PRE_COMMIT_MESSAGE, DistTXPrecommitMessage.class);
    registerDSFID(DISTTX_ROLLBACK_REPLY_MESSAGE,
        DistTXRollbackMessage.DistTXRollbackReplyMessage.class);
    registerDSFID(DISTTX_COMMIT_REPLY_MESSAGE, DistTXCommitMessage.DistTXCommitReplyMessage.class);
    registerDSFID(DISTTX_PRE_COMMIT_REPLY_MESSAGE,
        DistTXPrecommitMessage.DistTXPrecommitReplyMessage.class);
    registerDSFID(PR_PUT_MESSAGE, PutMessage.class);
    registerDSFID(INVALIDATE_MESSAGE, InvalidateOperation.InvalidateMessage.class);
    registerDSFID(DESTROY_MESSAGE, DestroyOperation.DestroyMessage.class);
    registerDSFID(DA_PROFILE, DistributionAdvisor.Profile.class);
    registerDSFID(CACHE_PROFILE, CacheDistributionAdvisor.CacheProfile.class);
    registerDSFID(HA_PROFILE, HARegion.HARegionAdvisor.HAProfile.class);
    registerDSFID(ENTRY_EVENT, EntryEventImpl.class);
    registerDSFID(UPDATE_ATTRIBUTES_MESSAGE,
        UpdateAttributesProcessor.UpdateAttributesMessage.class);
    registerDSFID(PROFILE_REPLY_MESSAGE, UpdateAttributesProcessor.ProfileReplyMessage.class);
    registerDSFID(PROFILES_REPLY_MESSAGE, UpdateAttributesProcessor.ProfilesReplyMessage.class);
    registerDSFID(REGION_EVENT, RegionEventImpl.class);
    registerDSFID(TX_COMMIT_MESSAGE, TXCommitMessage.class);
    registerDSFID(COMMIT_PROCESS_FOR_LOCKID_MESSAGE, CommitProcessForLockIdMessage.class);
    registerDSFID(COMMIT_PROCESS_FOR_TXID_MESSAGE, CommitProcessForTXIdMessage.class);
    registerDSFID(FILTER_PROFILE, FilterProfile.class);
    registerDSFID(REMOTE_PUTALL_REPLY_MESSAGE, RemotePutAllMessage.PutAllReplyMessage.class);
    registerDSFID(REMOTE_PUTALL_MESSAGE, RemotePutAllMessage.class);
    registerDSFID(VERSION_TAG, VMVersionTag.class);
    registerDSFID(ADD_CACHESERVER_PROFILE_UPDATE, AddCacheServerProfileMessage.class);
    registerDSFID(REMOVE_CACHESERVER_PROFILE_UPDATE, RemoveCacheServerProfileMessage.class);
    registerDSFID(SERVER_INTEREST_REGISTRATION_MESSAGE, ServerInterestRegistrationMessage.class);
    registerDSFID(FILTER_PROFILE_UPDATE, FilterProfile.OperationMessage.class);
    registerDSFID(PR_GET_MESSAGE, GetMessage.class);
    registerDSFID(R_FETCH_ENTRY_MESSAGE, RemoteFetchEntryMessage.class);
    registerDSFID(R_FETCH_ENTRY_REPLY_MESSAGE,
        RemoteFetchEntryMessage.FetchEntryReplyMessage.class);
    registerDSFID(R_CONTAINS_MESSAGE, RemoteContainsKeyValueMessage.class);
    registerDSFID(R_CONTAINS_REPLY_MESSAGE,
        RemoteContainsKeyValueMessage.RemoteContainsKeyValueReplyMessage.class);
    registerDSFID(R_DESTROY_MESSAGE, RemoteDestroyMessage.class);
    registerDSFID(R_DESTROY_REPLY_MESSAGE, RemoteDestroyMessage.DestroyReplyMessage.class);
    registerDSFID(R_INVALIDATE_MESSAGE, RemoteInvalidateMessage.class);
    registerDSFID(R_INVALIDATE_REPLY_MESSAGE, RemoteInvalidateMessage.InvalidateReplyMessage.class);
    registerDSFID(R_GET_MESSAGE, RemoteGetMessage.class);
    registerDSFID(R_GET_REPLY_MESSAGE, RemoteGetMessage.GetReplyMessage.class);
    registerDSFID(R_PUT_MESSAGE, RemotePutMessage.class);
    registerDSFID(R_PUT_REPLY_MESSAGE, RemotePutMessage.PutReplyMessage.class);
    registerDSFID(R_SIZE_MESSAGE, RemoteSizeMessage.class);
    registerDSFID(R_SIZE_REPLY_MESSAGE, RemoteSizeMessage.SizeReplyMessage.class);
    registerDSFID(PR_DESTROY_REPLY_MESSAGE, DestroyMessage.DestroyReplyMessage.class);
    registerDSFID(CLI_FUNCTION_RESULT, CliFunctionResult.class);
    registerDSFID(R_FETCH_KEYS_MESSAGE, RemoteFetchKeysMessage.class);
    registerDSFID(R_FETCH_KEYS_REPLY, RemoteFetchKeysMessage.RemoteFetchKeysReplyMessage.class);
    registerDSFID(R_REMOTE_COMMIT_REPLY_MESSAGE, TXRemoteCommitReplyMessage.class);
    registerDSFID(TRANSACTION_LOCK_ID, TXLockIdImpl.class);
    registerDSFID(PR_GET_REPLY_MESSAGE, GetReplyMessage.class);
    registerDSFID(PR_NODE, Node.class);
    registerDSFID(UPDATE_WITH_CONTEXT_MESSAGE, UpdateOperation.UpdateWithContextMessage.class);
    registerDSFID(DESTROY_WITH_CONTEXT_MESSAGE, DestroyOperation.DestroyWithContextMessage.class);
    registerDSFID(INVALIDATE_WITH_CONTEXT_MESSAGE,
        InvalidateOperation.InvalidateWithContextMessage.class);
    registerDSFID(REGION_VERSION_VECTOR, VMRegionVersionVector.class);
    registerDSFID(CLIENT_PROXY_MEMBERSHIPID, ClientProxyMembershipID.class);
    registerDSFID(EVENT_ID, EventID.class);
    registerDSFID(CLIENT_UPDATE_MESSAGE, ClientUpdateMessageImpl.class);
    registerDSFID(CLEAR_REGION_MESSAGE_WITH_CONTEXT, ClearRegionWithContextMessage.class);
    registerDSFID(CLIENT_INSTANTIATOR_MESSAGE, ClientInstantiatorMessage.class);
    registerDSFID(CLIENT_DATASERIALIZER_MESSAGE, ClientDataSerializerMessage.class);
    registerDSFID(REGISTRATION_MESSAGE, InternalInstantiator.RegistrationMessage.class);
    registerDSFID(REGISTRATION_CONTEXT_MESSAGE,
        InternalInstantiator.RegistrationContextMessage.class);
    registerDSFID(RESULTS_COLLECTION_WRAPPER, ResultsCollectionWrapper.class);
    registerDSFID(RESULTS_SET, ResultsSet.class);
    registerDSFID(SORTED_RESULT_SET, SortedResultSet.class);
    registerDSFID(SORTED_STRUCT_SET, SortedStructSet.class);
    registerDSFID(NWAY_MERGE_RESULTS, NWayMergeResults.class);
    registerDSFID(CUMULATIVE_RESULTS, CumulativeNonDistinctResults.class);
    registerDSFID(UNDEFINED, Undefined.class);
    registerDSFID(STRUCT_IMPL, StructImpl.class);
    registerDSFID(STRUCT_SET, StructSet.class);
    registerDSFID(END_OF_BUCKET, PRQueryProcessor.EndOfBucket.class);
    registerDSFID(STRUCT_BAG, StructBag.class);
    registerDSFID(LINKED_RESULTSET, LinkedResultSet.class);
    registerDSFID(LINKED_STRUCTSET, LinkedStructSet.class);
    registerDSFID(PR_BUCKET_BACKUP_MESSAGE, BucketBackupMessage.class);
    registerDSFID(PR_BUCKET_PROFILE_UPDATE_MESSAGE, BucketProfileUpdateMessage.class);
    registerDSFID(PR_ALL_BUCKET_PROFILES_UPDATE_MESSAGE, AllBucketProfilesUpdateMessage.class);
    registerDSFID(PR_BUCKET_SIZE_MESSAGE, BucketSizeMessage.class);
    registerDSFID(PR_CONTAINS_KEY_VALUE_MESSAGE, ContainsKeyValueMessage.class);
    registerDSFID(PR_DUMP_ALL_PR_CONFIG_MESSAGE, DumpAllPRConfigMessage.class);
    registerDSFID(PR_DUMP_BUCKETS_MESSAGE, DumpBucketsMessage.class);
    registerDSFID(PR_FETCH_ENTRIES_MESSAGE, FetchEntriesMessage.class);
    registerDSFID(PR_FETCH_ENTRY_MESSAGE, FetchEntryMessage.class);
    registerDSFID(PR_FETCH_KEYS_MESSAGE, FetchKeysMessage.class);
    registerDSFID(PR_FLUSH_MESSAGE, FlushMessage.class);
    registerDSFID(PR_IDENTITY_REQUEST_MESSAGE, IdentityRequestMessage.class);
    registerDSFID(PR_IDENTITY_UPDATE_MESSAGE, IdentityUpdateMessage.class);
    registerDSFID(PR_INDEX_CREATION_MSG, IndexCreationMsg.class);
    registerDSFID(PR_MANAGE_BUCKET_MESSAGE, ManageBucketMessage.class);
    registerDSFID(PR_PRIMARY_REQUEST_MESSAGE, PrimaryRequestMessage.class);
    registerDSFID(PR_PRIMARY_REQUEST_REPLY_MESSAGE, PrimaryRequestReplyMessage.class);
    registerDSFID(PR_SANITY_CHECK_MESSAGE, PRSanityCheckMessage.class);
    registerDSFID(PR_PUTALL_REPLY_MESSAGE, PutAllReplyMessage.class);
    registerDSFID(PR_PUT_REPLY_MESSAGE, PutReplyMessage.class);
    registerDSFID(PR_QUERY_MESSAGE, QueryMessage.class);
    registerDSFID(PR_REMOVE_INDEXES_MESSAGE, RemoveIndexesMessage.class);
    registerDSFID(PR_REMOVE_INDEXES_REPLY_MESSAGE, RemoveIndexesReplyMessage.class);
    registerDSFID(PR_SIZE_MESSAGE, SizeMessage.class);
    registerDSFID(PR_SIZE_REPLY_MESSAGE, SizeReplyMessage.class);
    registerDSFID(PR_BUCKET_SIZE_REPLY_MESSAGE, BucketSizeReplyMessage.class);
    registerDSFID(PR_CONTAINS_KEY_VALUE_REPLY_MESSAGE, ContainsKeyValueReplyMessage.class);
    registerDSFID(PR_FETCH_ENTRIES_REPLY_MESSAGE, FetchEntriesReplyMessage.class);
    registerDSFID(PR_FETCH_ENTRY_REPLY_MESSAGE, FetchEntryReplyMessage.class);
    registerDSFID(PR_IDENTITY_REPLY_MESSAGE, IdentityReplyMessage.class);
    registerDSFID(PR_INDEX_CREATION_REPLY_MSG, IndexCreationReplyMsg.class);
    registerDSFID(PR_MANAGE_BUCKET_REPLY_MESSAGE, ManageBucketReplyMessage.class);
    registerDSFID(PR_FETCH_KEYS_REPLY_MESSAGE, FetchKeysReplyMessage.class);
    registerDSFID(PR_DUMP_B2N_REGION_MSG, DumpB2NRegion.class);
    registerDSFID(PR_DUMP_B2N_REPLY_MESSAGE, DumpB2NReplyMessage.class);
    registerDSFID(DESTROY_PARTITIONED_REGION_MESSAGE, DestroyPartitionedRegionMessage.class);
    registerDSFID(INVALIDATE_PARTITIONED_REGION_MESSAGE, InvalidatePartitionedRegionMessage.class);
    registerDSFID(COMMIT_PROCESS_QUERY_MESSAGE, CommitProcessQueryMessage.class);
    registerDSFID(COMMIT_PROCESS_QUERY_REPLY_MESSAGE, CommitProcessQueryReplyMessage.class);
    registerDSFID(DESTROY_REGION_WITH_CONTEXT_MESSAGE,
        DestroyRegionOperation.DestroyRegionWithContextMessage.class);
    registerDSFID(PUT_ALL_MESSAGE, PutAllMessage.class);
    registerDSFID(REMOVE_ALL_MESSAGE, RemoveAllMessage.class);
    registerDSFID(CLEAR_REGION_MESSAGE, ClearRegionMessage.class);
    registerDSFID(TOMBSTONE_MESSAGE, TombstoneMessage.class);
    registerDSFID(INVALIDATE_REGION_MESSAGE, InvalidateRegionMessage.class);
    registerDSFID(STATE_MARKER_MESSAGE, StateMarkerMessage.class);
    registerDSFID(STATE_STABILIZATION_MESSAGE, StateStabilizationMessage.class);
    registerDSFID(STATE_STABILIZED_MESSAGE, StateStabilizedMessage.class);
    registerDSFID(CLIENT_MARKER_MESSAGE_IMPL, ClientMarkerMessageImpl.class);
    registerDSFID(TX_LOCK_UPDATE_PARTICIPANTS_MESSAGE, TXLockUpdateParticipantsMessage.class);
    registerDSFID(TX_ORIGINATOR_RECOVERY_MESSAGE, TXOriginatorRecoveryMessage.class);
    registerDSFID(TX_ORIGINATOR_RECOVERY_REPLY_MESSAGE, TXOriginatorRecoveryReplyMessage.class);
    registerDSFID(TX_REMOTE_COMMIT_MESSAGE, TXRemoteCommitMessage.class);
    registerDSFID(TX_REMOTE_ROLLBACK_MESSAGE, TXRemoteRollbackMessage.class);
    registerDSFID(JTA_BEFORE_COMPLETION_MESSAGE, JtaBeforeCompletionMessage.class);
    registerDSFID(JTA_AFTER_COMPLETION_MESSAGE, JtaAfterCompletionMessage.class);
    registerDSFID(QUEUE_REMOVAL_MESSAGE, QueueRemovalMessage.class);
    registerDSFID(DLOCK_RECOVER_GRANTOR_MESSAGE, DLockRecoverGrantorMessage.class);
    registerDSFID(DLOCK_RECOVER_GRANTOR_REPLY_MESSAGE, DLockRecoverGrantorReplyMessage.class);
    registerDSFID(NON_GRANTOR_DESTROYED_REPLY_MESSAGE, NonGrantorDestroyedReplyMessage.class);
    registerDSFID(IDS_REGISTRATION_MESSAGE, InternalDataSerializer.RegistrationMessage.class);
    registerDSFID(PR_FETCH_PARTITION_DETAILS_MESSAGE, FetchPartitionDetailsMessage.class);
    registerDSFID(PR_FETCH_PARTITION_DETAILS_REPLY, FetchPartitionDetailsReplyMessage.class);
    registerDSFID(PR_DEPOSE_PRIMARY_BUCKET_MESSAGE, DeposePrimaryBucketMessage.class);
    registerDSFID(PR_DEPOSE_PRIMARY_BUCKET_REPLY, DeposePrimaryBucketReplyMessage.class);
    registerDSFID(PR_BECOME_PRIMARY_BUCKET_MESSAGE, BecomePrimaryBucketMessage.class);
    registerDSFID(PR_BECOME_PRIMARY_BUCKET_REPLY, BecomePrimaryBucketReplyMessage.class);
    registerDSFID(PR_REMOVE_BUCKET_MESSAGE, RemoveBucketMessage.class);
    registerDSFID(EXPIRE_CLIENT_TRANSACTIONS, ExpireDisconnectedClientTransactionsMessage.class);
    registerDSFID(PR_REMOVE_BUCKET_REPLY, RemoveBucketReplyMessage.class);
    registerDSFID(PR_MOVE_BUCKET_MESSAGE, MoveBucketMessage.class);
    registerDSFID(PR_MOVE_BUCKET_REPLY, MoveBucketReplyMessage.class);
    registerDSFID(ADD_HEALTH_LISTENER_REQUEST, AddHealthListenerRequest.class);
    registerDSFID(ADD_HEALTH_LISTENER_RESPONSE, AddHealthListenerResponse.class);
    registerDSFID(ADD_STAT_LISTENER_REQUEST, AddStatListenerRequest.class);
    registerDSFID(ADD_STAT_LISTENER_RESPONSE, AddStatListenerResponse.class);
    registerDSFID(ADMIN_CONSOLE_DISCONNECT_MESSAGE, AdminConsoleDisconnectMessage.class);
    registerDSFID(ADMIN_CONSOLE_MESSAGE, AdminConsoleMessage.class);
    registerDSFID(MANAGER_STARTUP_MESSAGE, ManagerStartupMessage.class);
    registerDSFID(JMX_MANAGER_LOCATOR_REQUEST, JmxManagerLocatorRequest.class);
    registerDSFID(JMX_MANAGER_LOCATOR_RESPONSE, JmxManagerLocatorResponse.class);
    registerDSFID(ADMIN_FAILURE_RESPONSE, AdminFailureResponse.class);
    registerDSFID(ALERT_LEVEL_CHANGE_MESSAGE, AlertLevelChangeMessage.class);
    registerDSFID(ALERT_LISTENER_MESSAGE, AlertListenerMessage.class);
    registerDSFID(APP_CACHE_SNAPSHOT_MESSAGE, AppCacheSnapshotMessage.class);
    registerDSFID(BRIDGE_SERVER_REQUEST, BridgeServerRequest.class);
    registerDSFID(BRIDGE_SERVER_RESPONSE, BridgeServerResponse.class);
    registerDSFID(CACHE_CONFIG_REQUEST, CacheConfigRequest.class);
    registerDSFID(CACHE_CONFIG_RESPONSE, CacheConfigResponse.class);
    registerDSFID(CACHE_INFO_REQUEST, CacheInfoRequest.class);
    registerDSFID(CACHE_INFO_RESPONSE, CacheInfoResponse.class);
    registerDSFID(CANCELLATION_MESSAGE, CancellationMessage.class);
    registerDSFID(CANCEL_STAT_LISTENER_REQUEST, CancelStatListenerRequest.class);
    registerDSFID(CANCEL_STAT_LISTENER_RESPONSE, CancelStatListenerResponse.class);
    registerDSFID(DESTROY_ENTRY_MESSAGE, DestroyEntryMessage.class);
    registerDSFID(ADMIN_DESTROY_REGION_MESSAGE, DestroyRegionMessage.class);
    registerDSFID(FETCH_DIST_LOCK_INFO_REQUEST, FetchDistLockInfoRequest.class);
    registerDSFID(FETCH_DIST_LOCK_INFO_RESPONSE, FetchDistLockInfoResponse.class);
    registerDSFID(FETCH_HEALTH_DIAGNOSIS_REQUEST, FetchHealthDiagnosisRequest.class);
    registerDSFID(FETCH_HEALTH_DIAGNOSIS_RESPONSE, FetchHealthDiagnosisResponse.class);
    registerDSFID(FETCH_HOST_REQUEST, FetchHostRequest.class);
    registerDSFID(FETCH_HOST_RESPONSE, FetchHostResponse.class);
    registerDSFID(FETCH_RESOURCE_ATTRIBUTES_REQUEST, FetchResourceAttributesRequest.class);
    registerDSFID(FETCH_RESOURCE_ATTRIBUTES_RESPONSE, FetchResourceAttributesResponse.class);
    registerDSFID(FETCH_STATS_REQUEST, FetchStatsRequest.class);
    registerDSFID(FETCH_STATS_RESPONSE, FetchStatsResponse.class);
    registerDSFID(FETCH_SYS_CFG_REQUEST, FetchSysCfgRequest.class);
    registerDSFID(FETCH_SYS_CFG_RESPONSE, FetchSysCfgResponse.class);
    registerDSFID(FLUSH_APP_CACHE_SNAPSHOT_MESSAGE, FlushAppCacheSnapshotMessage.class);
    registerDSFID(HEALTH_LISTENER_MESSAGE, HealthListenerMessage.class);
    registerDSFID(OBJECT_DETAILS_REQUEST, ObjectDetailsRequest.class);
    registerDSFID(OBJECT_DETAILS_RESPONSE, ObjectDetailsResponse.class);
    registerDSFID(OBJECT_NAMES_REQUEST, ObjectNamesRequest.class);
    registerDSFID(LICENSE_INFO_REQUEST, LicenseInfoRequest.class);
    registerDSFID(LICENSE_INFO_RESPONSE, LicenseInfoResponse.class);
    registerDSFID(OBJECT_NAMES_RESPONSE, ObjectNamesResponse.class);
    registerDSFID(REGION_ATTRIBUTES_REQUEST, RegionAttributesRequest.class);
    registerDSFID(REGION_ATTRIBUTES_RESPONSE, RegionAttributesResponse.class);
    registerDSFID(REGION_REQUEST, RegionRequest.class);
    registerDSFID(REGION_RESPONSE, RegionResponse.class);
    registerDSFID(REGION_SIZE_REQUEST, RegionSizeRequest.class);
    registerDSFID(REGION_SIZE_RESPONSE, RegionSizeResponse.class);
    registerDSFID(REGION_STATISTICS_REQUEST, RegionStatisticsRequest.class);
    registerDSFID(REGION_STATISTICS_RESPONSE, RegionStatisticsResponse.class);
    registerDSFID(REMOVE_HEALTH_LISTENER_REQUEST, RemoveHealthListenerRequest.class);
    registerDSFID(REMOVE_HEALTH_LISTENER_RESPONSE, RemoveHealthListenerResponse.class);
    registerDSFID(RESET_HEALTH_STATUS_REQUEST, ResetHealthStatusRequest.class);
    registerDSFID(RESET_HEALTH_STATUS_RESPONSE, ResetHealthStatusResponse.class);
    registerDSFID(ROOT_REGION_REQUEST, RootRegionRequest.class);
    registerDSFID(ROOT_REGION_RESPONSE, RootRegionResponse.class);
    registerDSFID(SNAPSHOT_RESULT_MESSAGE, SnapshotResultMessage.class);
    registerDSFID(STAT_LISTENER_MESSAGE, StatListenerMessage.class);
    registerDSFID(STORE_SYS_CFG_REQUEST, StoreSysCfgRequest.class);
    registerDSFID(STORE_SYS_CFG_RESPONSE, StoreSysCfgResponse.class);
    registerDSFID(SUB_REGION_REQUEST, SubRegionRequest.class);
    registerDSFID(SUB_REGION_RESPONSE, SubRegionResponse.class);
    registerDSFID(TAIL_LOG_REQUEST, TailLogRequest.class);
    registerDSFID(TAIL_LOG_RESPONSE, TailLogResponse.class);
    registerDSFID(VERSION_INFO_REQUEST, VersionInfoRequest.class);
    registerDSFID(VERSION_INFO_RESPONSE, VersionInfoResponse.class);
    registerDSFID(HIGH_PRIORITY_ACKED_MESSAGE, HighPriorityAckedMessage.class);
    registerDSFID(SERIAL_ACKED_MESSAGE, SerialAckedMessage.class);
    registerDSFID(BUCKET_PROFILE, BucketAdvisor.BucketProfile.class);
    registerDSFID(SERVER_BUCKET_PROFILE, BucketAdvisor.ServerBucketProfile.class);
    registerDSFID(PARTITION_PROFILE, RegionAdvisor.PartitionProfile.class);
    registerDSFID(GATEWAY_SENDER_PROFILE, GatewaySenderAdvisor.GatewaySenderProfile.class);
    registerDSFID(ROLE_EVENT, RoleEventImpl.class);
    registerDSFID(CLIENT_REGION_EVENT, ClientRegionEventImpl.class);
    registerDSFID(PR_INVALIDATE_MESSAGE, InvalidateMessage.class);
    registerDSFID(PR_INVALIDATE_REPLY_MESSAGE, InvalidateMessage.InvalidateReplyMessage.class);
    registerDSFID(TX_LOCK_UPDATE_PARTICIPANTS_REPLY_MESSAGE,
        TXLockUpdateParticipantsReplyMessage.class);
    registerDSFID(STREAMING_REPLY_MESSAGE, StreamingReplyMessage.class);
    registerDSFID(PARTITION_REGION_CONFIG, PartitionRegionConfig.class);
    registerDSFID(PREFER_BYTES_CACHED_DESERIALIZABLE, PreferBytesCachedDeserializable.class);
    registerDSFID(VM_CACHED_DESERIALIZABLE, VMCachedDeserializable.class);
    registerDSFID(GATEWAY_SENDER_EVENT_IMPL, GatewaySenderEventImpl.class);
    registerDSFID(SUSPEND_LOCKING_TOKEN, DLockService.SuspendLockingToken.class);
    registerDSFID(OBJECT_TYPE_IMPL, ObjectTypeImpl.class);
    registerDSFID(STRUCT_TYPE_IMPL, StructTypeImpl.class);
    registerDSFID(COLLECTION_TYPE_IMPL, CollectionTypeImpl.class);
    registerDSFID(TX_LOCK_BATCH, TXLockBatch.class);
    registerDSFID(GATEWAY_SENDER_EVENT_CALLBACK_ARGUMENT, GatewaySenderEventCallbackArgument.class);
    registerDSFID(MAP_TYPE_IMPL, MapTypeImpl.class);
    registerDSFID(STORE_ALL_CACHED_DESERIALIZABLE, StoreAllCachedDeserializable.class);
    registerDSFID(INTEREST_EVENT_MESSAGE, InterestEventMessage.class);
    registerDSFID(INTEREST_EVENT_REPLY_MESSAGE, InterestEventReplyMessage.class);
    registerDSFID(HA_EVENT_WRAPPER, HAEventWrapper.class);
    registerDSFID(STAT_ALERTS_MGR_ASSIGN_MESSAGE, StatAlertsManagerAssignMessage.class);
    registerDSFID(UPDATE_ALERTS_DEFN_MESSAGE, UpdateAlertDefinitionMessage.class);
    registerDSFID(REFRESH_MEMBER_SNAP_REQUEST, RefreshMemberSnapshotRequest.class);
    registerDSFID(REFRESH_MEMBER_SNAP_RESPONSE, RefreshMemberSnapshotResponse.class);
    registerDSFID(REGION_SUB_SIZE_REQUEST, RegionSubRegionSizeRequest.class);
    registerDSFID(REGION_SUB_SIZE_RESPONSE, RegionSubRegionsSizeResponse.class);
    registerDSFID(CHANGE_REFRESH_INT_MESSAGE, ChangeRefreshIntervalMessage.class);
    registerDSFID(ALERTS_NOTIF_MESSAGE, AlertsNotificationMessage.class);
    registerDSFID(FIND_DURABLE_QUEUE, FindDurableQueueMessage.class);
    registerDSFID(FIND_DURABLE_QUEUE_REPLY, FindDurableQueueReply.class);
    registerDSFID(CACHE_SERVER_LOAD_MESSAGE, CacheServerLoadMessage.class);
    registerDSFID(CACHE_SERVER_PROFILE, CacheServerProfile.class);
    registerDSFID(CONTROLLER_PROFILE, ControllerProfile.class);
    registerDSFID(DLOCK_QUERY_MESSAGE, DLockQueryProcessor.DLockQueryMessage.class);
    registerDSFID(DLOCK_QUERY_REPLY, DLockQueryProcessor.DLockQueryReplyMessage.class);
    registerDSFID(LOCATOR_LIST_REQUEST, LocatorListRequest.class);
    registerDSFID(LOCATOR_LIST_RESPONSE, LocatorListResponse.class);
    registerDSFID(CLIENT_CONNECTION_REQUEST, ClientConnectionRequest.class);
    registerDSFID(CLIENT_CONNECTION_RESPONSE, ClientConnectionResponse.class);
    registerDSFID(QUEUE_CONNECTION_REQUEST, QueueConnectionRequest.class);
    registerDSFID(QUEUE_CONNECTION_RESPONSE, QueueConnectionResponse.class);
    registerDSFID(CLIENT_REPLACEMENT_REQUEST, ClientReplacementRequest.class);
    registerDSFID(OBJECT_PART_LIST, ObjectPartList.class);
    registerDSFID(VERSIONED_OBJECT_LIST, VersionedObjectList.class);
    registerDSFID(OBJECT_PART_LIST66, ObjectPartList651.class);
    registerDSFID(PUTALL_VERSIONS_LIST, EntryVersionsList.class);
    registerDSFID(INITIAL_IMAGE_VERSIONED_OBJECT_LIST, InitialImageVersionedEntryList.class);
    registerDSFID(FIND_VERSION_TAG, FindVersionTagMessage.class);
    registerDSFID(VERSION_TAG_REPLY, VersionTagReply.class);
    registerDSFID(DURABLE_CLIENT_INFO_REQUEST, DurableClientInfoRequest.class);
    registerDSFID(DURABLE_CLIENT_INFO_RESPONSE, DurableClientInfoResponse.class);
    registerDSFID(CLIENT_INTEREST_MESSAGE, ClientInterestMessageImpl.class);
    registerDSFID(LATEST_LAST_ACCESS_TIME_MESSAGE, LatestLastAccessTimeMessage.class);
    registerDSFID(STAT_ALERT_DEFN_NUM_THRESHOLD, NumberThresholdDecoratorImpl.class);
    registerDSFID(STAT_ALERT_DEFN_GAUGE_THRESHOLD, GaugeThresholdDecoratorImpl.class);
    registerDSFID(CLIENT_HEALTH_STATS, ClientHealthStats.class);
    registerDSFID(STAT_ALERT_NOTIFICATION, StatAlertNotification.class);
    registerDSFID(FILTER_INFO_MESSAGE, InitialImageOperation.FilterInfoMessage.class);
    registerDSFID(SIZED_BASED_LOAD_PROBE, SizedBasedLoadProbe.class);
    registerDSFID(PR_MANAGE_BACKUP_BUCKET_MESSAGE, ManageBackupBucketMessage.class);
    registerDSFID(PR_MANAGE_BACKUP_BUCKET_REPLY_MESSAGE, ManageBackupBucketReplyMessage.class);
    registerDSFID(PR_CREATE_BUCKET_MESSAGE, CreateBucketMessage.class);
    registerDSFID(PR_CREATE_BUCKET_REPLY_MESSAGE, CreateBucketReplyMessage.class);
    registerDSFID(RESOURCE_MANAGER_PROFILE, ResourceManagerProfile.class);
    registerDSFID(RESOURCE_PROFILE_MESSAGE, ResourceProfileMessage.class);
    registerDSFID(JMX_MANAGER_PROFILE, JmxManagerProfile.class);
    registerDSFID(JMX_MANAGER_PROFILE_MESSAGE, JmxManagerProfileMessage.class);
    registerDSFID(CLIENT_DENYLIST_MESSAGE, ClientDenylistMessage.class);
    registerDSFID(REMOVE_CLIENT_FROM_DENYLIST_MESSAGE, RemoveClientFromDenylistMessage.class);
    registerDSFID(PR_FUNCTION_STREAMING_MESSAGE, PartitionedRegionFunctionStreamingMessage.class);
    registerDSFID(MEMBER_FUNCTION_STREAMING_MESSAGE, MemberFunctionStreamingMessage.class);
    registerDSFID(DR_FUNCTION_STREAMING_MESSAGE, DistributedRegionFunctionStreamingMessage.class);
    registerDSFID(FUNCTION_STREAMING_REPLY_MESSAGE, FunctionStreamingReplyMessage.class);
    registerDSFID(GET_ALL_SERVERS_REQUEST, GetAllServersRequest.class);
    registerDSFID(GET_ALL_SERVRES_RESPONSE, GetAllServersResponse.class);
    registerDSFID(PERSISTENT_MEMBERSHIP_VIEW_REQUEST, MembershipViewRequest.class);
    registerDSFID(PERSISTENT_MEMBERSHIP_VIEW_REPLY, MembershipViewReplyMessage.class);
    registerDSFID(PERSISTENT_STATE_QUERY_REQUEST, PersistentStateQueryMessage.class);
    registerDSFID(PERSISTENT_STATE_QUERY_REPLY, PersistentStateQueryReplyMessage.class);
    registerDSFID(PREPARE_NEW_PERSISTENT_MEMBER_REQUEST, PrepareNewPersistentMemberMessage.class);
    registerDSFID(MISSING_PERSISTENT_IDS_REQUEST, MissingPersistentIDsRequest.class);
    registerDSFID(MISSING_PERSISTENT_IDS_RESPONSE, MissingPersistentIDsResponse.class);
    registerDSFID(REVOKE_PERSISTENT_ID_REQUEST, RevokePersistentIDRequest.class);
    registerDSFID(REVOKE_PERSISTENT_ID_RESPONSE, RevokePersistentIDResponse.class);
    registerDSFID(REMOVE_PERSISTENT_MEMBER_REQUEST, RemovePersistentMemberMessage.class);
    registerDSFID(FUNCTION_STREAMING_ORDERED_REPLY_MESSAGE,
        FunctionStreamingOrderedReplyMessage.class);
    registerDSFID(REQUEST_SYNC_MESSAGE, InitialImageOperation.RequestSyncMessage.class);
    registerDSFID(PERSISTENT_MEMBERSHIP_FLUSH_REQUEST, MembershipFlushRequest.class);
    registerDSFID(SHUTDOWN_ALL_REQUEST, ShutdownAllRequest.class);
    registerDSFID(SHUTDOWN_ALL_RESPONSE, ShutdownAllResponse.class);
    registerDSFID(CLIENT_MEMBERSHIP_MESSAGE, ClientMembershipMessage.class);
    registerDSFID(END_BUCKET_CREATION_MESSAGE, EndBucketCreationMessage.class);
    registerDSFID(PREPARE_BACKUP_REQUEST, PrepareBackupRequest.class);
    registerDSFID(BACKUP_RESPONSE, BackupResponse.class); // in older versions this was
                                                          // FinishBackupResponse which is
                                                          // compatible
    registerDSFID(FINISH_BACKUP_REQUEST, FinishBackupRequest.class);
    registerDSFID(FINISH_BACKUP_RESPONSE, BackupResponse.class); // for backwards compatibility map
                                                                 // FINISH_BACKUP_RESPONSE to
                                                                 // BackupResponse
    registerDSFID(COMPACT_REQUEST, CompactRequest.class);
    registerDSFID(COMPACT_RESPONSE, CompactResponse.class);
    registerDSFID(FLOW_CONTROL_PERMIT_MESSAGE, FlowControlPermitMessage.class);
    registerDSFID(REQUEST_FILTERINFO_MESSAGE, InitialImageOperation.RequestFilterInfoMessage.class);
    registerDSFID(PARALLEL_QUEUE_REMOVAL_MESSAGE, ParallelQueueRemovalMessage.class);
    registerDSFID(BATCH_DESTROY_MESSAGE, BatchDestroyOperation.DestroyMessage.class);
    registerDSFID(FIND_REMOTE_TX_MESSAGE, FindRemoteTXMessage.class);
    registerDSFID(FIND_REMOTE_TX_REPLY, FindRemoteTXMessageReply.class);
    registerDSFID(SERIALIZED_OBJECT_PART_LIST, SerializedObjectPartList.class);
    registerDSFID(FLUSH_TO_DISK_REQUEST, FlushToDiskRequest.class);
    registerDSFID(FLUSH_TO_DISK_RESPONSE, FlushToDiskResponse.class);
    registerDSFID(ENUM_ID, EnumId.class);
    registerDSFID(ENUM_INFO, EnumInfo.class);
    registerDSFID(CHECK_TYPE_REGISTRY_STATE, CheckTypeRegistryState.class);
    registerDSFID(PREPARE_REVOKE_PERSISTENT_ID_REQUEST, PrepareRevokePersistentIDRequest.class);
    registerDSFID(PERSISTENT_RVV, DiskRegionVersionVector.class);
    registerDSFID(PERSISTENT_VERSION_TAG, DiskVersionTag.class);
    registerDSFID(DISK_STORE_ID, DiskStoreID.class);
    registerDSFID(CLIENT_PING_MESSAGE_IMPL, ClientPingMessageImpl.class);
    registerDSFID(SNAPSHOT_PACKET, SnapshotPacket.class);
    registerDSFID(SNAPSHOT_RECORD, SnapshotRecord.class);
    registerDSFID(FLOW_CONTROL_ACK, FlowControlAckMessage.class);
    registerDSFID(FLOW_CONTROL_ABORT, FlowControlAbortMessage.class);
    registerDSFID(MGMT_COMPACT_REQUEST,
        org.apache.geode.management.internal.messages.CompactRequest.class);
    registerDSFID(MGMT_COMPACT_RESPONSE,
        org.apache.geode.management.internal.messages.CompactResponse.class);
    registerDSFID(MGMT_FEDERATION_COMPONENT,
        org.apache.geode.management.internal.FederationComponent.class);
    registerDSFID(LOCATOR_STATUS_REQUEST, LocatorStatusRequest.class);
    registerDSFID(LOCATOR_STATUS_RESPONSE, LocatorStatusResponse.class);
    registerDSFID(R_FETCH_VERSION_MESSAGE, RemoteFetchVersionMessage.class);
    registerDSFID(R_FETCH_VERSION_REPLY, RemoteFetchVersionMessage.FetchVersionReplyMessage.class);
    registerDSFID(RELEASE_CLEAR_LOCK_MESSAGE, ReleaseClearLockMessage.class);
    registerDSFID(PR_TOMBSTONE_MESSAGE, PRTombstoneMessage.class);
    registerDSFID(REQUEST_RVV_MESSAGE, InitialImageOperation.RequestRVVMessage.class);
    registerDSFID(RVV_REPLY_MESSAGE, InitialImageOperation.RVVReplyMessage.class);
    registerDSFID(SNAPPY_COMPRESSED_CACHED_DESERIALIZABLE,
        SnappyCompressedCachedDeserializable.class);
    registerDSFID(UPDATE_ENTRY_VERSION_MESSAGE, UpdateEntryVersionMessage.class);
    registerDSFID(PR_UPDATE_ENTRY_VERSION_MESSAGE, PRUpdateEntryVersionMessage.class);
    registerDSFID(PR_FETCH_BULK_ENTRIES_MESSAGE, FetchBulkEntriesMessage.class);
    registerDSFID(PR_FETCH_BULK_ENTRIES_REPLY_MESSAGE, FetchBulkEntriesReplyMessage.class);
    registerDSFID(PR_QUERY_TRACE_INFO, PRQueryTraceInfo.class);
    registerDSFID(INDEX_CREATION_DATA, IndexCreationData.class);
    registerDSFID(DIST_TX_OP, DistTxEntryEvent.class);
    registerDSFID(DIST_TX_PRE_COMMIT_RESPONSE, DistTxPrecommitResponse.class);
    registerDSFID(DIST_TX_THIN_ENTRY_STATE, TXEntryState.DistTxThinEntryState.class);
    registerDSFID(SERVER_PING_MESSAGE, ServerPingMessage.class);
    registerDSFID(PR_DESTROY_ON_DATA_STORE_MESSAGE, DestroyRegionOnDataStoreMessage.class);
    registerDSFID(SHUTDOWN_ALL_GATEWAYHUBS_REQUEST, ShutdownAllGatewayHubsRequest.class);
    registerDSFID(BUCKET_COUNT_LOAD_PROBE, BucketCountLoadProbe.class);
    registerDSFID(GATEWAY_SENDER_QUEUE_ENTRY_SYNCHRONIZATION_MESSAGE,
        GatewaySenderQueueEntrySynchronizationOperation.GatewaySenderQueueEntrySynchronizationMessage.class);
    registerDSFID(GATEWAY_SENDER_QUEUE_ENTRY_SYNCHRONIZATION_ENTRY,
        GatewaySenderQueueEntrySynchronizationOperation.GatewaySenderQueueEntrySynchronizationEntry.class);
    registerDSFID(ABORT_BACKUP_REQUEST, AbortBackupRequest.class);
  }

  /**
   * Creates a DataSerializableFixedID or StreamableFixedID instance by deserializing it from the
   * data input.
   */
  public static Object create(int dsfid, DataInput in) throws IOException, ClassNotFoundException {
    switch (dsfid) {
      case REGION:
        return (DataSerializableFixedID) DataSerializer.readRegion(in);
      case END_OF_STREAM_TOKEN:
        return Token.END_OF_STREAM;
      case DLOCK_REMOTE_TOKEN:
        return DLockRemoteToken.createFromDataInput(in);
      case TRANSACTION_ID:
        return TXId.createFromData(in);
      case INTEREST_RESULT_POLICY:
        return readInterestResultPolicy(in);
      case UNDEFINED:
        return readUndefined(in);
      case RESULTS_BAG:
        return readResultsBag(in);
      case TOKEN_INVALID:
        return Token.INVALID;
      case TOKEN_LOCAL_INVALID:
        return Token.LOCAL_INVALID;
      case TOKEN_DESTROYED:
        return Token.DESTROYED;
      case TOKEN_REMOVED:
        return Token.REMOVED_PHASE1;
      case TOKEN_REMOVED2:
        return Token.REMOVED_PHASE2;
      case TOKEN_TOMBSTONE:
        return Token.TOMBSTONE;
      case NULL_TOKEN:
        return readNullToken(in);
      case CONFIGURATION_RESPONSE:
        return readConfigurationResponse(in);
      case PR_DESTROY_ON_DATA_STORE_MESSAGE:
        return readDestroyOnDataStore(in);
      default:
        final Constructor<?> cons;
        if (dsfid >= Byte.MIN_VALUE && dsfid <= Byte.MAX_VALUE) {
          cons = dsfidMap[dsfid + Byte.MAX_VALUE + 1];
        } else {
          cons = (Constructor<?>) dsfidMap2.get(dsfid);
        }
        if (cons != null) {
          try {
            Object ds = cons.newInstance((Object[]) null);
            InternalDataSerializer.invokeFromData(ds, in);
            return ds;
          } catch (InstantiationException ie) {
            throw new IOException(ie.getMessage(), ie);
          } catch (IllegalAccessException iae) {
            throw new IOException(iae.getMessage(), iae);
          } catch (InvocationTargetException ite) {
            Throwable targetEx = ite.getTargetException();
            if (targetEx instanceof IOException) {
              throw (IOException) targetEx;
            } else if (targetEx instanceof ClassNotFoundException) {
              throw (ClassNotFoundException) targetEx;
            } else {
              throw new IOException(ite.getMessage(), targetEx);
            }
          }
        }
        throw new DSFIDNotFoundException("Unknown DataSerializableFixedID: " + dsfid, dsfid);

    }
  }


  ////////////////// Reading Internal Objects /////////////////
  /**
   * Reads an instance of <code>IpAddress</code> from a <code>DataInput</code>.
   *
   * @throws IOException A problem occurs while reading from <code>in</code>
   */
  public static InternalDistributedMember readInternalDistributedMember(DataInput in)
      throws IOException, ClassNotFoundException {

    InternalDistributedMember o = new InternalDistributedMember();
    InternalDataSerializer.invokeFromData(o, in);
    return o;
  }

  private static ResultsBag readResultsBag(DataInput in)
      throws IOException, ClassNotFoundException {
    ResultsBag o = new ResultsBag(true);
    InternalDataSerializer.invokeFromData(o, in);
    return o;
  }

  private static Undefined readUndefined(DataInput in) throws IOException, ClassNotFoundException {
    Undefined o = (Undefined) QueryService.UNDEFINED;
    InternalDataSerializer.invokeFromData(o, in);
    return o;
  }

  /**
   * Reads an instance of <code>InterestResultPolicy</code> from a <code>DataInput</code>.
   *
   * @throws IOException A problem occurs while reading from <code>in</code>
   */
  private static InterestResultPolicyImpl readInterestResultPolicy(DataInput in)
      throws IOException, ClassNotFoundException {
    byte ordinal = in.readByte();
    return (InterestResultPolicyImpl) InterestResultPolicy.fromOrdinal(ordinal);
  }

  private static DataSerializableFixedID readDestroyOnDataStore(DataInput in)
      throws IOException, ClassNotFoundException {
    DataSerializableFixedID serializable = new DestroyRegionOnDataStoreMessage();
    serializable.fromData(in);
    return serializable;
  }

  private static DataSerializableFixedID readNullToken(DataInput in)
      throws IOException, ClassNotFoundException {
    DataSerializableFixedID serializable = (NullToken) IndexManager.NULL;
    serializable.fromData(in);
    return serializable;
  }

  private static DataSerializableFixedID readConfigurationResponse(DataInput in)
      throws IOException, ClassNotFoundException {
    DataSerializableFixedID serializable = new ConfigurationResponse();
    serializable.fromData(in);
    return serializable;
  }

  public static Constructor<?>[] getDsfidmap() {
    return dsfidMap;
  }

  public static Int2ObjectOpenHashMap getDsfidmap2() {
    return dsfidMap2;
  }

}

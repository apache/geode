/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal;



import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.NotSerializableException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.admin.internal.FinishBackupRequest;
import com.gemstone.gemfire.admin.internal.FinishBackupResponse;
import com.gemstone.gemfire.admin.internal.FlushToDiskRequest;
import com.gemstone.gemfire.admin.internal.FlushToDiskResponse;
import com.gemstone.gemfire.admin.internal.PrepareBackupRequest;
import com.gemstone.gemfire.admin.internal.PrepareBackupResponse;
import com.gemstone.gemfire.admin.internal.SystemMemberCacheEventProcessor;
import com.gemstone.gemfire.admin.jmx.internal.StatAlertNotification;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.client.internal.CacheServerLoadMessage;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionResponse;
import com.gemstone.gemfire.cache.client.internal.locator.ClientReplacementRequest;
import com.gemstone.gemfire.cache.client.internal.locator.GetAllServersRequest;
import com.gemstone.gemfire.cache.client.internal.locator.GetAllServersResponse;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorListRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorListResponse;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusResponse;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionResponse;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSGatewayEventImpl;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.CqEntry;
import com.gemstone.gemfire.cache.query.internal.CumulativeNonDistinctResults;
import com.gemstone.gemfire.cache.query.internal.LinkedResultSet;
import com.gemstone.gemfire.cache.query.internal.LinkedStructSet;
import com.gemstone.gemfire.cache.query.internal.NWayMergeResults;
import com.gemstone.gemfire.cache.query.internal.NullToken;
import com.gemstone.gemfire.cache.query.internal.PRQueryTraceInfo;
import com.gemstone.gemfire.cache.query.internal.ResultsBag;
import com.gemstone.gemfire.cache.query.internal.ResultsCollectionWrapper;
import com.gemstone.gemfire.cache.query.internal.ResultsSet;
import com.gemstone.gemfire.cache.query.internal.SortedResultSet;
import com.gemstone.gemfire.cache.query.internal.SortedStructSet;
import com.gemstone.gemfire.cache.query.internal.StructBag;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.cache.query.internal.StructSet;
import com.gemstone.gemfire.cache.query.internal.Undefined;
import com.gemstone.gemfire.cache.query.internal.index.IndexCreationData;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.types.CollectionTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.MapTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.HighPriorityAckedMessage;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.SerialAckedMessage;
import com.gemstone.gemfire.distributed.internal.ShutdownMessage;
import com.gemstone.gemfire.distributed.internal.StartupMessage;
import com.gemstone.gemfire.distributed.internal.StartupResponseMessage;
import com.gemstone.gemfire.distributed.internal.StartupResponseWithVersionMessage;
import com.gemstone.gemfire.distributed.internal.WaitForViewInstallation;
import com.gemstone.gemfire.distributed.internal.locks.DLockQueryProcessor;
import com.gemstone.gemfire.distributed.internal.locks.DLockRecoverGrantorProcessor.DLockRecoverGrantorMessage;
import com.gemstone.gemfire.distributed.internal.locks.DLockRecoverGrantorProcessor.DLockRecoverGrantorReplyMessage;
import com.gemstone.gemfire.distributed.internal.locks.DLockReleaseProcessor;
import com.gemstone.gemfire.distributed.internal.locks.DLockRemoteToken;
import com.gemstone.gemfire.distributed.internal.locks.DLockRequestProcessor;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.distributed.internal.locks.DeposeGrantorProcessor;
import com.gemstone.gemfire.distributed.internal.locks.ElderInitProcessor;
import com.gemstone.gemfire.distributed.internal.locks.GrantorRequestProcessor;
import com.gemstone.gemfire.distributed.internal.locks.NonGrantorDestroyedProcessor;
import com.gemstone.gemfire.distributed.internal.locks.NonGrantorDestroyedProcessor.NonGrantorDestroyedReplyMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.streaming.StreamingOperation.StreamingReplyMessage;
import com.gemstone.gemfire.internal.admin.ClientMembershipMessage;
import com.gemstone.gemfire.internal.admin.remote.AddHealthListenerRequest;
import com.gemstone.gemfire.internal.admin.remote.AddHealthListenerResponse;
import com.gemstone.gemfire.internal.admin.remote.AddStatListenerRequest;
import com.gemstone.gemfire.internal.admin.remote.AddStatListenerResponse;
import com.gemstone.gemfire.internal.admin.remote.AdminConsoleDisconnectMessage;
import com.gemstone.gemfire.internal.admin.remote.AdminConsoleMessage;
import com.gemstone.gemfire.internal.admin.remote.AdminFailureResponse;
import com.gemstone.gemfire.internal.admin.remote.AlertLevelChangeMessage;
import com.gemstone.gemfire.internal.admin.remote.AlertListenerMessage;
import com.gemstone.gemfire.internal.admin.remote.AlertsNotificationMessage;
import com.gemstone.gemfire.internal.admin.remote.AppCacheSnapshotMessage;
import com.gemstone.gemfire.internal.admin.remote.BridgeServerRequest;
import com.gemstone.gemfire.internal.admin.remote.BridgeServerResponse;
import com.gemstone.gemfire.internal.admin.remote.CacheConfigRequest;
import com.gemstone.gemfire.internal.admin.remote.CacheConfigResponse;
import com.gemstone.gemfire.internal.admin.remote.CacheInfoRequest;
import com.gemstone.gemfire.internal.admin.remote.CacheInfoResponse;
import com.gemstone.gemfire.internal.admin.remote.CancelStatListenerRequest;
import com.gemstone.gemfire.internal.admin.remote.CancelStatListenerResponse;
import com.gemstone.gemfire.internal.admin.remote.CancellationMessage;
import com.gemstone.gemfire.internal.admin.remote.ChangeRefreshIntervalMessage;
import com.gemstone.gemfire.internal.admin.remote.ClientHealthStats;
import com.gemstone.gemfire.internal.admin.remote.CompactRequest;
import com.gemstone.gemfire.internal.admin.remote.CompactResponse;
import com.gemstone.gemfire.internal.admin.remote.DestroyEntryMessage;
import com.gemstone.gemfire.internal.admin.remote.DestroyRegionMessage;
import com.gemstone.gemfire.internal.admin.remote.DurableClientInfoRequest;
import com.gemstone.gemfire.internal.admin.remote.DurableClientInfoResponse;
import com.gemstone.gemfire.internal.admin.remote.FetchDistLockInfoRequest;
import com.gemstone.gemfire.internal.admin.remote.FetchDistLockInfoResponse;
import com.gemstone.gemfire.internal.admin.remote.FetchHealthDiagnosisRequest;
import com.gemstone.gemfire.internal.admin.remote.FetchHealthDiagnosisResponse;
import com.gemstone.gemfire.internal.admin.remote.FetchHostRequest;
import com.gemstone.gemfire.internal.admin.remote.FetchHostResponse;
import com.gemstone.gemfire.internal.admin.remote.FetchResourceAttributesRequest;
import com.gemstone.gemfire.internal.admin.remote.FetchResourceAttributesResponse;
import com.gemstone.gemfire.internal.admin.remote.FetchStatsRequest;
import com.gemstone.gemfire.internal.admin.remote.FetchStatsResponse;
import com.gemstone.gemfire.internal.admin.remote.FetchSysCfgRequest;
import com.gemstone.gemfire.internal.admin.remote.FetchSysCfgResponse;
import com.gemstone.gemfire.internal.admin.remote.FlushAppCacheSnapshotMessage;
import com.gemstone.gemfire.internal.admin.remote.HealthListenerMessage;
import com.gemstone.gemfire.internal.admin.remote.LicenseInfoRequest;
import com.gemstone.gemfire.internal.admin.remote.LicenseInfoResponse;
import com.gemstone.gemfire.internal.admin.remote.MissingPersistentIDsRequest;
import com.gemstone.gemfire.internal.admin.remote.MissingPersistentIDsResponse;
import com.gemstone.gemfire.internal.admin.remote.ObjectDetailsRequest;
import com.gemstone.gemfire.internal.admin.remote.ObjectDetailsResponse;
import com.gemstone.gemfire.internal.admin.remote.ObjectNamesRequest;
import com.gemstone.gemfire.internal.admin.remote.ObjectNamesResponse;
import com.gemstone.gemfire.internal.admin.remote.PrepareRevokePersistentIDRequest;
import com.gemstone.gemfire.internal.admin.remote.RefreshMemberSnapshotRequest;
import com.gemstone.gemfire.internal.admin.remote.RefreshMemberSnapshotResponse;
import com.gemstone.gemfire.internal.admin.remote.RegionAttributesRequest;
import com.gemstone.gemfire.internal.admin.remote.RegionAttributesResponse;
import com.gemstone.gemfire.internal.admin.remote.RegionRequest;
import com.gemstone.gemfire.internal.admin.remote.RegionResponse;
import com.gemstone.gemfire.internal.admin.remote.RegionSizeRequest;
import com.gemstone.gemfire.internal.admin.remote.RegionSizeResponse;
import com.gemstone.gemfire.internal.admin.remote.RegionStatisticsRequest;
import com.gemstone.gemfire.internal.admin.remote.RegionStatisticsResponse;
import com.gemstone.gemfire.internal.admin.remote.RegionSubRegionSizeRequest;
import com.gemstone.gemfire.internal.admin.remote.RegionSubRegionsSizeResponse;
import com.gemstone.gemfire.internal.admin.remote.RemoveHealthListenerRequest;
import com.gemstone.gemfire.internal.admin.remote.RemoveHealthListenerResponse;
import com.gemstone.gemfire.internal.admin.remote.ResetHealthStatusRequest;
import com.gemstone.gemfire.internal.admin.remote.ResetHealthStatusResponse;
import com.gemstone.gemfire.internal.admin.remote.RevokePersistentIDRequest;
import com.gemstone.gemfire.internal.admin.remote.RevokePersistentIDResponse;
import com.gemstone.gemfire.internal.admin.remote.RootRegionRequest;
import com.gemstone.gemfire.internal.admin.remote.RootRegionResponse;
import com.gemstone.gemfire.internal.admin.remote.ShutdownAllGatewayHubsRequest;
import com.gemstone.gemfire.internal.admin.remote.ShutdownAllRequest;
import com.gemstone.gemfire.internal.admin.remote.ShutdownAllResponse;
import com.gemstone.gemfire.internal.admin.remote.SnapshotResultMessage;
import com.gemstone.gemfire.internal.admin.remote.StatAlertsManagerAssignMessage;
import com.gemstone.gemfire.internal.admin.remote.StatListenerMessage;
import com.gemstone.gemfire.internal.admin.remote.StoreSysCfgRequest;
import com.gemstone.gemfire.internal.admin.remote.StoreSysCfgResponse;
import com.gemstone.gemfire.internal.admin.remote.SubRegionRequest;
import com.gemstone.gemfire.internal.admin.remote.SubRegionResponse;
import com.gemstone.gemfire.internal.admin.remote.TailLogRequest;
import com.gemstone.gemfire.internal.admin.remote.TailLogResponse;
import com.gemstone.gemfire.internal.admin.remote.UpdateAlertDefinitionMessage;
import com.gemstone.gemfire.internal.admin.remote.VersionInfoRequest;
import com.gemstone.gemfire.internal.admin.remote.VersionInfoResponse;
import com.gemstone.gemfire.internal.admin.statalerts.GaugeThresholdDecoratorImpl;
import com.gemstone.gemfire.internal.admin.statalerts.NumberThresholdDecoratorImpl;
import com.gemstone.gemfire.internal.cache.AddCacheServerProfileMessage;
import com.gemstone.gemfire.internal.cache.ClientRegionEventImpl;
import com.gemstone.gemfire.internal.cache.CacheServerAdvisor.CacheServerProfile;
import com.gemstone.gemfire.internal.cache.BucketAdvisor;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor;
import com.gemstone.gemfire.internal.cache.CloseCacheMessage;
import com.gemstone.gemfire.internal.cache.ControllerAdvisor.ControllerProfile;
import com.gemstone.gemfire.internal.cache.CreateRegionProcessor;
import com.gemstone.gemfire.internal.cache.DestroyOperation;
import com.gemstone.gemfire.internal.cache.DestroyPartitionedRegionMessage;
import com.gemstone.gemfire.internal.cache.DestroyRegionOperation;
import com.gemstone.gemfire.internal.cache.DistTXPrecommitMessage;
import com.gemstone.gemfire.internal.cache.DistTXCommitMessage;
import com.gemstone.gemfire.internal.cache.DistTXRollbackMessage;
import com.gemstone.gemfire.internal.cache.DistributedClearOperation.ClearRegionMessage;
import com.gemstone.gemfire.internal.cache.DistributedClearOperation.ClearRegionWithContextMessage;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation.PutAllMessage;
import com.gemstone.gemfire.internal.cache.DistributedRegionFunctionStreamingMessage;
import com.gemstone.gemfire.internal.cache.DistributedRemoveAllOperation.RemoveAllMessage;
import com.gemstone.gemfire.internal.cache.DistributedTombstoneOperation.TombstoneMessage;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.FilterProfile;
import com.gemstone.gemfire.internal.cache.FindDurableQueueProcessor.FindDurableQueueMessage;
import com.gemstone.gemfire.internal.cache.FindDurableQueueProcessor.FindDurableQueueReply;
import com.gemstone.gemfire.internal.cache.FindRemoteTXMessage;
import com.gemstone.gemfire.internal.cache.FindRemoteTXMessage.FindRemoteTXMessageReply;
import com.gemstone.gemfire.internal.cache.FindVersionTagOperation.FindVersionTagMessage;
import com.gemstone.gemfire.internal.cache.FindVersionTagOperation.VersionTagReply;
import com.gemstone.gemfire.internal.cache.FunctionStreamingOrderedReplyMessage;
import com.gemstone.gemfire.internal.cache.FunctionStreamingReplyMessage;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.InitialImageFlowControl.FlowControlPermitMessage;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.InitialImageVersionedEntryList;
import com.gemstone.gemfire.internal.cache.InvalidateOperation;
import com.gemstone.gemfire.internal.cache.InvalidatePartitionedRegionMessage;
import com.gemstone.gemfire.internal.cache.InvalidateRegionOperation.InvalidateRegionMessage;
import com.gemstone.gemfire.internal.cache.JtaAfterCompletionMessage;
import com.gemstone.gemfire.internal.cache.JtaBeforeCompletionMessage;
import com.gemstone.gemfire.internal.cache.MemberFunctionStreamingMessage;
import com.gemstone.gemfire.internal.cache.Node;
import com.gemstone.gemfire.internal.cache.PRQueryProcessor;
import com.gemstone.gemfire.internal.cache.PartitionRegionConfig;
import com.gemstone.gemfire.internal.cache.PreferBytesCachedDeserializable;
import com.gemstone.gemfire.internal.cache.RegionEventImpl;
import com.gemstone.gemfire.internal.cache.ReleaseClearLockMessage;
import com.gemstone.gemfire.internal.cache.RemoteContainsKeyValueMessage;
import com.gemstone.gemfire.internal.cache.RemoteDestroyMessage;
import com.gemstone.gemfire.internal.cache.RemoteFetchEntryMessage;
import com.gemstone.gemfire.internal.cache.RemoteFetchVersionMessage;
import com.gemstone.gemfire.internal.cache.RemoteGetMessage;
import com.gemstone.gemfire.internal.cache.RemoteInvalidateMessage;
import com.gemstone.gemfire.internal.cache.RemotePutAllMessage;
import com.gemstone.gemfire.internal.cache.RemotePutMessage;
import com.gemstone.gemfire.internal.cache.RemoteRegionOperation;
import com.gemstone.gemfire.internal.cache.RemoteRegionOperation.RemoteRegionOperationReplyMessage;
import com.gemstone.gemfire.internal.cache.RemoteRemoveAllMessage;
import com.gemstone.gemfire.internal.cache.RoleEventImpl;
import com.gemstone.gemfire.internal.cache.SearchLoadAndWriteProcessor;
import com.gemstone.gemfire.internal.cache.SendQueueOperation.SendQueueMessage;
import com.gemstone.gemfire.internal.cache.ServerPingMessage;
import com.gemstone.gemfire.internal.cache.StateFlushOperation.StateMarkerMessage;
import com.gemstone.gemfire.internal.cache.StateFlushOperation.StateStabilizationMessage;
import com.gemstone.gemfire.internal.cache.StateFlushOperation.StateStabilizedMessage;
import com.gemstone.gemfire.internal.cache.StoreAllCachedDeserializable;
import com.gemstone.gemfire.internal.cache.TXCommitMessage;
import com.gemstone.gemfire.internal.cache.TXCommitMessage.CommitProcessForLockIdMessage;
import com.gemstone.gemfire.internal.cache.TXCommitMessage.CommitProcessForTXIdMessage;
import com.gemstone.gemfire.internal.cache.TXCommitMessage.CommitProcessQueryMessage;
import com.gemstone.gemfire.internal.cache.TXCommitMessage.CommitProcessQueryReplyMessage;
import com.gemstone.gemfire.internal.cache.TXEntryState;
import com.gemstone.gemfire.internal.cache.TXId;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXRemoteCommitMessage;
import com.gemstone.gemfire.internal.cache.TXRemoteCommitMessage.TXRemoteCommitReplyMessage;
import com.gemstone.gemfire.internal.cache.TXRemoteRollbackMessage;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.UpdateAttributesProcessor;
import com.gemstone.gemfire.internal.cache.UpdateEntryVersionOperation.UpdateEntryVersionMessage;
import com.gemstone.gemfire.internal.cache.UpdateOperation;
import com.gemstone.gemfire.internal.cache.VMCachedDeserializable;
import com.gemstone.gemfire.internal.cache.compression.SnappyCompressedCachedDeserializable;
import com.gemstone.gemfire.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;
import com.gemstone.gemfire.internal.cache.control.ResourceAdvisor.ResourceProfileMessage;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue.DispatchedAndCurrentEvents;
import com.gemstone.gemfire.internal.cache.ha.QueueRemovalMessage;
import com.gemstone.gemfire.internal.cache.locks.TXLockBatch;
import com.gemstone.gemfire.internal.cache.locks.TXLockIdImpl;
import com.gemstone.gemfire.internal.cache.locks.TXLockUpdateParticipantsMessage;
import com.gemstone.gemfire.internal.cache.locks.TXLockUpdateParticipantsMessage.TXLockUpdateParticipantsReplyMessage;
import com.gemstone.gemfire.internal.cache.locks.TXOriginatorRecoveryProcessor.TXOriginatorRecoveryMessage;
import com.gemstone.gemfire.internal.cache.locks.TXOriginatorRecoveryProcessor.TXOriginatorRecoveryReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.AllBucketProfilesUpdateMessage;
import com.gemstone.gemfire.internal.cache.partitioned.BecomePrimaryBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.BecomePrimaryBucketMessage.BecomePrimaryBucketReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.BucketBackupMessage;
import com.gemstone.gemfire.internal.cache.partitioned.BucketProfileUpdateMessage;
import com.gemstone.gemfire.internal.cache.partitioned.BucketSizeMessage;
import com.gemstone.gemfire.internal.cache.partitioned.BucketSizeMessage.BucketSizeReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.ContainsKeyValueMessage;
import com.gemstone.gemfire.internal.cache.partitioned.ContainsKeyValueMessage.ContainsKeyValueReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.CreateBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.CreateBucketMessage.CreateBucketReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.DeposePrimaryBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.DeposePrimaryBucketMessage.DeposePrimaryBucketReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.DestroyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.DestroyRegionOnDataStoreMessage;
import com.gemstone.gemfire.internal.cache.partitioned.DumpAllPRConfigMessage;
import com.gemstone.gemfire.internal.cache.partitioned.DumpB2NRegion;
import com.gemstone.gemfire.internal.cache.partitioned.DumpB2NRegion.DumpB2NReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.DumpBucketsMessage;
import com.gemstone.gemfire.internal.cache.partitioned.EndBucketCreationMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FetchBulkEntriesMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FetchBulkEntriesMessage.FetchBulkEntriesReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FetchEntriesMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FetchEntriesMessage.FetchEntriesReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FetchEntryMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FetchEntryMessage.FetchEntryReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FetchKeysMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FetchKeysMessage.FetchKeysReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FetchPartitionDetailsMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FetchPartitionDetailsMessage.FetchPartitionDetailsReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.FlushMessage;
import com.gemstone.gemfire.internal.cache.partitioned.GetMessage;
import com.gemstone.gemfire.internal.cache.partitioned.GetMessage.GetReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.IdentityRequestMessage;
import com.gemstone.gemfire.internal.cache.partitioned.IdentityRequestMessage.IdentityReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.IdentityUpdateMessage;
import com.gemstone.gemfire.internal.cache.partitioned.IndexCreationMsg;
import com.gemstone.gemfire.internal.cache.partitioned.IndexCreationMsg.IndexCreationReplyMsg;
import com.gemstone.gemfire.internal.cache.partitioned.InterestEventMessage;
import com.gemstone.gemfire.internal.cache.partitioned.InterestEventMessage.InterestEventReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.InvalidateMessage;
import com.gemstone.gemfire.internal.cache.partitioned.ManageBackupBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.ManageBackupBucketMessage.ManageBackupBucketReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.ManageBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.ManageBucketMessage.ManageBucketReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.MoveBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.MoveBucketMessage.MoveBucketReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PRSanityCheckMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PRTombstoneMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PRUpdateEntryVersionMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionFunctionStreamingMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PrimaryRequestMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PrimaryRequestMessage.PrimaryRequestReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PutAllPRMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PutAllPRMessage.PutAllReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PutMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PutMessage.PutReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.QueryMessage;
import com.gemstone.gemfire.internal.cache.partitioned.RegionAdvisor;
import com.gemstone.gemfire.internal.cache.partitioned.RemoteFetchKeysMessage;
import com.gemstone.gemfire.internal.cache.partitioned.RemoteSizeMessage;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveAllPRMessage;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveBucketMessage;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveBucketMessage.RemoveBucketReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveIndexesMessage;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveIndexesMessage.RemoveIndexesReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.SizeMessage;
import com.gemstone.gemfire.internal.cache.partitioned.SizeMessage.SizeReplyMessage;
import com.gemstone.gemfire.internal.cache.partitioned.SizedBasedLoadProbe;
import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;
import com.gemstone.gemfire.internal.cache.persistence.MembershipFlushRequest;
import com.gemstone.gemfire.internal.cache.persistence.MembershipViewRequest;
import com.gemstone.gemfire.internal.cache.persistence.MembershipViewRequest.MembershipViewReplyMessage;
import com.gemstone.gemfire.internal.cache.persistence.PersistentStateQueryMessage;
import com.gemstone.gemfire.internal.cache.persistence.PersistentStateQueryMessage.PersistentStateQueryReplyMessage;
import com.gemstone.gemfire.internal.cache.persistence.PrepareNewPersistentMemberMessage;
import com.gemstone.gemfire.internal.cache.persistence.RemovePersistentMemberMessage;
import com.gemstone.gemfire.internal.cache.snapshot.FlowController.FlowControlAbortMessage;
import com.gemstone.gemfire.internal.cache.snapshot.FlowController.FlowControlAckMessage;
import com.gemstone.gemfire.internal.cache.snapshot.SnapshotPacket;
import com.gemstone.gemfire.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier.ServerInterestRegistrationMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientBlacklistProcessor.ClientBlacklistMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientDataSerializerMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientInstantiatorMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientInterestMessageImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientMarkerMessageImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientPingMessageImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientTombstoneMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.HAEventWrapper;
import com.gemstone.gemfire.internal.cache.tier.sockets.InterestResultPolicyImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ObjectPartList;
import com.gemstone.gemfire.internal.cache.tier.sockets.ObjectPartList651;
import com.gemstone.gemfire.internal.cache.tier.sockets.RemoveClientFromBlacklistMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.SerializedObjectPartList;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.cache.tx.DistTxEntryEvent;
import com.gemstone.gemfire.internal.cache.versions.DiskRegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.DiskVersionTag;
import com.gemstone.gemfire.internal.cache.versions.VMRegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VMVersionTag;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderAdvisor;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackArgument;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelQueueBatchRemovalMessage;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelQueueBatchRemovalMessage.BatchRemovalReplyMessage;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelQueueRemovalMessage;
import com.gemstone.gemfire.internal.cache.wan.serial.BatchDestroyOperation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.management.internal.JmxManagerAdvisor.JmxManagerProfile;
import com.gemstone.gemfire.management.internal.JmxManagerAdvisor.JmxManagerProfileMessage;
import com.gemstone.gemfire.management.internal.JmxManagerLocatorRequest;
import com.gemstone.gemfire.management.internal.JmxManagerLocatorResponse;
import com.gemstone.gemfire.management.internal.ManagerStartupMessage;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.configuration.messages.ConfigurationRequest;
import com.gemstone.gemfire.management.internal.configuration.messages.ConfigurationResponse;
import com.gemstone.gemfire.pdx.internal.CheckTypeRegistryState;
import com.gemstone.gemfire.pdx.internal.EnumId;
import com.gemstone.gemfire.pdx.internal.EnumInfo;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.protocols.pbcast.JoinRsp;
import com.gemstone.org.jgroups.stack.IpAddress;

/**
 * Factory for instances of DataSerializableFixedID instances.
 * Note that this class implements DataSerializableFixedID to inherit constants but
 * is not actually an instance of this interface.
 *
 * @author Darrel Schneider
 * @since 5.7
 */
public final class DSFIDFactory implements DataSerializableFixedID {
   
  private DSFIDFactory() {
    // no instances allowed
    throw new UnsupportedOperationException();
  }

  public final int getDSFID() {
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
  public static void registerDSFID(int dsfid,
      Class dsfidClass) {
    try {
      Constructor<?> cons = dsfidClass.getConstructor((Class[])null);
      cons.setAccessible(true);
      if (!cons.isAccessible()) {
        throw new InternalGemFireError("default constructor not accessible "
            + "for DSFID=" + dsfid + ": " + dsfidClass);
      }
      if (dsfid >= Byte.MIN_VALUE && dsfid <= Byte.MAX_VALUE) {
        dsfidMap[dsfid + Byte.MAX_VALUE + 1] = cons;
      }
      else {
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
    registerDSFID(CLIENT_TOMBSTONE_MESSAGE, ClientTombstoneMessage.class);
    registerDSFID(R_REGION_OP, RemoteRegionOperation.class);
    registerDSFID(R_REGION_OP_REPLY, RemoteRegionOperationReplyMessage.class);
    registerDSFID(WAIT_FOR_VIEW_INSTALLATION, WaitForViewInstallation.class);
    registerDSFID(DISPATCHED_AND_CURRENT_EVENTS,
        DispatchedAndCurrentEvents.class);
    registerDSFID(IP_ADDRESS, IpAddress.class);
    registerDSFID(DISTRIBUTED_MEMBER, InternalDistributedMember.class);
    registerDSFID(UPDATE_MESSAGE, UpdateOperation.UpdateMessage.class);
    registerDSFID(REPLY_MESSAGE, ReplyMessage.class);
    registerDSFID(PR_DESTROY, DestroyMessage.class);
    registerDSFID(CREATE_REGION_MESSAGE,
        CreateRegionProcessor.CreateRegionMessage.class);
    registerDSFID(CREATE_REGION_REPLY_MESSAGE,
        CreateRegionProcessor.CreateRegionReplyMessage.class);
    registerDSFID(REGION_STATE_MESSAGE,
        InitialImageOperation.RegionStateMessage.class);
    registerDSFID(QUERY_MESSAGE, SearchLoadAndWriteProcessor.QueryMessage.class);
    registerDSFID(RESPONSE_MESSAGE,
        SearchLoadAndWriteProcessor.ResponseMessage.class);
    registerDSFID(NET_SEARCH_REQUEST_MESSAGE,
        SearchLoadAndWriteProcessor.NetSearchRequestMessage.class);
    registerDSFID(NET_SEARCH_REPLY_MESSAGE,
        SearchLoadAndWriteProcessor.NetSearchReplyMessage.class);
    registerDSFID(NET_LOAD_REQUEST_MESSAGE,
        SearchLoadAndWriteProcessor.NetLoadRequestMessage.class);
    registerDSFID(NET_LOAD_REPLY_MESSAGE,
        SearchLoadAndWriteProcessor.NetLoadReplyMessage.class);
    registerDSFID(NET_WRITE_REQUEST_MESSAGE,
        SearchLoadAndWriteProcessor.NetWriteRequestMessage.class);
    registerDSFID(NET_WRITE_REPLY_MESSAGE,
        SearchLoadAndWriteProcessor.NetWriteReplyMessage.class);
    registerDSFID(DLOCK_REQUEST_MESSAGE,
        DLockRequestProcessor.DLockRequestMessage.class);
    registerDSFID(DLOCK_RESPONSE_MESSAGE,
        DLockRequestProcessor.DLockResponseMessage.class);
    registerDSFID(DLOCK_RELEASE_MESSAGE,
        DLockReleaseProcessor.DLockReleaseMessage.class);
    registerDSFID(ADMIN_CACHE_EVENT_MESSAGE,
        SystemMemberCacheEventProcessor.SystemMemberCacheMessage.class);
    registerDSFID(CQ_ENTRY_EVENT, CqEntry.class);
    registerDSFID(REQUEST_IMAGE_MESSAGE,
        InitialImageOperation.RequestImageMessage.class);
    registerDSFID(IMAGE_REPLY_MESSAGE,
        InitialImageOperation.ImageReplyMessage.class);
    registerDSFID(IMAGE_ENTRY, InitialImageOperation.Entry.class);
    registerDSFID(CLOSE_CACHE_MESSAGE, CloseCacheMessage.class);
    registerDSFID(NON_GRANTOR_DESTROYED_MESSAGE,
        NonGrantorDestroyedProcessor.NonGrantorDestroyedMessage.class);
    registerDSFID(DLOCK_RELEASE_REPLY,
        DLockReleaseProcessor.DLockReleaseReplyMessage.class);
    registerDSFID(GRANTOR_REQUEST_MESSAGE,
        GrantorRequestProcessor.GrantorRequestMessage.class);
    registerDSFID(GRANTOR_INFO_REPLY_MESSAGE,
        GrantorRequestProcessor.GrantorInfoReplyMessage.class);
    registerDSFID(ELDER_INIT_MESSAGE, ElderInitProcessor.ElderInitMessage.class);
    registerDSFID(ELDER_INIT_REPLY_MESSAGE,
        ElderInitProcessor.ElderInitReplyMessage.class);
    registerDSFID(DEPOSE_GRANTOR_MESSAGE,
        DeposeGrantorProcessor.DeposeGrantorMessage.class);
    registerDSFID(STARTUP_MESSAGE, StartupMessage.class);
    registerDSFID(STARTUP_RESPONSE_MESSAGE, StartupResponseMessage.class);
    registerDSFID(STARTUP_RESPONSE_WITHVERSION_MESSAGE,
        StartupResponseWithVersionMessage.class);
    registerDSFID(SHUTDOWN_MESSAGE, ShutdownMessage.class);
    registerDSFID(DESTROY_REGION_MESSAGE,
        DestroyRegionOperation.DestroyRegionMessage.class);
    registerDSFID(PR_PUTALL_MESSAGE, PutAllPRMessage.class);
    registerDSFID(PR_REMOVE_ALL_MESSAGE, RemoveAllPRMessage.class);
    registerDSFID(PR_REMOVE_ALL_REPLY_MESSAGE, RemoveAllPRMessage.RemoveAllReplyMessage.class);
    registerDSFID(REMOTE_REMOVE_ALL_MESSAGE, RemoteRemoveAllMessage.class);
    registerDSFID(REMOTE_REMOVE_ALL_REPLY_MESSAGE, RemoteRemoveAllMessage.RemoveAllReplyMessage.class);
    registerDSFID(DISTTX_ROLLBACK_MESSAGE, DistTXRollbackMessage.class);
    registerDSFID(DISTTX_COMMIT_MESSAGE, DistTXCommitMessage.class);
    registerDSFID(DISTTX_PRE_COMMIT_MESSAGE, DistTXPrecommitMessage.class);
    registerDSFID(DISTTX_ROLLBACK_REPLY_MESSAGE, DistTXRollbackMessage.DistTXRollbackReplyMessage.class);
    registerDSFID(DISTTX_COMMIT_REPLY_MESSAGE, DistTXCommitMessage.DistTXCommitReplyMessage.class);
    registerDSFID(DISTTX_PRE_COMMIT_REPLY_MESSAGE, DistTXPrecommitMessage.DistTXPrecommitReplyMessage.class);
    registerDSFID(PR_PUT_MESSAGE, PutMessage.class);
    registerDSFID(INVALIDATE_MESSAGE,
        InvalidateOperation.InvalidateMessage.class);
    registerDSFID(DESTROY_MESSAGE, DestroyOperation.DestroyMessage.class);
    registerDSFID(DA_PROFILE, DistributionAdvisor.Profile.class);
    registerDSFID(CACHE_PROFILE, CacheDistributionAdvisor.CacheProfile.class);
    registerDSFID(HA_PROFILE, HARegion.HARegionAdvisor.HAProfile.class);
    registerDSFID(ENTRY_EVENT, EntryEventImpl.class);
    registerDSFID(UPDATE_ATTRIBUTES_MESSAGE,
        UpdateAttributesProcessor.UpdateAttributesMessage.class);
    registerDSFID(PROFILE_REPLY_MESSAGE,
        UpdateAttributesProcessor.ProfileReplyMessage.class);
    registerDSFID(PROFILES_REPLY_MESSAGE,
        UpdateAttributesProcessor.ProfilesReplyMessage.class);
    registerDSFID(REGION_EVENT, RegionEventImpl.class);
    registerDSFID(TX_COMMIT_MESSAGE, TXCommitMessage.class);
    registerDSFID(COMMIT_PROCESS_FOR_LOCKID_MESSAGE,
        CommitProcessForLockIdMessage.class);
    registerDSFID(COMMIT_PROCESS_FOR_TXID_MESSAGE,
        CommitProcessForTXIdMessage.class);
    registerDSFID(FILTER_PROFILE, FilterProfile.class);
    registerDSFID(REMOTE_PUTALL_REPLY_MESSAGE,
        RemotePutAllMessage.PutAllReplyMessage.class);
    registerDSFID(REMOTE_PUTALL_MESSAGE, RemotePutAllMessage.class);
    registerDSFID(VERSION_TAG, VMVersionTag.class);
    registerDSFID(ADD_CACHESERVER_PROFILE_UPDATE,
        AddCacheServerProfileMessage.class);
    registerDSFID(SERVER_INTEREST_REGISTRATION_MESSAGE,
        ServerInterestRegistrationMessage.class);
    registerDSFID(FILTER_PROFILE_UPDATE, FilterProfile.OperationMessage.class);
    registerDSFID(PR_GET_MESSAGE, GetMessage.class);
    registerDSFID(R_FETCH_ENTRY_MESSAGE, RemoteFetchEntryMessage.class);
    registerDSFID(R_FETCH_ENTRY_REPLY_MESSAGE,
        RemoteFetchEntryMessage.FetchEntryReplyMessage.class);
    registerDSFID(R_CONTAINS_MESSAGE, RemoteContainsKeyValueMessage.class);
    registerDSFID(R_CONTAINS_REPLY_MESSAGE,
        RemoteContainsKeyValueMessage.RemoteContainsKeyValueReplyMessage.class);
    registerDSFID(R_DESTROY_MESSAGE, RemoteDestroyMessage.class);
    registerDSFID(R_DESTROY_REPLY_MESSAGE,
        RemoteDestroyMessage.DestroyReplyMessage.class);
    registerDSFID(R_INVALIDATE_MESSAGE, RemoteInvalidateMessage.class);
    registerDSFID(R_INVALIDATE_REPLY_MESSAGE,
        RemoteInvalidateMessage.InvalidateReplyMessage.class);
    registerDSFID(R_GET_MESSAGE, RemoteGetMessage.class);
    registerDSFID(R_GET_REPLY_MESSAGE, RemoteGetMessage.GetReplyMessage.class);
    registerDSFID(R_PUT_MESSAGE, RemotePutMessage.class);
    registerDSFID(R_PUT_REPLY_MESSAGE, RemotePutMessage.PutReplyMessage.class);
    registerDSFID(R_SIZE_MESSAGE, RemoteSizeMessage.class);
    registerDSFID(R_SIZE_REPLY_MESSAGE,
        RemoteSizeMessage.SizeReplyMessage.class);
    registerDSFID(PR_DESTROY_REPLY_MESSAGE,
        DestroyMessage.DestroyReplyMessage.class);
    registerDSFID(CLI_FUNCTION_RESULT, CliFunctionResult.class);
    registerDSFID(R_FETCH_KEYS_MESSAGE, RemoteFetchKeysMessage.class);
    registerDSFID(R_FETCH_KEYS_REPLY,
        RemoteFetchKeysMessage.RemoteFetchKeysReplyMessage.class);
    registerDSFID(R_REMOTE_COMMIT_REPLY_MESSAGE,
        TXRemoteCommitReplyMessage.class);
    registerDSFID(TRANSACTION_LOCK_ID, TXLockIdImpl.class);
    registerDSFID(PR_GET_REPLY_MESSAGE, GetReplyMessage.class);
    registerDSFID(PR_NODE, Node.class);
    registerDSFID(UPDATE_WITH_CONTEXT_MESSAGE,
        UpdateOperation.UpdateWithContextMessage.class);
    registerDSFID(DESTROY_WITH_CONTEXT_MESSAGE,
        DestroyOperation.DestroyWithContextMessage.class);
    registerDSFID(INVALIDATE_WITH_CONTEXT_MESSAGE,
        InvalidateOperation.InvalidateWithContextMessage.class);
    registerDSFID(REGION_VERSION_VECTOR, VMRegionVersionVector.class);
    registerDSFID(CLIENT_PROXY_MEMBERSHIPID, ClientProxyMembershipID.class);
    registerDSFID(EVENT_ID, EventID.class);
    registerDSFID(CLIENT_UPDATE_MESSAGE, ClientUpdateMessageImpl.class);
    registerDSFID(CLEAR_REGION_MESSAGE_WITH_CONTEXT,
        ClearRegionWithContextMessage.class);
    registerDSFID(CLIENT_INSTANTIATOR_MESSAGE, ClientInstantiatorMessage.class);
    registerDSFID(CLIENT_DATASERIALIZER_MESSAGE,
        ClientDataSerializerMessage.class);
    registerDSFID(REGISTRATION_MESSAGE,
        InternalInstantiator.RegistrationMessage.class);
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
    registerDSFID(PR_BUCKET_PROFILE_UPDATE_MESSAGE,
        BucketProfileUpdateMessage.class);
    registerDSFID(PR_ALL_BUCKET_PROFILES_UPDATE_MESSAGE,
        AllBucketProfilesUpdateMessage.class);
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
    registerDSFID(PR_PRIMARY_REQUEST_REPLY_MESSAGE,
        PrimaryRequestReplyMessage.class);
    registerDSFID(PR_SANITY_CHECK_MESSAGE, PRSanityCheckMessage.class);
    registerDSFID(PR_PUTALL_REPLY_MESSAGE, PutAllReplyMessage.class);
    registerDSFID(PR_PUT_REPLY_MESSAGE, PutReplyMessage.class);
    registerDSFID(PR_QUERY_MESSAGE, QueryMessage.class);
    registerDSFID(PR_REMOVE_INDEXES_MESSAGE, RemoveIndexesMessage.class);
    registerDSFID(PR_REMOVE_INDEXES_REPLY_MESSAGE,
        RemoveIndexesReplyMessage.class);
    registerDSFID(PR_SIZE_MESSAGE, SizeMessage.class);
    registerDSFID(PR_SIZE_REPLY_MESSAGE, SizeReplyMessage.class);
    registerDSFID(PR_BUCKET_SIZE_REPLY_MESSAGE, BucketSizeReplyMessage.class);
    registerDSFID(PR_CONTAINS_KEY_VALUE_REPLY_MESSAGE,
        ContainsKeyValueReplyMessage.class);
    registerDSFID(PR_FETCH_ENTRIES_REPLY_MESSAGE,
        FetchEntriesReplyMessage.class);
    registerDSFID(PR_FETCH_ENTRY_REPLY_MESSAGE, FetchEntryReplyMessage.class);
    registerDSFID(PR_IDENTITY_REPLY_MESSAGE, IdentityReplyMessage.class);
    registerDSFID(PR_INDEX_CREATION_REPLY_MSG, IndexCreationReplyMsg.class);
    registerDSFID(PR_MANAGE_BUCKET_REPLY_MESSAGE,
        ManageBucketReplyMessage.class);
    registerDSFID(PR_FETCH_KEYS_REPLY_MESSAGE, FetchKeysReplyMessage.class);
    registerDSFID(PR_DUMP_B2N_REGION_MSG, DumpB2NRegion.class);
    registerDSFID(PR_DUMP_B2N_REPLY_MESSAGE, DumpB2NReplyMessage.class);
    registerDSFID(DESTROY_PARTITIONED_REGION_MESSAGE,
        DestroyPartitionedRegionMessage.class);
    registerDSFID(INVALIDATE_PARTITIONED_REGION_MESSAGE,
        InvalidatePartitionedRegionMessage.class);
    registerDSFID(COMMIT_PROCESS_QUERY_MESSAGE,
        CommitProcessQueryMessage.class);
    registerDSFID(COMMIT_PROCESS_QUERY_REPLY_MESSAGE,
        CommitProcessQueryReplyMessage.class);
    registerDSFID(DESTROY_REGION_WITH_CONTEXT_MESSAGE,
        DestroyRegionOperation.DestroyRegionWithContextMessage.class);
    registerDSFID(PUT_ALL_MESSAGE, PutAllMessage.class);
    registerDSFID(REMOVE_ALL_MESSAGE, RemoveAllMessage.class);
    registerDSFID(CLEAR_REGION_MESSAGE, ClearRegionMessage.class);
    registerDSFID(TOMBSTONE_MESSAGE, TombstoneMessage.class);
    registerDSFID(INVALIDATE_REGION_MESSAGE, InvalidateRegionMessage.class);
    registerDSFID(SEND_QUEUE_MESSAGE, SendQueueMessage.class);
    registerDSFID(STATE_MARKER_MESSAGE, StateMarkerMessage.class);
    registerDSFID(STATE_STABILIZATION_MESSAGE, StateStabilizationMessage.class);
    registerDSFID(STATE_STABILIZED_MESSAGE, StateStabilizedMessage.class);
    registerDSFID(CLIENT_MARKER_MESSAGE_IMPL, ClientMarkerMessageImpl.class);
    registerDSFID(TX_LOCK_UPDATE_PARTICIPANTS_MESSAGE,
        TXLockUpdateParticipantsMessage.class);
    registerDSFID(TX_ORIGINATOR_RECOVERY_MESSAGE,
        TXOriginatorRecoveryMessage.class);
    registerDSFID(TX_ORIGINATOR_RECOVERY_REPLY_MESSAGE,
        TXOriginatorRecoveryReplyMessage.class);
    registerDSFID(TX_REMOTE_COMMIT_MESSAGE, TXRemoteCommitMessage.class);
    registerDSFID(TX_REMOTE_ROLLBACK_MESSAGE, TXRemoteRollbackMessage.class);
    registerDSFID(JTA_BEFORE_COMPLETION_MESSAGE,
        JtaBeforeCompletionMessage.class);
    registerDSFID(JTA_AFTER_COMPLETION_MESSAGE,
        JtaAfterCompletionMessage.class);
    registerDSFID(QUEUE_REMOVAL_MESSAGE, QueueRemovalMessage.class);
    registerDSFID(DLOCK_RECOVER_GRANTOR_MESSAGE,
        DLockRecoverGrantorMessage.class);
    registerDSFID(DLOCK_RECOVER_GRANTOR_REPLY_MESSAGE,
        DLockRecoverGrantorReplyMessage.class);
    registerDSFID(NON_GRANTOR_DESTROYED_REPLY_MESSAGE,
        NonGrantorDestroyedReplyMessage.class);
    registerDSFID(IDS_REGISTRATION_MESSAGE,
        InternalDataSerializer.RegistrationMessage.class);
    registerDSFID(PR_FETCH_PARTITION_DETAILS_MESSAGE,
        FetchPartitionDetailsMessage.class);
    registerDSFID(PR_FETCH_PARTITION_DETAILS_REPLY,
        FetchPartitionDetailsReplyMessage.class);
    registerDSFID(PR_DEPOSE_PRIMARY_BUCKET_MESSAGE,
        DeposePrimaryBucketMessage.class);
    registerDSFID(PR_DEPOSE_PRIMARY_BUCKET_REPLY,
        DeposePrimaryBucketReplyMessage.class);
    registerDSFID(PR_BECOME_PRIMARY_BUCKET_MESSAGE,
        BecomePrimaryBucketMessage.class);
    registerDSFID(PR_BECOME_PRIMARY_BUCKET_REPLY,
        BecomePrimaryBucketReplyMessage.class);
    registerDSFID(PR_REMOVE_BUCKET_MESSAGE, RemoveBucketMessage.class);
    registerDSFID(TX_MANAGER_REMOVE_TRANSACTIONS,
        TXManagerImpl.TXRemovalMessage.class);
    registerDSFID(PR_REMOVE_BUCKET_REPLY, RemoveBucketReplyMessage.class);
    registerDSFID(PR_MOVE_BUCKET_MESSAGE, MoveBucketMessage.class);
    registerDSFID(PR_MOVE_BUCKET_REPLY, MoveBucketReplyMessage.class);
    registerDSFID(ADD_HEALTH_LISTENER_REQUEST, AddHealthListenerRequest.class);
    registerDSFID(ADD_HEALTH_LISTENER_RESPONSE, AddHealthListenerResponse.class);
    registerDSFID(ADD_STAT_LISTENER_REQUEST, AddStatListenerRequest.class);
    registerDSFID(ADD_STAT_LISTENER_RESPONSE, AddStatListenerResponse.class);
    registerDSFID(ADMIN_CONSOLE_DISCONNECT_MESSAGE,
        AdminConsoleDisconnectMessage.class);
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
    registerDSFID(CANCEL_STAT_LISTENER_RESPONSE,
        CancelStatListenerResponse.class);
    registerDSFID(DESTROY_ENTRY_MESSAGE, DestroyEntryMessage.class);
    registerDSFID(ADMIN_DESTROY_REGION_MESSAGE, DestroyRegionMessage.class);
    registerDSFID(FETCH_DIST_LOCK_INFO_REQUEST, FetchDistLockInfoRequest.class);
    registerDSFID(FETCH_DIST_LOCK_INFO_RESPONSE,
        FetchDistLockInfoResponse.class);
    registerDSFID(FETCH_HEALTH_DIAGNOSIS_REQUEST,
        FetchHealthDiagnosisRequest.class);
    registerDSFID(FETCH_HEALTH_DIAGNOSIS_RESPONSE,
        FetchHealthDiagnosisResponse.class);
    registerDSFID(FETCH_HOST_REQUEST, FetchHostRequest.class);
    registerDSFID(FETCH_HOST_RESPONSE, FetchHostResponse.class);
    registerDSFID(FETCH_RESOURCE_ATTRIBUTES_REQUEST,
        FetchResourceAttributesRequest.class);
    registerDSFID(FETCH_RESOURCE_ATTRIBUTES_RESPONSE,
        FetchResourceAttributesResponse.class);
    registerDSFID(FETCH_STATS_REQUEST, FetchStatsRequest.class);
    registerDSFID(FETCH_STATS_RESPONSE, FetchStatsResponse.class);
    registerDSFID(FETCH_SYS_CFG_REQUEST, FetchSysCfgRequest.class);
    registerDSFID(FETCH_SYS_CFG_RESPONSE, FetchSysCfgResponse.class);
    registerDSFID(FLUSH_APP_CACHE_SNAPSHOT_MESSAGE,
        FlushAppCacheSnapshotMessage.class);
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
    registerDSFID(REMOVE_HEALTH_LISTENER_REQUEST,
        RemoveHealthListenerRequest.class);
    registerDSFID(REMOVE_HEALTH_LISTENER_RESPONSE,
        RemoveHealthListenerResponse.class);
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
    registerDSFID(SERVER_BUCKET_PROFILE,
        BucketAdvisor.ServerBucketProfile.class);
    registerDSFID(PARTITION_PROFILE, RegionAdvisor.PartitionProfile.class);
    registerDSFID(GATEWAY_SENDER_PROFILE,
        GatewaySenderAdvisor.GatewaySenderProfile.class);
    registerDSFID(ROLE_EVENT, RoleEventImpl.class);
    registerDSFID(CLIENT_REGION_EVENT, ClientRegionEventImpl.class);
    registerDSFID(PR_INVALIDATE_MESSAGE, InvalidateMessage.class);
    registerDSFID(PR_INVALIDATE_REPLY_MESSAGE,
        InvalidateMessage.InvalidateReplyMessage.class);
    registerDSFID(TX_LOCK_UPDATE_PARTICIPANTS_REPLY_MESSAGE,
        TXLockUpdateParticipantsReplyMessage.class);
    registerDSFID(STREAMING_REPLY_MESSAGE, StreamingReplyMessage.class);
    registerDSFID(PARTITION_REGION_CONFIG, PartitionRegionConfig.class);
    registerDSFID(PREFER_BYTES_CACHED_DESERIALIZABLE,
        PreferBytesCachedDeserializable.class);
    registerDSFID(VM_CACHED_DESERIALIZABLE, VMCachedDeserializable.class);
    registerDSFID(GATEWAY_SENDER_EVENT_IMPL, GatewaySenderEventImpl.class);
    registerDSFID(SUSPEND_LOCKING_TOKEN, DLockService.SuspendLockingToken.class);
    registerDSFID(OBJECT_TYPE_IMPL, ObjectTypeImpl.class);
    registerDSFID(STRUCT_TYPE_IMPL, StructTypeImpl.class);
    registerDSFID(COLLECTION_TYPE_IMPL, CollectionTypeImpl.class);
    registerDSFID(TX_LOCK_BATCH, TXLockBatch.class);
    registerDSFID(GATEWAY_SENDER_EVENT_CALLBACK_ARGUMENT,
        GatewaySenderEventCallbackArgument.class);
    registerDSFID(MAP_TYPE_IMPL, MapTypeImpl.class);
    registerDSFID(STORE_ALL_CACHED_DESERIALIZABLE,
        StoreAllCachedDeserializable.class);
    registerDSFID(INTEREST_EVENT_MESSAGE, InterestEventMessage.class);
    registerDSFID(INTEREST_EVENT_REPLY_MESSAGE, InterestEventReplyMessage.class);
    registerDSFID(HA_EVENT_WRAPPER, HAEventWrapper.class);
    registerDSFID(STAT_ALERTS_MGR_ASSIGN_MESSAGE,
        StatAlertsManagerAssignMessage.class);
    registerDSFID(UPDATE_ALERTS_DEFN_MESSAGE,
        UpdateAlertDefinitionMessage.class);
    registerDSFID(REFRESH_MEMBER_SNAP_REQUEST,
        RefreshMemberSnapshotRequest.class);
    registerDSFID(REFRESH_MEMBER_SNAP_RESPONSE,
        RefreshMemberSnapshotResponse.class);
    registerDSFID(REGION_SUB_SIZE_REQUEST, RegionSubRegionSizeRequest.class);
    registerDSFID(REGION_SUB_SIZE_RESPONSE, RegionSubRegionsSizeResponse.class);
    registerDSFID(CHANGE_REFRESH_INT_MESSAGE,
        ChangeRefreshIntervalMessage.class);
    registerDSFID(ALERTS_NOTIF_MESSAGE, AlertsNotificationMessage.class);
    registerDSFID(FIND_DURABLE_QUEUE, FindDurableQueueMessage.class);
    registerDSFID(FIND_DURABLE_QUEUE_REPLY, FindDurableQueueReply.class);
    registerDSFID(CACHE_SERVER_LOAD_MESSAGE, CacheServerLoadMessage.class);
    registerDSFID(CACHE_SERVER_PROFILE, CacheServerProfile.class);
    registerDSFID(CONTROLLER_PROFILE, ControllerProfile.class);
    registerDSFID(DLOCK_QUERY_MESSAGE,
        DLockQueryProcessor.DLockQueryMessage.class);
    registerDSFID(DLOCK_QUERY_REPLY,
        DLockQueryProcessor.DLockQueryReplyMessage.class);
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
    registerDSFID(JGROUPS_VIEW, View.class);
    registerDSFID(JGROUPS_JOIN_RESP, JoinRsp.class);
    registerDSFID(PUTALL_VERSIONS_LIST, EntryVersionsList.class);
    registerDSFID(INITIAL_IMAGE_VERSIONED_OBJECT_LIST,
        InitialImageVersionedEntryList.class);
    registerDSFID(FIND_VERSION_TAG, FindVersionTagMessage.class);
    registerDSFID(VERSION_TAG_REPLY, VersionTagReply.class);
    registerDSFID(DURABLE_CLIENT_INFO_REQUEST, DurableClientInfoRequest.class);
    registerDSFID(DURABLE_CLIENT_INFO_RESPONSE, DurableClientInfoResponse.class);
    registerDSFID(CLIENT_INTEREST_MESSAGE, ClientInterestMessageImpl.class);
    registerDSFID(STAT_ALERT_DEFN_NUM_THRESHOLD,
        NumberThresholdDecoratorImpl.class);
    registerDSFID(STAT_ALERT_DEFN_GAUGE_THRESHOLD,
        GaugeThresholdDecoratorImpl.class);
    registerDSFID(CLIENT_HEALTH_STATS, ClientHealthStats.class);
    registerDSFID(STAT_ALERT_NOTIFICATION, StatAlertNotification.class);
    registerDSFID(FILTER_INFO_MESSAGE,
        InitialImageOperation.FilterInfoMessage.class);
    registerDSFID(SIZED_BASED_LOAD_PROBE, SizedBasedLoadProbe.class);
    registerDSFID(PR_MANAGE_BACKUP_BUCKET_MESSAGE,
        ManageBackupBucketMessage.class);
    registerDSFID(PR_MANAGE_BACKUP_BUCKET_REPLY_MESSAGE,
        ManageBackupBucketReplyMessage.class);
    registerDSFID(PR_CREATE_BUCKET_MESSAGE, CreateBucketMessage.class);
    registerDSFID(PR_CREATE_BUCKET_REPLY_MESSAGE,
        CreateBucketReplyMessage.class);
    registerDSFID(RESOURCE_MANAGER_PROFILE, ResourceManagerProfile.class);
    registerDSFID(RESOURCE_PROFILE_MESSAGE, ResourceProfileMessage.class);
    registerDSFID(JMX_MANAGER_PROFILE, JmxManagerProfile.class);
    registerDSFID(JMX_MANAGER_PROFILE_MESSAGE, JmxManagerProfileMessage.class);
    registerDSFID(CLIENT_BLACKLIST_MESSAGE, ClientBlacklistMessage.class);
    registerDSFID(REMOVE_CLIENT_FROM_BLACKLIST_MESSAGE,
        RemoveClientFromBlacklistMessage.class);
    registerDSFID(PR_FUNCTION_STREAMING_MESSAGE,
        PartitionedRegionFunctionStreamingMessage.class);
    registerDSFID(MEMBER_FUNCTION_STREAMING_MESSAGE,
        MemberFunctionStreamingMessage.class);
    registerDSFID(DR_FUNCTION_STREAMING_MESSAGE,
        DistributedRegionFunctionStreamingMessage.class);
    registerDSFID(FUNCTION_STREAMING_REPLY_MESSAGE,
        FunctionStreamingReplyMessage.class);
    registerDSFID(GET_ALL_SERVERS_REQUEST, GetAllServersRequest.class);
    registerDSFID(GET_ALL_SERVRES_RESPONSE, GetAllServersResponse.class);
    registerDSFID(PERSISTENT_MEMBERSHIP_VIEW_REQUEST,
        MembershipViewRequest.class);
    registerDSFID(PERSISTENT_MEMBERSHIP_VIEW_REPLY,
        MembershipViewReplyMessage.class);
    registerDSFID(PERSISTENT_STATE_QUERY_REQUEST,
        PersistentStateQueryMessage.class);
    registerDSFID(PERSISTENT_STATE_QUERY_REPLY,
        PersistentStateQueryReplyMessage.class);
    registerDSFID(PREPARE_NEW_PERSISTENT_MEMBER_REQUEST,
        PrepareNewPersistentMemberMessage.class);
    registerDSFID(MISSING_PERSISTENT_IDS_REQUEST,
        MissingPersistentIDsRequest.class);
    registerDSFID(MISSING_PERSISTENT_IDS_RESPONSE,
        MissingPersistentIDsResponse.class);
    registerDSFID(REVOKE_PERSISTENT_ID_REQUEST, RevokePersistentIDRequest.class);
    registerDSFID(REVOKE_PERSISTENT_ID_RESPONSE,
        RevokePersistentIDResponse.class);
    registerDSFID(REMOVE_PERSISTENT_MEMBER_REQUEST,
        RemovePersistentMemberMessage.class);
    registerDSFID(FUNCTION_STREAMING_ORDERED_REPLY_MESSAGE,
        FunctionStreamingOrderedReplyMessage.class);
    registerDSFID(REQUEST_SYNC_MESSAGE,
        InitialImageOperation.RequestSyncMessage.class);
    registerDSFID(PERSISTENT_MEMBERSHIP_FLUSH_REQUEST,
        MembershipFlushRequest.class);
    registerDSFID(SHUTDOWN_ALL_REQUEST, ShutdownAllRequest.class);
    registerDSFID(SHUTDOWN_ALL_RESPONSE, ShutdownAllResponse.class);
    registerDSFID(CLIENT_MEMBERSHIP_MESSAGE, ClientMembershipMessage.class);
    registerDSFID(END_BUCKET_CREATION_MESSAGE, EndBucketCreationMessage.class);
    registerDSFID(PREPARE_BACKUP_REQUEST, PrepareBackupRequest.class);
    registerDSFID(PREPARE_BACKUP_RESPONSE, PrepareBackupResponse.class);
    registerDSFID(FINISH_BACKUP_REQUEST, FinishBackupRequest.class);
    registerDSFID(FINISH_BACKUP_RESPONSE, FinishBackupResponse.class);
    registerDSFID(COMPACT_REQUEST, CompactRequest.class);
    registerDSFID(COMPACT_RESPONSE, CompactResponse.class);
    registerDSFID(FLOW_CONTROL_PERMIT_MESSAGE, FlowControlPermitMessage.class);
    registerDSFID(REQUEST_FILTERINFO_MESSAGE,
        InitialImageOperation.RequestFilterInfoMessage.class);
    registerDSFID(PARALLEL_QUEUE_REMOVAL_MESSAGE,
        ParallelQueueRemovalMessage.class);
    registerDSFID(PARALLEL_QUEUE_BATCH_REMOVAL_MESSAGE,
        ParallelQueueBatchRemovalMessage.class);
    registerDSFID(PARALLEL_QUEUE_BATCH_REMOVAL_REPLY,
        BatchRemovalReplyMessage.class);
    registerDSFID(BATCH_DESTROY_MESSAGE,
        BatchDestroyOperation.DestroyMessage.class);
    registerDSFID(FIND_REMOTE_TX_MESSAGE, FindRemoteTXMessage.class);
    registerDSFID(FIND_REMOTE_TX_REPLY, FindRemoteTXMessageReply.class);
    registerDSFID(SERIALIZED_OBJECT_PART_LIST, SerializedObjectPartList.class);
    registerDSFID(FLUSH_TO_DISK_REQUEST, FlushToDiskRequest.class);
    registerDSFID(FLUSH_TO_DISK_RESPONSE, FlushToDiskResponse.class);
    registerDSFID(ENUM_ID, EnumId.class);
    registerDSFID(ENUM_INFO, EnumInfo.class);
    registerDSFID(CHECK_TYPE_REGISTRY_STATE, CheckTypeRegistryState.class);
    registerDSFID(PREPARE_REVOKE_PERSISTENT_ID_REQUEST,
        PrepareRevokePersistentIDRequest.class);
    registerDSFID(PERSISTENT_RVV, DiskRegionVersionVector.class);
    registerDSFID(PERSISTENT_VERSION_TAG, DiskVersionTag.class);
    registerDSFID(DISK_STORE_ID, DiskStoreID.class);
    registerDSFID(CLIENT_PING_MESSAGE_IMPL, ClientPingMessageImpl.class);
    registerDSFID(SNAPSHOT_PACKET, SnapshotPacket.class);
    registerDSFID(SNAPSHOT_RECORD, SnapshotRecord.class);
    registerDSFID(FLOW_CONTROL_ACK, FlowControlAckMessage.class);
    registerDSFID(FLOW_CONTROL_ABORT, FlowControlAbortMessage.class);
    registerDSFID(MGMT_COMPACT_REQUEST,
        com.gemstone.gemfire.management.internal.messages.CompactRequest.class);
    registerDSFID(MGMT_COMPACT_RESPONSE,
        com.gemstone.gemfire.management.internal.messages.CompactResponse.class);
    registerDSFID(MGMT_FEDERATION_COMPONENT,
        com.gemstone.gemfire.management.internal.FederationComponent.class);
    registerDSFID(LOCATOR_STATUS_REQUEST, LocatorStatusRequest.class);
    registerDSFID(LOCATOR_STATUS_RESPONSE, LocatorStatusResponse.class);
    registerDSFID(R_FETCH_VERSION_MESSAGE, RemoteFetchVersionMessage.class);
    registerDSFID(R_FETCH_VERSION_REPLY,
        RemoteFetchVersionMessage.FetchVersionReplyMessage.class);
    registerDSFID(RELEASE_CLEAR_LOCK_MESSAGE, ReleaseClearLockMessage.class);
    registerDSFID(PR_TOMBSTONE_MESSAGE, PRTombstoneMessage.class);
    registerDSFID(HDFS_GATEWAY_EVENT_IMPL, HDFSGatewayEventImpl.class);
    
    registerDSFID(REQUEST_RVV_MESSAGE, InitialImageOperation.RequestRVVMessage.class);
    registerDSFID(RVV_REPLY_MESSAGE, InitialImageOperation.RVVReplyMessage.class);
    registerDSFID(SNAPPY_COMPRESSED_CACHED_DESERIALIZABLE, SnappyCompressedCachedDeserializable.class);
    registerDSFID(UPDATE_ENTRY_VERSION_MESSAGE, UpdateEntryVersionMessage.class);
    registerDSFID(PR_UPDATE_ENTRY_VERSION_MESSAGE,
        PRUpdateEntryVersionMessage.class);
    registerDSFID(PR_FETCH_BULK_ENTRIES_MESSAGE, FetchBulkEntriesMessage.class);
    registerDSFID(PR_FETCH_BULK_ENTRIES_REPLY_MESSAGE, FetchBulkEntriesReplyMessage.class);
    registerDSFID(PR_QUERY_TRACE_INFO, PRQueryTraceInfo.class);
    registerDSFID(INDEX_CREATION_DATA, IndexCreationData.class);
    registerDSFID(DIST_TX_OP, DistTxEntryEvent.class);
    registerDSFID(DIST_TX_PRE_COMMIT_RESPONSE, DistTXPrecommitMessage.DistTxPrecommitResponse.class);
    registerDSFID(DIST_TX_THIN_ENTRY_STATE, TXEntryState.DistTxThinEntryState.class);
    registerDSFID(SERVER_PING_MESSAGE, ServerPingMessage.class);
    registerDSFID(PR_DESTROY_ON_DATA_STORE_MESSAGE,
        DestroyRegionOnDataStoreMessage.class);
    registerDSFID(SHUTDOWN_ALL_GATEWAYHUBS_REQUEST,
        ShutdownAllGatewayHubsRequest.class);
  }

  /**
   * Creates a DataSerializableFixedID or StreamableFixedID instance by deserializing it from
   * the data input.
   */
  public static Object create(int dsfid, DataInput in)
      throws IOException, ClassNotFoundException {
    switch (dsfid) {
      case REGION:
        return (DataSerializableFixedID)DataSerializer.readRegion(in);
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
      case SQLF_TYPE:
        return readSqlfMessage(in);
      case SQLF_DVD_OBJECT:
        return readDVD(in);
      case SQLF_GLOBAL_ROWLOC:
        return readGlobalRowLocation(in);
      case SQLF_GEMFIRE_KEY:
        return readGemFireKey(in);
      case SQLF_FORMATIBLEBITSET:
        return readSqlFormatibleBitSet(in);
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
      case CONFIGURATION_REQUEST :
        return readConfigurationRequest(in);
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
            Object ds = cons
                    .newInstance((Object[]) null);
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
        throw new DSFIDNotFoundException("Unknown DataSerializableFixedID: "
                + dsfid, dsfid);

    }
  }

 
  //////////////////  Reading Internal Objects  /////////////////
  /**
   * Reads an instance of <code>IpAddress</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static InternalDistributedMember readInternalDistributedMember(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDistributedMember o = new InternalDistributedMember();
    InternalDataSerializer.invokeFromData(o, in);
    return o;
  }

  private static ResultsBag readResultsBag(DataInput in) throws IOException, ClassNotFoundException {
    ResultsBag o = new ResultsBag(true);
    InternalDataSerializer.invokeFromData(o, in);
    return o;
  }

  private static Undefined readUndefined(DataInput in) throws IOException, ClassNotFoundException {
    Undefined o = (Undefined)QueryService.UNDEFINED;
    InternalDataSerializer.invokeFromData(o, in);
    return o;
  }

  /**
   * Reads an instance of <code>InterestResultPolicy</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  private static InterestResultPolicyImpl readInterestResultPolicy(DataInput in) throws IOException, ClassNotFoundException {
    byte ordinal = in.readByte();
    return (InterestResultPolicyImpl)InterestResultPolicy.fromOrdinal(ordinal);
  }

  private static DataSerializableFixedID readDestroyOnDataStore(DataInput in) 
      throws IOException, ClassNotFoundException {
    DataSerializableFixedID serializable = new DestroyRegionOnDataStoreMessage();
    serializable.fromData(in);
    return serializable;
  }
  /**
   * Map for SQLFabric specific classIds to the {@link Class} of an
   * implementation. We maintain this separate map for SQLFabric to allow
   * separation of GemFire and SQLFabric trees. This is particularly required
   * when implementing a new <code>DistributionMessage</code>. This requires the
   * classes to have a zero argument constructor.
   */
  @SuppressWarnings("unchecked")
  private static Class<? extends DataSerializableFixedID>[] sqlfDSFIDClassMap =
    new Class[Byte.MAX_VALUE + 1 - Byte.MIN_VALUE];
  
  /**
   * Map for SQLFabric specific classIds to the {@link DataSerializableFixedID} 
   * singleton instance. We maintain this separate map for SQLFabric to allow
   * separation of GemFire and SQLFabric trees. This approach is needed to 
   * allow transparent serialization of singleton objects
   */
  private static DataSerializableFixedID[] sqlfDSFIDFixedInstanceMap =
    new DataSerializableFixedID[Byte.MAX_VALUE + 1 - Byte.MIN_VALUE];

  /**
   * Exception to indicate SQLFabric specific serialization exceptions
   */
  public static class SqlfSerializationException extends
      NotSerializableException {

    private static final long serialVersionUID = 5076687296705595933L;

    /**
     * Constructs a SqlfSerializationException object with message string.
     * 
     * @param msg
     *          exception message
     */
    public SqlfSerializationException(String msg) {
      super(msg);
    }
  }

  private static DataSerializableFixedID readSqlfMessage(DataInput in)
      throws IOException, ClassNotFoundException {
    // Use the first byte as the typeId of SQLFabric messages
    final byte sqlfId = in.readByte();
    final int sqlfIdIndex = sqlfId & 0xFF;
    final Class<? extends DataSerializableFixedID> sqlfClass =
      sqlfDSFIDClassMap[sqlfIdIndex];
    if (sqlfClass != null) {
      try {
        final DataSerializableFixedID sqlfObj = sqlfClass.newInstance();
        InternalDataSerializer.invokeFromData(sqlfObj, in);
        return sqlfObj;
      } catch (InstantiationException ex) {
        throw new SqlfSerializationException(LocalizedStrings.
            DSFIDFactory_COULD_NOT_INSTANTIATE_SQLFABRIC_MESSAGE_CLASSID_0_1
              .toLocalizedString(new Object[] { sqlfId, ex }));
      } catch (IllegalAccessException ex) {
        throw new SqlfSerializationException(LocalizedStrings.
            DSFIDFactory_ILLEGAL_ACCESS_FOR_SQLFABRIC_MESSAGE_CLASSID_0_1
              .toLocalizedString(new Object[] { sqlfId, ex }));
      }
    }//check for fixed instance
    DataSerializableFixedID fixedInstance = sqlfDSFIDFixedInstanceMap[sqlfIdIndex];
    if (fixedInstance != null) {
      InternalDataSerializer.invokeFromData(fixedInstance, in);
      return fixedInstance;
    }
    // if possible set the processor ID before throwing exception so
    // that failure exception is received by the sender
    if (sqlfIdIndex < 60) {
      try {
        // both SqlfMessage and SqlfReplyMessage write a byte for status first
        // followed by the processor ID, if any
        final byte status = in.readByte();
        int processorId = 0;
        if ((status & ReplyMessage.PROCESSOR_ID_FLAG) != 0) {
          processorId = in.readInt();
        }
        ReplyProcessor21.setMessageRPId(processorId);
      } catch (IOException ex) {
        // give up
      }
    }
    throw new SqlfSerializationException(
        LocalizedStrings.DSFIDFactory_UNKNOWN_CLASSID_0_FOR_SQLFABRIC_MESSAGE
            .toLocalizedString(sqlfId));
  }

  public static synchronized void registerSQLFabricClass(byte classId,
      Class<? extends DataSerializableFixedID> c) {
    final int sqlfIdIndex = classId & 0xFF;
    Class<?> oldClass = sqlfDSFIDClassMap[sqlfIdIndex];
    if (oldClass != null) {
      throw new AssertionError("DSFIDFactory#registerSQLFabricClass: cannot "
          + "re-register classId " + classId + " for class " + c
          + "; existing class: " + oldClass);
    }
    sqlfDSFIDClassMap[sqlfIdIndex] = c;
  }
  
  public static synchronized void registerSQLFabricFixedInstance(byte classId,
      DataSerializableFixedID fixedInstance)
  {
    final int sqlfIdIndex = classId & 0xFF;
    DataSerializableFixedID oldInstance = sqlfDSFIDFixedInstanceMap[sqlfIdIndex];
    if (oldInstance != null) {
      throw new AssertionError("DSFIDFactory#registerSQLFabricClass: cannot "
          + "re-register classId " + classId + " for instance " + fixedInstance
          + "; existing instance: " + oldInstance);
    }
    sqlfDSFIDFixedInstanceMap[sqlfIdIndex] = fixedInstance;
  }

  public static synchronized void unregisterSQLFabricClass(byte classId,
      Class<? extends DataSerializableFixedID> c) {
    final int sqlfIdIndex = classId & 0xFF;
    sqlfDSFIDClassMap[sqlfIdIndex] = null;
  }
  
  public static synchronized void unregisterSQLFabricFixedInstance(
      byte classId, Object dataSerializableFixedID)
  {
    final int sqlfIdIndex = classId & 0xFF;
    sqlfDSFIDFixedInstanceMap[sqlfIdIndex] = null;
  }

  public static synchronized void clearSQLFabricClasses() {
    for (int index = 0; index < sqlfDSFIDClassMap.length; ++index) {
      sqlfDSFIDClassMap[index] = null;
    }
    for (int index = 0; index < sqlfDSFIDFixedInstanceMap.length; ++index) {
      sqlfDSFIDFixedInstanceMap[index] = null;
    }
  }  

  public interface DeserializeDVD {

    public DataSerializableFixedID getDSFID(DataInput in) throws IOException,
        ClassNotFoundException;

    public DataSerializableFixedID getGlobalRowLocation(DataInput in)
        throws IOException, ClassNotFoundException;

    public DataSerializableFixedID getGemFireKey(DataInput in)
        throws IOException, ClassNotFoundException;

    public DataSerializableFixedID getSqlPSQArgs(DataInput in)
        throws IOException, ClassNotFoundException;

    public DataSerializableFixedID getSqlFormatibleBitSet(DataInput in)
        throws IOException, ClassNotFoundException;
  }

  private static DeserializeDVD dvdDeserializer;

  private static DataSerializableFixedID readDVD(DataInput in)
      throws IOException, ClassNotFoundException {
    return dvdDeserializer.getDSFID(in);
  }

  private static DataSerializableFixedID readGlobalRowLocation(DataInput in)
      throws IOException, ClassNotFoundException {
    return dvdDeserializer.getGlobalRowLocation(in);
  }

  private static DataSerializableFixedID readGemFireKey(DataInput in)
      throws IOException, ClassNotFoundException {
    return dvdDeserializer.getGemFireKey(in);
  }

  private static DataSerializableFixedID readSqlFormatibleBitSet(DataInput in)
      throws IOException, ClassNotFoundException {
    return dvdDeserializer.getSqlFormatibleBitSet(in);
  }

  public static void registerDVDDeserializer(DeserializeDVD d) {
    dvdDeserializer = d;
  }

  private static DataSerializableFixedID readSnappyCompressedCachedDeserializable(DataInput in) 
  throws IOException, ClassNotFoundException {
    DataSerializableFixedID serializable = new SnappyCompressedCachedDeserializable();
    serializable.fromData(in);
    return serializable;
  }
  
  private static DataSerializableFixedID readNullToken(DataInput in) 
  throws IOException, ClassNotFoundException {
    DataSerializableFixedID serializable = (NullToken)IndexManager.NULL;
    serializable.fromData(in);
    return serializable;
  }
  
  private static DataSerializableFixedID readConfigurationRequest(DataInput in) throws IOException, ClassNotFoundException {
    DataSerializableFixedID serializable = new ConfigurationRequest();
    serializable.fromData(in);
    return serializable;
  }
  
  private static DataSerializableFixedID readConfigurationResponse(DataInput in) throws IOException, ClassNotFoundException {
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

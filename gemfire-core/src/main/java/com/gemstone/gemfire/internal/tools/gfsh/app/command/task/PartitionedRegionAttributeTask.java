package com.gemstone.gemfire.internal.tools.gfsh.app.command.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.admin.SystemMemberCache;
import com.gemstone.gemfire.admin.SystemMemberRegion;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;

public class PartitionedRegionAttributeTask implements CommandTask
{
	private static final long serialVersionUID = 1L;
	
	private String regionPath;

	public CommandResults runTask(Object userData) 
	{
		CommandResults results = new CommandResults();
		try {
			PartitionAttributeInfo pai = getPartitionAttributeInfo(regionPath);
			if (pai == null) {
				results.setCode(CommandResults.CODE_ERROR);
				results.setCodeMessage(regionPath + " is not partitioned regon");
			}
		} catch (Exception ex) {
			results.setCode(CommandResults.CODE_ERROR);
			results.setCodeMessage(ex.getMessage());
			results.setException(ex);
		}
		return results;
	}
	
	private static PartitionAttributeInfo getPartitionAttributeInfo(String regionPath) throws AdminException
	{
		Cache cache = CacheFactory.getAnyInstance();
		DistributedSystem ds = cache.getDistributedSystem();
        DistributedSystemConfig config = AdminDistributedSystemFactory.defineDistributedSystem(ds, null);
        AdminDistributedSystem adminSystem = AdminDistributedSystemFactory.getDistributedSystem(config);
        try {
        	adminSystem.connect();
        } catch (Exception ex) {
        	// already connected
        }
        SystemMember[] members = adminSystem.getSystemMemberApplications();
        
        boolean isPR = true;
        int redundantCopies = 0;
        int totalNumBuckets = 0;
        
        PartitionAttributeInfo pai = new PartitionAttributeInfo();
        
        for (int i = 0; i < members.length; i++) {
            SystemMemberCache scache = members[i].getCache();

            if (scache != null) {
            	SystemMemberRegion region = scache.getRegion(regionPath);
            	PartitionAttributes pa = region.getPartitionAttributes();
            	if (pa == null) {
            		isPR = false;
            		break;
            	}
            	PartitionAttributeInfo.Partition part = new PartitionAttributeInfo.Partition();
            	
            	part.localMaxMemory = region.getPartitionAttributes().getLocalMaxMemory();
            	part.toalMaxMemory = region.getPartitionAttributes().getTotalMaxMemory();
            	pai.addPartition(part);
            	
            	redundantCopies = region.getPartitionAttributes().getRedundantCopies();
            	totalNumBuckets = region.getPartitionAttributes().getTotalNumBuckets();
            }
        }
        
        if (isPR) {
        	pai.redundantCopies = redundantCopies;
        	pai.regionPath = regionPath;
        	pai.totalNumBuckets = totalNumBuckets;
        } else {
        	pai = null;
        }
        
       return pai;
	}
	
	public static class PartitionAttributeInfo implements DataSerializable
	{
		private static final long serialVersionUID = 1L;

		private String regionPath;
		
		private int redundantCopies;
		private int totalNumBuckets;
        
        private List partitionList = new ArrayList();
        
        public PartitionAttributeInfo() {}
        
        public void addPartition(Partition partition)
        {
        	partitionList.add(partition);
        }
        
        public List getPartitionList()
        {
        	return partitionList;
        }

		public String getRegionPath()
		{
			return regionPath;
		}

		public int getRedundantCopies()
		{
			return redundantCopies;
		}

		public int getTotalNumBuckets()
		{
			return totalNumBuckets;
		}

		public void fromData(DataInput in) throws IOException, ClassNotFoundException
		{
			regionPath = in.readUTF();
			redundantCopies = in.readInt();
			totalNumBuckets = in.readInt();
			
			partitionList = new ArrayList();
			int size = in.readInt();
			for (int i = 0; i < size; i++) {
				Partition part = new Partition();
				part.memberName = in.readUTF();
				part.localMaxMemory = in.readInt();
				part.toalMaxMemory = in.readLong();
				partitionList.add(part);
			}
		}

		public void toData(DataOutput out) throws IOException
		{
			out.writeUTF(regionPath);
			out.writeInt(redundantCopies);
			out.writeInt(totalNumBuckets);
			
			int size = partitionList.size();
			out.writeInt(size);
			for (int i = 0; i < size; i++) {
				Partition part = (Partition)partitionList.get(i);
				out.writeUTF(part.memberName);
				out.writeInt(part.localMaxMemory);
				out.writeLong(part.toalMaxMemory);
			}
			
		}
		
		public static class Partition
        {
			public Partition() {}
			
        	private String memberName;
        	private int localMaxMemory ;
        	private long toalMaxMemory;
        	
			public String getMemberName()
			{
				return memberName;
			}
			
			public int getLocalMaxMemory()
			{
				return localMaxMemory;
			}
			
			public long getToalMaxMemory()
			{
				return toalMaxMemory;
			}
        }

	}

	public void fromData(DataInput in) throws IOException, ClassNotFoundException
	{
		regionPath = in.readUTF();
	}

	public void toData(DataOutput out) throws IOException
	{
		out.writeUTF(regionPath);
	}
}

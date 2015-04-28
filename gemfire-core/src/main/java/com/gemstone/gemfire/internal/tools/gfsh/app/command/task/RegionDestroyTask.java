package com.gemstone.gemfire.internal.tools.gfsh.app.command.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;

/**
 * RegionDestroyTask destroys a remote region. Invoke this class using
 * CommandClient.execute() or CommandClient.broadcast(). CommandClient.execute()
 * destroys the specified region in the connected server and distributes it to
 * other servers only if the region scope is not LOCAL.
 * CommandClient.Broadcast() destroys the specified region in all peers in the
 * distributed system. Both methods return CommandResults with the code set.
 * Check CommandResults.getCode() to see the task execution status.
 * RegionDestroyTask.SUCCESS_DESTROYED for region successfully destroyed,
 * RegionDestroyTask.ERROR_REGION_DESTROY for an error creating region.
 * CommandResults.getDataObject() returns MemberInfo.
 * 
 * @author dpark
 */
public class RegionDestroyTask implements CommandTask
{
	private static final long serialVersionUID = 1L;

	public static final byte ERROR_REGION_DESTROY = 1;

	private String regionFullPath;

	public RegionDestroyTask()
	{
	}

	/**
	 * Constructs a RegionDestroyTask object.
	 * 
	 * @param regionFullPath
	 *            The path of the region to destroy.
	 */
	public RegionDestroyTask(String regionFullPath)
	{
		this.regionFullPath = regionFullPath;
	}

	public CommandResults runTask(Object userData)
	{
		CommandResults results = new CommandResults();

		MemberInfo memberInfo = new MemberInfo();

		try {
			Cache cache = CacheFactory.getAnyInstance();
			Region region = cache.getRegion(regionFullPath);
			DistributedMember member = cache.getDistributedSystem().getDistributedMember();
			memberInfo.setHost(member.getHost());
			memberInfo.setMemberId(member.getId());
			memberInfo.setMemberName(cache.getName());
			memberInfo.setPid(member.getProcessId());

			results.setDataObject(memberInfo);

			if (region == null) {
				results.setCode(ERROR_REGION_DESTROY);
				results.setCodeMessage("Region undefined: " + regionFullPath);
			} else {
				synchronized (region) {
					region.destroyRegion();
				}
			}
		} catch (Exception ex) {
			results.setCode(ERROR_REGION_DESTROY);
			results.setCodeMessage(ex.getMessage());
			results.setException(ex);
		}

		return results;
	}

	public void fromData(DataInput input) throws IOException, ClassNotFoundException
	{
		regionFullPath = DataSerializer.readString(input);
	}

	public void toData(DataOutput output) throws IOException
	{
		DataSerializer.writeString(regionFullPath, output);
	}

}

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
 * RegionClearTask destroys a region in the server. The region destroy will be
 * distributed to other caches if the scope is not Scope.LOCAL.
 * CommandResults.getDataObject() returns MemberInfo.
 * 
 * @author dpark
 * 
 */
public class RegionClearTask implements CommandTask
{
	private static final long serialVersionUID = 1L;

	public static final byte ERROR_REGION_CLEAR = 1;

	private String regionFullPath;

	public RegionClearTask()
	{
	}

	public RegionClearTask(String regionFullPath)
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
				results.setCode(ERROR_REGION_CLEAR);
				results.setCodeMessage("Region undefined: " + regionFullPath);
			} else {
				synchronized (region) {
					region.clear();
				}
			}
		} catch (Exception ex) {
			results.setCode(ERROR_REGION_CLEAR);
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

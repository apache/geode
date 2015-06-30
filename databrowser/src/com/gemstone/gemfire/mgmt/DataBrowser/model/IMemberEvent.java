package com.gemstone.gemfire.mgmt.DataBrowser.model;

import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;

/**
 * Event representing change of member status(Left, Crashed, Joined etc)
 * 
 * @author mghosh
 * 
 */
public interface IMemberEvent extends IDistributedSystemEvent {
	/**
	 * 
	 * @return the member for which the event is raised
	 */
	public GemFireMember getMember();
	
	/**
	 * 
	 * @return the list of members for which the event is raised
	 */
	public GemFireMember[] getMembers();
}

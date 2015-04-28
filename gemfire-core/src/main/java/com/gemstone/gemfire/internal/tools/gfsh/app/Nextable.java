package com.gemstone.gemfire.internal.tools.gfsh.app;

import java.util.List;

public interface Nextable
{
	List next(Object arg) throws Exception;
}

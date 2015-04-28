package com.gemstone.gemfire.internal.tools.gfsh.app.aggregator;

/**
 * AggregatorException is thrown by Aggregator if there is
 * any error related to the aggregator.
 * 
 * @author dpark
 *
 */
public class AggregatorException extends Exception
{
	private static final long serialVersionUID = 1L;
	
	private Throwable functionExceptions[];
	
    public AggregatorException()
    {
        super();
    }
    public AggregatorException(String message)
    {
        super(message);
    }

    public AggregatorException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public AggregatorException(Throwable cause)
    {
        super(cause);
    }
    
    public AggregatorException(String message, Throwable functionExceptions[])
    {
    	super(message);
    	this.functionExceptions = functionExceptions;
    }
    
    /**
     * The exception caught in AggregateFunction.run().
     * @return exception caught in AggregateFunction.run()
     */
    public Throwable[] getFunctionExceptions()
    {
    	return functionExceptions;
    }
}


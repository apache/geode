/*
 The MIT License

 Copyright (c) 2004-2011 Paul R. Holser, Jr.

 Permission is hereby granted, free of charge, to any person obtaining
 a copy of this software and associated documentation files (the
 "Software"), to deal in the Software without restriction, including
 without limitation the rights to use, copy, modify, merge, publish,
 distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to
 the following conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package joptsimple;

import java.util.Collection;

/**
 * Thrown when a problem occurs converting an argument of an option from {@link String} to another type.
 *
 * @author <a href="mailto:pholser@alumni.rice.edu">Paul Holser</a>
 * @author Nikhil Jadhav
 */
public class OptionArgumentConversionException extends OptionException {
    private static final long serialVersionUID = -1L;

    private final String argument;
    private final Class<?> valueType;

    OptionArgumentConversionException( Collection<String> options, String argument, Class<?> valueType,
        Throwable cause ) {

        super( options, cause );

        this.argument = argument;
        this.valueType = valueType;
    }

    // GemFire Addition: Added to include OptionSet
    public OptionArgumentConversionException( Collection<String> options, String argument, Class<?> valueType,
        OptionSet detected, Throwable cause ) {
        super( options, detected );
        this.argument = argument;
        this.valueType = valueType;
    }
    
    @Override
    public String getMessage() {
        return "Cannot convert argument '" + argument + "' of option " + multipleOptionMessage() + " to " + valueType;
    }
}

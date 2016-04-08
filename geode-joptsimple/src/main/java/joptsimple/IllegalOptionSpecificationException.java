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

import static java.util.Collections.*;

/**
 * Thrown when the option parser is asked to recognize an option with illegal characters in it.
 *
 * @author <a href="mailto:pholser@alumni.rice.edu">Paul Holser</a>
 * @author Nikhil Jadhav
 */
public class IllegalOptionSpecificationException extends OptionException {
    private static final long serialVersionUID = -1L;

    IllegalOptionSpecificationException( String option ) {
        super( singletonList( option ) );
    }

    // GemFire Addition: Added to include OptionSet
    IllegalOptionSpecificationException( String option, OptionSet detected ) {
        super( singletonList( option ), detected );
    }
    
    @Override
    public String getMessage() {
        return singleOptionMessage() + " is not a legal option character";
    }
}

/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.blocks;


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;


/**
 * A method call is the JGroups representation of a remote method.
 * It includes the name of the method (case sensitive) and a list of arguments.
 * A method call is serializable and can be passed over the wire.
 * @author Bela Ban
 * @version $Revision: 1.19 $
 */
public class MethodCall implements Externalizable {

    static final long serialVersionUID=7873471327078957662L;

    /** The name of the method, case sensitive. */
    protected String method_name=null;

    /** The ID of a method, maps to a java.lang.reflect.Method */
    protected short method_id=-1;

    /** The arguments of the method. */
    protected Object[] args=null;

    /** The class types, e.g., new Class[]{String.class, int.class}. */
    protected Class[] types=null;

    /** The signature, e.g., new String[]{String.class.getName(), int.class.getName()}. */
    protected String[] signature=null;

    /** The Method of the call. */
    protected Method method=null;

    /** To carry arbitrary data with a method call, data needs to be serializable if sent across the wire */
    protected Map payload=null;

    protected static final GemFireTracer log=GemFireTracer.getLog(MethodCall.class);

    /** Which mode to use. */
    protected short mode=OLD;

    /** Infer the method from the arguments. */
    protected static final short OLD=1;

    /** Explicitly ship the method, caller has to determine method himself. */
    protected static final short METHOD=2;

    /** Use class information. */
    protected static final short TYPES=3;

    /** Provide a signature, similar to JMX. */
    protected static final short SIGNATURE=4;

    /** Use an ID to map to a method */
    protected static final short ID=5;


    /**
     * Creates an empty method call, this is always invalid, until
     * <code>setName()</code> has been called.
     */
    public MethodCall() {
    }


    public MethodCall(Method method) {
        this(method, null);
    }

    public MethodCall(Method method, Object[] arguments) {
        init(method);
        if(arguments != null) args=arguments;
    }

    /**
     *
     * @param method_name
     * @param args
     * @deprecated Use one of the constructors that take class types as arguments
     */
   @Deprecated // GemStoneAddition
   public MethodCall(String method_name, Object[] args) {
        this.method_name=method_name;
        this.mode=OLD;
        this.args=args;
    }

    public MethodCall(short method_id, Object[] args) {
        this.method_id=method_id;
        this.mode=ID;
        this.args=args;
    }


    public MethodCall(String method_name, Object[] args, Class[] types) {
        this.method_name=method_name;
        this.args=args;
        this.types=types;
        this.mode=TYPES;
    }

    public MethodCall(String method_name, Object[] args, String[] signature) {
        this.method_name=method_name;
        this.args=args;
        this.signature=signature;
        this.mode=SIGNATURE;
    }

    private void init(Method method) {
        this.method=method;
        this.mode=METHOD;
        method_name=method.getName();
    }


    public int getMode() {
        return mode;
    }



    /**
     * returns the name of the method to be invoked using this method call object
     * @return a case sensitive name, can be null for an invalid method call
     */
    public String getName() {
        return method_name;
    }

    /**
     * sets the name for this MethodCall and allowing you to reuse the same object for
     * a different method invokation of a different method
     * @param n - a case sensitive method name
     */
    public void setName(String n) {
        method_name=n;
    }

    public short getId() {
        return method_id;
    }

    public void setId(short method_id) {
        this.method_id=method_id;
    }

    /**
     * returns an ordered list of arguments used for the method invokation
     * @return returns the list of ordered arguments
     */
    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        if(args != null)
            this.args=args;
    }

    public Method getMethod() {
        return method;
    }


    public void setMethod(Method m) {
        init(m);
    }


    public synchronized Object put(Object key, Object value) {
        if(payload == null)
            payload=new HashMap();
        return payload.put(key, value);
    }

    public synchronized Object get(Object key) {
        return payload != null? payload.get(key) : null;
    }


    /**
     *
     * @param target_class
     * @return the Method
     * @throws Exception
     */
    Method findMethod(Class target_class) throws Exception {
        int     len=args != null? args.length : 0;
        Method  m;

        Method[] methods=getAllMethods(target_class);
        for(int i=0; i < methods.length; i++) {
            m=methods[i];
            if(m.getName().equals(method_name)) {
                if(m.getParameterTypes().length == len)
                    return m;
            }
        }

        return null;
    }


    /**
     * The method walks up the class hierarchy and returns <i>all</i> methods of this class
     * and those inherited from superclasses and superinterfaces.
     */
    Method[] getAllMethods(Class target) {

        Class superclass = target;
        List methods = new ArrayList();
        int size = 0;

        while(superclass != null) {
            Method[] m = superclass.getDeclaredMethods();
            methods.add(m);
            size += m.length;
            superclass = superclass.getSuperclass();
        }

        Method[] result = new Method[size];
        int index = 0;
        for(Iterator i = methods.iterator(); i.hasNext();) {
            Method[] m = (Method[])i.next();
            System.arraycopy(m, 0, result, index, m.length);
            index += m.length;
        }
        return result;
    }

    /**
     * Returns the first method that matches the specified name and parameter types. The overriding
     * methods have priority. The method is chosen from all the methods of the current class and all
     * its superclasses and superinterfaces.
     *
     * @return the matching method or null if no mathching method has been found.
     */
    Method getMethod(Class target, String methodName, Class[] types) {

        if (types == null) {
            types = new Class[0];
        }

        Method[] methods = getAllMethods(target);
        methods: for(int i = 0; i < methods.length; i++) {
            Method m = methods[i];
            if (!methodName.equals(m.getName())) {
                continue;
            }
            Class[] parameters = m.getParameterTypes();
            if (types.length != parameters.length) {
                continue;
            }
            for(int j = 0; j < types.length; j++) {
                if (!types[j].equals(parameters[j])) {
                    continue methods;
                }
            }
            return m;
        }
        return null;
    }


    /**
     * Invokes the method with the supplied arguments against the target object.
     * If a method lookup is provided, it will be used. Otherwise, the default
     * method lookup will be used.
     * @param target - the object that you want to invoke the method on
     * @return an object
     */
    public Object invoke(Object target) throws Throwable {
        Class  cl;
        Method meth=null;
        Object retval=null;


        if(method_name == null || target == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.MethodCall_METHOD_NAME_OR_TARGET_IS_NULL);
            return null;
        }
        cl=target.getClass();
        try {
            switch(mode) {
            case OLD:
                meth=findMethod(cl);
                break;
            case METHOD:
                if(this.method != null)
                    meth=this.method;
                break;
            case TYPES:
                //meth=cl.getDeclaredMethod(method_name, types);
                meth = getMethod(cl, method_name, types);
                break;
            case SIGNATURE:
                Class[] mytypes=null;
                if(signature != null)
                    mytypes=getTypesFromString(cl, signature);
                //meth=cl.getDeclaredMethod(method_name, mytypes);
                meth = getMethod(cl, method_name, mytypes);
                break;
            case ID:
                break;
            default:
                if(log.isErrorEnabled()) log.error(ExternalStrings.MethodCall_MODE__0__IS_INVALID, mode);
                break;
            }

            if(meth != null) {
                retval=meth.invoke(target, args);
            }
            else {
                if(log.isErrorEnabled()) log.error(ExternalStrings.MethodCall_METHOD__0__NOT_FOUND, method_name);
            }
            return retval;
        }
        catch(InvocationTargetException inv_ex) {
            throw inv_ex.getTargetException();
        }
        catch(NoSuchMethodException no) {
            StringBuffer sb=new StringBuffer();
            sb.append("found no method called ").append(method_name).append(" in class ");
            sb.append(cl.getName()).append(" with (");
            if(args != null) {
                for(int i=0; i < args.length; i++) {
                    if(i > 0)
                        sb.append(", ");
                    sb.append((args[i] != null)? args[i].getClass().getName() : "null");
                }
            }
            sb.append(") formal parameters");
            log.error(sb.toString());
            throw no;
        }
     catch(Throwable e) {
            // e.printStackTrace(System.err);
            if(log.isErrorEnabled()) log.error(ExternalStrings.MethodCall_EXCEPTION_IN_INVOKE, e);
            throw e;
        }
    }

    public Object invoke(Object target, Object[] args) throws Throwable {
        if(args != null)
            this.args=args;
        return invoke(target);
    }


    Class[] getTypesFromString(Class cl, String[] signature) throws Exception {
        String  name;
        Class   parameter;
        Class[] mytypes=new Class[signature.length];

        for(int i=0; i < signature.length; i++) {
            name=signature[i];
            if("long".equals(name))
                parameter=long.class;
            else if("int".equals(name))
                parameter=int.class;
            else if("short".equals(name))
                parameter=short.class;
            else if("char".equals(name))
                parameter=char.class;
            else if("byte".equals(name))
                parameter=byte.class;
            else if("float".equals(name))
                parameter=float.class;
            else if("double".equals(name))
                parameter=double.class;
            else if("boolean".equals(name))
                parameter=boolean.class;
            else
                parameter=Class.forName(name);
            mytypes[i]=parameter;
        }
        return mytypes;
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer ret=new StringBuffer();
        boolean first=true;
        if(method_name != null)
            ret.append(method_name);
        else
            ret.append(method_id);
        ret.append('(');
        if(args != null) {
            for(int i=0; i < args.length; i++) {
                if(first)
                    first=false;
                else
                    ret.append(", ");
                ret.append(args[i]);
            }
        }
        ret.append(')');
        return ret.toString();
    }

    public String toStringDetails() {
        StringBuffer ret=new StringBuffer();
        ret.append("MethodCall ");
        if(method_name != null)
            ret.append("name=").append(method_name);
        else
            ret.append("id=").append(method_id);
        ret.append(", number of args=").append((args != null? args.length : 0)).append(')');
        if(args != null) {
            ret.append("\nArgs:");
            for(int i=0; i < args.length; i++) {
                ret.append("\n[").append(args[i]).append(" (").
                        append((args[i] != null? args[i].getClass().getName() : "null")).append(")]");
            }
        }
        return ret.toString();
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        if(method_name != null) {
            out.writeBoolean(true);
            out.writeUTF(method_name);
        }
        else {
            out.writeBoolean(false);
            out.writeShort(method_id);
        }
        out.writeObject(args);
        out.writeShort(mode);

        switch(mode) {
        case OLD:
            break;
        case METHOD:
            out.writeObject(method.getParameterTypes());
            out.writeObject(method.getDeclaringClass());
            break;
        case TYPES:
            out.writeObject(types);
            break;
        case SIGNATURE:
            out.writeObject(signature);
            break;
        case ID:
            break;
        default:
            if(log.isErrorEnabled()) log.error(ExternalStrings.MethodCall_MODE__0__IS_INVALID, mode);
            break;
        }

        if(payload != null) {
            out.writeBoolean(true);
            out.writeObject(payload);
        }
        else {
            out.writeBoolean(false);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean name_available=in.readBoolean();
        if(name_available)
            method_name=in.readUTF();
        else
            method_id=in.readShort();
        args=(Object[])in.readObject();
        mode=in.readShort();

        switch(mode) {
        case OLD:
            break;
        case METHOD:
            Class[] parametertypes=(Class[])in.readObject();
            Class   declaringclass=(Class)in.readObject();
            try {
                method=declaringclass.getDeclaredMethod(method_name, parametertypes);
            }
            catch(NoSuchMethodException e) {
                throw new IOException(e.toString());
            }
            break;
        case TYPES:
            types=(Class[])in.readObject();
            break;
        case SIGNATURE:
            signature=(String[])in.readObject();
            break;
        case ID:
            break;
        default:
            if(log.isErrorEnabled()) log.error(ExternalStrings.MethodCall_MODE__0__IS_INVALID, mode);
            break;
        }

        boolean payload_available=in.readBoolean();
        if(payload_available) {
            payload=(Map)in.readObject();
        }
    }




}




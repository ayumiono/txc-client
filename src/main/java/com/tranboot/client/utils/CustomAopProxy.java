package com.tranboot.client.utils;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import org.springframework.aop.AopInvocationException;
import org.springframework.aop.RawTargetAccess;
import org.springframework.aop.framework.ReflectiveMethodInvocation;
import org.springframework.cglib.proxy.Callback;
import org.springframework.cglib.proxy.CallbackFilter;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * 
 * 基于spring的cglib生成的代理对象在做DataSourceTransactionManager代理对 象会出现bug,
 * 这里自己使用cglib生成DataSourceTransactionManager代理
 * @author xuelong.chen
 *
 */
public class CustomAopProxy {
	
//	private static final ThreadLocal<Object> currentProxy = new NamedThreadLocal<Object>("Current AOP proxy");
	
	public static Class<?> getTargetClass(Object object) {
		if(ClassUtils.isCglibProxy(object)) {
			return object.getClass().getSuperclass();
		}else {
			return object.getClass();
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T proxy(Class<T> clazz, Callback interceptor) {
		Enhancer enhancer = new Enhancer();
//		enhancer.setSuperclass(AopUtils.getTargetClass(target));
		enhancer.setSuperclass(clazz);//FIXME 暂时写死
//		enhancer.setSuperclass(AbstractPlatformTransactionManager.class);
		CallbackFilter filter = new ConcreteClassCallbackFilter();
		enhancer.setCallbackFilter(filter);
//		Callback[] callbacks = new Callback[2];
//		callbacks[0] = new StaticUnadvisedInterceptor(target);
//		callbacks[1] = interceptor;
		Callback[] callbacks = new Callback[] {interceptor};
		enhancer.setCallbackFilter(new ConcreteClassCallbackFilter());
		enhancer.setCallbacks(callbacks);
		return (T) enhancer.create();
	}

	/**
	 * 映射doCommit,doRollback,doBegin方法,其他的方法固定使用index=0的StaticUnadvisedInterceptor
	 * 
	 * @author xuelong.chen
	 */
	public static class ConcreteClassCallbackFilter implements CallbackFilter {
		public int accept(Method method) {
//			if ("doCommit".equals(method.getName()) || "doRollback".equals(method.getName())
//					|| "doBegin".equals(method.getName())) {
//				return 1;
//			}
			return 0;
		}
	}

	/**
	 * 
	 * 没有advice的方法使用此interceptor
	 * 
	 * @author xuelong.chen
	 *
	 */
	public static class StaticUnadvisedInterceptor implements MethodInterceptor, Serializable {

		private static final long serialVersionUID = 1770180708216539010L;
		private final Object target;

		public StaticUnadvisedInterceptor(Object target) {
			this.target = target;
		}

		@Override
		public Object intercept(Object proxy, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
			Object retVal = methodProxy.invoke(this.target, args);
			return processReturnType(proxy, this.target, method, retVal);
		}
	}

	private static Object processReturnType(Object proxy, Object target, Method method, Object retVal) {
		if (retVal != null && retVal == target && !RawTargetAccess.class.isAssignableFrom(method.getDeclaringClass())) {
			retVal = proxy;
		}
		Class<?> returnType = method.getReturnType();
		if (retVal == null && returnType != Void.TYPE && returnType.isPrimitive()) {
			throw new AopInvocationException(
					"Null return value from advice does not match primitive return type for: " + method);
		}
		return retVal;
	}

//	private static class DynamicAdvisedInterceptor implements MethodInterceptor, Serializable {
//
//		private final AdvisedSupport advised;
//
//		public DynamicAdvisedInterceptor(AdvisedSupport advised) {
//			this.advised = advised;
//		}
//
//		@Override
//		public Object intercept(Object proxy, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
//			Object oldProxy = null;
//			boolean setProxyContext = false;
//			Class<?> targetClass = null;
//			Object target = null;
//			try {
//				if (this.advised.isExposeProxy()) {
//					oldProxy = setCurrentProxy(proxy);
//					setProxyContext = true;
//				}
//				target = getTarget();
//				if (target != null) {
//					targetClass = target.getClass();
//				}
//				List<Object> chain = this.advised.getInterceptorsAndDynamicInterceptionAdvice(method, targetClass);
//				Object retVal;
//				if (chain.isEmpty() && Modifier.isPublic(method.getModifiers())) {
//					Object[] argsToUse = adaptArgumentsIfNecessary(method, args);
//					retVal = methodProxy.invoke(target, argsToUse);
//				} else {
//					retVal = new CglibMethodInvocation(proxy, target, method, args, targetClass, chain, methodProxy)
//							.proceed();
//				}
//				retVal = processReturnType(proxy, target, method, retVal);
//				return retVal;
//			} finally {
//				if (target != null) {
//					releaseTarget(target);
//				}
//				if (setProxyContext) {
//					setCurrentProxy(oldProxy);
//				}
//			}
//		}
//
//		@Override
//		public boolean equals(Object other) {
//			return (this == other || (other instanceof DynamicAdvisedInterceptor
//					&& this.advised.equals(((DynamicAdvisedInterceptor) other).advised)));
//		}
//
//		/**
//		 * CGLIB uses this to drive proxy creation.
//		 */
//		@Override
//		public int hashCode() {
//			return this.advised.hashCode();
//		}
//
//		protected Object getTarget() throws Exception {
//			return this.advised.getTargetSource().getTarget();
//		}
//
//		protected void releaseTarget(Object target) throws Exception {
//			this.advised.getTargetSource().releaseTarget(target);
//		}
//	}

	
	public static Object[] adaptArgumentsIfNecessary(Method method, Object... arguments) {
		if (method.isVarArgs() && !ObjectUtils.isEmpty(arguments)) {
			Class<?>[] paramTypes = method.getParameterTypes();
			if (paramTypes.length == arguments.length) {
				int varargIndex = paramTypes.length - 1;
				Class<?> varargType = paramTypes[varargIndex];
				if (varargType.isArray()) {
					Object varargArray = arguments[varargIndex];
					if (varargArray instanceof Object[] && !varargType.isInstance(varargArray)) {
						Object[] newArguments = new Object[arguments.length];
						System.arraycopy(arguments, 0, newArguments, 0, varargIndex);
						Class<?> targetElementType = varargType.getComponentType();
						int varargLength = Array.getLength(varargArray);
						Object newVarargArray = Array.newInstance(targetElementType, varargLength);
						System.arraycopy(varargArray, 0, newVarargArray, 0, varargLength);
						newArguments[varargIndex] = newVarargArray;
						return newArguments;
					}
				}
			}
		}
		return arguments;
	}
	
	private static class CglibMethodInvocation extends ReflectiveMethodInvocation {

		private final MethodProxy methodProxy;

		private final boolean publicMethod;

		public CglibMethodInvocation(Object proxy, Object target, Method method, Object[] arguments,
				Class<?> targetClass, List<Object> interceptorsAndDynamicMethodMatchers, MethodProxy methodProxy) {

			super(proxy, target, method, arguments, targetClass, interceptorsAndDynamicMethodMatchers);
			this.methodProxy = methodProxy;
			this.publicMethod = Modifier.isPublic(method.getModifiers());
		}

		/**
		 * Gives a marginal performance improvement versus using reflection to
		 * invoke the target when invoking public methods.
		 */
		@Override
		protected Object invokeJoinpoint() throws Throwable {
			if (this.publicMethod) {
				return this.methodProxy.invoke(this.target, this.arguments);
			}
			else {
				return super.invokeJoinpoint();
			}
		}
	}
	
//	private static Object setCurrentProxy(Object proxy) {
//		Object old = currentProxy.get();
//		if (proxy != null) {
//			currentProxy.set(proxy);
//		}
//		else {
//			currentProxy.remove();
//		}
//		return old;
//	}
}

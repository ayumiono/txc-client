package com.gb.dbsync.client;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

import org.springframework.aop.AopInvocationException;
import org.springframework.aop.RawTargetAccess;
import org.springframework.cglib.core.ClassGenerator;
import org.springframework.cglib.proxy.Callback;
import org.springframework.cglib.proxy.CallbackFilter;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.cglib.transform.impl.UndeclaredThrowableStrategy;
import org.springframework.util.ClassUtils;

public class CGLIBDemo {

	public static class Demo extends Parent {

		private String t = "finalMethod";

		public final void finalMethod() {
			System.out.println(parent);
		}

		public void normalMethod() {
			System.out.println(t);
		}
	}

	public static class Parent {
		protected String parent = "parent";
	}

	public static class StaticUnadvisedInterceptor implements MethodInterceptor, Serializable {

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
		// Massage return value if necessary
		if (retVal != null && retVal == target && !RawTargetAccess.class.isAssignableFrom(method.getDeclaringClass())) {
			// Special case: it returned "this". Note that we can't help
			// if the target sets a reference to itself in another returned object.
			retVal = proxy;
		}
		Class<?> returnType = method.getReturnType();
		if (retVal == null && returnType != Void.TYPE && returnType.isPrimitive()) {
			throw new AopInvocationException(
					"Null return value from advice does not match primitive return type for: " + method);
		}
		return retVal;
	}

	public static class ConcreteClassCallbackFilter implements CallbackFilter {
		public int accept(Method method) {
			if ("finalMethod".equals(method.getName())) {
				return 0;
			}
			return 0;
		}
	}

	static class ClassLoaderAwareUndeclaredThrowableStrategy extends UndeclaredThrowableStrategy {

		private final ClassLoader classLoader;

		public ClassLoaderAwareUndeclaredThrowableStrategy(ClassLoader classLoader) {
			super(UndeclaredThrowableException.class);
			this.classLoader = classLoader;
		}

		@Override
		public byte[] generate(ClassGenerator cg) throws Exception {
			if (this.classLoader == null) {
				return super.generate(cg);
			}

			Thread currentThread = Thread.currentThread();
			ClassLoader threadContextClassLoader;
			try {
				threadContextClassLoader = currentThread.getContextClassLoader();
			} catch (Throwable ex) {
				// Cannot access thread context ClassLoader - falling back...
				return super.generate(cg);
			}

			boolean overrideClassLoader = !this.classLoader.equals(threadContextClassLoader);
			if (overrideClassLoader) {
				currentThread.setContextClassLoader(this.classLoader);
			}
			try {
				return super.generate(cg);
			} finally {
				if (overrideClassLoader) {
					// Reset original thread context ClassLoader.
					currentThread.setContextClassLoader(threadContextClassLoader);
				}
			}
		}
	}

	public static void main(String[] args) {
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(Demo.class);
		CallbackFilter filter = new ConcreteClassCallbackFilter();
		enhancer.setCallbackFilter(filter);

		Callback[] callbacks = new Callback[] { new StaticUnadvisedInterceptor(new Demo()) };
		enhancer.setCallbacks(callbacks);
		enhancer.setStrategy(new ClassLoaderAwareUndeclaredThrowableStrategy(ClassUtils.getDefaultClassLoader()));

		Demo proxy = (Demo) enhancer.create();

		proxy.finalMethod();
		proxy.normalMethod();
	}
}

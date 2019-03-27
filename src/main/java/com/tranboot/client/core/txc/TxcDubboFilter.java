package com.tranboot.client.core.txc;

import static com.tranboot.client.utils.MetricsReporter.invokeTimer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.codahale.metrics.Timer;

/**
 * dubbo filter，添加此filter到dubbo中后，被调用服务方法中不再需要手动写TxcContext.bind/unbind方法
 * @author xuelong.chen
 *
 */
public class TxcDubboFilter implements Filter {
	
	private static final Logger logger = LoggerFactory.getLogger(TxcDubboFilter.class);
	
	@Override
	public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
		Timer.Context context = invokeTimer.time();
		boolean isProvider = false;
		try {
			String xid = RpcContext.getContext().getAttachment("xid");
			String txcStart = RpcContext.getContext().getAttachment("txc_start");
			String txcTimeout = RpcContext.getContext().getAttachment("txc_timeout");
			if(xid != null && TxcContext.getCurrentXid() == null) {//如果rpccontext中存在xid，则需要进行txc处理，如果不需要，则跳过
				//provider端逻辑
				isProvider = true;
				logger.debug("dubbo 服务调用存在txc上下文，开始自动绑定");
				TxcContext.bind(Long.parseLong(xid),Long.parseLong(txcStart),Integer.parseInt(txcTimeout));
			}
			if(xid == null && TxcContext.getCurrentXid() != null) {
				//consumer端逻辑
				RpcContext.getContext().setAttachment("xid", String.valueOf(TxcContext.getCurrentXid()));
				RpcContext.getContext().setAttachment("txc_start", String.valueOf(TxcContext.getTxcStart()));
				RpcContext.getContext().setAttachment("txc_timeout", String.valueOf(TxcContext.getTxcTimeout()));
			}
			return invoker.invoke(invocation);
		} catch (RpcException e) {
			throw e;
		} finally {
			if(isProvider) {
				TxcContext.unbind();
			}
			context.stop();
		}
	}
}

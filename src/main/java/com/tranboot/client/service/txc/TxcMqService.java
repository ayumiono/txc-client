package com.tranboot.client.service.txc;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.gb.soa.omp.transaction.model.MqTransactionModel;
import com.gb.soa.omp.transaction.util.TransactionApiUtil;

public class TxcMqService implements InitializingBean{
	
	protected Logger logger = LoggerFactory.getLogger(TxcMqService.class);

	private DefaultMQProducer producer;
	
	private String nameServerAddr;
	
	public void setNameServerAddr(String nameServerAddr) {
		this.nameServerAddr = nameServerAddr;
	}

	public void sendCommit(long xid, Date transactionCommitDate) throws Exception {
		logger.debug("开始发送commit消息 xid:{},name server address:{}",xid,this.nameServerAddr);
		MqTransactionModel model = new MqTransactionModel();
		model.setTransactionId(xid);
		model.setTransactionCommmitDate(transactionCommitDate);
		MessageQueueSelector mqs = new MessageQueueSelector() {
			public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
				int index = (int) (Math.abs(xid) % mqs.size());
				return mqs.get(index);
			}
		};
		Message message = new Message();
		message.setBody(JSON.toJSONBytes(model));
		message.setTags(TransactionApiUtil.mqUpdateSqlTag);
		message.setTopic(TransactionApiUtil.mqUpdateSqlTopic);
		message.setKeys(String.valueOf(xid));
		message.setDelayTimeLevel(2);//delay 5 seconds
		producer.send(message, mqs, null);
		logger.debug("发送commit消息完成{}",message.toString());
	}
	
	public void sendRollback(long xid, Date transactionCommitDate) throws Exception {
		logger.debug("开始发送rollback消息 xid:{}, name server address:{}",xid,this.nameServerAddr);
		MqTransactionModel model = new MqTransactionModel();
		model.setTransactionId(xid);
		model.setTransactionCommmitDate(transactionCommitDate);
		MessageQueueSelector mqs = new MessageQueueSelector() {
			public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
				int index = (int) (Math.abs(xid) % mqs.size());
				return mqs.get(index);
			}
		};
		Message message = new Message();
		message.setBody(JSON.toJSONBytes(model));
		message.setTags(TransactionApiUtil.mqTag);
		message.setTopic(TransactionApiUtil.mqTopic);
		message.setKeys(String.valueOf(xid));
		message.setDelayTimeLevel(2);//delay 5 seconds
		producer.send(message, mqs, null);
		logger.debug("开始发送rollback消息完成{}",message.toString());
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		producer = new DefaultMQProducer("productor_group");
		producer.setNamesrvAddr(nameServerAddr);
		producer.setInstanceName(UUID.randomUUID().toString());
		producer.setSendMsgTimeout(20000);
		producer.setDefaultTopicQueueNums(30);
		try {
			producer.start();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("DbsyncMqService Producer start error");
		}
	}
}

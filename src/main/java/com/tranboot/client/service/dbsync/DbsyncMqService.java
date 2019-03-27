package com.tranboot.client.service.dbsync;

import java.util.List;
import java.util.UUID;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.gb.soa.omp.dbsync.model.MqSyncModel;
import com.gb.soa.omp.dbsync.model.RdisSyncModel;
import com.gb.soa.omp.dbsync.util.DbsyncUtil;

public class DbsyncMqService {

	private DefaultMQProducer producer;

	public DbsyncMqService(String nameServerAddr) {
		producer = new DefaultMQProducer("productor_group");
		producer.setNamesrvAddr(nameServerAddr);//"192.168.26.163:9876"
		producer.setInstanceName(UUID.randomUUID().toString());
		producer.setSendMsgTimeout(20000);
		producer.setDefaultTopicQueueNums(30);
		try {
			producer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
			throw new RuntimeException("DbsyncMqService Producer start error");
		}
	}

	public void send(MqSyncModel model, RdisSyncModel _model)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		MessageQueueSelector mqs = new MessageQueueSelector() {
			public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
				Integer id = _model.getMessageKey().hashCode();
				int index = Math.abs(id) % mqs.size();
				return mqs.get(index);
			}
		};
		Message message = new Message();
		message.setBody(JSON.toJSONBytes(model));
		message.setTags(DbsyncUtil.mqTag);
		message.setTopic(DbsyncUtil.mqTopic);
		message.setKeys(_model.getMessageKey());
		producer.send(message, mqs, null);
	}
	
	public static String messageKey(String bizKey,String sourceDb,String sourceTable,String targetDb) {
		return UUID.randomUUID().toString()+"_"+bizKey + "_" + sourceDb + "_" + sourceTable + "_" + targetDb;
	}
}

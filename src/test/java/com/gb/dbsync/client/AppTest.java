package com.gb.dbsync.client;

import java.sql.SQLException;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.tranboot.client.core.txc.TxcTransaction;
import com.tranboot.client.model.txc.TxcRollbackMode;

/**
 * Hello world!
 *
 */
public class AppTest {
	@Autowired
	public JdbcTemplate template;
	@Autowired
	public DataSourceTransactionManager transactionManager;
	
	private String test = "ttt";
	
	/**
	 * 测试用例1：
	 * 开启事务
	 */
	@Test
	public void case0WithoutTransaction() {
		template.update("insert into sd_bl_tml_jiajie_test(series,tml_num_id,oper_desc,shard_id,create_user_id,last_update_user_id,create_dtme,last_updtme) values(?,?,'inser a new one',?,?,?,now(),now())"
				, new Object[] {0L,90171019181799L,321,2,2});
	}
	
	@Test
	public void case1Transaction() {
		TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());
		for(int i=0;i<100;i++) {
			template.update("update sd_bl_tml_jiajie_test set oper_desc=?,last_updtme=now() where series=? and shard_id=?"
					, new Object[] {"case1WithoutTransaction"+i,1L,321});
		}
		transactionManager.commit(status);
	}
	
	@TxcTransaction(rollbackMode=TxcRollbackMode.SERIAL)
	public void case11Transaction() {
		for(int i=0;i<20;i++) {
			template.update("update sd_bl_tml_jiajie_test set oper_desc=?,last_updtme=now() where series=? and shard_id=?"
					, new Object[] {"case1WithoutTransaction"+i,1L,321});
		}
	}
	
	@Test
	public void case2MultipleThread() {
		template.update("insert into sd_bl_tml_jiajie_test(series,tml_num_id,oper_desc,shard_id,create_user_id,last_update_user_id,create_dtme,last_updtme) values(?,?,'multiple thread insert',?,?,?,now(),now())"
				, new Object[] {3L,90171019181799L,321,2,2});
		for(int i=0;i<100;i++) {
			final int index = i;
			new Thread(new Runnable() {
				@Override
				public void run() {
					TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());
					template.update("update sd_bl_tml_jiajie_test set oper_desc=?,last_updtme=now() where series=? and shard_id=?"
							, new Object[] {"case1WithoutTransaction"+index,3L,321});
					transactionManager.commit(status);
				}
			}).start();
		}
	}
	
	@Test
	public void case3MultipleThread() {
		template.update("insert into sd_bl_tml_jiajie_test(series,tml_num_id,oper_desc,shard_id,create_user_id,last_update_user_id,create_dtme,last_updtme) values(?,?,'multiple thread insert',?,?,?,now(),now())"
				, new Object[] {25L,90171019181799L,321,2,2});
		template.update("insert into sd_bl_tml_jiajie_test(series,tml_num_id,oper_desc,shard_id,create_user_id,last_update_user_id,create_dtme,last_updtme) values(?,?,'multiple thread insert',?,?,?,now(),now())"
				, new Object[] {26L,90171019181799L,321,2,2});
		//change thread
		for(int i=0;i<100;i++) {
			final int index = i;
			new Thread(new Runnable() {
				@Override
				public void run() {
					TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());
					template.update("update sd_bl_tml_jiajie_test set oper_desc=?,last_updtme=now() where series=? and shard_id=?"
							, new Object[] {"case3 change thread"+index,25L,321});
					transactionManager.commit(status);
				}
			}).start();
		}
		//query thread
		for(int i=0;i<100;i++) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());
					Map<String, Object> result = template.queryForMap("select oper_desc from sd_bl_tml_jiajie_test where series=7");
					template.update("update sd_bl_tml_jiajie_test set oper_desc=?,last_updtme=now() where series=? and shard_id=?"
							, new Object[] {result.get("oper_desc"),26L,321});
					transactionManager.commit(status);
				}
			}).start();
		}
	}
	
	@TxcTransaction(rollbackMode=TxcRollbackMode.SERIAL)
	public void txcTestCase() {
		Object[] args = new Object[] {"txctestcase7",26L,321};
		long start = System.currentTimeMillis();
		System.out.println();
		String updateSql = "update sd_bl_tml_jiajie_test set oper_desc=?,last_updtme=now() where series=? and shard_id=?";
		template.update(updateSql, args);
		long end = System.currentTimeMillis();
		System.out.println("-----------------------------------耗时:"+(end-start));
//		
//		args = new Object[] {"txctestcase8",26L,321};
//		start = System.currentTimeMillis();
//		template.update(updateSql, args);
//		end = System.currentTimeMillis();
//		System.out.println("-----------------------------------耗时:"+(end-start));
//		
//		args = new Object[] {"txctestcase9",26L,321};
//		start = System.currentTimeMillis();
//		template.update(updateSql, args);
//		end = System.currentTimeMillis();
//		System.out.println("-----------------------------------耗时:"+(end-start));
//		
//		args = new Object[] {"txctestcase10",26L,321};
//		start = System.currentTimeMillis();
//		template.update(updateSql, args);
//		end = System.currentTimeMillis();
//		System.out.println("-----------------------------------耗时:"+(end-start));
	}
	
	@TxcTransaction(rollbackMode=TxcRollbackMode.SERIAL)
	public void txcTestCase2() {
		Object[] args = new Object[] {82220L,90171019181799L,321,2,2};
		template.update("insert into sd_bl_tml_jiajie_test(series,tml_num_id,oper_desc,shard_id,create_user_id,last_update_user_id,create_dtme,last_updtme) values(?,?,'multiple thread insert',?,?,?,now(),now())"
				, args);
	}
	
	@TxcTransaction(rollbackMode=TxcRollbackMode.SERIAL)
	public void txcTestCase3() {
		Object[] args = new Object[] {87220L};
		template.update("delete from sd_bl_tml_jiajie_test where series=?"
				, args);
	}
	
	public final void test() {
		System.out.println(test);
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, NoSuchMethodException, SecurityException {
		ApplicationContext context = new ClassPathXmlApplicationContext("application.xml");
		AppTest app = context.getBean(AppTest.class);
		app.txcTestCase();
	}
}

package com.tranboot.client.model.txc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * query->args->exportsql query 查出回滚行原记录（不必须） args 拼出args数组，从query，原args数组组合而来
 * visitor导出带参数的sql
 * Thread Safe
 * @author xuelong.chen
 */
public class SQLParamExtractorPipeline {
	
	private static final Logger logger = LoggerFactory.getLogger(SQLParamExtractorPipeline.class);
	
	/**
	 * 外部参数
	 */
	public static final String OUTTER_ARGS = "outter_args";//数组
	/**
	 * fetch出来的原数据
	 */
	public static final String QUERY_ROW = "query_row";//map
	/**
	 * 最终执行回滚语句的参数
	 */
	public static final String FINAL_ARGS = "final_args";//list
	/**
	 * 查询语句的参数
	 */
	public static final String WHERE_ARGS = "where_args";//list

	ParamExtractor start0;
	
	private static final ThreadLocal<Map<String, Object>> context = new ThreadLocal<>();//FIXME 不够好，想一种替代方案
	
	public SQLParamExtractorPipeline() {
	}
	
	public Object getParam(String key) {
		return context.get().get(key);
	}
	
	public void setParam(String key, Object value) {
		context.get().put(key, value);
	}
	
	@SuppressWarnings("unchecked")
	public List<Object> getWhereArgs() {
		if(getParam(WHERE_ARGS) == null) {
			setParam(WHERE_ARGS, new ArrayList<>());
		}
		return (List<Object>) getParam(WHERE_ARGS);
	}
	
	public void addExtractor(ParamExtractor extractor) {
		if(start0 == null) {
			start0 = extractor;
			return;
		}
		start0.next(extractor);
	}
	
	public void addHeadExtractor (ParamExtractor extractor) {
		if(start0 == null) {
			start0 = extractor;
		}else {
			ParamExtractor tmp = start0;
			start0 = extractor;
			start0.next = tmp;
		}
	}

	/**
	 * 提取原参数
	 * @param outter_args
	 */
	public void start(Object[] outter_args) {
		context.remove();
		context.set(new HashMap<>());
		setParam(OUTTER_ARGS,outter_args);
		ParamExtractor tmp = start0;
		while(tmp != null) {
			if(tmp instanceof OriginalParamExtractor) {
				logger.debug(tmp.desc());
				tmp.extract();
			}
			tmp = tmp.next();
		}
	}
	
	/**
	 * 提取原数据列
	 */
	public void render() {
		ParamExtractor tmp = start0;
		while(tmp != null) {
			if(tmp instanceof UpdateSetItemExtractor) {
				logger.debug(tmp.desc());
				tmp.extract();
			}
			tmp = tmp.next();
		}
	}
	
	@SuppressWarnings("unchecked")
	public List<Object> getFinalArgs(){
		return (List<Object>) getParam(FINAL_ARGS);
	}
	
	@Override
	public String toString() {
		StringBuilder sbuilder = new StringBuilder();
		ParamExtractor tmp = start0;
		while(tmp != null) {
			sbuilder.append(tmp.toString()+System.lineSeparator());
			tmp = tmp.next;
		}
		String str = StringUtils.substringBeforeLast(sbuilder.toString(), System.lineSeparator());
		return str;
	}
	
	/**
	 * 参数来自于源参数数组
	 * @author xuelong.chen
	 *
	 */
	public static abstract class OriginalParamExtractor extends ParamExtractor{
		public OriginalParamExtractor(SQLParamExtractorPipeline context) {
			super(context);
		}
	}
	
	/**
	 * 参数来自于select语句
	 * @author xuelong.chen
	 *
	 */
	public static abstract class UpdateSetItemExtractor extends ParamExtractor {
		public UpdateSetItemExtractor(SQLParamExtractorPipeline context) {
			super(context);
		}
	}
	
	
	/**
	 * 主键提取器
	 * @author xuelong.chen
	 *
	 */
	public static abstract class PrimaryKeyExtractor {
		
		protected SQLParamExtractorPipeline context;
		
		public PrimaryKeyExtractor(SQLParamExtractorPipeline context) {
			this.context = context;
		}
		
		@SuppressWarnings("unchecked")
		protected Object extractFromRecord(String primaryKey) {
			if(primaryKey == null) return null;
			if(context.getParam(QUERY_ROW) == null) {
				return null;
			}else {
				return ((Map<String, Object>)context.getParam(QUERY_ROW)).get(primaryKey);
			}
		}
		
		protected Object extractFromArgs(int index) {
			Object[] args = (Object[]) context.getParam(OUTTER_ARGS);
			return args[index];
		}
		
		public List<KeyValuePair> render() {
			logger.debug(desc());
			return extract();
		}
		
		/**
		 * 返回主键字段及主键字段值
		 * @return
		 */
		public abstract List<KeyValuePair> extract();
		
		public abstract String shard();
		
		public abstract String desc();
		
		@Override
		public String toString() {
			return desc();
		}
	}
	
	protected static class KeyValuePair {
		String column;
		Object value;
		
		public KeyValuePair(String column,Object value) {
			this.column = column;
			this.value = value;
		}

		public String getColumn() {
			return column;
		}

		public void setColumn(String column) {
			this.column = column;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}
	}

	public static abstract class ParamExtractor {
		
		public ParamExtractor(SQLParamExtractorPipeline context) {
			this.context = context;
		}
		protected SQLParamExtractorPipeline context;
		protected ParamExtractor next;
		
		public void next(ParamExtractor extractor) {
			if(next == null) {
				next = extractor;
			}else {
				next.next(extractor);
			}
		};
		
		public Object[] getArgs() {
			return (Object[]) context.getParam(OUTTER_ARGS);
		}
		
		public void setQuery(Map<String, Object> row) {
			context.setParam(QUERY_ROW, row);
		}
		
		public boolean alreadyQuery() {
			return context.getParam(QUERY_ROW) != null;
		}
		
		@SuppressWarnings("unchecked")
		public Object getQueryColumn(String column) {
			if(column == null) return null;
			if(context.getParam(QUERY_ROW) == null) {
				return null;
			}else {
				return ((Map<String, Object>)context.getParam(QUERY_ROW)).get(column);
			}
		}
		
		/**
		 * 因为query和回滚sql共用同一个finalArg参数列表、同一个where条件对像。
		 * 所以在进行query时，finalArg里需要用到query结果集的列的index都将由null占位,
		 * 当query结束后需要重新render一遍把finalArg列表填充完整 
		 * @param index
		 * @param arg
		 */
		public void addFinalArg(int index, Object arg) {
			if(context.getParam(FINAL_ARGS) == null) {
				context.setParam(FINAL_ARGS, new ArrayList<>(20));
			}
			@SuppressWarnings("unchecked")
			List<Object> finalArgs = (List<Object>) context.getParam(FINAL_ARGS);
			if(finalArgs.size()-1>=index) {
				finalArgs.set(index, arg);
			}else {
				//null填充
				int size = finalArgs.size();
				while(size<index) {
					finalArgs.add(size, null);
					size++;
				}
				finalArgs.add(arg);
			}
		}
		
		public ParamExtractor next() {
			return next;
		};

		public abstract void extract();
		public abstract String desc();
		
		@Override
		public String toString() {
			return desc();
		}
	}
}
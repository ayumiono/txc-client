package com.tranboot.client.service.dbsync;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tranboot.client.druid.sql.SQLUtils;
import com.tranboot.client.druid.sql.ast.SQLStatement;
import com.tranboot.client.druid.sql.ast.expr.SQLIdentifierExpr;
import com.tranboot.client.druid.sql.ast.statement.SQLExprTableSource;
import com.tranboot.client.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.tranboot.client.druid.sql.dialect.oracle.visitor.OracleASTVisitorAdapter;
import com.tranboot.client.druid.util.JdbcConstants;
import com.tranboot.client.model.dbsync.SqlMapper;
import com.tranboot.client.model.dbsync.SqlParser;
import com.tranboot.client.model.dbsync.SqlParserProcessor;
import com.tranboot.client.model.dbsync.SqlRouter;
import com.tranboot.client.model.dbsync.SqlTransformResult;
import com.tranboot.client.utils.LRUCache;

public class SqlParserService {

	private static final Logger logger = LoggerFactory.getLogger(SqlParserService.class);

	private Map<String, SqlParser> parsers;
	
	private String dbType;
	
	public SqlParserService() {
		try {
			parseXml();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
	
	/**
	 * @param sql
	 * @return
	 */
	public SqlTransformResult parseSql(String sql) {
		try {
			if(LRUCache.getDbsyncSqlTransformResult(sql) == null) {
				List<SQLStatement> stmtList = SQLUtils.parseStatements(cleanSql(sql), dbType);
				SQLStatement statement = stmtList.get(0);
				String tableName = extractTableName(statement, dbType);
				logger.debug("解析出sql表名为"+tableName);
				SqlParser parser = parsers.get(tableName);
				if(parser == null) {
					logger.debug("没有找到表"+tableName+"对应的setting配置");
					LRUCache.cacheDbsyncSqlTransformResult(sql, SqlTransformResult.a);
				}else {
					SqlParserProcessor parserResult = new SqlParserProcessor(statement,parser);
					SqlTransformResult transformResult = parserResult.parse();
					LRUCache.cacheDbsyncSqlTransformResult(sql, transformResult);
				}
			}
			return LRUCache.getDbsyncSqlTransformResult(sql);
		} catch (Exception e) {
			logger.error("数据库双-生成SqlTransformResult失败"+sql, e);
			e.printStackTrace();
			return null;
		}
	}

	public void parseXml() throws Exception {
		// 解析配setting.xml文件
		parsers = new HashMap<>();
		SAXReader reader = new SAXReader();
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("setting.xml");
		if(is == null) throw new Exception("classpath下没有找到数据库双写配置文件setting.xml");
		Document doc = reader.read(is);
		Element root = doc.getRootElement();
		String dbType = root.attributeValue("dbtype");
		this.dbType = dbType;
		@SuppressWarnings("unchecked")
		Iterator<Element> iterator = root.elementIterator();
		while(iterator.hasNext()) {
			Element parser = iterator.next();
			parserXml(parser);
		}
		logger.info(parsers.toString());
	}
	
	private void parserXml(Element parser) {
		String parser_id = parser.attributeValue("id").toLowerCase();
		SqlParser sqlParser = new SqlParser(parser_id);
		//mapper
		Element mapper = parser.element("mapper");
		Element mapper_table = mapper.element("table");
		Element mapper_table_source = mapper_table.element("source");
		Element mapper_table_target = mapper_table.element("target");
		String sourceTable = mapper_table_source.attributeValue("name").toLowerCase();
		String sourceDbType = mapper_table_source.attributeValue("dbtype").toLowerCase();
		String sourceDb = mapper_table_source.attributeValue("dbname").toLowerCase();
		String sourcePrimaryKey = mapper_table_source.attributeValue("primary").toLowerCase();
		String targetTable = mapper_table_target.attributeValue("name").toLowerCase();
		String targetDbType = mapper_table_target.attributeValue("dbtype").toLowerCase();
		String targetDb = mapper_table_target.attributeValue("dbname").toLowerCase();
		String targetPrimaryKey = mapper_table_target.attributeValue("primary").toLowerCase();
		Element fields = mapper.element("fields");
		Map<String, String> fieldMapper = new HashMap<>();
		if(fields != null) {
			@SuppressWarnings("unchecked")
			Iterator<Element> iterator = fields.elementIterator();
			while(iterator.hasNext()) {
				Element field = iterator.next();
				String sourceField = field.attributeValue("sourceField").toLowerCase();
				String targetField = field.attributeValue("targetField").toLowerCase();
				fieldMapper.put(sourceField, targetField);
			}
		}
		
		Element exclude = mapper.element("exclude");
		Set<String> excludeFields = new HashSet<>();
		if(exclude != null) {
			@SuppressWarnings("unchecked")
			Iterator<Element> iterator = exclude.elementIterator();
			while(iterator.hasNext()) {
				Element excludeField = iterator.next();
				String sourceField = excludeField.attributeValue("sourceField").toLowerCase();
				excludeFields.add(sourceField);
			}
		}
		
		SqlMapper sqlMapper = new SqlMapper();
		sqlMapper.setSourceDb(sourceDb);
		sqlMapper.setSourceDbType(sourceDbType);
		sqlMapper.setSourceTable(sourceTable);
		sqlMapper.setTargetDb(targetDb);
		sqlMapper.setTargetDbType(targetDbType);
		sqlMapper.setTargetTable(targetTable);
		sqlMapper.setSourceKeyField(sourcePrimaryKey);
		sqlMapper.setTargetKeyField(targetPrimaryKey);
		sqlMapper.setFieldMapper(fieldMapper);
		sqlMapper.setExcludeFields(excludeFields);
		Element router = parser.element("route");
		if(router != null) {
			Attribute partionKey = router.attribute("sourcePartitionKey");
			Attribute _partionKey = router.attribute("targetPartitionKey");
			SqlRouter sqlRouter = new SqlRouter(partionKey.getStringValue().toLowerCase(), _partionKey.getStringValue().toLowerCase());
			sqlParser.setRouter(sqlRouter);
		}
		sqlParser.setMapper(sqlMapper);
		parsers.put(parser_id, sqlParser);
	}
	
	private String extractTableName(SQLStatement statement,String dbType) {
		final Map<String, Object> tableName = new HashMap<>();
		if(JdbcConstants.MYSQL.equals(dbType)) {
			statement.accept(new MySqlASTVisitorAdapter() {
				@Override
				public boolean visit(SQLExprTableSource x) {
					tableName.put("result", ((SQLIdentifierExpr) x.getExpr()).getName());
					return true;
				}
			});
		}else if(JdbcConstants.ORACLE.equals(dbType)) {
			statement.accept(new OracleASTVisitorAdapter() {
				@Override
				public boolean visit(SQLExprTableSource x) {
					tableName.put("result", ((SQLIdentifierExpr) x.getExpr()).getName());
					return true;
				}
			});
		}
		
		return ((String)tableName.get("result")).toLowerCase();
	}
	
	private String cleanSql(String sql) {
		//清洗掉mycat中的注解   /*!mycat:db_type=master*/update .....
		if(sql.indexOf("mycat")>-1) {
			return StringUtils.substringAfterLast(sql, "*/");
		}
		return sql;
	}
}

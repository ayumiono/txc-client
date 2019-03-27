package com.tranboot.client.sqlast;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.tranboot.client.druid.sql.ast.SQLExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLBinaryOperator;
import com.tranboot.client.druid.sql.ast.expr.SQLCharExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLIdentifierExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLInListExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLLiteralExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLPropertyExpr;
import com.tranboot.client.druid.sql.ast.expr.SQLVariantRefExpr;
import com.tranboot.client.druid.sql.ast.statement.SQLDeleteStatement;
import com.tranboot.client.druid.sql.ast.statement.SQLExprTableSource;
import com.tranboot.client.druid.sql.ast.statement.SQLInsertStatement;
import com.tranboot.client.druid.sql.ast.statement.SQLUpdateSetItem;
import com.tranboot.client.druid.sql.ast.statement.SQLUpdateStatement;
import com.tranboot.client.druid.sql.dialect.mysql.visitor.MySqlExportParameterVisitor;

/**
 * 
 * 根据SqlParserResult重写源sql
 * 需要覆盖的场景：
 * 1.表变更	ok
 * 2.字段变更	ok
 * 3.字段减少	ok
 * 4.主键变更
 * 5.数据库类型变更	ok
 * @author xuelong.chen
 *
 */
public class MySQLRewriteVistorAop extends MySqlExportParameterVisitor {
	
	private SQLASTVisitorAspect aspect;
	
	private int point = 0;//0代表开始处理表部分，1代表开始处理where
	
	public MySQLRewriteVistorAop(List<Object> args,StringBuilder sbuilder,SQLASTVisitorAspect aspect) {
		super(args, sbuilder, true);//如果最后一个参数设置成false,会导致原sql带参数的变来占位符，且index=-1
		setParameterized(false);//需要调用这一步，否则在输出到appender时会导致原sql带参数的变为占位符,且会报错
		setPrettyFormat(false);
		this.aspect = aspect;
	}

	public MySQLRewriteVistorAop(SQLASTVisitorAspect aspect) {
		super(new ArrayList<>(), null, true);
		this.aspect = aspect;
	}
	
	/* 
	 * 存在表达式的情况有:
	 * update table set column1 = 【column1 + 1】;
	 * select 【column1+1】 from table
	 * where 【column = 1】;
	 * 
	 */
	@Override
	public boolean visit(SQLBinaryOpExpr x) {
		if(x.getParent() instanceof SQLUpdateStatement || x.getParent() instanceof SQLDeleteStatement) {
			aspect.whereEnterPoint(x);
			point = 1;
			boolean r = super.visit(x);
			point = 0;
			aspect.whereExitPoint(x);
			return r;
		}else {
			return super.visit(x);
		}
	}

	@Override
	public boolean visit(SQLIdentifierExpr x) {
		String targetColumn = x.getName();
		if(x.getParent() instanceof SQLInsertStatement) {
			SQLInsertStatement parent = (SQLInsertStatement) x.getParent();
			targetColumn  = aspect.insertColumnAspect(x.getName(),parent);
		}else if(x.getParent() instanceof SQLBinaryOpExpr && point == 1) {//开始处理where 部分中的操作原子
			SQLExpr right = ((SQLBinaryOpExpr)x.getParent()).getRight();//txc中不关心x是不是条件列，只要检测到有binaryop原子就可以进行index 重排
			if(right instanceof SQLVariantRefExpr) {
				aspect.whereBinaryOpAspect((SQLVariantRefExpr) right);
			}
			//dbsync中只会处理主键，分片字段，目前只可能存在简单的column = ? 
			//txc中只处理针对主键 primaryKey=?/constant 暂不支持column=函数或其他操作
			if(((SQLBinaryOpExpr)x.getParent()).getOperator() == SQLBinaryOperator.Equality) {//只处理操作数为=号的，因为不可能会有column = (column =xxx)这种类型,所以处理操作数为=号的即可断定左操作数就是一个目标条件列
				if(right instanceof SQLVariantRefExpr) {
					targetColumn = aspect.whereColumnAspect(x.getName(),(SQLVariantRefExpr) right);
				}else if(right instanceof SQLLiteralExpr) {
					targetColumn = aspect.whereColumnAspect(x.getName(), (SQLLiteralExpr)right);
				}
			}
			
		}else if(x.getParent() instanceof SQLInListExpr){
			targetColumn = aspect.whereColumnAspect(x.getName(), (SQLInListExpr) x.getParent());
		}else {
			return super.visit(x);
		}
		x.setName(targetColumn);
		return super.visit(x);
	}

	@Override
	public boolean visit(SQLPropertyExpr x) {
		String targetColumn = x.getName();
		if(x.getParent() instanceof SQLBinaryOpExpr && point == 1) {//开始处理where 部分中的操作原子
			SQLExpr right = ((SQLBinaryOpExpr)x.getParent()).getRight();//txc中不关心x是不是条件列，只要检测到有binaryop原子就可以进行index 重排
			if(right instanceof SQLVariantRefExpr) {
				aspect.whereBinaryOpAspect((SQLVariantRefExpr) right);
			}
			//dbsync中只会处理主键，分片字段，目前只可能存在简单的column = ?
			if(((SQLBinaryOpExpr)x.getParent()).getOperator() == SQLBinaryOperator.Equality) {//只处理操作数为=号的，因为不可能会有column = (column =xxx)这种类型,所以处理操作数为=号的即可断定左操作数就是一个目标条件列
				if(right instanceof SQLVariantRefExpr) {
					targetColumn = aspect.whereColumnAspect(x.getName(),(SQLVariantRefExpr) right);
				}else if(right instanceof SQLLiteralExpr) {
					targetColumn = aspect.whereColumnAspect(x.getName(), (SQLLiteralExpr)right);
				}
			}
			
		}else if(x.getParent() instanceof SQLInListExpr){
			targetColumn = aspect.whereColumnAspect(x.getName(), (SQLInListExpr) x.getParent());
		}else {
			return super.visit(x);
		}
		x.setName(targetColumn);
		return super.visit(x);
	} 
	
	@Override
	public void print(Date date) {
        if (this.appender == null) {
            return;
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        print0("'" + dateFormat.format(date) + "'");
    }
	
	//重写此方法，原方法无视所有的字符串类型的参数变为?
	@Override
	public boolean visit(SQLCharExpr x) {
		print('\'');

        String text = x.getText();
        
        StringBuilder buf = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); ++i) {
            char ch = text.charAt(i);
            if (ch == '\'') {
                buf.append('\'');
                buf.append('\'');
            } else if (ch == '\\') {
                buf.append('\\');
                buf.append('\\');
            } else if (ch == '\0') {
                buf.append('\\');
                buf.append('0');
            } else {
                buf.append(ch);
            }
        }
        if (buf.length() != text.length()) {
            text = buf.toString();
        }

        print0(text);

        print('\'');
        return false;
	}
	
	@Override
	public boolean visit(SQLExprTableSource x) {
		x = aspect.tableAspect(x);
		if(x.getParent() instanceof SQLInsertStatement) {
			aspect.insertEnterPoint((SQLInsertStatement) x.getParent());
		}else if(x.getParent() instanceof SQLUpdateStatement) {
			aspect.updateEnterPoint((SQLUpdateStatement) x.getParent());
		}else if(x.getParent() instanceof SQLDeleteStatement) {
			aspect.deleteEnterPoint((SQLDeleteStatement) x.getParent());
		}
		return super.visit(x);
    }
	
	/* 
	 * 重写visit(SQLUpdateSetItem x)方法，否则无法得知SQLPropertyExpr和SQLIdentyExpr是属于update item 左项
	 */
	@Override
	public boolean visit(SQLUpdateSetItem x) {
		x = aspect.updateItemAspect(x);
		if(x == null) return false;
		SQLExpr left = x.getColumn();
		if(left instanceof SQLPropertyExpr) {
			String targetName = aspect.updateColumnAspect(((SQLPropertyExpr) left).getName());
			((SQLPropertyExpr) left).setName(targetName);
		}else if(left instanceof SQLIdentifierExpr) {
			String targetName = aspect.updateColumnAspect(((SQLIdentifierExpr) left).getName());
			((SQLIdentifierExpr) left).setName(targetName);
		}
		left.accept(this);
        print0(" = ");
        x.getValue().accept(this);
        return false;
	}
	
	@Override
	public boolean visit(SQLVariantRefExpr x) {
		aspect.variantRefAspect(x);
		return super.visit(x);
	}
}

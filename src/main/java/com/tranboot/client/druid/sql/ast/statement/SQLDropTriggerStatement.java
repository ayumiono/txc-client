package com.tranboot.client.druid.sql.ast.statement;

import com.tranboot.client.druid.sql.ast.SQLName;
import com.tranboot.client.druid.sql.ast.SQLStatementImpl;
import com.tranboot.client.druid.sql.visitor.SQLASTVisitor;

public class SQLDropTriggerStatement extends SQLStatementImpl implements SQLDDLStatement {

    private SQLName name;
    
    public SQLDropTriggerStatement() {
        
    }
    
    public SQLDropTriggerStatement(String dbType) {
        super (dbType);
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        this.name = name;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, name);
        }
        visitor.endVisit(this);
    }

}

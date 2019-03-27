/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tranboot.client.druid.sql.dialect.mysql.visitor;

import com.tranboot.client.druid.sql.dialect.mysql.ast.MySqlForceIndexHint;
import com.tranboot.client.druid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint;
import com.tranboot.client.druid.sql.dialect.mysql.ast.MySqlKey;
import com.tranboot.client.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.tranboot.client.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.tranboot.client.druid.sql.dialect.mysql.ast.MySqlUseIndexHint;
import com.tranboot.client.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlCursorDeclareStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlDeclareConditionStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlDeclareHandlerStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlDeclareStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlIterateStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlLeaveStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlRepeatStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlSelectIntoStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlWhileStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement.MySqlWhenStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.tranboot.client.druid.sql.dialect.mysql.ast.expr.MySqlExtractExpr;
import com.tranboot.client.druid.sql.dialect.mysql.ast.expr.MySqlIntervalExpr;
import com.tranboot.client.druid.sql.dialect.mysql.ast.expr.MySqlMatchAgainstExpr;
import com.tranboot.client.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.tranboot.client.druid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr;
import com.tranboot.client.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.CobarShowStatus;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableAlterColumn;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableCharacter;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableDiscardTablespace;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableImportTablespace;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlAlterUserStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlAnalyzeStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlBinlogStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlCommitStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlDescribeStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlExecuteStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlHelpStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlLoadXmlStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlOptimizeStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlResetStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlRollbackStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlSetCharSetStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlSetNamesStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlSetPasswordStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowAuthorsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowBinLogEventsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowBinaryLogsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowContributorsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateEventStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateProcedureStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTableStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTriggerStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateViewStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowEngineStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowErrorsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowEventsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionCodeStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionStatusStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowGrantsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowIndexesStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowKeysStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterLogsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterStatusStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowOpenTablesStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowPluginsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowPrivilegesStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureCodeStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureStatusStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowProfileStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowProfilesStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowRelayLogEventsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveHostsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveStatusStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowTriggersStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlSubPartitionByKey;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlSubPartitionByList;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlUnlockTablesStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlUpdateTableSource;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MysqlDeallocatePrepareStatement;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement.TableSpaceOption;
import com.tranboot.client.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement.UserSpecification;
import com.tranboot.client.druid.sql.visitor.SQLASTVisitorAdapter;

public class MySqlASTVisitorAdapter extends SQLASTVisitorAdapter implements MySqlASTVisitor {

    @Override
    public boolean visit(MySqlTableIndex x) {
        return true;
    }

    @Override
    public void endVisit(MySqlTableIndex x) {

    }

    @Override
    public boolean visit(MySqlKey x) {
        return true;
    }

    @Override
    public void endVisit(MySqlKey x) {

    }

    @Override
    public boolean visit(MySqlPrimaryKey x) {

        return true;
    }

    @Override
    public void endVisit(MySqlPrimaryKey x) {

    }

    @Override
    public void endVisit(MySqlIntervalExpr x) {

    }

    @Override
    public boolean visit(MySqlIntervalExpr x) {

        return true;
    }

    @Override
    public void endVisit(MySqlExtractExpr x) {

    }

    @Override
    public boolean visit(MySqlExtractExpr x) {

        return true;
    }

    @Override
    public void endVisit(MySqlMatchAgainstExpr x) {

    }

    @Override
    public boolean visit(MySqlMatchAgainstExpr x) {

        return true;
    }

    @Override
    public void endVisit(MySqlPrepareStatement x) {

    }

    @Override
    public boolean visit(MySqlPrepareStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlExecuteStatement x) {

    }

    @Override
    public boolean visit(MySqlExecuteStatement x) {

        return true;
    }
    
    @Override
    public void endVisit(MysqlDeallocatePrepareStatement x) {
    	
    }
    
    @Override
    public boolean visit(MysqlDeallocatePrepareStatement x) {
    	return true;
    }

    @Override
    public void endVisit(MySqlDeleteStatement x) {

    }

    @Override
    public boolean visit(MySqlDeleteStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlInsertStatement x) {

    }

    @Override
    public boolean visit(MySqlInsertStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlLoadDataInFileStatement x) {

    }

    @Override
    public boolean visit(MySqlLoadDataInFileStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlLoadXmlStatement x) {

    }

    @Override
    public boolean visit(MySqlLoadXmlStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlReplaceStatement x) {

    }

    @Override
    public boolean visit(MySqlReplaceStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlCommitStatement x) {

    }

    @Override
    public boolean visit(MySqlCommitStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlRollbackStatement x) {

    }

    @Override
    public boolean visit(MySqlRollbackStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlShowColumnsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowColumnsStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlShowDatabasesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowDatabasesStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlShowWarningsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowWarningsStatement x) {

        return true;
    }

    @Override
    public void endVisit(MySqlShowStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowStatusStatement x) {

        return true;
    }

    @Override
    public void endVisit(CobarShowStatus x) {

    }

    @Override
    public boolean visit(CobarShowStatus x) {
        return true;
    }

    @Override
    public void endVisit(MySqlKillStatement x) {

    }

    @Override
    public boolean visit(MySqlKillStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlBinlogStatement x) {

    }

    @Override
    public boolean visit(MySqlBinlogStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlResetStatement x) {

    }

    @Override
    public boolean visit(MySqlResetStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateUserStatement x) {

    }

    @Override
    public boolean visit(MySqlCreateUserStatement x) {
        return true;
    }

    @Override
    public void endVisit(UserSpecification x) {

    }

    @Override
    public boolean visit(UserSpecification x) {
        return true;
    }

    @Override
    public void endVisit(MySqlPartitionByKey x) {

    }

    @Override
    public boolean visit(MySqlPartitionByKey x) {
        return true;
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSelectQueryBlock x) {

    }

    @Override
    public boolean visit(MySqlOutFileExpr x) {
        return true;
    }

    @Override
    public void endVisit(MySqlOutFileExpr x) {

    }

    @Override
    public boolean visit(MySqlDescribeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlDescribeStatement x) {

    }

    @Override
    public boolean visit(MySqlUpdateStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUpdateStatement x) {

    }

    @Override
    public boolean visit(MySqlSetTransactionStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSetTransactionStatement x) {

    }

    @Override
    public boolean visit(MySqlSetNamesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSetNamesStatement x) {

    }

    @Override
    public boolean visit(MySqlSetCharSetStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSetCharSetStatement x) {

    }

    @Override
    public boolean visit(MySqlShowAuthorsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowAuthorsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowBinaryLogsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowBinaryLogsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowMasterLogsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowMasterLogsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCollationStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCollationStatement x) {

    }

    @Override
    public boolean visit(MySqlShowBinLogEventsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowBinLogEventsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCharacterSetStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCharacterSetStatement x) {

    }

    @Override
    public boolean visit(MySqlShowContributorsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowContributorsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateDatabaseStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateDatabaseStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateEventStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateEventStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateFunctionStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateFunctionStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateProcedureStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateProcedureStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateTableStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateTriggerStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateTriggerStatement x) {

    }

    @Override
    public boolean visit(MySqlShowCreateViewStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowCreateViewStatement x) {

    }

    @Override
    public boolean visit(MySqlShowEngineStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowEngineStatement x) {

    }

    @Override
    public boolean visit(MySqlShowEnginesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowEnginesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowErrorsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowErrorsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowEventsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowEventsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowFunctionCodeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowFunctionCodeStatement x) {

    }

    @Override
    public boolean visit(MySqlShowFunctionStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowFunctionStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowGrantsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowGrantsStatement x) {
    }

    @Override
    public boolean visit(MySqlUserName x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUserName x) {

    }

    @Override
    public boolean visit(MySqlShowIndexesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowIndexesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowKeysStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowKeysStatement x) {

    }

    @Override
    public boolean visit(MySqlShowMasterStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowMasterStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowOpenTablesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowOpenTablesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowPluginsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowPluginsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowPrivilegesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowPrivilegesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowProcedureCodeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProcedureCodeStatement x) {

    }

    @Override
    public boolean visit(MySqlShowProcedureStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProcedureStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowProcessListStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProcessListStatement x) {

    }

    @Override
    public boolean visit(MySqlShowProfileStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProfileStatement x) {

    }

    @Override
    public boolean visit(MySqlShowProfilesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowProfilesStatement x) {

    }

    @Override
    public boolean visit(MySqlShowRelayLogEventsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowRelayLogEventsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowSlaveHostsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowSlaveHostsStatement x) {

    }

    @Override
    public boolean visit(MySqlShowSlaveStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowSlaveStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowTableStatusStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowTableStatusStatement x) {

    }

    @Override
    public boolean visit(MySqlShowTriggersStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowTriggersStatement x) {

    }

    @Override
    public boolean visit(MySqlShowVariantsStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlShowVariantsStatement x) {

    }

    @Override
    public boolean visit(MySqlRenameTableStatement.Item x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRenameTableStatement.Item x) {

    }

    @Override
    public boolean visit(MySqlRenameTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlRenameTableStatement x) {

    }

    @Override
    public boolean visit(MySqlUnionQuery x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUnionQuery x) {

    }

    @Override
    public boolean visit(MySqlUseIndexHint x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUseIndexHint x) {

    }

    @Override
    public boolean visit(MySqlIgnoreIndexHint x) {
        return true;
    }

    @Override
    public void endVisit(MySqlIgnoreIndexHint x) {

    }

    @Override
    public boolean visit(MySqlLockTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlLockTableStatement x) {

    }

    @Override
    public boolean visit(MySqlUnlockTablesStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUnlockTablesStatement x) {

    }

    @Override
    public boolean visit(MySqlForceIndexHint x) {
        return true;
    }

    @Override
    public void endVisit(MySqlForceIndexHint x) {

    }

    @Override
    public boolean visit(MySqlAlterTableChangeColumn x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableChangeColumn x) {

    }

    @Override
    public boolean visit(MySqlAlterTableCharacter x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableCharacter x) {

    }

    @Override
    public boolean visit(MySqlAlterTableOption x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableOption x) {

    }

    @Override
    public boolean visit(MySqlCreateTableStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCreateTableStatement x) {

    }

    @Override
    public boolean visit(MySqlHelpStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlHelpStatement x) {

    }

    @Override
    public boolean visit(MySqlCharExpr x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCharExpr x) {

    }

    @Override
    public boolean visit(MySqlUnique x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUnique x) {

    }

    @Override
    public boolean visit(MysqlForeignKey x) {
        return true;
    }

    @Override
    public void endVisit(MysqlForeignKey x) {

    }

    @Override
    public boolean visit(MySqlAlterTableModifyColumn x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableModifyColumn x) {

    }

    @Override
    public boolean visit(MySqlAlterTableDiscardTablespace x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableDiscardTablespace x) {

    }

    @Override
    public boolean visit(MySqlAlterTableImportTablespace x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableImportTablespace x) {

    }

    @Override
    public boolean visit(TableSpaceOption x) {
        return true;
    }

    @Override
    public void endVisit(TableSpaceOption x) {

    }

    @Override
    public boolean visit(MySqlAnalyzeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAnalyzeStatement x) {

    }

    @Override
    public boolean visit(MySqlAlterUserStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterUserStatement x) {

    }

    @Override
    public boolean visit(MySqlOptimizeStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlOptimizeStatement x) {

    }

    @Override
    public boolean visit(MySqlSetPasswordStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSetPasswordStatement x) {

    }

    @Override
    public boolean visit(MySqlHintStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlHintStatement x) {

    }

    @Override
    public boolean visit(MySqlOrderingExpr x) {
        return true;
    }

    @Override
    public void endVisit(MySqlOrderingExpr x) {

    }

    @Override
    public boolean visit(MySqlWhileStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlWhileStatement x) {

    }

    @Override
    public boolean visit(MySqlCaseStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlCaseStatement x) {

    }

    @Override
    public boolean visit(MySqlDeclareStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlDeclareStatement x) {

    }

    @Override
    public boolean visit(MySqlSelectIntoStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSelectIntoStatement x) {

    }

    @Override
    public boolean visit(MySqlWhenStatement x) {
        return true;
    }

    @Override
    public void endVisit(MySqlWhenStatement x) {

    }
    // add:end

    @Override
    public boolean visit(MySqlLeaveStatement x) {
        return false;
    }

    @Override
    public void endVisit(MySqlLeaveStatement x) {

    }

    @Override
    public boolean visit(MySqlIterateStatement x) {
        return false;
    }

    @Override
    public void endVisit(MySqlIterateStatement x) {

    }

    @Override
    public boolean visit(MySqlRepeatStatement x) {
        return false;
    }

    @Override
    public void endVisit(MySqlRepeatStatement x) {

    }

    @Override
    public boolean visit(MySqlCursorDeclareStatement x) {
        return false;
    }

    @Override
    public void endVisit(MySqlCursorDeclareStatement x) {

    }

    @Override
    public boolean visit(MySqlUpdateTableSource x) {
        return true;
    }

    @Override
    public void endVisit(MySqlUpdateTableSource x) {

    }

    @Override
    public boolean visit(MySqlAlterTableAlterColumn x) {
        return true;
    }

    @Override
    public void endVisit(MySqlAlterTableAlterColumn x) {

    }

    @Override
    public boolean visit(MySqlSubPartitionByKey x) {
        return true;
    }

    @Override
    public void endVisit(MySqlSubPartitionByKey x) {

    }
    
    @Override
    public boolean visit(MySqlSubPartitionByList x) {
        return true;
    }
    
    @Override
    public void endVisit(MySqlSubPartitionByList x) {
    }

	@Override
	public boolean visit(MySqlDeclareHandlerStatement x) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void endVisit(MySqlDeclareHandlerStatement x) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean visit(MySqlDeclareConditionStatement x) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void endVisit(MySqlDeclareConditionStatement x) {
		// TODO Auto-generated method stub
		
	}

} //

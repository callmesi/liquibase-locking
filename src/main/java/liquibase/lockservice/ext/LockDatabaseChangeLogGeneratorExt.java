package liquibase.lockservice.ext;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.UpdateStatement;

public class LockDatabaseChangeLogGeneratorExt extends LockDatabaseChangeLogGenerator {

    public static final String LOCKED_BY_SEPARATOR = "@@";
    private static final Logger log = LoggerFactory.getLogger(LockDatabaseChangeLogGeneratorExt.class);

    @Override
    public int getPriority() {
        return super.getPriority() + 1000;
    }

    @Override
    public boolean supports(LockDatabaseChangeLogStatement statement, Database database) {
        return true;
    }

    @Override
    public Sql[] generateSql(LockDatabaseChangeLogStatement statement, Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        String lockedByValue = generateLockedBy(database);
        String liquibaseSchema = database.getLiquibaseSchemaName();
        String liquibaseCatalog = database.getLiquibaseCatalogName();
        UpdateStatement updateStatement = new UpdateStatement(liquibaseCatalog, liquibaseSchema,
                database.getDatabaseChangeLogLockTableName());
        updateStatement.addNewColumnValue("LOCKED", true);
        updateStatement.addNewColumnValue("LOCKGRANTED", new Timestamp(new java.util.Date().getTime()));
        updateStatement.addNewColumnValue("LOCKEDBY", lockedByValue);
        updateStatement.setWhereClause(database.escapeColumnName(liquibaseCatalog, liquibaseSchema,
                database.getDatabaseChangeLogTableName(), "ID") + " = 1 AND "
                + database.escapeColumnName(liquibaseCatalog, liquibaseSchema,
                database.getDatabaseChangeLogTableName(), "LOCKED")
                + " = " + DataTypeFactory.getInstance()
                .fromDescription("boolean", database).objectToSql(false, database));
        return SqlGeneratorFactory.getInstance().generateSql(updateStatement, database);
    }

    private String generateLockedBy(Database database) {
        String dbPid = "0000";
        String dbPidStartTime = "";
        try {
            List<Map<String, ?>> rs = ExecutorService.getInstance().getExecutor(database)
                    .queryForList(new RawSqlStatement("select * from pg_stat_activity where pid=pg_backend_pid()"));
            if (rs.size() > 0) { // expected exactly one row
                Map<String, ?> row = rs.get(0);
                dbPid = Integer.toString((Integer) row.get("PID"));
                dbPidStartTime = row.get("BACKEND_START").toString();
            }
        } catch (DatabaseException e) {
            log.error("Failed to read current Liquibase locking info", e);
        }
        return dbPid + LOCKED_BY_SEPARATOR + dbPidStartTime;
    }
}

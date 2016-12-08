/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo;

import dk.dbc.commons.jdbc.util.JDBCUtil;
import dk.dbc.ticklerepo.dto.Batch;
import dk.dbc.ticklerepo.dto.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.eclipse.persistence.config.PersistenceUnitProperties.JDBC_DRIVER;
import static org.eclipse.persistence.config.PersistenceUnitProperties.JDBC_PASSWORD;
import static org.eclipse.persistence.config.PersistenceUnitProperties.JDBC_URL;
import static org.eclipse.persistence.config.PersistenceUnitProperties.JDBC_USER;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class TickleRepoIT {
    protected static final PGSimpleDataSource datasource;

    static {
        datasource = new PGSimpleDataSource();
        datasource.setDatabaseName("ticklerepo");
        datasource.setServerName("localhost");
        datasource.setPortNumber(getPostgresqlPort());
        datasource.setUser(System.getProperty("user.name"));
        datasource.setPassword(System.getProperty("user.name"));
    }

    private static Map<String, String> entityManagerProperties = new HashMap<>();
    private static EntityManagerFactory entityManagerFactory;
    private EntityManager entityManager;

    @BeforeClass
    public static void migrateDatabase() throws Exception {
        final TickleRepoDatabaseMigrator dbMigrator = new TickleRepoDatabaseMigrator(datasource);
        dbMigrator.migrate();
    }

    @BeforeClass
    public static void createEntityManagerFactory() {
        entityManagerProperties.put(JDBC_USER, datasource.getUser());
        entityManagerProperties.put(JDBC_PASSWORD, datasource.getPassword());
        entityManagerProperties.put(JDBC_URL, datasource.getUrl());
        entityManagerProperties.put(JDBC_DRIVER, "org.postgresql.Driver");
        entityManagerProperties.put("eclipselink.logging.level", "FINE");
        entityManagerFactory = Persistence.createEntityManagerFactory("tickleRepoIT", entityManagerProperties);
    }

    @Before
    public void createEntityManager() {
        entityManager = entityManagerFactory.createEntityManager(entityManagerProperties);
    }

    @Before
    public void resetDatabase() throws SQLException {
        try (Connection conn = datasource.getConnection();
             Statement statement = conn.createStatement()) {
            statement.executeUpdate("DELETE FROM record");
            statement.executeUpdate("DELETE FROM batch");
            statement.executeUpdate("DELETE FROM dataset");
            statement.executeUpdate("ALTER SEQUENCE record_id_seq RESTART");
            statement.executeUpdate("ALTER SEQUENCE batch_id_seq RESTART");
            statement.executeUpdate("ALTER SEQUENCE dataset_id_seq RESTART");
        }
    }

    @Before
    public void populateDatabase() throws URISyntaxException {
        executeScriptResource("/populate.sql");
    }

    @After
    public void clearEntityManagerCache() {
        entityManager.clear();
        entityManager.getEntityManagerFactory().getCache().evictAll();
    }

    @Test
    public void gettingBatchRecordsFromNonExistingBatchReturnsEmptyResultSet() {
        final Batch batch = new Batch()
                .withId(42);

        int recordsInBatch = 0;
        try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(batch)) {
            for (Record record : rs) {
                recordsInBatch++;
            }
        }
        assertThat(recordsInBatch, is(0));
    }

    @Test
    public void gettingBatchRecordsFromNonExistingBatchReturnsResultWithRecords() {
        final Batch batch = new Batch()
                .withId(1);

        int recordsInBatch = 0;
        try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(batch)) {
            for (Record record : rs) {
                recordsInBatch++;
                assertThat("record ID", record.getId(), is(recordsInBatch));
            }
        }
        assertThat("number of records in batch", recordsInBatch, is(10));
    }

    @Test
    public void closingTotalBatchEnsuresThatAllRecordsWithStatusResetAreSetToDeletedAndSetsTimeOfCompletion() {
        final LinkedList<Record> expectedRecords = new LinkedList<>();
        expectedRecords.add(new Record().withLocalId("local2_2_1").withStatus(Record.Status.DELETED));
        expectedRecords.add(new Record().withLocalId("local2_2_3").withStatus(Record.Status.DELETED));
        expectedRecords.add(new Record().withLocalId("local2_2_5").withStatus(Record.Status.DELETED));
        expectedRecords.add(new Record().withLocalId("local2_2_7").withStatus(Record.Status.DELETED));
        expectedRecords.add(new Record().withLocalId("local2_2_9").withStatus(Record.Status.DELETED));
        expectedRecords.add(new Record().withLocalId("local3_2_1").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_2").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_3").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_4").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_5").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_6").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_7").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_8").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_9").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_10").withStatus(Record.Status.DELETED));

        final Batch batch = entityManager.find(Batch.class, 3);

        final TickleRepo tickleRepo = tickleRepo();
        transaction_scoped(() -> tickleRepo.closeBatch(batch));

        try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(batch)) {
            for (Record record : rs) {
                final Record expectedRecord = expectedRecords.remove();
                assertThat("record local ID", record.getLocalId(), is(expectedRecord.getLocalId()));
                assertThat("record status", record.getStatus(), is(expectedRecord.getStatus()));
            }
        }
        assertThat("number of records in batch is 15", expectedRecords.isEmpty(), is(true));

        entityManager.refresh(batch);
        assertThat("batch is marked as completed", batch.getTimeOfCompletion(), is(notNullValue()));
    }

    @Test
    public void closingIncrementalBatchSetsTimeOfCompletion() {
       final LinkedList<Record> expectedRecords = new LinkedList<>();
        expectedRecords.add(new Record().withLocalId("local3_2_1").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_2").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_3").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_4").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_5").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_6").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_7").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_8").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_9").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local3_2_10").withStatus(Record.Status.DELETED));

        final Batch batch = entityManager.find(Batch.class, 3)
                .withType(Batch.Type.INCREMENTAL);

        final TickleRepo tickleRepo = tickleRepo();
        transaction_scoped(() -> tickleRepo.closeBatch(batch));

        try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(batch)) {
            for (Record record : rs) {
                final Record expectedRecord = expectedRecords.remove();
                assertThat("record local ID", record.getLocalId(), is(expectedRecord.getLocalId()));
                assertThat("record status", record.getStatus(), is(expectedRecord.getStatus()));
            }
        }
        assertThat("number of records in batch is 10", expectedRecords.isEmpty(), is(true));

        entityManager.refresh(batch);
        assertThat("batch is marked as completed", batch.getTimeOfCompletion(), is(notNullValue()));
    }

    @Test
    public void gettingNextBatchWhenCompleted() {
        final Batch batch2 = entityManager.find(Batch.class, 2);
        final Batch batch3 = entityManager.find(Batch.class, 3);

        transaction_scoped(() -> batch3.withTimeOfCompletion(new Timestamp(new Date().getTime())));

        assertThat(tickleRepo().getNextBatch(batch2).orElse(null).getId(), is(batch3.getId()));
    }

    @Test
    public void gettingNextBatchWhenNotCompleted() {
        final Batch batch2 = entityManager.find(Batch.class, 2);
        assertThat(tickleRepo().getNextBatch(batch2).isPresent(), is(false));
    }

    @Test
    public void gettingNextBatchWhenNoneExist() {
        final Batch batch3 = entityManager.find(Batch.class, 3);
        assertThat(tickleRepo().getNextBatch(batch3).isPresent(), is(false));
    }

    @Test
    public void lookingUpRecordWhenPlaceholderValueIsEmpty() {
        assertThat(tickleRepo().lookupRecord(new Record()).isPresent(), is(false));
    }

    @Test
    public void lookingUpRecordWhenPlaceholderValueIsIncomplete() {
        final Record record = new Record().withLocalId("local1_1_!");
        assertThat(tickleRepo().lookupRecord(record).isPresent(), is(false));
    }

    @Test
    public void lookingUpRecordById() {
        final Record record = new Record().withId(1);
        assertThat(tickleRepo().lookupRecord(record).orElse(null).getLocalId(), is("local1_1_1"));
    }

    @Test
    public void lookingUpRecordByDatasetAndLocalId() {
        final Record record = new Record().withDataset(1).withLocalId("local1_1_1");
        assertThat(tickleRepo().lookupRecord(record).orElse(null).getId(), is(1));
    }

    private TickleRepo tickleRepo() {
        final TickleRepo tickleRepo = new TickleRepo();
        tickleRepo.entityManager = entityManager;
        return tickleRepo;
    }

    private <T> T transaction_scoped(CodeBlockExecution<T> codeBlock) {
        final EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        try {
            return codeBlock.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            transaction.commit();
        }
    }

    private void transaction_scoped(CodeBlockVoidExecution codeBlock) {
        final EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        try {
            codeBlock.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            transaction.commit();
        }
    }

    /**
     * Represents a code block execution with return value
     * @param <T> return type of the code block execution
     */
    @FunctionalInterface
    interface CodeBlockExecution<T> {
        T execute() throws Exception;
    }

    /**
     * Represents a code block execution without return value
     */
    @FunctionalInterface
    interface CodeBlockVoidExecution {
        void execute() throws Exception;
    }

    private static int getPostgresqlPort() {
        final String port = System.getProperty("postgresql.port");
        if (port != null && !port.isEmpty()) {
            return Integer.parseInt(port);
        }
        return 5432;
    }

    private static void executeScriptResource(String resourcePath) {
        final URL resource = TickleRepoIT.class.getResource(resourcePath);
        try {
            executeScript(new File(resource.toURI()));
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void executeScript(File scriptFile) {
        try (Connection conn = datasource.getConnection()) {
            JDBCUtil.executeScript(conn, scriptFile, StandardCharsets.UTF_8.name());
        } catch (SQLException | IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
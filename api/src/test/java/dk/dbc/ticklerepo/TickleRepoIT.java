/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo;

import dk.dbc.commons.persistence.JpaIntegrationTest;
import dk.dbc.commons.persistence.JpaTestEnvironment;
import dk.dbc.ticklerepo.dto.Batch;
import dk.dbc.ticklerepo.dto.DataSet;
import dk.dbc.ticklerepo.dto.Record;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

import javax.persistence.RollbackException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedList;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TickleRepoIT extends JpaIntegrationTest {
    @Override
    public JpaTestEnvironment setup() {
        final PGSimpleDataSource dataSource = createDataSource();
        migrateDatabase(dataSource);
        return new JpaTestEnvironment(dataSource, "tickleRepoIT");
    }

    @Before
    public void resetDatabase() throws SQLException {
        try (Connection conn = env().getDatasource().getConnection();
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
    public void populateDatabase() {
        executeScriptResource("/populate.sql");
    }

    @Test
    public void gettingBatchRecordsFromNonExistingBatchReturnsEmptyResultSet() {
        final Batch batch = new Batch()
                .withId(42);

        env().getPersistenceContext().run(() -> {
            int recordsInBatch = 0;
            try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(batch)) {
                for (Record record : rs) {
                    recordsInBatch++;
                }
            }
            assertThat(recordsInBatch, is(0));
        });
    }

    @Test
    public void gettingBatchRecordsFromExistingBatchReturnsResultWithRecords() {
        final Batch batch = new Batch()
                .withId(1);

        env().getPersistenceContext().run(() -> {
            int recordsInBatch = 0;
            try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(batch)) {
                for (Record record : rs) {
                    recordsInBatch++;
                    assertThat("record ID", record.getId(), is(recordsInBatch));
                }
            }
            assertThat("number of records in batch", recordsInBatch, is(10));
        });
    }

    @Test
    public void creatingTotalBatchMarksAllActiveRecordsAsReset() {
        final Batch batch = new Batch()
                .withBatchKey(1000004)
                .withType(Batch.Type.TOTAL)
                .withDataset(env().getEntityManager().find(DataSet.class, 1).getId());

        final TickleRepo tickleRepo = tickleRepo();
        Batch batchCreated = env().getPersistenceContext().run(() -> tickleRepo.createBatch(batch));

        assertThat("batch ID", batchCreated.getId(), is(4));
        assertThat("batch key", batchCreated.getBatchKey(), is(batch.getBatchKey()));
        assertThat("batch type", batchCreated.getType(), is(batch.getType()));
        assertThat("batch dateset", batchCreated.getDataset(), is(batch.getDataset()));
        assertThat("batch time of creation", batchCreated.getTimeOfCreation(), is(notNullValue()));
        assertThat("batch time of completion", batchCreated.getTimeOfCompletion(), is(nullValue()));

        final LinkedList<Record> expectedRecords = new LinkedList<>();
        expectedRecords.add(new Record().withLocalId("local1_1_1").withStatus(Record.Status.RESET));
        expectedRecords.add(new Record().withLocalId("local1_1_2").withStatus(Record.Status.RESET));
        expectedRecords.add(new Record().withLocalId("local1_1_3").withStatus(Record.Status.RESET));
        expectedRecords.add(new Record().withLocalId("local1_1_4").withStatus(Record.Status.RESET));
        expectedRecords.add(new Record().withLocalId("local1_1_5").withStatus(Record.Status.RESET));
        expectedRecords.add(new Record().withLocalId("local1_1_6").withStatus(Record.Status.RESET));
        expectedRecords.add(new Record().withLocalId("local1_1_7").withStatus(Record.Status.RESET));
        expectedRecords.add(new Record().withLocalId("local1_1_8").withStatus(Record.Status.RESET));
        expectedRecords.add(new Record().withLocalId("local1_1_9").withStatus(Record.Status.RESET));
        expectedRecords.add(new Record().withLocalId("local1_1_10").withStatus(Record.Status.DELETED));

        env().getPersistenceContext().run(() -> {
            try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(
                    env().getEntityManager().find(Batch.class, 1))) {
                for (Record record : rs) {
                    final Record expectedRecord = expectedRecords.remove();
                    assertThat("record local ID " + expectedRecords, record.getLocalId(), is(expectedRecord.getLocalId()));
                    assertThat("record status " + expectedRecords, record.getStatus(), is(expectedRecord.getStatus()));
                }
            }});
        assertThat("number of records in batch is 10", expectedRecords.isEmpty(), is(true));
    }

    @Test
    public void creatingIncrementalBatch() {
        final Batch batch = new Batch()
                .withBatchKey(1000004)
                .withType(Batch.Type.INCREMENTAL)
                .withDataset(env().getEntityManager().find(DataSet.class, 1).getId());

        final TickleRepo tickleRepo = tickleRepo();
        Batch batchCreated = env().getPersistenceContext().run(() -> tickleRepo.createBatch(batch));

        assertThat("batch ID", batchCreated.getId(), is(4));
        assertThat("batch key", batchCreated.getBatchKey(), is(batch.getBatchKey()));
        assertThat("batch type", batchCreated.getType(), is(batch.getType()));
        assertThat("batch dateset", batchCreated.getDataset(), is(batch.getDataset()));
        assertThat("batch time of creation", batchCreated.getTimeOfCreation(), is(notNullValue()));
        assertThat("batch time of completion", batchCreated.getTimeOfCompletion(), is(nullValue()));

        final LinkedList<Record> expectedRecords = new LinkedList<>();
        expectedRecords.add(new Record().withLocalId("local1_1_1").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local1_1_2").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local1_1_3").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local1_1_4").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local1_1_5").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local1_1_6").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local1_1_7").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local1_1_8").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local1_1_9").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local1_1_10").withStatus(Record.Status.DELETED));

        env().getPersistenceContext().run(() -> {
            try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(
                    env().getEntityManager().find(Batch.class, 1))) {
                for (Record record : rs) {
                    final Record expectedRecord = expectedRecords.remove();
                    assertThat("record local ID " + expectedRecords, record.getLocalId(), is(expectedRecord.getLocalId()));
                    assertThat("record status " + expectedRecords, record.getStatus(), is(expectedRecord.getStatus()));
                }
            }
            assertThat("number of records in batch is 10", expectedRecords.isEmpty(), is(true));
        });
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

        final Batch batch = env().getEntityManager().find(Batch.class, 3);

        final TickleRepo tickleRepo = tickleRepo();
        env().getPersistenceContext().run(() -> tickleRepo.closeBatch(batch));

        env().getPersistenceContext().run(() -> {
            try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(batch)) {
                for (Record record : rs) {
                    final Record expectedRecord = expectedRecords.remove();
                    assertThat("record local ID " + expectedRecords, record.getLocalId(), is(expectedRecord.getLocalId()));
                    assertThat("record status " + expectedRecords, record.getStatus(), is(expectedRecord.getStatus()));
                }}});
        assertThat("number of records in batch is 15", expectedRecords.isEmpty(), is(true));

        final Record deletedRecord = tickleRepo.lookupRecord(new Record().withId(11)).orElse(null);
        assertThat("timeOfLastModification set on deleted records", deletedRecord.getTimeOfLastModification(),
                is(notNullValue()));
        assertThat("checksum reset on deleted records", deletedRecord.getChecksum(),
                is(""));

        env().getEntityManager().refresh(batch);
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

        final Batch batch = env().getEntityManager().find(Batch.class, 3)
                .withType(Batch.Type.INCREMENTAL);

        final TickleRepo tickleRepo = tickleRepo();
        env().getPersistenceContext().run(() -> tickleRepo.closeBatch(batch));

        env().getPersistenceContext().run(() -> {
            try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(batch)) {
                for (Record record : rs) {
                    final Record expectedRecord = expectedRecords.remove();
                    assertThat("record local ID " + expectedRecords, record.getLocalId(), is(expectedRecord.getLocalId()));
                    assertThat("record status " + expectedRecords, record.getStatus(), is(expectedRecord.getStatus()));
                }
            }
            assertThat("number of records in batch is 10", expectedRecords.isEmpty(), is(true));
        });

        env().getEntityManager().refresh(batch);
        assertThat("batch is marked as completed", batch.getTimeOfCompletion(), is(notNullValue()));
    }

    @Test
    public void gettingNextBatchWhenCompleted() {
        final Batch batch2 = env().getEntityManager().find(Batch.class, 2);
        final Batch batch3 = env().getEntityManager().find(Batch.class, 3);

        env().getPersistenceContext().run(() -> batch3.withTimeOfCompletion(new Timestamp(new Date().getTime())));

        assertThat(tickleRepo().getNextBatch(batch2).orElse(null).getId(), is(batch3.getId()));
    }

    @Test
    public void gettingNextBatchWhenNotCompleted() {
        final Batch batch2 = env().getEntityManager().find(Batch.class, 2);
        assertThat(tickleRepo().getNextBatch(batch2).isPresent(), is(false));
    }

    @Test
    public void gettingNextBatchWhenNoneExist() {
        final Batch batch3 = env().getEntityManager().find(Batch.class, 3);
        assertThat(tickleRepo().getNextBatch(batch3).isPresent(), is(false));
    }

    @Test
    public void lookingUpBatchWhenPlaceholderValueIsEmpty() {
        assertThat(tickleRepo().lookupBatch(new Batch()).isPresent(), is(false));
    }

    @Test
    public void lookingUpBatchById() {
        final Batch batch = new Batch().withId(1);
        assertThat(tickleRepo().lookupBatch(batch).orElse(null).getBatchKey(), is(1000001));
    }

    @Test
    public void lookingUpBatchByKey() {
        final Batch batch = new Batch().withBatchKey(1000001);
        assertThat(tickleRepo().lookupBatch(batch).orElse(null).getId(), is(1));
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

    @Test
    public void lookupDataSetById_notPersisted_returnsOptionalEmpty() {
        Optional<DataSet> dataSetOptional = tickleRepo().lookupDataSet(new DataSet().withId(42));
        assertThat("DataSet not present", dataSetOptional.isPresent(), is (false));
    }

    @Test
    public void lookupDataSetByName_notPersisted_returnsOptionalEmpty() {
        Optional<DataSet> dataSetOptional = tickleRepo().lookupDataSet(new DataSet().withName("dataset3"));
        assertThat("DataSet not present", dataSetOptional.isPresent(), is (false));
    }

    @Test
    public void lookupDataSet_findsPersistedThroughId_returnsPersistedDataSet() {
        DataSet dataSet = new DataSet().withId(2);
        Optional<DataSet> dataSetOptional = tickleRepo().lookupDataSet(dataSet);
        assertThat(dataSetOptional.isPresent(), is (true));
        DataSet persisted = dataSetOptional.get();
        assertThat("dataSet ID", persisted.getId(), is(2));
        assertThat("dataSet name", persisted.getName(), is("dataset2"));
        assertThat("dataSet agencyId", persisted.getAgencyId(), is(123457));
        assertThat("dataSet displayName",persisted.getDisplayName(), is("displayname2"));
    }

    @Test
    public void lookupDataSet_findsPersistedThroughName_returnsPersistedDataSet() {
        DataSet dataSet = new DataSet().withName("dataset1");
        Optional<DataSet> dataSetOptional = tickleRepo().lookupDataSet(dataSet);
        assertThat(dataSetOptional.isPresent(), is (true));
        DataSet persisted = dataSetOptional.get();
        assertThat("dataSet ID", persisted.getId(), is(1));
        assertThat("dataSet name", persisted.getName(), is("dataset1"));
        assertThat("dataSet agencyId", persisted.getAgencyId(), is(123456));
        assertThat("dataSet displayName",persisted.getDisplayName(), is("displayname1"));
    }

    @Test
    public void createDataSet_returns() {
        final DataSet persisted = env().getPersistenceContext().run(() ->
                tickleRepo().createDataSet(new DataSet().withName("dataset3").withAgencyId(123458)));
        assertThat("dataSet ID", persisted.getId(), is(3));
        assertThat("dataSet name", persisted.getName(), is("dataset3"));
        assertThat("dataSet agencyId", persisted.getAgencyId(), is(123458));
        assertThat("dataSet displayName", persisted.getDisplayName(), is(nullValue()));
    }

    @Test
    public void localIdMustBeUniqueForDataset() {
        try {
            env().getPersistenceContext().run(() -> {
                final Record record = new Record()
                        .withBatch(1)
                        .withDataset(1)
                        .withLocalId("local1_1_1")
                        .withTrackingId("tid")
                        .withStatus(Record.Status.ACTIVE)
                        .withContent("content".getBytes())
                        .withChecksum("checksum");
                env().getEntityManager().persist(record);
            });
            fail("No exception thrown");
        } catch (RollbackException e) {
            assertThat(e.getMessage().contains("record_unique_dataset_localid_constraint"), is(true));
        }
    }

    @Test
    public void abortingBatchUndoMarks() {
        final Batch batch = env().getEntityManager().find(Batch.class, 2);

        final TickleRepo tickleRepo = tickleRepo();
        env().getPersistenceContext().run(() -> tickleRepo.abortBatch(batch));

        env().getEntityManager().refresh(batch);
        assertThat("batch time of completion", batch.getTimeOfCompletion(), is(notNullValue()));

        final LinkedList<Record> expectedRecords = new LinkedList<>();
        expectedRecords.add(new Record().withLocalId("local2_2_1").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local2_2_2").withStatus(Record.Status.DELETED));
        expectedRecords.add(new Record().withLocalId("local2_2_3").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local2_2_4").withStatus(Record.Status.DELETED));
        expectedRecords.add(new Record().withLocalId("local2_2_5").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local2_2_6").withStatus(Record.Status.DELETED));
        expectedRecords.add(new Record().withLocalId("local2_2_7").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local2_2_8").withStatus(Record.Status.DELETED));
        expectedRecords.add(new Record().withLocalId("local2_2_9").withStatus(Record.Status.ACTIVE));
        expectedRecords.add(new Record().withLocalId("local2_2_10").withStatus(Record.Status.DELETED));

        env().getPersistenceContext().run(() -> {
            try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInBatch(batch)) {
                for (Record record : rs) {
                    final Record expectedRecord = expectedRecords.remove();
                    assertThat("record local ID " + expectedRecords, record.getLocalId(), is(expectedRecord.getLocalId()));
                    assertThat("record status " + expectedRecords, record.getStatus(), is(expectedRecord.getStatus()));
                }
            }
            assertThat("number of records in batch is 10", expectedRecords.isEmpty(), is(true));
        });

    }

    @Test
    public void timeOfLastModificationSetOnRecordPersistAndRecordUpdate() {
        final Record record = new Record()
                .withBatch(3)
                .withDataset(1)
                .withTrackingId("trackingId")
                .withStatus(Record.Status.ACTIVE)
                .withLocalId("localId")
                .withContent("content".getBytes())
                .withChecksum("checksum");

        final Timestamp timestamp = env().getPersistenceContext().run(() -> {
            env().getEntityManager().persist(record);
            assertThat("timeOfLastModification after persist", record.getTimeOfLastModification(), is(notNullValue()));
            return record.getTimeOfLastModification();
        });

        env().getPersistenceContext().run(() -> record.withStatus(Record.Status.DELETED));

        assertThat("timeOfLastModification after update", record.getTimeOfLastModification(), is(notNullValue()));
        assertThat("timeOfLastModification after update", record.getTimeOfLastModification(), is(not(timestamp)));
    }

    @Test
    public void getRecordsInDataSet() {
        final DataSet dataSet = new DataSet()
                .withId(1);

        env().getPersistenceContext().run(() -> {
            int recordsInDataSet = 0;
            try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInDataSet(dataSet)) {
                for (Record record : rs) {
                    recordsInDataSet++;
                    assertThat("record ID", record.getId(), is(recordsInDataSet));
                }
            }
            assertThat("number of records in data set", recordsInDataSet, is(10));
        });
    }

    @Test
    public void getRecordsInDataSet_nonExistingDataSet() {
        final DataSet dataSet = new DataSet()
                .withId(42);

        env().getPersistenceContext().run(() -> {
            int recordsInDataSet = 0;
            try (TickleRepo.ResultSet<Record> rs = tickleRepo().getRecordsInDataSet(dataSet)) {
                for (Record record : rs) {
                    recordsInDataSet++;
                }
            }
            assertThat(recordsInDataSet, is(0));
        });
    }

    @Test
    public void estimateSizeOf_dataset() {
        final DataSet dataSet = new DataSet().withId(1);
        assertThat(tickleRepo().estimateSizeOf(dataSet), is(10));
    }

    private TickleRepo tickleRepo() {
        final TickleRepo tickleRepo = new TickleRepo();
        tickleRepo.entityManager = env().getEntityManager();
        return tickleRepo;
    }

    private static int getPostgresqlPort() {
        final String port = System.getProperty("postgresql.port");
        if (port != null && !port.isEmpty()) {
            return Integer.parseInt(port);
        }
        return 5432;
    }

    private PGSimpleDataSource createDataSource() {
        final PGSimpleDataSource datasource = new PGSimpleDataSource();
        datasource.setDatabaseName("ticklerepo");
        datasource.setServerName("localhost");
        datasource.setPortNumber(getPostgresqlPort());
        datasource.setUser(System.getProperty("user.name"));
        datasource.setPassword(System.getProperty("user.name"));
        return datasource;
    }

    private void migrateDatabase(DataSource dataSource) {
        final TickleRepoDatabaseMigrator dbMigrator = new TickleRepoDatabaseMigrator(dataSource);
        dbMigrator.migrate();
    }
}

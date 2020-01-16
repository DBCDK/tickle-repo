/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.SingleInstancePostgresRule;
import dk.dbc.commons.persistence.JpaIntegrationTest;
import dk.dbc.commons.persistence.JpaTestEnvironment;
import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.ticklerepo.dto.Batch;
import dk.dbc.ticklerepo.dto.DataSet;
import dk.dbc.ticklerepo.dto.DataSetSummary;
import dk.dbc.ticklerepo.dto.Record;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

import javax.persistence.Query;
import javax.persistence.RollbackException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TickleRepoIT extends JpaIntegrationTest {
    @ClassRule
    public static SingleInstancePostgresRule tickleDB = EmbeddedPostgresRules.singleInstance();

    @Override
    public JpaTestEnvironment setup() {
        migrateDatabase(tickleDB.getEmbeddedPostgres().getPostgresDatabase());
        return new JpaTestEnvironment((PGSimpleDataSource) tickleDB.getEmbeddedPostgres().getPostgresDatabase(),
                "tickleRepoIT");
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

    TickleRepo tickleRepo;

    @Before
    public void tickleRepo() {
        tickleRepo = new TickleRepo();
        tickleRepo.entityManager = env().getEntityManager();
    }

    @Test
    public void gettingBatchRecordsFromNonExistingBatchReturnsEmptyResultSet() {
        final Batch batch = new Batch()
                .withId(42);

        env().getPersistenceContext().run(() -> {
            int recordsInBatch = 0;
            try (TickleRepo.ResultSet<Record> rs = tickleRepo.getRecordsInBatch(batch)) {
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
            try (TickleRepo.ResultSet<Record> rs = tickleRepo.getRecordsInBatch(batch)) {
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

        Batch batchCreated = env().getPersistenceContext().run(() -> tickleRepo.createBatch(batch));

        assertThat("batch ID", batchCreated.getId(), is(6));
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
            try (TickleRepo.ResultSet<Record> rs = tickleRepo.getRecordsInBatch(
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

        Batch batchCreated = env().getPersistenceContext().run(() -> tickleRepo.createBatch(batch));

        assertThat("batch ID", batchCreated.getId(), is(6));
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
            try (TickleRepo.ResultSet<Record> rs = tickleRepo.getRecordsInBatch(
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
    public void creatingBatchWithMetadata() throws JSONBException {
        final JSONBContext jsonbContext = new JSONBContext();
        final Metadata metadata = new Metadata(42, "test");

        final Batch batch = new Batch()
                .withBatchKey(1000004)
                .withType(Batch.Type.INCREMENTAL)
                .withDataset(env().getEntityManager().find(DataSet.class, 1).getId())
                .withMetadata(jsonbContext.marshall(metadata));

        Batch batchCreated = env().getPersistenceContext().run(() -> tickleRepo.createBatch(batch));

        assertThat(jsonbContext.unmarshall(batchCreated.getMetadata(), Metadata.class), is(metadata));
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

        env().getPersistenceContext().run(() -> tickleRepo.closeBatch(batch));

        env().getPersistenceContext().run(() -> {
            try (TickleRepo.ResultSet<Record> rs = tickleRepo.getRecordsInBatch(batch)) {
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

        env().getPersistenceContext().run(() -> tickleRepo.closeBatch(batch));

        env().getPersistenceContext().run(() -> {
            try (TickleRepo.ResultSet<Record> rs = tickleRepo.getRecordsInBatch(batch)) {
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

        assertThat(tickleRepo.getNextBatch(batch2).orElse(null).getId(), is(batch3.getId()));
    }

    @Test
    public void gettingNextBatchWhenNotCompleted() {
        final Batch batch2 = env().getEntityManager().find(Batch.class, 2);
        assertThat(tickleRepo.getNextBatch(batch2).isPresent(), is(false));
    }

    @Test
    public void gettingNextBatchWhenNoneExist() {
        final Batch batch3 = env().getEntityManager().find(Batch.class, 3);
        assertThat(tickleRepo.getNextBatch(batch3).isPresent(), is(false));
    }

    @Test
    public void lookingUpBatchWhenPlaceholderValueIsEmpty() {
        assertThat(tickleRepo.lookupBatch(new Batch()).isPresent(), is(false));
    }

    @Test
    public void lookingUpBatchById() {
        final Batch batch = new Batch().withId(1);
        assertThat(tickleRepo.lookupBatch(batch).orElse(null).getBatchKey(), is(1000001));
    }

    @Test
    public void lookingUpBatchByKey() {
        final Batch batch = new Batch().withBatchKey(1000001);
        assertThat(tickleRepo.lookupBatch(batch).orElse(null).getId(), is(1));
    }

    @Test
    public void lookingUpRecordWhenPlaceholderValueIsEmpty() {
        assertThat(tickleRepo.lookupRecord(new Record()).isPresent(), is(false));
    }

    @Test
    public void lookingUpRecordWhenPlaceholderValueIsIncomplete() {
        final Record record = new Record().withLocalId("local1_1_!");
        assertThat(tickleRepo.lookupRecord(record).isPresent(), is(false));
    }

    @Test
    public void lookingUpRecordById() {
        final Record record = new Record().withId(1);
        assertThat(tickleRepo.lookupRecord(record).orElse(null).getLocalId(), is("local1_1_1"));
    }

    @Test
    public void lookingUpRecordByDatasetAndLocalId() {
        final Record record = new Record().withDataset(1).withLocalId("local1_1_1");
        assertThat(tickleRepo.lookupRecord(record).orElse(null).getId(), is(1));
    }

    @Test
    public void lookingUpRecordByIdRefreshed() throws SQLException {
        final Record record = new Record().withId(1);
        final Record found = tickleRepo.lookupRecord(record).orElse(null);
        assertThat(found.getLocalId(), is("local1_1_1"));
        assertThat(new String(found.getContent()), is("data1_1_1"));

        try (Connection conn = env().getDatasource().getConnection();
             Statement statement = conn.createStatement()) {
            statement.executeUpdate("UPDATE record SET content = 'data1_1_1_updated' WHERE id = 1");
        }

        final Record updatedRecord = new Record().withId(1);
        final Record updatedFound = tickleRepo.lookupRecord(updatedRecord).orElse(null);
        assertThat(updatedFound.getLocalId(), is("local1_1_1"));
        assertThat(new String(updatedFound.getContent()), is("data1_1_1_updated"));
    }

    @Test
    public void lookingUpRecordByDatasetAndLocalIdRefreshed() throws SQLException {
        final Record record = new Record().withDataset(1).withLocalId("local1_1_1");
        final Record found = tickleRepo.lookupRecord(record).orElse(null);
        assertThat(found.getId(), is(1));
        assertThat(new String(found.getContent()), is("data1_1_1"));

        try (Connection conn = env().getDatasource().getConnection();
             Statement statement = conn.createStatement()) {
            statement.executeUpdate("UPDATE record SET content = 'data1_1_1_updated' WHERE id = 1");
        }

        final Record updatedRecord = new Record().withDataset(1).withLocalId("local1_1_1");
        final Record updatedFound = tickleRepo.lookupRecord(updatedRecord).orElse(null);
        assertThat(updatedFound.getId(), is(1));
        assertThat(new String(updatedFound.getContent()), is("data1_1_1_updated"));
    }

    @Test
    public void getDataSetSummary() {
        List<DataSetSummary> summary = tickleRepo.getDataSetSummary();

        assertThat(summary.size(), is(4));

        assertThat(summary.get(0).getName(), is("dataset1"));
        assertThat(summary.get(0).getSum(), is(10L));
        assertThat(summary.get(0).getActive(), is(9L));
        assertThat(summary.get(0).getDeleted(), is(1L));
        assertThat(summary.get(0).getReset(), is(0L));
        assertThat(summary.get(0).getTimeOfLastModification(), is(nullValue()));
        assertThat(summary.get(0).getBatchId(), is(1));

        assertThat(summary.get(1).getName(), is("dataset2"));
        assertThat(summary.get(1).getSum(), is(20L));
        assertThat(summary.get(1).getActive(), is(9L));
        assertThat(summary.get(1).getDeleted(), is(6L));
        assertThat(summary.get(1).getReset(), is(5L));
        assertThat(summary.get(1).getTimeOfLastModification(), is(nullValue()));
        assertThat(summary.get(1).getBatchId(), is(3));

        assertThat(summary.get(2).getName(), is("dataset3"));
        assertThat(summary.get(2).getSum(), is(1L));
        assertThat(summary.get(2).getActive(), is(1L));
        assertThat(summary.get(2).getDeleted(), is(0L));
        assertThat(summary.get(2).getReset(), is(0L));
        assertThat(summary.get(2).getTimeOfLastModification(), is(nullValue()));
        assertThat(summary.get(2).getBatchId(), is(4));

        assertThat(summary.get(3).getName(), is("dataset4"));
        assertThat(summary.get(3).getSum(), is(1L));
        assertThat(summary.get(3).getActive(), is(1L));
        assertThat(summary.get(3).getDeleted(), is(0L));
        assertThat(summary.get(3).getReset(), is(0L));
        assertThat(summary.get(3).getTimeOfLastModification(), is(nullValue()));
        assertThat(summary.get(3).getBatchId(), is(5));
    }

    @Test
    public void getDataSetSummaryByDataSetIdNonExistingDataSet() {
        DataSetSummary summary = tickleRepo.getDataSetSummaryByDataSetId(21);
        assertThat(summary, is(org.hamcrest.CoreMatchers.nullValue()));
    }

    @Test
    public void getDataSetSummaryByDataSetId() {
        DataSetSummary summary = tickleRepo.getDataSetSummaryByDataSetId(3);

        assertThat(summary.getName(), is("dataset3"));
        assertThat(summary.getSum(), is(1L));
        assertThat(summary.getActive(), is(1L));
        assertThat(summary.getDeleted(), is(0L));
        assertThat(summary.getReset(), is(0L));
        assertThat(summary.getTimeOfLastModification(), is(nullValue()));
        assertThat(summary.getBatchId(), is(4));
    }

    @Test
    public void getDataSetsBySubmitter() {
        List<DataSet> datasets = tickleRepo.getDataSetsBySubmitter(123458);

        assertThat(datasets.size(), is(1));

        assertThat(datasets.get(0).getName(), is("dataset3"));
        assertThat(datasets.get(0).getDisplayName(), is("displayname3"));
        assertThat(datasets.get(0).getId(), is(3));
        assertThat(datasets.get(0).getAgencyId(), is(123458));
    }

    @Test
    public void lookupDataSetById_notPersisted_returnsOptionalEmpty() {
        Optional<DataSet> dataSetOptional = tickleRepo.lookupDataSet(new DataSet().withId(42));
        assertThat("DataSet not present", dataSetOptional.isPresent(), is (false));
    }

    @Test
    public void lookupDataSetByName_notPersisted_returnsOptionalEmpty() {
        Optional<DataSet> dataSetOptional = tickleRepo.lookupDataSet(new DataSet().withName("datasetnotfound"));
        assertThat("DataSet not present", dataSetOptional.isPresent(), is (false));
    }

    @Test
    public void lookupDataSet_findsPersistedThroughId_returnsPersistedDataSet() {
        DataSet dataSet = new DataSet().withId(2);
        Optional<DataSet> dataSetOptional = tickleRepo.lookupDataSet(dataSet);
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
        Optional<DataSet> dataSetOptional = tickleRepo.lookupDataSet(dataSet);
        assertThat(dataSetOptional.isPresent(), is (true));
        DataSet persisted = dataSetOptional.get();
        assertThat("dataSet ID", persisted.getId(), is(1));
        assertThat("dataSet name", persisted.getName(), is("dataset1"));
        assertThat("dataSet agencyId", persisted.getAgencyId(), is(123456));
        assertThat("dataSet displayName",persisted.getDisplayName(), is("displayname1"));
    }

    @Test
    public void lookupDataSetByRecordLocalId_SingleDataSet() {
        Record record = new Record().withLocalId("local1_1_3");

        List<DataSet> dataSets = tickleRepo.lookupDataSetByRecord(record);

        assertThat(dataSets.size(), is(1));
        assertThat(dataSets.get(0).getDisplayName(), is("displayname1"));
    }

    @Test
    public void lookupDataSetByRecordLocalId_NoDataSet() {
        Record record = new Record().withLocalId("doesn't exist");

        List<DataSet> dataSets = tickleRepo.lookupDataSetByRecord(record);

        assertThat(dataSets.size(), is(0));
    }

    @Test
    public void lookupDataSetByRecordLocalId_MultipleDataSets() {
        Record record = new Record().withLocalId("local_match");

        List<DataSet> dataSets = tickleRepo.lookupDataSetByRecord(record);

        assertThat(dataSets.size(), is(2));
        assertThat(dataSets.get(0).getDisplayName(), is("displayname3"));
        assertThat(dataSets.get(1).getDisplayName(), is("displayname4"));
    }

    @Test
    public void createDataSet_returns() {
        final DataSet persisted = env().getPersistenceContext().run(() ->
                tickleRepo.createDataSet(new DataSet().withName("dataset6").withAgencyId(123460)));
        assertThat("dataSet ID", persisted.getId(), is(5));
        assertThat("dataSet name", persisted.getName(), is("dataset6"));
        assertThat("dataSet agencyId", persisted.getAgencyId(), is(123460));
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
            try (TickleRepo.ResultSet<Record> rs = tickleRepo.getRecordsInBatch(batch)) {
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
            try (TickleRepo.ResultSet<Record> rs = tickleRepo.getRecordsInDataSet(dataSet)) {
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
            try (TickleRepo.ResultSet<Record> rs = tickleRepo.getRecordsInDataSet(dataSet)) {
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
        assertThat(tickleRepo.estimateSizeOf(dataSet), is(10));
    }

    @Test
    public void deleteOutdatedRecords() {
        final Set<Integer> expectedRecords = new HashSet<>();

        final DataSet dataSet = env().getEntityManager().find(DataSet.class, 1);

        /* Force two records from the dataset to have a
           timeOfLastModification value in the past and
           remember their IDs. */
        env().getPersistenceContext().run(() -> {
            try (TickleRepo.ResultSet<Record> rs = tickleRepo.getRecordsInDataSet(dataSet)) {
                final Query updateTimeOfLastModification = env().getEntityManager()
                        .createNativeQuery("UPDATE record SET timeOfLastModification = ?1 WHERE id = ?2")
                        .setParameter(1, Timestamp.from(
                                Instant.now().minus(2, ChronoUnit.DAYS)));
                for (Record record : rs) {
                    if (expectedRecords.size() == 2) {
                        break;
                    }
                    updateTimeOfLastModification.setParameter(2, record.getId());
                    updateTimeOfLastModification.executeUpdate();
                    expectedRecords.add(record.getId());
                }
            }
        });

        /* Create a batch for which to delete outdated records. */
        final Batch batch = env().getPersistenceContext().run(()
                -> tickleRepo.createBatch(new Batch()
                        .withBatchKey(42)
                        .withType(Batch.Type.INCREMENTAL)
                        .withDataset(dataSet.getId())));

        /* Call deleteOutdatedRecordsInBatch while ensuring that
           the two records modified above are included. */
        env().getPersistenceContext().run(() -> tickleRepo.deleteOutdatedRecordsInBatch(
                batch, Instant.now().minus(1, ChronoUnit.DAYS)));

        /* Verify the expected records. */
        int numberOfRecordsInBatch = 0;
        for (Record record : env().getPersistenceContext().run(
                        () -> tickleRepo.getRecordsInBatch(batch))) {
            assertThat("expected set of records contains ID " + record.getId(),
                    expectedRecords.contains(record.getId()), is(true));
            numberOfRecordsInBatch++;
        }
        assertThat("number of outdated records",
                numberOfRecordsInBatch, is(expectedRecords.size()));
    }

    private void migrateDatabase(DataSource dataSource) {
        final TickleRepoDatabaseMigrator dbMigrator = new TickleRepoDatabaseMigrator(dataSource);
        dbMigrator.migrate();
    }

    private static class Metadata {
        private final int id;
        private final String value;

        @JsonCreator
        Metadata(
                @JsonProperty("id") int id,
                @JsonProperty("value") String value) {
            this.id = id;
            this.value = value;
        }

        public int getId() {
            return id;
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Metadata metadata = (Metadata) o;

            if (id != metadata.id) {
                return false;
            }
            return Objects.equals(value, metadata.value);
        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Metadata{" +
                    "id=" + id +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}

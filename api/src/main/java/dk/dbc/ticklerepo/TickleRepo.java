/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo;

import dk.dbc.ticklerepo.dto.Batch;
import dk.dbc.ticklerepo.dto.DataSet;
import dk.dbc.ticklerepo.dto.DataSetSummary;
import dk.dbc.ticklerepo.dto.Record;
import dk.dbc.ticklerepo.dto.RecordStatusConverter;
import jakarta.ejb.Stateless;
import jakarta.ejb.TransactionAttribute;
import jakarta.ejb.TransactionAttributeType;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Parameter;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import org.eclipse.persistence.config.QueryHints;
import org.eclipse.persistence.internal.jpa.EJBQueryImpl;
import org.eclipse.persistence.jpa.JpaEntityManager;
import org.eclipse.persistence.queries.DatabaseQuery;
import org.eclipse.persistence.sessions.DatabaseRecord;
import org.eclipse.persistence.sessions.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class contains the tickle repository API
 */
@Stateless
public class TickleRepo {
    private static final Logger LOGGER = LoggerFactory.getLogger(TickleRepo.class);
    private static final int DATASET_SIZE_ESTIMATE_THRESHOLD = 1000000;

    private static final String GET_DATASET_SUMMARY_QUERY = "SELECT NEW dk.dbc.ticklerepo.dto.DataSetSummary(d.name," +
            " COUNT(r)," +
            " SUM(CASE WHEN r.status = dk.dbc.ticklerepo.dto.Record.Status.ACTIVE THEN 1 ELSE 0 END)," +
            " SUM(CASE WHEN r.status = dk.dbc.ticklerepo.dto.Record.Status.DELETED THEN 1 ELSE 0 END)," +
            " SUM(CASE WHEN r.status = dk.dbc.ticklerepo.dto.Record.Status.RESET THEN 1 ELSE 0 END)," +
            " MAX(r.timeOfLastModification), " +
            " MAX(r.batch))" +
            " FROM Record r, DataSet d" +
            " WHERE d.id = r.dataset" +
            " GROUP BY d.name";

    private static final String GET_DATASET_SUMMARY_BY_DATASET_ID_QUERY = "SELECT NEW dk.dbc.ticklerepo.dto.DataSetSummary(d.name," +
            " COUNT(1)," +
            " SUM(CASE WHEN r.status = dk.dbc.ticklerepo.dto.Record.Status.ACTIVE THEN 1 ELSE 0 END)," +
            " SUM(CASE WHEN r.status = dk.dbc.ticklerepo.dto.Record.Status.DELETED THEN 1 ELSE 0 END)," +
            " SUM(CASE WHEN r.status = dk.dbc.ticklerepo.dto.Record.Status.RESET THEN 1 ELSE 0 END)," +
            " MAX(r.timeOfLastModification), " +
            " MAX(r.batch))" +
            " FROM Record r, DataSet d" +
            " WHERE d.id = ?1 AND d.id = r.dataset" +
            " GROUP BY d.name";

    private static final String GET_DATASETS_BY_SUBMITTER_QUERY = "SELECT dataset FROM DataSet dataSet WHERE dataset.agencyId = ?1";

    @PersistenceContext(unitName = "tickleRepoPU")
    EntityManager entityManager;

    public TickleRepo() {
    }

    public TickleRepo(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    /**
     * Persists given batch.
     * <p>
     * Batches of type TOTAL will also have their records marked,
     * meaning any record remaining in the dataset with a status of ACTIVE
     * will have its status set to RESET.
     * </p>
     *
     * @param batch batch to create
     * @return managed Batch object
     */
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    public Batch createBatch(Batch batch) {
        entityManager.persist(batch);
        entityManager.flush();
        entityManager.refresh(batch);
        if (batch.getType() == Batch.Type.TOTAL) {
            LOGGER.info("{} records marked by batch {}", mark(batch), batch);
        }
        return batch;
    }

    /**
     * Closes given batch by setting its time-of-completion.
     * <p>
     * Batches of type TOTAL will also have their records swept,
     * meaning any record remaining in the dataset with a status of RESET
     * will have its status set to DELETED and its batch ID updated
     * to that of the given batch.
     * </p>
     *
     * @param batch batch to close
     */
    public Batch closeBatch(Batch batch) {
        batch = entityManager.merge(batch);
        if (batch.getType() == Batch.Type.TOTAL) {
            LOGGER.info("{} records swept for batch {}", sweep(batch), batch);
        }
        batch.withTimeOfCompletion(new Timestamp(new Date().getTime()));
        return batch;
    }

    /**
     * Aborts given batch by setting its time-of-completion.
     * <p>
     * Batches of type TOTAL will also have their remaining sweep markers undone,
     * meaning any record remaining in the dataset with a status of RESET
     * will have its status set back to ACTIVE and its batch ID left untouched.
     * </p>
     *
     * @param batch batch to abort
     */
    public Batch abortBatch(Batch batch) {
        if (batch.getType() == Batch.Type.TOTAL) {
            LOGGER.info("{} marks undone for batch {}", undoMark(batch), batch);
        }
        return closeBatch(batch);
    }

    /**
     * Returns next batch compared to last batch seen if it is completed
     *
     * @param lastSeenBatch last seen batch for a dataset
     * @return next available batch
     */
    public Optional<Batch> getNextBatch(Batch lastSeenBatch) {
        /* The eclipselink.refresh hint below breaks portability, the
           alternative is to do a refresh on each entity returned, but
           this entails suboptimal performance.
           Note: javax.persistence.cache.retrieveMode hint does not seem to work currently */
        return entityManager.createNamedQuery(Batch.GET_NEXT_BATCH_QUERY_NAME, Batch.class)
                .setHint("eclipselink.refresh", true)
                .setParameter("lastSeenId", lastSeenBatch.getId())
                .setParameter("dataset", lastSeenBatch.getDataset())
                .setMaxResults(1)
                .getResultList()
                .stream()
                .filter(batch -> batch.getTimeOfCompletion() != null)
                .findFirst();
    }

    /**
     * Changes the status of records in the dataset to DELETED if their time
     * of last modification is before given cut-off time and updates the
     * batch ID of these records to that of the given batch.
     *
     * @param batch      batch for which to delete outdated records
     * @param cutOffTime threshold for outdated records
     */
    public void deleteOutdatedRecordsInBatch(Batch batch, Instant cutOffTime) {
        LOGGER.info("Deleted {} outdated records in dataset {} batch {}",
                batch.getDataset(), batch.getId(),
                entityManager.createNamedQuery(Record.SWEEP_OUTDATED_QUERY_NAME)
                        .setParameter("batch", batch.getId())
                        .setParameter("dataset", batch.getDataset())
                        .setParameter("now", new Date())
                        .setParameter("cutOffTime", Timestamp.from(cutOffTime))
                        .executeUpdate());
    }

    /**
     * Returns iterator for all records belonging to given batch
     * <p>
     * This method needs to run in a transaction.
     * </p>
     *
     * @param batch batch
     * @return batch iterator as ResultSet abstraction
     */
    public ResultSet<Record> getRecordsInBatch(Batch batch) {
        final Query query = entityManager.createNamedQuery(Record.GET_RECORDS_IN_BATCH_QUERY_NAME)
                .setParameter(1, batch.getId());
        return new ResultSet<>(query, new RecordMapping());
    }

    /**
     * Returns iterator for all records belonging to given data set
     * <p>
     * This method needs to run in a transaction.
     * </p>
     *
     * @param dataSet data set
     * @return batch iterator as ResultSet abstraction
     */
    public ResultSet<Record> getRecordsInDataSet(DataSet dataSet) {
        final Query query = entityManager.createNamedQuery(Record.GET_RECORDS_IN_DATASET_QUERY_NAME)
                .setParameter(1, dataSet.getId());
        return new ResultSet<>(query, new RecordMapping());
    }

    /**
     * Tries to lookup batch in repository either by batch ID or by batch key
     *
     * @param value values placeholder
     * @return managed Batch object if found
     */
    public Optional<Batch> lookupBatch(Batch value) {
        return lookupBatch(value, false);
    }


    public Optional<Batch> lookupBatch(Batch value, boolean readOnly) {
        if (value != null) {
            if (value.getId() > 0) {
                return Optional.ofNullable(entityManager.find(Batch.class, value.getId(), Map.of(QueryHints.READ_ONLY, readOnly)));
            } else if (value.getBatchKey() > 0) {
                return entityManager.createNamedQuery(Batch.GET_BATCH_BY_KEY_QUERY_NAME, Batch.class)
                        .setParameter("key", value.getBatchKey())
                        .setMaxResults(1)
                        .getResultList()
                        .stream()
                        .findFirst();
            }
        }
        return Optional.empty();
    }

    /**
     * Lookup a list of records by their localId, belonging to a dataset
     */
    public List<Record> lookupRecords(int dataset, List<String> localIds) {
        if(localIds.isEmpty()) return List.of();
        return entityManager.createNamedQuery(Record.GET_RECORDS_BY_LOCALIDS_QUERY_NAME, Record.class)
                .setParameter("dataset", dataset)
                .setParameter("localIds", localIds)
                .getResultList();
    }

    /**
     * Tries to lookup record in repository either by record ID or by (dataset,localId) combination
     *
     * @param value values placeholder
     * @return managed Record object if found
     */
    public Optional<Record> lookupRecord(Record value) {
        Optional<Record> record = Optional.empty();
        if (value != null) {
            if (value.getId() > 0) {
                record = Optional.ofNullable(entityManager.find(Record.class, value.getId()));
            } else if (value.getLocalId() != null && value.getDataset() > 0) {
                record = entityManager.createNamedQuery(Record.GET_RECORD_BY_LOCALID_QUERY_NAME, Record.class)
                        .setParameter("dataset", value.getDataset())
                        .setParameter("localId", value.getLocalId())
                        .setMaxResults(1)
                        .getResultList()
                        .stream()
                        .findFirst();
            }
        }

        // Other systems may update the record so we need to refresh the object to make sure the current
        // version is returned.
        if( record.isPresent() && record.get() != null ) {
            entityManager.refresh(record.get());
        }

        return record;
    }

    public List<DataSetSummary> getDataSetSummary() {
        return entityManager.createQuery(GET_DATASET_SUMMARY_QUERY, DataSetSummary.class)
                .getResultList();
    }

    public DataSetSummary getDataSetSummaryByDataSetId(int dataSetId) {
        return entityManager.createQuery(GET_DATASET_SUMMARY_BY_DATASET_ID_QUERY, DataSetSummary.class)
                .setParameter(1, dataSetId)
                .getResultList()
                .stream()
                .findFirst()
                .orElse(null);
    }

    public List<DataSet> getDataSetsBySubmitter(int submitter) {
        return entityManager.createQuery(GET_DATASETS_BY_SUBMITTER_QUERY, DataSet.class)
                .setParameter(1, submitter)
                .getResultList();
    }

    private int mark(Batch batch) {
        return entityManager.createNamedQuery(Record.MARK_QUERY_NAME)
                .setParameter("dataset", batch.getDataset())
                .executeUpdate();
    }

    private int sweep(Batch batch) {
        return entityManager.createNamedQuery(Record.SWEEP_QUERY_NAME)
                .setParameter("batch", batch.getId())
                .setParameter("dataset", batch.getDataset())
                .setParameter("now", new Date())
                .executeUpdate();
    }

    private int undoMark(Batch batch) {
        return entityManager.createNamedQuery(Record.UNDO_MARK_QUERY_NAME)
                .setParameter("dataset", batch.getDataset())
                .executeUpdate();
    }

    /**
     * This class represents a one-time iteration of a tickle repository
     * result set of non-managed entities
     * <p>
     * Note that only positional bind parameters are supported.
     * </p>
     */
    public class ResultSet<T> implements Iterable<T>, AutoCloseable {
        private final int BUFFER_SIZE = 1000;

        private final PreparedStatement statement;
        private final java.sql.ResultSet resultSet;
        private final Function<java.sql.ResultSet, T> resultSetMapping;
        private final boolean hasRows;

        ResultSet(Query query, Function<java.sql.ResultSet, T> resultSetMapping) {
            try {
                this.statement = createStatement(query);
                this.resultSet = statement.executeQuery();
                this.resultSetMapping = resultSetMapping;
                // This may not be supported by all drivers and/or query types
                this.hasRows = resultSet.isBeforeFirst();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }

        private PreparedStatement createStatement(Query query) {
            /*
                Yes we are breaking general JPA compatibility here but we need
                to be able to handle very large result sets without exhausting
                the main memory.

                Both CursoredStream and ScrollableCursor solutions have been
                tested, but it seems like the PostgreSQL JDBC driver insists
                on pulling in the entire result set upfront nonetheless.
            */

            final Session session = entityManager.unwrap(JpaEntityManager.class).getActiveSession();
            final DatabaseQuery databaseQuery = query.unwrap(EJBQueryImpl.class).getDatabaseQuery();
            databaseQuery.prepareCall(session, new DatabaseRecord());
            String queryString = databaseQuery.getSQLString();
            final int limit = query.getMaxResults();
            if (limit > 0 && limit != Integer.MAX_VALUE) {
                queryString += " LIMIT " + limit;
            }
            final int offset = query.getFirstResult();
            if (offset > 0) {
                queryString += " OFFSET " + offset;
            }
            LOGGER.info(queryString);

            final Connection connection = entityManager.unwrap(Connection.class);
            if (connection == null) {
                throw new IllegalStateException("Connection is null - maybe not in scope of a transaction?");
            }
            try {
                final PreparedStatement statement = connection.prepareStatement(queryString);
                statement.setFetchSize(BUFFER_SIZE);
                final Set<Parameter<?>> parameters = query.getParameters();
                for (Parameter<?> parameter : parameters) {
                    if (parameter.getName() != null) {
                        throw new IllegalStateException(
                                "This query must only have positional parameters '" +
                                        parameter.getName() + "' was named");
                    }
                    statement.setObject(parameter.getPosition(),
                            query.getParameterValue(parameter.getPosition()));
                }
                return statement;
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }

        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    try {
                        return hasRows && !resultSet.isLast();
                    } catch (SQLException e) {
                        throw new PersistenceException(e);
                    }
                }

                @Override
                public T next() {
                    try {
                        if (resultSet.next()) {
                            return resultSetMapping.apply(resultSet);
                        }
                        return null;
                    } catch (SQLException e) {
                        throw new PersistenceException(e);
                    }
                }
            };
        }

        @Override
        public void close() {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
    }

    /**
     * Maps result set of an SQL query to a {@link Record}
     */
    private static class RecordMapping implements Function<java.sql.ResultSet, Record> {
        private final RecordStatusConverter recordStatusConverter = new RecordStatusConverter();

        @Override
        public Record apply(java.sql.ResultSet resultSet) {
            if (resultSet != null) {
                try {
                    return new Record()
                            .withId(resultSet.getInt("ID"))
                            .withBatch(resultSet.getInt("BATCH"))
                            .withChecksum(resultSet.getString("CHECKSUM"))
                            .withContent(resultSet.getBytes("CONTENT"))
                            .withDataset(resultSet.getInt("DATASET"))
                            .withLocalId(resultSet.getString("LOCALID"))
                            .withStatus(recordStatusConverter.convertToEntityAttribute(
                                    resultSet.getString("STATUS")))
                            .withTimeOfCreation(resultSet.getTimestamp("TIMEOFCREATION"))
                            .withTimeOfLastModification(resultSet.getTimestamp("TIMEOFLASTMODIFICATION"))
                            .withTrackingId(resultSet.getString("TRACKINGID"));
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
            return null;
        }
    }

    /**
     * checks if the given dataSet is persisted in the underlying database
     *
     * @param dataset to search for
     * @return Optional.empty() if the dataSet is not persisted, otherwise the persisted dataSet.
     */
    public Optional<DataSet> lookupDataSet(DataSet dataset) {
        if (dataset != null) {
            if (dataset.getId() > 0) {
                return Optional.ofNullable(entityManager.find(DataSet.class, dataset.getId()));
            } else if (dataset.getName() != null) {
                return entityManager.createNamedQuery(DataSet.GET_DATASET_BY_NAME_QUERY_NAME, DataSet.class)
                        .setParameter("name", dataset.getName())
                        .setMaxResults(1)
                        .getResultList()
                        .stream()
                        .findFirst();
            }
        }
        return Optional.empty();
    }

    public List<DataSet> lookupDataSetByRecord(Record record) {
        if (record != null && record.getLocalId() != null) {
            return new ArrayList<>(entityManager.createNamedQuery(DataSet.GET_DATASET_BY_RECORD_LOCALID_NAME, DataSet.class)
                    .setParameter("localId", record.getLocalId())
                    .getResultList());
        }

        return Collections.emptyList();
    }

    /**
     * Returns an estimate of the number of records in the given dataset or
     * the exact number if the estimate is below {@link #DATASET_SIZE_ESTIMATE_THRESHOLD}
     *
     * @param dataSet dataset
     * @return estimated number of records
     */
    public int estimateSizeOf(DataSet dataSet) {
        if (dataSet != null) {
            final Optional<String> estimate = entityManager.createNamedQuery(
                    Record.ESTIMATED_NUMBER_OF_RECORDS_IN_DATASET_QUERY_NAME, String.class)
                    .setParameter(1, dataSet.getId())
                    .getResultList().stream()
                    .filter(row -> row.contains(" on record "))
                    .findFirst();
            if (estimate.isPresent()) {
                final Pattern rowsPattern = Pattern.compile(" rows=(\\d+) ");
                final Matcher rowsMatcher = rowsPattern.matcher(estimate.get());
                if (rowsMatcher.find()) {
                    final int sizeEstimate = Integer.parseInt(rowsMatcher.group(1));
                    if (sizeEstimate < DATASET_SIZE_ESTIMATE_THRESHOLD) {
                        return sizeOf(dataSet);
                    }
                    return sizeEstimate;
                }
            }
        }
        return 0;
    }

    /**
     * Returns the number of records in the given dataset
     *
     * @param dataSet dataset
     * @return number of records
     */
    public int sizeOf(DataSet dataSet) {
        if (dataSet != null) {
            return Math.toIntExact(entityManager.createNamedQuery(
                    Record.NUMBER_OF_RECORDS_IN_DATASET_QUERY_NAME, Long.class)
                    .setParameter(1, dataSet.getId())
                    .getSingleResult());
        }
        return 0;
    }

    /**
     * Persists the dataSet given as input
     *
     * @param dataSet to persist
     * @return persisted dataSet
     */
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    public DataSet createDataSet(DataSet dataSet) {
        entityManager.persist(dataSet);
        entityManager.flush();
        entityManager.refresh(dataSet);
        return dataSet;
    }

    public EntityManager getEntityManager() {
        return entityManager;
    }
}

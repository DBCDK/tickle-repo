/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo;

import dk.dbc.ticklerepo.dto.Batch;
import dk.dbc.ticklerepo.dto.DataSet;
import dk.dbc.ticklerepo.dto.Record;
import dk.dbc.ticklerepo.dto.RecordStatusConverter;
import org.eclipse.persistence.internal.jpa.EJBQueryImpl;
import org.eclipse.persistence.jpa.JpaEntityManager;
import org.eclipse.persistence.queries.DatabaseQuery;
import org.eclipse.persistence.sessions.DatabaseRecord;
import org.eclipse.persistence.sessions.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.Parameter;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
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

    @PersistenceContext(unitName = "tickleRepoPU")
    EntityManager entityManager;

    public TickleRepo() {}

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
     * @param batch batch to close
     */
    public void closeBatch(Batch batch) {
        batch = entityManager.merge(batch);
        if (batch.getType() == Batch.Type.TOTAL) {
            LOGGER.info("{} records swept for batch {}", sweep(batch), batch);
        }
        batch.withTimeOfCompletion(new Timestamp(new Date().getTime()));
    }

    /**
     * Aborts given batch by setting its time-of-completion.
     * <p>
     * Batches of type TOTAL will also have their remaining sweep markers undone,
     * meaning any record remaining in the dataset with a status of RESET
     * will have its status set back to ACTIVE and its batch ID left untouched.
     * </p>
     * @param batch batch to abort
     */
    public void abortBatch(Batch batch) {
        if (batch.getType() == Batch.Type.TOTAL) {
            LOGGER.info("{} marks undone for batch {}", undoMark(batch), batch);
        }
        closeBatch(batch);
    }

    /**
     * Returns next batch compared to last batch seen if it is completed
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
     * Returns iterator for all records belonging to given batch
     * <p>
     * This method needs to run in a transaction.
     * </p>
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
     @param dataSet data set
    * @return batch iterator as ResultSet abstraction
    */
    public ResultSet<Record> getRecordsInDataSet(DataSet dataSet) {
        final Query query = entityManager.createNamedQuery(Record.GET_RECORDS_IN_DATASET_QUERY_NAME)
                .setParameter(1, dataSet.getId());
        return new ResultSet<>(query, new RecordMapping());
    }

    /**
     * Tries to lookup batch in repository either by batch ID or by batch key
     * @param value values placeholder
     * @return managed Batch object if found
     */
    public Optional<Batch> lookupBatch(Batch value) {
        if (value != null) {
            if (value.getId() > 0) {
                return Optional.ofNullable(entityManager.find(Batch.class, value.getId()));
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
     * Tries to lookup record in repository either by record ID or by (dataset,localId) combination
     * @param value values placeholder
     * @return managed Record object if found
     */
    public Optional<Record> lookupRecord(Record value) {
        if (value != null) {
            if (value.getId() > 0) {
                return Optional.ofNullable(entityManager.find(Record.class, value.getId()));
            } else if (value.getLocalId() != null && value.getDataset() > 0) {
                return entityManager.createNamedQuery(Record.GET_RECORD_BY_LOCALID_QUERY_NAME, Record.class)
                        .setParameter("dataset", value.getDataset())
                        .setParameter("localId", value.getLocalId())
                        .setMaxResults(1)
                        .getResultList()
                        .stream()
                        .findFirst();
            }
        }
        return Optional.empty();
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

    /**
     * Returns an estimate of the number of records in the given dataset
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
                    return Integer.parseInt(rowsMatcher.group(1));
                }
            }
        }
        return 0;
    }

    /**
     * Persists the dataSet given as input
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

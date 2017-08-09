/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo;

import dk.dbc.ticklerepo.dto.Batch;
import dk.dbc.ticklerepo.dto.DataSet;
import dk.dbc.ticklerepo.dto.Record;
import org.eclipse.persistence.config.HintValues;
import org.eclipse.persistence.config.QueryHints;
import org.eclipse.persistence.queries.CursoredStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;

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
     * @param batch batch
     * @return batch iterator as ResultSet abstraction
     */
    public ResultSet<Record> getRecordsInBatch(Batch batch) {
        final Query query = entityManager.createNamedQuery(Record.GET_RECORDS_IN_BATCH_QUERY_NAME)
                .setParameter("batch", batch.getId());
        return new ResultSet<>(query);
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
     * This class represents a one-time iteration of a tickle repository result set of non-managed entities.
     */
    public class ResultSet<T> implements Iterable<T>, AutoCloseable {
        private final int BUFFER_SIZE = 50;

        final CursoredStream cursor;

        ResultSet(Query query) {
            // Yes we are breaking general JPA compatibility using below QueryHints and CursoredStream,
            // but we need to be able to handle very large result sets.

            // Configures the query to return a CursoredStream, which is a stream of the JDBC ResultSet.
            query.setHint(QueryHints.CURSOR, HintValues.TRUE);
            // Configures the CursoredStream with the number of objects fetched from the stream on a next() call.
            query.setHint(QueryHints.CURSOR_PAGE_SIZE, BUFFER_SIZE);
            // Configures the JDBC fetch-size for the result set.
            query.setHint(QueryHints.JDBC_FETCH_SIZE, BUFFER_SIZE);
            // Configures the query to not use the shared cache and the transactional cache/persistence context.
            // Resulting objects will be read and built directly from the database, and not registered in the
            // persistence context. Changes made to the objects will not be updated unless merged and object identity
            // will not be maintained.
            // This is necessary to avoid OutOfMemoryError from very large persistence contexts.
            query.setHint(QueryHints.MAINTAIN_CACHE, HintValues.FALSE);
            cursor = (CursoredStream) query.getSingleResult();
        }

        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    return cursor.hasNext();
                }

                @Override
                @SuppressWarnings("unchecked")
                public T next() {
                    // To avoid OutOfMemoryError we occasionally need to clear the internal data structure of the
                    // CursoredStream.
                    if (cursor.getPosition() % BUFFER_SIZE == 0) {
                        cursor.clear();
                    }
                    return (T) cursor.next();
                }
            };
        }

        @Override
        public void close() {
            if (cursor != null) {
                cursor.close();
            }
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

/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo;

import dk.dbc.ticklerepo.dto.Batch;
import dk.dbc.ticklerepo.dto.DataSet;
import dk.dbc.ticklerepo.dto.Record;
import org.eclipse.persistence.queries.CursoredStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ejb.Stateless;
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
    public Batch createBatch(Batch batch) {
        if (batch.getType() == Batch.Type.TOTAL) {
            LOGGER.info("{} records marked by batch {}", mark(batch), batch);
        }
        entityManager.persist(batch);
        entityManager.flush();
        entityManager.refresh(batch);
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
        return entityManager.createNamedQuery(Batch.GET_NEXT_BATCH_QUERY_NAME, Batch.class)
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
                .executeUpdate();
    }

    private int undoMark(Batch batch) {
        return entityManager.createNamedQuery(Record.UNDO_MARK_QUERY_NAME)
                .setParameter("dataset", batch.getDataset())
                .executeUpdate();
    }

    /**
     * This class represents a one-time iteration of a tickle repository result set
     */
    public class ResultSet<T> implements Iterable<T>, AutoCloseable {
        final CursoredStream cursor;

        ResultSet(Query query) {
            // Yes we are breaking general JPA compatibility here,
            // but we need to be able to handle very large result sets.
            query.setHint("eclipselink.cursor", true);
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
        Optional<DataSet> dataSetOptional = Optional.empty();
        if (dataset.getId() > 0) {
            dataSetOptional = Optional.ofNullable(entityManager.find(DataSet.class, dataset.getId()));
        }
        else if (dataset.getName() != null) {
            final Query query = entityManager.createNamedQuery(DataSet.GET_DATASET_BY_NAME)
                    .setParameter("name", dataset.getName());
            if(query.getResultList().size() == 1) {
                dataSetOptional = Optional.ofNullable((DataSet) query.getResultList().get(0));
            }
        }
        return dataSetOptional;
    }

    /**
     * Persists the dataSet given as input
     * @param dataSet to persist
     * @return persisted dataSet
     */
    public DataSet createDataSet(DataSet dataSet) {
        entityManager.persist(dataSet);
        entityManager.flush();
        entityManager.refresh(dataSet);
        return dataSet;
    }
}

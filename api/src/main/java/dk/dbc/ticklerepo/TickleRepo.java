/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo;

import dk.dbc.ticklerepo.dto.Batch;
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
     * Returns iterator for all records belonging to given batch
     * @param batch batch
     * @return batch iterator as ResultSet abstraction
     */
    public ResultSet<Record> getRecordsInBatch(Batch batch) {
        final Query query = entityManager.createNamedQuery(Record.GET_RECORDS_IN_BATCH_QUERY_NAME)
                .setParameter("batch", batch.getId());
        return new ResultSet<>(query);
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
}

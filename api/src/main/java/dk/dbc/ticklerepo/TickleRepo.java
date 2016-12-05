/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

@Stateless
public class TickleRepo {
    private static final Logger LOGGER = LoggerFactory.getLogger(TickleRepo.class);

    @PersistenceContext(unitName = "tickleRepoPU")
    EntityManager entityManager;

    public TickleRepo() {
        LOGGER.debug("Entity manager injected as {}", entityManager);
    }
}

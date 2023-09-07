/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import javax.sql.DataSource;

@Startup
@Singleton
public class TickleRepoDatabaseMigrator {
    private static final Logger LOGGER = LoggerFactory.getLogger(TickleRepoDatabaseMigrator.class);

    @Resource(lookup = "jdbc/tickle-repo")
    DataSource dataSource;

    @SuppressWarnings("unused")
    public TickleRepoDatabaseMigrator() {
    }

    public TickleRepoDatabaseMigrator(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void migrate() {
        final Flyway flyway = Flyway.configure()
                .table("schema_version")
                .dataSource(dataSource)
                .baselineOnMigrate(true)
                .locations("classpath:dk/dbc/ticklerepo/db/migration")
                .load();
        for (MigrationInfo info : flyway.info().all()) {
            LOGGER.info("database migration {} : {} from file '{}'",
                    info.getVersion(), info.getDescription(), info.getScript());
        }
        flyway.migrate();
    }
}

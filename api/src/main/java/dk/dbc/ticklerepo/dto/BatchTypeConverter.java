/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import org.postgresql.util.PGobject;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.sql.SQLException;

@Converter
public class BatchTypeConverter implements AttributeConverter<Batch.Type, Object> {
    @Override
    public Object convertToDatabaseColumn(Batch.Type type) {
        String typeValue = null;
        if (type != null) {
            typeValue = type.name();
        }

        final PGobject pgObject = new PGobject();
        pgObject.setType("batch_type");
        try {
            pgObject.setValue(typeValue);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return pgObject;
    }

    @Override
    public Batch.Type convertToEntityAttribute(Object dbValue) {
        if (dbValue == null) {
            throw new IllegalArgumentException("dbValue can not be null");
        }
        switch ((String) dbValue) {
            case "INCREMENTAL":
                return Batch.Type.INCREMENTAL;
            case "TOTAL":
                return Batch.Type.TOTAL;
            default:
                return null;
        }
    }
}

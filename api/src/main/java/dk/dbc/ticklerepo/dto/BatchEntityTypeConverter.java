/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU 3
 * See license text in LICENSE.txt
 */

package dk.dbc.ticklerepo.dto;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

@Converter
public class BatchEntityTypeConverter implements AttributeConverter<BatchEntity.Type, Object> {
    private static final BatchEntityTypeConversion CONVERSION = new BatchEntityTypeConversion();

    @Override
    public Object convertToDatabaseColumn(BatchEntity.Type type) {
        return CONVERSION.toDatabaseColumn(type);
    }

    @Override
    public BatchEntity.Type convertToEntityAttribute(Object dbValue) {
        return CONVERSION.toEntityAttribute(dbValue);
    }
}

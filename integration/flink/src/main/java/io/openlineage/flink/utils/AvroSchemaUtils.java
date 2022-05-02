package io.openlineage.flink.utils;

import static org.apache.avro.Schema.Type.NULL;

import io.openlineage.client.OpenLineage;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;

/** Utility class for translating Avro schema into open lineage schema */
public class AvroSchemaUtils {

  /**
   * Converts Avro {@link Schema} to {@link OpenLineage.SchemaDatasetFacet}
   *
   * @param avroSchema
   * @return
   */
  public static OpenLineage.SchemaDatasetFacet convert(OpenLineage openLineage, Schema avroSchema) {
    OpenLineage.SchemaDatasetFacetBuilder builder = openLineage.newSchemaDatasetFacetBuilder();
    List<OpenLineage.SchemaDatasetFacetFields> fields = new LinkedList<>();

    avroSchema.getFields().stream()
        .forEach(
            avroField -> {
              fields.add(
                  openLineage.newSchemaDatasetFacetFields(
                      avroField.name(), getTypeName(avroField.schema()), avroField.doc()));
            });

    return builder.fields(fields).build();
  }

  private static String getTypeName(Schema fieldSchema) {
    // in case of union type containing some type and null type, non-null type name is returned
    return Optional.of(fieldSchema)
        .filter(Schema::isUnion)
        .map(Schema::getTypes)
        .filter(types -> types.size() == 2)
        .filter(types -> types.get(0).getType().equals(NULL) || types.get(1).getType().equals(NULL))
        .stream()
        .flatMap(Collection::stream)
        .filter(type -> !type.getType().equals(NULL))
        .map(type -> type.getType().getName())
        .findAny()
        .orElse(fieldSchema.getType().getName());
  }
}

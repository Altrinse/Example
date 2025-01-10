import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.ReflectDatumWriter;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroToJsonExample {

    public static void main(String[] args) {
        try {
            // Step 1: Create an AVRO object
            GenericRecord user = createAvroObject();

            // Step 2: Convert the AVRO object to JSON and print the result
            String jsonOutput = convertAvroToJson(user);
            System.out.println("Converted JSON: " + jsonOutput);
        } catch (IOException | JSONException e) {
            // Handle I/O exceptions and JSON-related exceptions
            e.printStackTrace();
        }
    }

    /**
     * Method to create the AVRO object with predefined schema and data.
     * @return a GenericRecord (AVRO object)
     * @throws IOException if there is an error during creation
     */
    private static GenericRecord createAvroObject() throws IOException {
        // Define the AVRO schema
        String schemaString = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"User\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"name\", \"type\": \"string\"},\n" +
                "    {\"name\": \"age\", \"type\": \"int\"}\n" +
                "  ]\n" +
                "}";

        // Parse the schema
        Schema schema = new Schema.Parser().parse(schemaString);

        // Create an AVRO object (GenericRecord) based on the schema
        GenericRecord user = new GenericData.Record(schema);
        user.put("name", new Utf8("John Doe"));
        user.put("age", 30);

        return user;
    }

    /**
     * Method to convert an AVRO object to JSON.
     * @param avroObject the AVRO object to be converted
     * @return JSON string representation of the AVRO object
     * @throws IOException if there is an error during conversion
     * @throws JSONException if there is a JSON-related error (for example, invalid JSON)
     */
    private static String convertAvroToJson(GenericRecord avroObject) throws IOException, JSONException {
        // Use try-with-resources for ByteArrayOutputStream
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            // Get the schema from the AVRO object
            Schema schema = avroObject.getSchema();
            DatumWriter<GenericRecord> writer = new ReflectDatumWriter<>(schema);
            JsonEncoder encoder = new JsonEncoder(schema, outputStream);

            // Write the AVRO object to JSON
            writer.write(avroObject, encoder);
            encoder.flush();

            // Return the JSON data as a string
            return outputStream.toString("UTF-8");
        } catch (IOException e) {
            // Handle I/O exceptions specifically
            throw new IOException("Error while converting AVRO object to JSON.", e);
        } catch (Exception e) {
            // If a different exception occurs, like an invalid JSON format
            throw new JSONException("An error occurred while processing JSON.", e);
        }
    }
}

/**
 * Custom JSONException class to simulate the usage of JSON exceptions
 */
class JSONException extends Exception {
    public JSONException(String message, Throwable cause) {
        super(message, cause);
    }
}

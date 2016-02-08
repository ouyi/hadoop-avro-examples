package com.github.ouyi;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Unit test for retrieving nested record.
 */
public class AvroWriteGenericRecordTest extends TestCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AvroWriteGenericRecordTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AvroWriteGenericRecordTest.class);
    }

    public void testWrite() throws IOException {

        URL url = this.getClass().getClassLoader().getResource("input/Company.avsc");
        assertNotNull(url);
        Schema schema = new Schema.Parser().parse(new File(url.getFile()));
        assertNotNull(schema);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        // Another way of loading a file
        File file = new File("src/test/resources/input/companies.avro");
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);

        File fileOut = new File("target/companies2.avro");
        Schema schemaOut = new Schema.Parser().parse(new File("src/test/resources/input/Company2.avsc"));
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schemaOut);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        GenericRecord company = null;
        int count = 0;
        while (dataFileReader.hasNext()) {
            company = dataFileReader.next(company);
            if (company.get("name").toString().equals("aol")) {
                dataFileWriter.create(schemaOut, fileOut);

                GenericRecord recordOut = new GenericData.Record(schemaOut);
                recordOut.put("id", company.get("id"));
                recordOut.put("name", company.get("name"));
                GenericRecord address = new GenericData.Record((GenericData.Record) company.get("address"), true);
                recordOut.put("address", address);

                dataFileWriter.append(recordOut);

                count ++;
            }
        }
        assertTrue(count > 0);

        dataFileWriter.close();
    }
}

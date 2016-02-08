package com.github.ouyi;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Unit test for retrieving nested record.
 */
public class AvroReadGenericRecordTest extends TestCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AvroReadGenericRecordTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AvroReadGenericRecordTest.class);
    }

    public void testApp() throws IOException {

        URL url = this.getClass().getClassLoader().getResource("input/Company.avsc");
        assertNotNull(url);
        Schema schema = new Schema.Parser().parse(new File(url.getFile()));
        assertNotNull(schema);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        String file = this.getClass().getClassLoader().getResource("input/companies.avro").getFile();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(file), datumReader);

        GenericRecord company = null;
        while (dataFileReader.hasNext()) {
            company = dataFileReader.next(company);
            GenericRecord address = (GenericRecord) company.get("address");
            if (company.get("name").equals("aol")) {
                assertEquals(address.get("city"), "NY City");
            }
        }
    }
}

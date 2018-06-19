package etl.api.dataset;

import etl.api.parameter.Parameter;
import etl.api.parameter.Parameters;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class DefaultDatasetMetadataKeyGeneratorTest {

    private DatasetMetadataKeyGenerator datasetMetadataKeyGenerator;

    @Before
    public void setUp() throws Exception {
        datasetMetadataKeyGenerator = new DefaultDatasetMetadataKeyGenerator();
    }

    @Test
    public void testCreateParameter() throws Exception {
        String name = "formCode";
        String value = "001";
        Parameters parameters = new Parameters();
        parameters.setString(name, value);

        String md5 = datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters.getParameters().values()));

        //if this case failed means algorithm changed you need to consider compatible issue the legacy data in database
        assertThat(md5).isEqualTo("52d974b271841723c94b7689a229a5a5");

        parameters = new Parameters();
        parameters.setString(name, value);

        assertThat(md5).isEqualTo(datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters.getParameters().values())));

        parameters = new Parameters();
        parameters.setString("code", "002");
        assertThat(md5).isNotEqualTo(datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters.getParameters().values())));

        parameters = new Parameters();
        parameters.setString("code", "2");

        Parameters parameters2 = new Parameters();
        parameters.setString("code", "3");

        assertThat(datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters.getParameters().values())))
                .isNotEqualTo(datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters2.getParameters().values())));

        parameters = new Parameters();
        parameters.setInt("code", "2");

        parameters2 = new Parameters();
        parameters.setInt("code", "3");
        assertThat(datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters.getParameters().values())))
                .isNotEqualTo(datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters2.getParameters().values())));

        parameters = new Parameters();
        parameters.setDouble("code", "2.2");

        parameters2 = new Parameters();
        parameters.setDouble("code", "3.3");
        assertThat(datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters.getParameters().values())))
                .isNotEqualTo(datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters2.getParameters().values())));


        parameters = new Parameters();
        parameters.setDate("code", "2017/11/27", "yyyy/MM/dd");

        parameters2 = new Parameters();
        parameters.setDate("code", "2017/11/25", "yyyy/MM/dd");
        assertThat(datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters.getParameters().values())))
                .isNotEqualTo(datasetMetadataKeyGenerator.generateKey(new HashSet<>(parameters2.getParameters().values())));

    }

    @Test
    public void testMixedParameters() throws Exception {

        String name = "formCode";
        String value = "001";

        String name2 = "version";
        String value2 = "1";

        String name3 = "referenceDate";
        String value3 = "2017-11-27";

        String name4 = "long";
        String value4 = "2147483648";

        String name5 = "double";
        String value5 = "214.74";

        Parameters parameters = new Parameters();
        parameters.setString(name, value);
        parameters.setInt(name2, value2);
        parameters.setDate(name3, value3, "YYYY-MM-DD");
        parameters.setLong(name4, value4);
        parameters.setDouble(name5, value5);

        Set<Parameter> p1 = new HashSet<>(parameters.getParameters().values());

        parameters = new Parameters();
        parameters.setString(name, value);
        parameters.setInt(name2, value2);
        parameters.setDate(name3, value3, "YYYY-MM-DD");
        parameters.setLong(name4, value4);
        parameters.setDouble(name5, value5);

        Set<Parameter> p2 = new HashSet<>(parameters.getParameters().values());

        String key1 = datasetMetadataKeyGenerator.generateKey(p1);
        String key2 = datasetMetadataKeyGenerator.generateKey(p2);
        assertEquals(key1, key2);
    }

    @Test
    public void testCreateParameterKeyWithNullValue() throws Exception {
        String name = "formCode";
        String name2 = "version";
        String name3 = "referenceDate";

        Parameters parameters = new Parameters();
        parameters.setString(name, null);
        parameters.setLong(name2, (Long) null);
        parameters.setDate(name3, null, "YYYY-MM-DD");
        Set<Parameter> p1 = new HashSet<>(parameters.getParameters().values());

        parameters = new Parameters();
        parameters.setString(name, null);
        parameters.setLong(name2, (Long) null);
        parameters.setDate(name3, null, "YYYY-MM-DD");
        Set<Parameter> p2 = new HashSet<>(parameters.getParameters().values());

        String key1 = datasetMetadataKeyGenerator.generateKey(p1);
        String key2 = datasetMetadataKeyGenerator.generateKey(p2);
        assertEquals(key1, key2);
    }

    @Test
    public void testCreateParameterKeyOrdering() throws Exception {
        String name = "formCode";
        String value = "001";

        String name2 = "version";
        String value2 = "1";

        String name3 = "referenceDate";
        String value3 = "2017-11-27";

        Parameters parameters = new Parameters();
        parameters.setString(name, value);
        parameters.setLong(name2, value2);
        parameters.setDate(name3, value3, "yyyy-MM-dd");

        Set<Parameter> p1 = new HashSet<>(parameters.getParameters().values());

        parameters = new Parameters();
        parameters.setDate(name3, value3, "yyyy-MM-dd");
        parameters.setString(name, value);
        parameters.setLong(name2, value2);

        Set<Parameter> p2 = new HashSet<>(parameters.getParameters().values());

        String key1 = datasetMetadataKeyGenerator.generateKey(p1);
        String key2 = datasetMetadataKeyGenerator.generateKey(p2);
        assertEquals(key1, key2);
    }

    @Test
    public void testDateParameterWithDifferentFormat() throws Exception {

        String name = "referenceDate";
        String value = "2017-11-27";
        String value2 = "2017/11/27";

        Parameters parameters = new Parameters();
        parameters.setDate(name, value, "yyyy-MM-dd");
        Set<Parameter> p1 = new HashSet<>(parameters.getParameters().values());

        parameters = new Parameters();
        parameters.setDate(name, value2, "yyyy/MM/dd");
        Set<Parameter> p2 = new HashSet<>(parameters.getParameters().values());


        String key1 = datasetMetadataKeyGenerator.generateKey(p1);
        String key2 = datasetMetadataKeyGenerator.generateKey(p2);
        assertEquals(key1, key2);
    }
}

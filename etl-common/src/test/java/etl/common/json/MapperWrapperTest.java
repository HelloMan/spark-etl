package etl.common.json;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class MapperWrapperTest {


    // Test to check that the jackson-datatype-guava dependency has not been removed from the POM.xml
    // and that we can still serialize/deserialize immutable objects.

    @Test
    public void serializationTest() throws IOException {

        //Arrange
        ImmutableSet immutableTest = ImmutableSet.copyOf(Arrays.asList("a", "b", "c"));
        String testJsonString = "[\"a\",\"b\",\"c\"]";

        //Act
        String jsonString = MapperWrapper.MAPPER.writeValueAsString(immutableTest);
        ImmutableSet testObject = MapperWrapper.MAPPER.readValue(jsonString, ImmutableSet.class);

        //Assert
        assertThat(jsonString, is(testJsonString));
        assertThat(immutableTest, is(testObject));
    }
}
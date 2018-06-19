package etl.api.dataset;

import etl.api.AbstractJavaBeanTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
public class DatasetMetadataTest  extends AbstractJavaBeanTest<DatasetMetadata> {

    @Test
    public void testOf() throws Exception {

        assertThat(DatasetMetadata.of("name").getName()).isEqualTo("name");
    }

    @Test
    public void testGetMetadata() throws Exception {
        assertThat(DatasetMetadata.of("name").getMetadata() ).isNull();
    }

    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();
        map.put("a", "b");
        map.put("b", "c");

        Stream.of("1","2").filter(s->s.equals("1"))
                .filter(s->s.equals("1"))
                .collect(Collectors.toList());


        System.out.println(tableSizeFor(12));


    }
    static final int MAXIMUM_CAPACITY = 1 << 30;
    static final int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }
}
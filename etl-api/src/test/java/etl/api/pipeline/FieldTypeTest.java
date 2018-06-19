package etl.api.pipeline;


import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

public class FieldTypeTest {

    @Test
    public void testParse() throws Exception {
        FieldType fieldTypeParser =  FieldType.of(FieldType.Type.LONG);
        assertThat(fieldTypeParser.parse(2l)).isEqualTo(2l);
        fieldTypeParser =  FieldType.of(FieldType.Type.BOOLEAN) ;
        assertThat(fieldTypeParser.parse("true")).isEqualTo(true);
        fieldTypeParser =  FieldType.of(FieldType.Type.INT) ;
        assertThat(fieldTypeParser.parse(2)).isEqualTo(2);

        fieldTypeParser = FieldType.of(FieldType.Type.FLOAT);
        assertThat(fieldTypeParser.parse("0.2f")).isEqualTo(0.2f);

        fieldTypeParser = FieldType.of(FieldType.Type.DOUBLE);
        assertThat(fieldTypeParser.parse("0.2d")).isEqualTo(0.2d);


        fieldTypeParser =  FieldType.of(FieldType.Type.STRING) ;
        assertThat(fieldTypeParser.parse("hello world")).isEqualTo("hello world");

        LocalDateTime localDateTime = LocalDateTime.of(2017, 06, 07, 10, 10, 10);
        fieldTypeParser =  FieldType.of(FieldType.Type.TIMESTAMP,"dd-MM-yyyy hh:mm:ss") ;
        Timestamp date1 = Timestamp.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
        assertThat(fieldTypeParser.parse("07-06-2017 10:10:10")).isEqualTo(date1);

        fieldTypeParser =  FieldType.of(FieldType.Type.NULL) ;
        assertThat(fieldTypeParser.parse("NULL")).isEqualTo(null);

    }
}

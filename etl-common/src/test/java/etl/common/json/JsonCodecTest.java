package etl.common.json;

import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
public class JsonCodecTest {

    @Test
    public void testEncodeWithUtf8() throws Exception {

        String result = new JsonCodec().encode("{\"type\" : \"Join\"}");
        assertThat(result).isEqualTo("%7B%22type%22+%3A+%22Join%22%7D");
    }

    @Test
    public void testDecodeWithUtf8() throws Exception {
        String result = new JsonCodec().decode("%7B%22type%22+%3A+%22Join%22%7D");
        assertThat(result).isEqualTo("{\"type\" : \"Join\"}");
    }

    @Test
    public void testEncode() throws Exception {
        String result = new JsonCodec().encode("{\"type\" : \"Join\"}");
        assertThat(result).isEqualTo("%7B%22type%22+%3A+%22Join%22%7D");
    }

    @Test
    public void testDecode() throws Exception {
        String result = new JsonCodec().decode("%7B%22type%22+%3A+%22Join%22%7D");
        assertThat(result).isEqualTo("{\"type\" : \"Join\"}");
    }

    @Test
    public void testEncodeWithException() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new JsonCodec( buildNonExistCharset()).encode("{\"type\" : \"Join\"}"));

    }

    @Test
    public void testDecodeWithException() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new JsonCodec( buildNonExistCharset()).decode("%7B%22type%22+%3A+%22Join%22%7D"));

    }

    private Charset buildNonExistCharset() {
        return new Charset("AB", new String[]{"AB"}) {
            @Override
            public boolean contains(Charset cs) {
                return false;
            }

            @Override
            public CharsetDecoder newDecoder() {
                return null;
            }

            @Override
            public CharsetEncoder newEncoder() {
                return null;
            }
        };
    }
}
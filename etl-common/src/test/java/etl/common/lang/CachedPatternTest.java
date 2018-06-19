package etl.common.lang;

import org.junit.Test;

import java.util.Arrays;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
public class CachedPatternTest {

    @Test
    public void testCompile() throws Exception {

        CachedPattern cachedPattern = CachedPattern.compile("\\d+\\s+");
        assertThat(cachedPattern).isNotNull();
        assertThat(cachedPattern.getRegex()).isEqualTo("\\d+\\s+");
    }

    @Test
    public void testCompile1() throws Exception {
        CachedPattern cachedPattern = CachedPattern.compile("\\d+\\S+\\s", Pattern.CASE_INSENSITIVE);
        assertThat(cachedPattern).isNotNull();
        assertThat(cachedPattern.getFlag()).isEqualTo(Pattern.CASE_INSENSITIVE);
    }

    @Test
    public void testFind() throws Exception {
        CachedPattern cachedPattern = CachedPattern.compile("\\d+\\S+\\s", Pattern.CASE_INSENSITIVE);
        assertThat(cachedPattern.find("12 s12")).isTrue();
    }

    @Test
    public void testMatches() throws Exception {
        CachedPattern cachedPattern = CachedPattern.compile("\\d+\\s+\\S", Pattern.CASE_INSENSITIVE);
        assertThat(cachedPattern.matches("12 s")).isTrue();

    }

    @Test
    public void testToStream() throws Exception {
        CachedPattern cachedPattern = CachedPattern.compile("\\d+\\s+\\S", Pattern.CASE_INSENSITIVE);
        assertThat(cachedPattern.toStream("12 s 23 f").flatMap(Arrays::stream).anyMatch(s -> s.equals("12 s"))).isTrue();
        assertThat(cachedPattern.toStream("12 s 23 f").count()).isEqualTo(2);
    }
}
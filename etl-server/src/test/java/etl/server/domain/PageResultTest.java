package etl.server.domain;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;

import static org.assertj.core.api.Assertions.assertThat;

public class PageResultTest {

    @Test
    public void testFrom() throws Exception {

        Page<String> page = new PageImpl<String>(ImmutableList.of("1"));
        assertThat(PageResult.from(page).getContent().size()).isEqualTo(1);
    }

    @Test
    public void testMap() throws Exception {
        Page<String> page = new PageImpl<>(ImmutableList.of("1"));
        assertThat(PageResult.from(page).map(s -> s + "1").getContent().iterator().next()).isEqualTo("11");
    }

    @Test
    public void testGetPageNumber() throws Exception {
        Page<String> page = new PageImpl<String>(ImmutableList.of("1"));
        assertThat(PageResult.from(page).getPageNumber()).isEqualTo(0);
    }

    @Test
    public void testGetPageSize() throws Exception {
        Page<String> page = new PageImpl<String>(ImmutableList.of("1"));
        assertThat(PageResult.from(page).getPageSize()).isEqualTo(0);
    }

    @Test
    public void testGetTotalPages() throws Exception {
        Page<String> page = new PageImpl<String>(ImmutableList.of("1"));
        assertThat(PageResult.from(page).getTotalPages()).isEqualTo(1);
    }

    @Test
    public void testGetContent() throws Exception {
        Page<String> page = new PageImpl<String>(ImmutableList.of("1"));
        assertThat(PageResult.from(page).getContent()).isEqualTo(ImmutableList.of("1"));
    }
}
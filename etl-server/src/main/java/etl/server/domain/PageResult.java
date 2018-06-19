package etl.server.domain;

import lombok.Getter;
import lombok.experimental.Tolerate;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.domain.Page;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * A pageResult is a sublist of a list of objects.
 * It allows gain information about the position of it in the containing entire list.
 *
 * @param <T>
 */
@Getter
public class PageResult<T> {

    /**
     * Returns the number of the current page
     */
    int pageNumber;

    /**
     * Returns the size of the page
     */
    int pageSize;

    /**
     * Returns the number of total pages.
     */
    int totalPages;

    /**
     * Returns the page content as {@link List}.
     */
    List<T> content;

    public PageResult(int pageNumber, int pageSize, int totalPages, List<T> content) {
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.totalPages = totalPages;
        this.content = content;
    }

    @Tolerate
    public PageResult() {
    }

    /**
     * convert spring page domain model to PageResult
     *
     * @param page spring page domain model
     * @param <T>
     * @return an instance of PageResult
     */
    public static <T> PageResult<T> from(Page<T> page) {
        return new PageResult(page.getNumber(), page.getSize(), page.getTotalPages(), page.getContent());
    }

    /**
     * Applies the given {@link Converter} to the content
     *
     * @param converter must not be {@literal null}.
     * @return
     */
    private <S> List<S> getConvertedContent(Converter<? super T, ? extends S> converter) {
        Assert.notNull(converter, "Converter must not be null!");
        List<S> result = new ArrayList<S>(content.size());
        for (T element : this.getContent()) {
            result.add(converter.convert(element));
        }
        return result;
    }

    public <S> PageResult<S> map(Converter<? super T, ? extends S> converter) {
        return new PageResult(getPageNumber(), getPageSize(), getTotalPages(), getConvertedContent(converter));
    }

}

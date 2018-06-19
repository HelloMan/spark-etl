package etl.api.datasource;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Created by jason zhang on 11/2/2016.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Getter
@NoArgsConstructor
public class CsvDataSource implements DataSource {
	
	private String filePath;
	
	private boolean header;
	
	/**
	 * The delimiter character (separates each cell in a row).
	 *
	 * @see <a href="http://supercsv.sourceforge.net/preferences.html">CSV Preferences</a>
	 */
	private char delimiter;
	
	/**
	 * Specify the start position (a positive integer starting from 0) to read the data. If reading from the beginning
	 * of the input CSV, there is no need to specify this property.
	 */
	private int start;
	
	/**
	 * Specify the end position in the data set (inclusive). Optional property, and defaults to {@code Integer.MAX_VALUE}.
	 * If reading till the end of the input CSV, there is no need to specify this property.
	 */
	private int end;
	
	/**
	 * The quote character (used when a cell contains special characters, such as the delimiter char, a quote char,
	 * or spans multiple lines). See <a href="http://supercsv.sourceforge.net/preferences.html">CSV Preferences</a>.
	 * The default quoteChar is double quote ("). If " is present in the CSV data cells, specify quoteChar to some
	 * other characters, e.g., |.
	 */
	private String quote;
	
	@Builder
	public CsvDataSource(String filePath, boolean header, char delimiter, String quote, int end, int start) {
		this.filePath = filePath;
		this.delimiter = delimiter;
		this.quote = quote;
		this.end = end;
		this.start = start;
		this.header = header;
	}
	
}
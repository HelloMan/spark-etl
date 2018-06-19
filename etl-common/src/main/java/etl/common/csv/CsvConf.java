package etl.common.csv;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Getter
@Builder
public final class CsvConf implements Serializable {
    private char delimiter;
	private String quote;
	public static CsvConf getDefault() {
		return CsvConf.builder().delimiter(',').quote("\"").build();
	}
}

package etl.server.exception.staging;

import lombok.Getter;

@Getter
public class NoSuchDatasetStateException extends Exception {

    private final String state;
    public NoSuchDatasetStateException(String state) {
        super(String.format("No such dataset state with state=%s", state));
        this.state = state;
    }

}

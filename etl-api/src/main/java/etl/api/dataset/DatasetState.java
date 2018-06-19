package etl.api.dataset;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public enum DatasetState {

    UPLOADING {
        @Override
        public List<DatasetState> getValidTransitions() {
            return  ImmutableList.of(UPLOADED, UPLOAD_FAILED);
        }
    },
    UPLOADED {
        @Override
        public List<DatasetState> getValidTransitions() {
            return ImmutableList.of(VALIDATING);
        }
    },
    UPLOAD_FAILED {
        @Override
        public List<DatasetState> getValidTransitions() {
            return ImmutableList.of();
        }
    },
    VALIDATING {
        @Override
        public List<DatasetState> getValidTransitions() {
            return ImmutableList.of(VALIDATED, VALIDATION_FAILED);
        }
    },
    VALIDATION_FAILED {
        @Override
        public List<DatasetState> getValidTransitions() {
            return ImmutableList.of();
        }
    },
    VALIDATED {
        @Override
        public List<DatasetState> getValidTransitions() {
            return ImmutableList.of(REGISTERING);
        }
    },
	REGISTERING {
        @Override
        public List<DatasetState> getValidTransitions() {
            return ImmutableList.of(REGISTERED, REGISTRATION_FAILED);
        }

    },
	REGISTERED {
        @Override
        public List<DatasetState> getValidTransitions() {
            return ImmutableList.of();
        }
    },
	REGISTRATION_FAILED {
        @Override
        public List<DatasetState> getValidTransitions() {
            return ImmutableList.of();
        }
    };

    public abstract List<DatasetState> getValidTransitions();

    public boolean canTransistTo(DatasetState toState) {
        return getValidTransitions().contains(toState);
    }

    public static Optional<DatasetState> lookupState(String value) {
        return Stream.of(DatasetState.values()).filter(ds -> ds.name().equals(value)).findFirst();
    }

}

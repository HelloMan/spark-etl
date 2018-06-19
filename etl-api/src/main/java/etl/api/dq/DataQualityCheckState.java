package etl.api.dq;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public enum DataQualityCheckState {

	ACCEPTED {
		@Override
		protected List<DataQualityCheckState> getValidTransitions() {
			return ImmutableList.of(CHECKING, CHECK_FAILED);
		}
	},
	CHECKING {
		@Override
		protected List<DataQualityCheckState> getValidTransitions() {
			return ImmutableList.of(CHECKED, CHECK_FAILED);
		}
	},
	CHECKED {
		@Override
		protected List<DataQualityCheckState> getValidTransitions() {
			return ImmutableList.of();
		}
	},
	CHECK_FAILED {
		@Override
		protected List<DataQualityCheckState> getValidTransitions() {
			return ImmutableList.of();
		}
	};

	protected abstract List<DataQualityCheckState> getValidTransitions();

	public boolean canTransistTo(DataQualityCheckState toState) {
		return getValidTransitions().contains(toState);
	}

	public static Optional<DataQualityCheckState> lookupState(String value) {
		return Stream.of(DataQualityCheckState.values()).filter(ds -> ds.name().equals(value)).findFirst();
	}

}
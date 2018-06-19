package etl.api.dq;

import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class DataQualityCheckStateTest {
	@Test
	public void lookup() {
		Optional<DataQualityCheckState> opt = DataQualityCheckState.lookupState("CHECKING");
		assertThat(opt.isPresent()).isTrue();
	}

	@Test
	public void lookup_notFound() {
		Optional<DataQualityCheckState> opt = DataQualityCheckState.lookupState("123");
		assertThat(opt.isPresent()).isFalse();
	}

	@Test
	public void transitTo_accepted_checking_can() {
		boolean res = DataQualityCheckState.ACCEPTED.canTransistTo(DataQualityCheckState.CHECKING);
		assertThat(res).isTrue();
	}

	@Test
	public void transitTo_accepted_checkFailed_can() {
		boolean res = DataQualityCheckState.ACCEPTED.canTransistTo(DataQualityCheckState.CHECK_FAILED);
		assertThat(res).isTrue();
	}

	@Test
	public void transitTo_checking_checked_can() {
		boolean res = DataQualityCheckState.CHECKING.canTransistTo(DataQualityCheckState.CHECKED);
		assertThat(res).isTrue();
	}

	@Test
	public void transitTo_accepted_checked_cannot() {
		boolean res = DataQualityCheckState.ACCEPTED.canTransistTo(DataQualityCheckState.CHECKED);
		assertThat(res).isFalse();
	}

	@Test
	public void transitTo_checkFailed_checked_cannot() {
		boolean res = DataQualityCheckState.CHECK_FAILED.canTransistTo(DataQualityCheckState.CHECKED);
		assertThat(res).isFalse();
	}

	@Test
	public void transitTo_checked__checkFailed_cannot() {
		boolean res = DataQualityCheckState.CHECKED.canTransistTo(DataQualityCheckState.CHECK_FAILED);
		assertThat(res).isFalse();
	}
}
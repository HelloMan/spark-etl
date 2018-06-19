package etl.api.dataset;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class DatasetStateTest {

    @Test
    public void testLookupState_NotPresent_With_Invalid_Value(){
        assertThat(DatasetState.lookupState("hello").isPresent()).isFalse();
    }

    @Test
    public void testLookupState_Present(){
        assertThat(DatasetState.lookupState("UPLOADING").isPresent()).isTrue();
    }

    @Test
    public void testLookupState_NotPresent_With_Null(){
        assertThat(DatasetState.lookupState(null).isPresent()).isFalse();
    }

    @Test
    public void testGetValidTransitionsForUploading(){
        assertThat(DatasetState.UPLOADING.getValidTransitions()).contains(DatasetState.UPLOADED, DatasetState.UPLOAD_FAILED);
    }


    @Test
    public void testGetValidTransitionsForUploaded(){
        assertThat(DatasetState.UPLOADED.getValidTransitions()).contains(DatasetState.VALIDATING);
    }

    @Test
    public void testGetValidTransitionsForUploadFailed(){
        assertThat(DatasetState.UPLOAD_FAILED.getValidTransitions()).isEmpty();

    }

    @Test
    public void testGetValidTransitionsForValidating(){
        assertThat(DatasetState.VALIDATING.getValidTransitions()).contains(DatasetState.VALIDATED, DatasetState.VALIDATION_FAILED);
    }

    @Test
    public void testGetValidTransitionsForValidated(){
        assertThat(DatasetState.VALIDATED.getValidTransitions()).contains(DatasetState.REGISTERING);
    }

    @Test
    public void testGetValidTransitionsForValidationFailed(){
        assertThat(DatasetState.VALIDATION_FAILED.getValidTransitions()).isEmpty();
    }

    @Test
    public void testGetValidTransitionsForRegistering(){
        assertThat(DatasetState.REGISTERING.getValidTransitions()).contains(DatasetState.REGISTERED, DatasetState.REGISTRATION_FAILED);
    }

    @Test
    public void testGetValidTransitionsForRegistered(){
        assertThat(DatasetState.REGISTERED.getValidTransitions()).isEmpty();
    }

    @Test
    public void testGetValidTransitionsForRegistrationFailed(){
        assertThat(DatasetState.REGISTRATION_FAILED.getValidTransitions()).isEmpty();
    }
    @Test
    public void testCanTransistTo() {
        assertThat(DatasetState.REGISTERED.canTransistTo(DatasetState.REGISTERING)).isFalse();
    }
}
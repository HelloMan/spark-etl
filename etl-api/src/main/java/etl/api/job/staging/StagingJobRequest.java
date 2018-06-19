package etl.api.job.staging;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.experimental.Tolerate;

import java.util.Set;

/**
 * Job definition for staging data set
 *
 * @author jason zhang
 */
@Getter
@Builder
public class StagingJobRequest  {

    private @NonNull String name;

    private @Singular Set<StagingItemRequest> items;

    @Tolerate
    public StagingJobRequest(){}

}

package etl.api.dataset;


import java.util.Objects;

public class DatasetPath {

    private final long jobExecutionId;

    private final String dataset;

    private final String rootPath;

    public DatasetPath(long jobExecutionId, String dataset, String rootPath) {
        Objects.requireNonNull(rootPath);
        Objects.requireNonNull(dataset);
        this.jobExecutionId = jobExecutionId;
        this.dataset = dataset;
        this.rootPath = rootPath;
    }


    public  String getStagingFile() {
        return String.join("/",rootPath, String.valueOf(jobExecutionId) ,dataset);
    }

    public String getStagingErrorFile() {
        return String.join("/",rootPath, String.valueOf(jobExecutionId),"E_" + dataset);
    }
}

package etl.api.pipeline;

import lombok.experimental.UtilityClass;

import java.util.Set;

@UtilityClass
public class DatasetRefs {

    private static final String SPLIT_LEFT_SIDE = "_1";
    private static final String SPLIT_RIGHT_SIDE = "_2";

    public static DatasetRef fromTransform(Split split,boolean leftSide) {
        if (leftSide) {
            return split.getOutput1() != null ? split.getOutput1() : datasetRef(split.getName() + SPLIT_LEFT_SIDE);
        }else {
            return split.getOutput2() != null ? split.getOutput2() : datasetRef(split.getName() + SPLIT_RIGHT_SIDE);
        }
    }

    public static DatasetRef fromTransform(SingleOutTransform transform) {
        return fromTransform(transform, null);
    }

    public static DatasetRef fromTransform(SingleOutTransform transform, String filter) {
        if (transform.getOutput() == null) {
            return DatasetRef.builder().name(transform.getName()).filter(filter).immediate(true).build();
        }else {
            return clone(transform.getOutput(), filter);
		}
	}

	public static DatasetRef datasetRef(String name, String filter) {
        return DatasetRef.builder().name(name).filter(filter).build();
    }

    public static DatasetRef datasetRef(String name){
        return DatasetRef.builder().name(name).build();
    }

    public static DatasetRef datasetRef(String name,Set<DatasetRefParam> parameters){
        return DatasetRef.builder().name(name).parameters(parameters).build();
    }

    public static DatasetRef datasetRef(String name,Set<DatasetRefParam> parameters,String filter){
        return DatasetRef.builder().name(name).parameters(parameters).filter(filter).build();
    }

	public static DatasetRef clone(DatasetRef ref, String filter){
		return DatasetRef.builder()
				.name(ref.getName())
				.includes(ref.getIncludes())
				.excludes(ref.getExcludes())
				.parameters(ref.getParameters())
				.filter(filter)
                .immediate(ref.isImmediate())
				.build();
	}
}

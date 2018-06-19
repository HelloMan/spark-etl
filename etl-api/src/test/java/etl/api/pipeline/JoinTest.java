package etl.api.pipeline;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class JoinTest {

    private Join createJoin() {
        return Join.builder()
                .name("join")
                .left(DatasetRefs.datasetRef("employee"))
                .right(DatasetRefs.datasetRef("dept"))
                .output(DatasetRefs.datasetRef("joinOutput"))
                .on(JoinOn.on("deptId", "deptId"))
                .joinType(JoinType.INNER)
                .field(JoinField.left("name"))
                .build();
    }

    @Test
    public void testJoin() {
        //act
        Join join = createJoin();
        //assert
        assertThat(join.getLeft().getName()).isEqualTo("employee");
        assertThat(join.getRight().getName()).isEqualTo("dept");
        assertThat(join.getJoinType()).isEqualTo(JoinType.INNER);
        assertThat(join.getFields().size()).isEqualTo(1);
        assertThat(join.getOns().size()).isEqualTo(1);

    }
}

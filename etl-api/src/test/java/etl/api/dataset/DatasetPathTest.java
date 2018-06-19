package etl.api.dataset;

import lombok.*;
import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
public class DatasetPathTest {

    @Test
    public void testGetStagingFile() throws Exception {
        DatasetPath datasetPath = new DatasetPath(1l, "a", "b");
       assertThat( datasetPath.getStagingFile()).isEqualTo("b/1/a");
    }

    @Test
    public void testGetStagingErrorFile() throws Exception {
        DatasetPath datasetPath = new DatasetPath(1l, "a", "b");
       assertThat( datasetPath.getStagingErrorFile()).isEqualTo("b/1/E_a");
    }

    @Getter
    @Setter
    @Builder
    @EqualsAndHashCode
    public static class KeyGroup {
        private int value;

        private int group;

    }

    public int[]  countArray(int[] a) {
        int group = 0;
        Map<KeyGroup,Integer> resultGroup = new LinkedHashMap<>();
        for (int i=0; i<a.length; i++) {
            KeyGroup keyGroup ;
            if (i == 0 || a[i-1]==a[i]){
                keyGroup =  KeyGroup.builder().value(a[i]).group(group).build();
            }else{
                keyGroup =  KeyGroup.builder().value(a[i]).group(group++).build();
            }

            Integer count = resultGroup.get(keyGroup);
            if (count == null) {
                resultGroup.put(keyGroup, 1);
            }else{
                resultGroup.put(keyGroup, count +1);
            }
        }

        int[] result = new int[resultGroup.size()*2];
        int i =0;

        for (Map.Entry<KeyGroup, Integer> keyGroupIntegerEntry : resultGroup.entrySet()) {

            result[i++] = keyGroupIntegerEntry.getValue();

            result[i++] = keyGroupIntegerEntry.getKey().getValue();

        }

        return result;
      

    }
}
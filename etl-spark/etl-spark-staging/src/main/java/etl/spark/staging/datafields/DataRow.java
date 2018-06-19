package etl.spark.staging.datafields;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * Created by chaojun on 2017/11/19.
 */
public class DataRow implements Serializable {

    @Getter
    @Setter
    private List<? extends DataField> fields;


    public DataRow(List<? extends DataField> fields) {
        this.fields = fields;
    }

    public DataField getField(int columnIndex){
        return fields.get(columnIndex);
    }

    public int size(){
        return fields.size();
    }


}

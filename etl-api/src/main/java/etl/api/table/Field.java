

package etl.api.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import javaslang.control.Validation;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BooleanField.class, name = "boolean"),
        @JsonSubTypes.Type(value = DateField.class, name = "date"),
        @JsonSubTypes.Type(value = DoubleField.class, name = "double"),
        @JsonSubTypes.Type(value = IntField.class, name = "int"),
        @JsonSubTypes.Type(value = LongField.class, name = "long"),
        @JsonSubTypes.Type(value = DecimalField.class, name = "decimal"),
        @JsonSubTypes.Type(value = StringField.class, name = "string"),
        @JsonSubTypes.Type(value = TimestampField.class, name = "timestamp")

})
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Entity
@javax.persistence.Table(name = "DATASET_SCHEMA_FIELD")
@DiscriminatorColumn(name = "FIELD_TYPE")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
public abstract class Field<T extends Comparable> implements Serializable{

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "ID")
    private Long id;

    @Column(name = "NAME")
    private String name;

    @Column(name = "IS_KEY")
    @Deprecated
    private boolean key;

    @Column(name = "NULLABLE")
    private boolean nullable = true;

    @ManyToOne
    @JoinColumn(name = "DATASET_SCHEMA_ID")
    @JsonIgnore
    private Table table;


    public Validation<List<String>,String> validate(String value){
        if (StringUtils.isBlank(value)) {
            if (isKey()) {
                return Validation.invalid(ImmutableList.of(String.format("Field [%s] is a key, it can not be empty", getName().toUpperCase())));
            }
            if (!isNullable()) {
                return Validation.invalid(ImmutableList.of(String.format("Field [%s] is not nullable", getName().toUpperCase())));
            }
            return Validation.valid(value);
        }else {
            return this.doValidate(value);
        }

    }

    protected  abstract Validation<List<String>,String> doValidate(String value);

    public T parse(String value){
        if (StringUtils.isBlank(value)) {
            return null;
        }
        return doParse(value);
    }

    protected abstract T doParse(String value);

}

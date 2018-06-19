package etl.api;

import lombok.extern.slf4j.Slf4j;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.meanbean.lang.Factory;
import org.meanbean.test.BeanTester;

import java.lang.reflect.ParameterizedType;
import java.time.LocalDateTime;

/**
 * see - https://www.javacodegeeks.com/2014/09/tips-for-unit-testing-javabeans.html
 * @param <T>
 */
@Slf4j
public abstract class AbstractJavaBeanTest<T> {

    protected Class<T> tClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];

    @Test
    public void testEquals() throws Exception {
        try {
            tClass.getDeclaredMethod("equals", Object.class);
            EqualsVerifier equalsVerifier = EqualsVerifier.forClass(getBeanInstance().getClass());
            if (getEqualsIgnoredFields() != null) {
                equalsVerifier = equalsVerifier.withIgnoredFields(getEqualsIgnoredFields());
            }
            equalsVerifier.suppress(Warning.STRICT_INHERITANCE, Warning.NONFINAL_FIELDS).verify();
        } catch (NoSuchMethodException e) {
        }
    }

    @Test
    public void testGetterAndSetterMethods() throws Exception {
        final BeanTester beanTester = new BeanTester();
        beanTester.getFactoryCollection().addFactory(LocalDateTime.class, new LocalDateTimeFactory());
        beanTester.testBean(getBeanInstance().getClass());
    }

    protected T getBeanInstance() throws Exception {
       return tClass.newInstance();
    }

    protected String[] getEqualsIgnoredFields() {
        return null;
    }

    /**
     * Concrete Factory that creates a LocalDateTime.
     */
    class LocalDateTimeFactory implements Factory {

        @Override
        public LocalDateTime create() {
            return LocalDateTime.now();
        }

    }

}
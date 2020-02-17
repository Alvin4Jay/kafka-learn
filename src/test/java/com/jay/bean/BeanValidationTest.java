package com.jay.bean;

import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Set;

/**
 * @author xuweijie
 */
public class BeanValidationTest {

    @Test
    public void test() {
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        Set<ConstraintViolation<User>> validate = validator.validate(new User("     ", null, 110));

        for (ConstraintViolation<User> violation : validate) {
            System.out.println(violation.getMessage());
        }
    }


}

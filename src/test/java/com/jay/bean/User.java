package com.jay.bean;

import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.*;
import java.math.BigDecimal;

/**
 * @author xuweijie
 */
public class User {

    @DecimalMin("10")
    @DecimalMax("100")
    private BigDecimal salary;

    @NotNull(message = "用户名不能为空")
    @NotBlank(message = "nnnnn")
    private String name;

    @Min(value = 1, message = "年龄不能小于1")
    @Max(value = 200, message = "年龄不能大于200")
    @NotNull(message = "age不能为空")
    private Integer age;

    public User(String name, Integer age, double salary) {
        this.name = name;
        this.age = age;
        this.salary = BigDecimal.valueOf(salary);
    }

    public BigDecimal getSalary() {
        return salary;
    }

    public void setSalary(BigDecimal salary) {
        this.salary = salary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}

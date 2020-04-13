package entity;

import java.io.Serializable;

/**
 * @ date: 2020/04/02 10:38
 * @ author: Cheney
 * @ description: 学生实体类
 */
public class Student implements Serializable {

    public String name;

    public long personalID;

    public int age;

    public double weight;

    public Student(){}

    public Student(String name, long personalID, int age, double weight) {
        this.name = name;
        this.personalID = personalID;
        this.age = age;
        this.weight = weight;
    }

    // {"name":"张三","personalID":12345678901,"age":21,"weight":62.3}
    // {"name":"李四","personalID":12345678902,"age":24,"weight":65.43}
    // {"name":"王五","personalID":12345678903,"age":26,"weight":78。44}
    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", personalID=" + personalID +
                ", age=" + age +
                ", weight=" + weight +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getPersonalID() {
        return personalID;
    }

    public void setPersonalID(long personalID) {
        this.personalID = personalID;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }
}

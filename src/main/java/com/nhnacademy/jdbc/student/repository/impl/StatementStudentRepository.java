package com.nhnacademy.jdbc.student.repository.impl;

import com.nhnacademy.jdbc.student.domain.Student;
import com.nhnacademy.jdbc.student.repository.StudentRepository;
import com.nhnacademy.jdbc.util.DbUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

@Slf4j
public class StatementStudentRepository implements StudentRepository {

    public int save(Student student){
        //todo#1 insert student
        try (Connection connection = DbUtils.getConnection();
             Statement statement = connection.createStatement()) {
            String sql = String.format("INSERT INTO jdbc_students (id, name, gender, age) VALUES ('%s', '%s', '%s', %d)",
                    student.getId(), student.getName(), student.getGender().toString(), student.getAge());
            return statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Student> findById(String id){
        //todo#2 student 조회
        try (Connection connection = DbUtils.getConnection();
             Statement statement = connection.createStatement();
        ) {
            String sql = String.format("SELECT * FROM jdbc_students WHERE id = '%s'", id);
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
               Student student = new Student(
                       rs.getString("id"),
                       rs.getString("name"),
                       Student.GENDER.valueOf(rs.getString("gender")),
                       rs.getInt("age"));
               return Optional.of(student);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.empty();
    }

    @Override
    public int update(Student student){
        //todo#3 student 수정, name <- 수정합니다.
        try (Connection connection = DbUtils.getConnection();
            Statement statement = connection.createStatement();
        ) {
            String sql = String.format("UPDATE jdbc_students SET name='%s', gender='%s', age=%d WHERE id='%s'", student.getName(), student.getGender(), student.getAge(), student.getId());
            return statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int deleteById(String id){
       //todo#4 student 삭제
        try (Connection connection = DbUtils.getConnection();
            Statement statement = connection.createStatement();
        ) {
            String sql = String.format("DELETE FROM jdbc_students WHERE id = '%s'", id);
            return statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

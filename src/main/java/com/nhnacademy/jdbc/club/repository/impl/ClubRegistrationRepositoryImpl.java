package com.nhnacademy.jdbc.club.repository.impl;

import com.nhnacademy.jdbc.club.domain.ClubStudent;
import com.nhnacademy.jdbc.club.repository.ClubRegistrationRepository;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class ClubRegistrationRepositoryImpl implements ClubRegistrationRepository {

    @Override
    public int save(Connection connection, String studentId, String clubId) {
        //todo#11 - 핵생 -> 클럽 등록, executeUpdate() 결과를 반환
        String sql = "INSERT INTO jdbc_club_registrations SET student_id=?, club_id=?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, studentId);
            preparedStatement.setString(2, clubId);
            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int deleteByStudentIdAndClubId(Connection connection, String studentId, String clubId) {
        //todo#12 - 핵생 -> 클럽 탈퇴, executeUpdate() 결과를 반환
        String sql = "DELETE FROM jdbc_club_registrations WHERE student_id=? AND club_id=?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, studentId);
            preparedStatement.setString(2, clubId);
            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ClubStudent> findClubStudentsByStudentId(Connection connection, String studentId) {
        //todo#13 - 핵생 -> 클럽 등록, executeUpdate() 결과를 반환
        String sql = "select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a inner join jdbc_club_registrations b on a.id=b.student_id inner join jdbc_club c on b.club_id=c.club_id where a.id=?";
        List<ClubStudent> list = new ArrayList<>();

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, studentId);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                list.add(new ClubStudent(rs.getString("student_id"),
                                        rs.getString("student_name"),
                                        rs.getString("club_id"),
                                        rs.getString("club_name")));
            }
            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<ClubStudent> getClubStudents(Connection connection, String sql) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            ResultSet rs = preparedStatement.executeQuery();
            List<ClubStudent> list = new ArrayList<>();
            while (rs.next()) {
                list.add(new ClubStudent(rs.getString("student_id"),
                                        rs.getString("student_name"),
                                        rs.getString("club_id"),
                                        rs.getString("club_name")));
            }
            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ClubStudent> findClubStudents(Connection connection) {
        //todo#21 - join
        String sql = "SELECT a.id as student_id, a.name as student_name, c.club_id, c.club_name FROM jdbc_students a INNER JOIN jdbc_club_registrations b ON a.id = b.student_id INNER JOIN jdbc_club c ON b.club_id = c.club_id ";
        return getClubStudents(connection, sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_left_join(Connection connection) {
        //todo#22 - left join
        String sql = "SELECT a.id as student_id, a.name as student_name, c.club_id, c.club_name FROM jdbc_students a LEFT JOIN jdbc_club_registrations b ON a.id = b.student_id LEFT JOIN jdbc_club c ON b.club_id = c.club_id ";
        return getClubStudents(connection, sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_right_join(Connection connection) {
        //todo#23 - right join
        String sql = "SELECT a.id as student_id, a.name as student_name, c.club_id, c.club_name FROM jdbc_students a RIGHT JOIN jdbc_club_registrations b ON a.id = b.student_id RIGHT JOIN jdbc_club c ON b.club_id = c.club_id ";
        return getClubStudents(connection, sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_full_join(Connection connection) {
        //todo#24 - full join = left join union right join
        String sql = "SELECT a.id as student_id, a.name as student_name, c.club_id, c.club_name FROM jdbc_students a LEFT JOIN jdbc_club_registrations b ON a.id = b.student_id LEFT JOIN jdbc_club c ON b.club_id = c.club_id "
        + " UNION " + "SELECT a.id as student_id, a.name as student_name, c.club_id, c.club_name FROM jdbc_students a RIGHT JOIN jdbc_club_registrations b ON a.id = b.student_id RIGHT JOIN jdbc_club c ON b.club_id = c.club_id ";
        return getClubStudents(connection, sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_left_excluding_join(Connection connection) {
        //todo#25 - left excluding join
        String sql = "SELECT a.id as student_id, a.name as student_name, c.club_id, c.club_name FROM jdbc_students a LEFT JOIN jdbc_club_registrations b ON a.id = b.student_id LEFT JOIN jdbc_club c ON b.club_id = c.club_id WHERE c.club_id IS NULL";
        return getClubStudents(connection, sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_right_excluding_join(Connection connection) {
        //todo#26 - right excluding join
        String sql = "SELECT a.id as student_id, a.name as student_name, c.club_id, c.club_name FROM jdbc_students a RIGHT JOIN jdbc_club_registrations b ON a.id = b.student_id RIGHT JOIN jdbc_club c ON b.club_id = c.club_id WHERE a.id IS NULL";
        return getClubStudents(connection, sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_outher_excluding_join(Connection connection) {
        //todo#27 - outher_excluding_join = left excluding join union right excluding join
        String sql = "SELECT a.id as student_id, a.name as student_name, c.club_id, c.club_name FROM jdbc_students a LEFT JOIN jdbc_club_registrations b ON a.id = b.student_id LEFT JOIN jdbc_club c ON b.club_id = c.club_id WHERE c.club_id IS NULL"
        + " UNION " + "SELECT a.id as student_id, a.name as student_name, c.club_id, c.club_name FROM jdbc_students a RIGHT JOIN jdbc_club_registrations b ON a.id = b.student_id RIGHT JOIN jdbc_club c ON b.club_id = c.club_id WHERE a.id IS NULL";
        return getClubStudents(connection, sql);
    }
}
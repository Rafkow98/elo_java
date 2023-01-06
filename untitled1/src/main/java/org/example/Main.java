package org.example;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.time.temporal.ChronoUnit.DAYS;

public class Main {
    public static Connection connection;


    public static void main(String[] args) throws SQLException {

        Instant start = Instant.now();
        LocalDate today = LocalDate.now();
        try {
            openDatabaseConnection();
            resetElo();
            for (int raceId = 431; raceId <= 465; raceId++) {
                int count = getDriversCount(raceId);
                if (count != 0) {
                    if (getRaceYear(raceId) == 2019) {
                        String game = getGame(raceId);
                        updateEloF1(game, count, raceId, today);
                    }
                    System.out.println("race:" + raceId + " done");
                }
            }
            for (int raceId = 1; raceId < 1000; raceId++) {
                int count = getDriversCount(raceId);
                if (count != 0) {
                    if (getRaceYear(raceId) > 2019 && getPlatform(raceId).equals("PC")) {
                        String game = getGame(raceId);
                        if (game.startsWith("F1")) {updateEloF1(game, count, raceId, today);}
                        else if (game.equals("AC")) {updateEloAcAcc("AC", count, raceId, today);}
                        else if (game.equals("ACC")) {updateEloAcAcc("ACC", count, raceId, today);}
                    }
                    System.out.println("race:" + raceId + " done");
                }
            }
        } finally {
            closeDatabaseConnection();
        }
        Instant end = Instant.now();
        System.out.println("Elapsed time: " + Duration.between(start, end).toString());
    }

    private static void openDatabaseConnection() throws SQLException {
        connection = DriverManager.getConnection(
                "jdbc:mariadb://localhost:3306/$DATABASE", // database name
                "$USER", // username
                "$PASSWORD" // password
        );
    }

    private static void closeDatabaseConnection() throws SQLException {
        connection.close();
    }

    private static void updateElo(int id, int new_elo, String game) throws SQLException {
        switch (game) {
            case "F1 2019":
                try (PreparedStatement statement = connection.prepareStatement("update elo set elo_f12019 = ? where driverid = ?")) {
                    statement.setInt(1, new_elo);
                    statement.setInt(2, id);
                    statement.executeUpdate();
                }
                break;
            case "F1 2020":
                try (PreparedStatement statement = connection.prepareStatement("update elo set elo_f12020 = ? where driverid = ?")) {
                    statement.setInt(1, new_elo);
                    statement.setInt(2, id);
                    statement.executeUpdate();
                }
                break;
            case "F1 2021":
                try (PreparedStatement statement = connection.prepareStatement("update elo set elo_f12021 = ? where driverid = ?")) {
                    statement.setInt(1, new_elo);
                    statement.setInt(2, id);
                    statement.executeUpdate();
                }
                break;
            case "F1 22":
                try (PreparedStatement statement = connection.prepareStatement("update elo set elo_f122 = ? where driverid = ?")) {
                    statement.setInt(1, new_elo);
                    statement.setInt(2, id);
                    statement.executeUpdate();
                }
                break;
            case "AC":
                try (PreparedStatement statement = connection.prepareStatement("update elo set elo_ac = ? where driverid = ?")) {
                    statement.setInt(1, new_elo);
                    statement.setInt(2, id);
                    statement.executeUpdate();
                }
                break;
            case "ACC":
                try (PreparedStatement statement = connection.prepareStatement("update elo set elo_acc = ? where driverid = ?")) {
                    statement.setInt(1, new_elo);
                    statement.setInt(2, id);
                    statement.executeUpdate();
                }
                break;
            case "F1":
                try (PreparedStatement statement = connection.prepareStatement("update elo set elo_f1_all = ? where driverid = ?")) {
                    statement.setInt(1, new_elo);
                    statement.setInt(2, id);
                    statement.executeUpdate();
                }
                break;
        }
    }

    private static int getDriversCount(int raceId) throws SQLException {
        int count = 0;
        try (PreparedStatement statement = connection.prepareStatement("select count(*) from raceresults where raceid = " + raceId)) {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            count = resultSet.getInt(1);
        }
        return count;
    }

    private static void resetElo() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("update elo set elo_f12019 = 1500, elo_f12020 = 1500, " +
                "elo_f12021 = 1500, elo_f122 = 1500, elo_ac = 1500, elo_acc = 1500, elo_f1_all = 1500")) {
            statement.executeUpdate();
        }
    }

    private static int getDriverId(int raceId, int position) throws SQLException {
        int id = 0;
        try (PreparedStatement statement = connection.prepareStatement("select driver from raceresults where position =" + position + " and raceid = " + raceId)) {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            id = resultSet.getInt(1);
        }
        return id;
    }

    private static int getDriverElo(int driverId, String game) throws SQLException {
        int elo = 0;
        switch (game) {
            case "F1 2019":
                try (PreparedStatement statement = connection.prepareStatement("select elo_f12019 from elo where driverid = " + driverId)) {
                    ResultSet resultSet = statement.executeQuery();
                    resultSet.next();
                    elo = resultSet.getInt(1);
                }
                break;
            case "F1 2020":
                try (PreparedStatement statement = connection.prepareStatement("select elo_f12020 from elo where driverid = " + driverId)) {
                    ResultSet resultSet = statement.executeQuery();
                    resultSet.next();
                    elo = resultSet.getInt(1);
                }
                break;
            case "F1 2021":
                try (PreparedStatement statement = connection.prepareStatement("select elo_f12021 from elo where driverid = " + driverId)) {
                    ResultSet resultSet = statement.executeQuery();
                    resultSet.next();
                    elo = resultSet.getInt(1);
                }
                break;
            case "F1 22":
                try (PreparedStatement statement = connection.prepareStatement("select elo_f122 from elo where driverid = " + driverId)) {
                    ResultSet resultSet = statement.executeQuery();
                    resultSet.next();
                    elo = resultSet.getInt(1);
                }
                break;
            case "AC":
                try (PreparedStatement statement = connection.prepareStatement("select elo_ac from elo where driverid = " + driverId)) {
                    ResultSet resultSet = statement.executeQuery();
                    resultSet.next();
                    elo = resultSet.getInt(1);
                }
                break;
            case "ACC":
                try (PreparedStatement statement = connection.prepareStatement("select elo_acc from elo where driverid = " + driverId)) {
                    ResultSet resultSet = statement.executeQuery();
                    resultSet.next();
                    elo = resultSet.getInt(1);
                }
                break;
            case "F1":
                try (PreparedStatement statement = connection.prepareStatement("select elo_f1_all from elo where driverid = " + driverId)) {
                    ResultSet resultSet = statement.executeQuery();
                    resultSet.next();
                    elo = resultSet.getInt(1);
                }
                break;
        }
        return elo;
    }

    private static int getRaceYear(int raceId) throws SQLException {
        int year = 0;
        try (PreparedStatement statement = connection.prepareStatement("select year(starts) from raceresults r left join events e on r.eventid = e.id where raceid = " + raceId + " limit 1")) {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            year = resultSet.getInt(1);
        }
        return year;
    }

    private static String getGame(int raceId) throws SQLException {
        String game;
        try (PreparedStatement statement = connection.prepareStatement("select game from raceresults r left join leagues l on r.relatedleague = l.id where raceid = " + raceId + " limit 1")) {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            game = resultSet.getString(1);
        }
        return game;
    }

    private static String getSplit(int raceId) throws SQLException {
        String split;
        try (PreparedStatement statement = connection.prepareStatement("select split from raceresults r left join leagues l on r.relatedleague = l.id where raceid = " + raceId + " limit 1")) {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            split = resultSet.getString(1);
        }
        return split;
    }

    private static boolean getPole(int raceId, int position) throws SQLException {
        boolean pole;
        try (PreparedStatement statement = connection.prepareStatement("select pole from raceresults r left join leagues l on r.relatedleague = l.id where raceid = " + raceId + " and position = " + position)) {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            String output = resultSet.getString(1);
            if (output.equals("true")) {pole = true;}
            else {pole = false;}
        }
        return pole;
    }

    private static boolean getFastestLap(int raceId, int position) throws SQLException {
        boolean fl;
        try (PreparedStatement statement = connection.prepareStatement("select lap from raceresults r left join leagues l on r.relatedleague = l.id where raceid = " + raceId + " and position = " + position)) {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            String output = resultSet.getString(1);
            if (output.equals("true")) {fl = true;}
            else {fl = false;}
        }
        return fl;
    }

    private static boolean getDnf(int raceId, int position) throws SQLException {
        boolean dnf;
        try (PreparedStatement statement = connection.prepareStatement("select dnf from raceresults r left join leagues l on r.relatedleague = l.id where raceid = " + raceId + " and position = " + position)) {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            String output = resultSet.getString(1);
            if (output.equals("true")) {dnf = true;}
            else {dnf = false;}
        }
        return dnf;
    }

    private static String getPlatform(int raceId) throws SQLException {
        String platform;
        try (PreparedStatement statement = connection.prepareStatement("select platform from raceresults r left join leagues l on r.relatedleague = l.id where raceid = " + raceId + " limit 1")) {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            platform = resultSet.getString(1);
        }
        return platform;
    }

    private static LocalDate getDate(int raceId) throws SQLException {
        LocalDate date;
        try (PreparedStatement statement = connection.prepareStatement("select starts from raceresults r left join events e on r.eventid = e.id where raceid = " + raceId)) {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            String output = resultSet.getString(1);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            date = LocalDate.parse(output, formatter);
        }
        return date;
    }

    private static void updateEloF1(String game, int driverCount, int raceId, LocalDate today) throws SQLException {
        Map<Integer, Integer> eloMapAll = new LinkedHashMap<>();
        Map<Integer, Integer> eloMapGame = new LinkedHashMap<>();
        for (int positionA = 1; positionA <= driverCount; positionA++) {
            eloMapAll.put(positionA, getDriverElo(getDriverId(raceId, positionA), "F1"));
            eloMapGame.put(positionA, getDriverElo(getDriverId(raceId, positionA), game));
        }
        for (int positionA = 1; positionA <= driverCount; positionA++) {
            int driverA = getDriverId(raceId, positionA);
            int driverAElo = eloMapAll.get(positionA);
            int driverAEloGame = eloMapGame.get(positionA);
            int eloDifference = 0;
            int eloDifferenceGame = 0;
            if (getPole(raceId, positionA)) {eloDifference += 3; eloDifferenceGame += 3;}
            if (getFastestLap(raceId, positionA)) {eloDifference += 3; eloDifferenceGame += 3;}
            for (int positionB = 1; positionB <= driverCount; positionB++) {
                int driverBElo = eloMapAll.get(positionB);
                int driverBEloGame = eloMapGame.get(positionB);
                if (positionA < positionB) {
                    int newEloPart = calculate2PlayersRating(driverAElo, driverBElo, "+");
                    eloDifference += newEloPart - driverAElo;
                    int newEloPartGame = calculate2PlayersRating(driverAEloGame, driverBEloGame, "+");
                    eloDifferenceGame += newEloPartGame - driverAEloGame;
                }
                else if (positionA > positionB) {
                    int newEloPart = calculate2PlayersRating(driverAElo, driverBElo, "-");
                    eloDifference += newEloPart - driverAElo;
                    int newEloPartGame = calculate2PlayersRating(driverAEloGame, driverBEloGame, "-");
                    eloDifferenceGame += newEloPartGame - driverAEloGame;
                }
            }
            if (getDnf(raceId, positionA)) {eloDifference = eloDifference / 2; eloDifferenceGame = eloDifferenceGame / 2;}
            if (getSplit(raceId).equals("B")) {eloDifference = eloDifference - 15; eloDifferenceGame = eloDifferenceGame - 15;}
            else if (getSplit(raceId).equals("C")) {eloDifference = eloDifference - 30; eloDifferenceGame = eloDifferenceGame - 30;}
            double dateFraction = -0.001 * (DAYS.between(getDate(raceId), today)) + 1;
            int newElo = (int)(driverAElo + Math.round(eloDifference * dateFraction));
            int newEloGame = driverAEloGame + eloDifferenceGame;
            updateElo(driverA, newElo, "F1");
            updateElo(driverA, newEloGame, game);
        }
    }

    private static void updateEloAcAcc(String game, int driverCount, int raceId, LocalDate today) throws SQLException {
        Map<Integer, Integer> eloMap = new LinkedHashMap<>();
        for (int positionA = 1; positionA <= driverCount; positionA++) {
            eloMap.put(positionA, getDriverElo(getDriverId(raceId, positionA), game));
        }
        for (int positionA = 1; positionA <= driverCount; positionA++) {
            int driverA = getDriverId(raceId, positionA);
            int driverAElo = eloMap.get(positionA);
            int eloDifference = 0;
            if (getPole(raceId, positionA)) {eloDifference += 3;}
            if (getFastestLap(raceId, positionA)) {eloDifference += 3;}
            for (int positionB = 1; positionB <= driverCount; positionB++) {
                int driverBElo = eloMap.get(positionB);
                if (positionA < positionB) {
                    int newEloPart = calculate2PlayersRating(driverAElo, driverBElo, "+");
                    eloDifference += newEloPart - driverAElo;
                }
                else if (positionA > positionB) {
                    int newEloPart = calculate2PlayersRating(driverAElo, driverBElo, "-");
                    eloDifference += newEloPart - driverAElo;
                }
            }
            if (getDnf(raceId, positionA)) {eloDifference = eloDifference / 2;}
            double dateFraction = -0.001 * (DAYS.between(getDate(raceId), today)) + 1;
            int newElo = (int)(driverAElo + Math.round(eloDifference * dateFraction));
            updateElo(driverA, newElo, game);
        }
    }

    private static int calculate2PlayersRating(int player1Rating, int player2Rating, String outcome) {
            double actualScore;
            if (outcome.equals("+")) {
                actualScore = 1.0;
            } else if (outcome.equals("-")) {
                actualScore = 0;
            } else {
                return player1Rating;
            }
            double exponent = (double) (player2Rating - player1Rating) / 400;
            double expectedOutcome = (1 / (1 + (Math.pow(10, exponent))));
            int K = 6;
            int newRating = (int) Math.round(player1Rating + K * (actualScore - expectedOutcome));
            return newRating;
        }
}
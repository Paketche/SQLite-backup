package valchanov.georgi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

class Utils {

    static String ANY_PATTERN = "%";

//    /**
//     * Iterates over a result set
//     *
//     * @param resultSet
//     *            to be iterated over
//     * @param consumer
//     *            function to use the current iteration in the result set
//     *
//     * @return true if the result set was not empty;
//     * @throws SQLException
//     *             if a database access error occurs or this method is called on a
//     *             closed connection
//     */
//    public <T> ArrayList<T> resultSetIterator(ResultSet resultSet, CheckedConsumer<ResultSet> consumer) throws SQLException {
//        if (resultSet.isAfterLast())
//            return new ArrayList<>();
//
//        while (resultSet.next()) {
//            consumer.accept(resultSet);
//        }
//        return true;
//    }

    static <T> List<T> collectResultSetToList(ResultSet set, CheckedFunction<ResultSet, T> mappingFunction) throws SQLException {
        ArrayList<T> list = new ArrayList<>();
        if (!set.isAfterLast())
            while (set.next()) {
                list.add(mappingFunction.apply(set));
            }
        return list;
    }

    static <K, V> Map<K, V> collectResultSetToMap(ResultSet set, CheckedFunction<ResultSet, K> keyFunction, CheckedFunction<ResultSet, V> valueFunction) throws SQLException {
        HashMap<K, V> map = new LinkedHashMap<>();
        if (!set.isAfterLast()) {
            while (set.next()) {
                map.put(keyFunction.apply(set), valueFunction.apply(set));
            }
        }
        return map;
    }

    static <K, V> Map<K, List<V>> collectResultToMapOfLists(ResultSet set, CheckedFunction<ResultSet, K> keyFunction, CheckedFunction<ResultSet, V> valueFunction) throws SQLException {
        Map<K, List<V>> map = new LinkedHashMap<>();
        if (!set.isAfterLast()) {
            while (set.next()) {
                K key = keyFunction.apply(set);
                V value = valueFunction.apply(set);

                map.putIfAbsent(key, new ArrayList<>());

                map.get(key).add(value);
            }
        }
        return map;
    }

    static void forEachResult(ResultSet set, CheckedConsumer<ResultSet> consumer) throws SQLException {
        if (!set.isAfterLast()) {
            while (set.next()) {
                consumer.accept(set);
            }
        }
    }

    static <T> String commaSeparatedList(Collection<T> collection, Function<T, String> mappingFunction, String formatting) {
        return collection.stream().map(mappingFunction).collect(Collectors.joining("," + formatting));
    }

    static String commaSeparatedList(Collection<String> collection, String formatting) {
        return commaSeparatedList(collection, s -> s, formatting);
    }
}

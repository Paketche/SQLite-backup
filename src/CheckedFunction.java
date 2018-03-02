import java.sql.SQLException;

/**
 * It a version of the {@link Function} interface but this one throws an
 * {@link SQLException}
 *
 * @param <T>
 *            the type of the input to the function
 * @param <R>
 *            the type of the output of the function
 */
public interface CheckedFunction<T, R> {
	/**
	 * Performs this operation on the given argument
	 * 
	 * @param t
	 *            the function argument
	 * @return the function result
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public R apply(T t) throws SQLException;
}
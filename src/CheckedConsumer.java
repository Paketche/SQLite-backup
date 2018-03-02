import java.sql.SQLException;

/**
 * It a version of the {@link Consumer} interface but this one throws an
 * {@link SQLException}
 * 
 * @param <T>
 *            the type of the output of the function
 */
public interface CheckedConsumer<T> {
	/**
	 * Performs this operation on the given argument
	 * 
	 * @param t
	 *            the operation argument
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void accept(T t) throws SQLException;
}
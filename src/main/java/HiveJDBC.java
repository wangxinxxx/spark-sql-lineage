//
////import org.apache.hive.jdbc.HiveStatement;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.lang.reflect.Field;
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
///**
// * Hive JDBC Utils
// */
//public class HiveJDBC {
//	private static final Logger logger = LoggerFactory.getLogger(HiveJDBC.class);
//
//	//Connection
//	public static final String URL = "jdbc:hive2://hiveserver.58dns.org:10000/hdp_zhuanzhuan_dw_global";
//	public static final String URL2 = "jdbc:hive2://hiveserver.58dns.org:10000";
//	public static final String USER_NAME = "hdp_ubu_zhuanzhuan";
//	public static final String PASSWORD = "";
//    //driver
//	public static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
//    //hive 执行参数
//	public static final String HIVE_QUEUE = "set mapreduce.job.queuename=root.offline.hdp_ubu_zhuanzhuan.platform";
//	public static final String JON_NAME = "set wbdp.job.name=zhuanzhuan_zeye_hive_zhangyecheng";
//	public static final String DEPARTMENT = "set 58.department=转转技术部";
//	public static final String HIVE_UER = "set 58.user=zhangyecheng";
//	public static final String PARAMS = "set hive.merge.smallfiles.avgsize=32000000";
//	public static final String PARALLEL = "set hive.exec.parallel=true";
////	public static final String JOIN = "set hive.auto.convert.join=true";
//
//    //UDF 参数
//	public static final String JSON_UDF_JAR = "ADD JAR hdfs://hdp-58-cluster/home/hdp_58dp/udf/brickhouse-0.7.1.jar";
//	public static final String JSON_UDF_METHOD = "CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF'";
//
//	//参数组合
//	public static final String[] hiveQarams = {HIVE_QUEUE,JON_NAME,DEPARTMENT,HIVE_UER,PARAMS,PARALLEL};
//
//	/**
//    * 查询表示例
//    * @param hql
//    * @return List<String> 返回每行数据 逗号分隔
//    */
//    public static List<String> queryDataByHiveSQL(String hql) {
//
//    	List<String> rsList = new ArrayList<String>();
//
//    	Connection conn = null;
//        Statement stmt = null;
//        ResultSet rs = null;
//        try {
//			Class.forName(DRIVER).newInstance();
//			conn = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
//			//设置参数
//			stmt = conn.createStatement();
//            System.out.println("");
//			System.out.println("----------------------------------------------------begin("+DateUtil.getTodayDate("yyyy-MM-dd HH:mm:ss")+")------------------------------------------------------");
//            for(String setting:hiveQarams){
//                System.out.println(setting);
//                stmt.execute(setting);
//			}
//
//			Thread logThread = new Thread(new LogRunnable((HiveStatement) stmt));
//			logThread.setDaemon(true);
//			logThread.start();
//
//			long start = System.currentTimeMillis();
//			System.out.println(hql);
//			rs = stmt.executeQuery(hql);
//			System.out.println("-------------------------finish(耗时："+(System.currentTimeMillis()-start)/1000/60+" 分 ,结束时间 "+DateUtil.getTodayDate("yyyy-MM-dd HH:mm:ss")+")------------------------");
//
//			ResultSetMetaData rsmd = rs.getMetaData();
//			int col = rsmd.getColumnCount();
//			while (rs.next()) {
//				StringBuffer tempRowBuffer = new StringBuffer();
//				for(int i=0;i<col;i++){
//					tempRowBuffer.append(rs.getString(i+1)).append(",");
//				}
//				rsList.add(tempRowBuffer.toString().substring(0, tempRowBuffer.length()-1));
//			}
//			rs.close();
//			stmt.close();
//			conn.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}finally {
//			try {
//				rs.close();
//				stmt.close();
//				conn.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//        return rsList;
//    }
//
//	/**
//	 * 查询表示例
//	 * @param hql
//	 * @return List<String> 返回每行数据 指定分隔符
//	 */
//	public static List<String> queryDataByHiveSQLWithSplit(String hql,String split) {
//
//		List<String> rsList = new ArrayList<String>();
//
//		Connection conn = null;
//		Statement stmt = null;
//		ResultSet rs = null;
//		try {
//			Class.forName(DRIVER).newInstance();
//			conn = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
//			//设置参数
//			stmt = conn.createStatement();
//			System.out.println("");
//			System.out.println("----------------------------------------------------begin("+DateUtil.getTodayDate("yyyy-MM-dd HH:mm:ss")+")------------------------------------------------------");
//			for(String setting:hiveQarams){
//				System.out.println(setting);
//				stmt.execute(setting);
//			}
//
//			Thread logThread = new Thread(new LogRunnable((HiveStatement) stmt));
//			logThread.setDaemon(true);
//			logThread.start();
//
//			long start = System.currentTimeMillis();
//			System.out.println(hql);
//			rs = stmt.executeQuery(hql);
//			System.out.println("-------------------------finish(耗时："+(System.currentTimeMillis()-start)/1000/60+" 分 ,结束时间 "+DateUtil.getTodayDate("yyyy-MM-dd HH:mm:ss")+")------------------------");
//
//			ResultSetMetaData rsmd = rs.getMetaData();
//			int col = rsmd.getColumnCount();
//			while (rs.next()) {
//				StringBuffer tempRowBuffer = new StringBuffer();
//				for(int i=0;i<col;i++){
//					tempRowBuffer.append(rs.getString(i+1)).append(split);
//				}
//				rsList.add(tempRowBuffer.toString().substring(0, tempRowBuffer.length()-1));
//			}
//			rs.close();
//			stmt.close();
//			conn.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}finally {
//			try {
//				rs.close();
//				stmt.close();
//				conn.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		return rsList;
//	}
//	/**
//	 * 查询表示例
//	 * @param hql
//	 * @return List<String> 返回每行数据 逗号分隔
//	 */
//	public static List<String> queryDataByHiveSQLWithOutLog(String hql) {
//
//		List<String> rsList = new ArrayList<String>();
//
//		Connection conn = null;
//		Statement stmt = null;
//		ResultSet rs = null;
//		try {
//			Class.forName(DRIVER).newInstance();
//			conn = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
//			//设置参数
//			stmt = conn.createStatement();
//			for(String setting:hiveQarams){
//				stmt.execute(setting);
//			}
//
//			Thread logThread = new Thread(new LogRunnable((HiveStatement) stmt));
//			logThread.setDaemon(true);
//			logThread.start();
//
//			rs = stmt.executeQuery(hql);
//
//			ResultSetMetaData rsmd = rs.getMetaData();
//			int col = rsmd.getColumnCount();
//			while (rs.next()) {
//				StringBuilder tempRowBuffer = new StringBuilder();
//				for(int i=0;i<col;i++){
//					tempRowBuffer.append(rs.getString(i+1)).append(",");
//				}
//				rsList.add(tempRowBuffer.substring(0, tempRowBuffer.length()-1));
//			}
//			rs.close();
//			stmt.close();
//			conn.close();
//		}catch(SQLException e){
//			logger.error("表为非分区表, 忽略异常", e);
//			//检查分区专用查询sql，异常为非分区表，不予处理
//		}catch (Exception e) {
//			logger.error("查询异常", e);
//		}finally {
//			try {
//				if(rs!=null){
//					rs.close();
//				}
//				if(stmt!=null){
//					stmt.close();
//				}
//				if(conn!=null){
//					conn.close();
//				}
//			} catch (Exception e) {
//				logger.error("", e);
//			}
//		}
//		return rsList;
//	}
//
//	public static Set<String> queryTableExists(String hql) {
//
//		Set<String> rsList = new HashSet<String>();
//
//		Connection conn = null;
//		Statement stmt = null;
//		ResultSet rs = null;
//		try {
//			Class.forName(DRIVER).newInstance();
//			conn = DriverManager.getConnection(URL2, USER_NAME, PASSWORD);
//			//设置参数
//			stmt = conn.createStatement();
//			for(String setting:hiveQarams){
//				stmt.execute(setting);
//			}
//
//			Thread logThread = new Thread(new LogRunnable((HiveStatement) stmt));
//			logThread.setDaemon(true);
//			logThread.start();
//
//			rs = stmt.executeQuery(hql);
//
//			ResultSetMetaData rsmd = rs.getMetaData();
//			int col = rsmd.getColumnCount();
//			while (rs.next()) {
//				StringBuffer tempRowBuffer = new StringBuffer();
//				for(int i=0;i<col;i++){
//					tempRowBuffer.append(rs.getString(i+1)).append(",");
//				}
//				rsList.add(tempRowBuffer.toString().substring(0, tempRowBuffer.length()-1));
//			}
//			rs.close();
//			stmt.close();
//			conn.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if(rs!=null){
//					rs.close();
//				}
//				if(stmt!=null){
//					stmt.close();
//				}
//				if(conn!=null){
//					conn.close();
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		return rsList;
//	}
//
//	private static Connection conn = null;
//
//	private static Connection getConnection2() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
//		if (conn == null) {
//			Class.forName(DRIVER).newInstance();
//			conn = DriverManager.getConnection(URL2, USER_NAME, PASSWORD);
//		}
//		return conn;
//	}
//	private static void closeConn() throws SQLException {
//		if (conn != null){
//			conn.close();
//			conn = null;
//		}
//	}
//	public static <M> List<M> queryTableDetails(String hql, Class<M> clazz, Object... params) {
//		List<M> list = new ArrayList<>();
//		try {
//			Class.forName(DRIVER).newInstance();
//			try (Connection conn = DriverManager.getConnection(URL2, USER_NAME, PASSWORD);
//				 PreparedStatement ps = conn.prepareStatement(hql)) {
//				//设置参数
//				setParameters(ps, params);
//				for (String setting : hiveQarams) {
//					ps.execute(setting);
//				}
//				Thread logThread = new Thread(new LogRunnable((HiveStatement) ps));
//				logThread.setDaemon(true);
//				logThread.start();
//
//				try (ResultSet rs = ps.executeQuery()) {
//					Field[] fields = clazz.getDeclaredFields();
//					ResultSetMetaData rsmd = rs.getMetaData();
//					int columnCount = rsmd.getColumnCount();
//
//					while (rs.next()) {
//						M object = clazz.getDeclaredConstructor().newInstance();
//						for (int i = 1; i <= columnCount; i++) {
//							String columnLabel = rsmd.getColumnLabel(i);
//							for (Field field : fields) {
//								if (field.getName().equals(columnLabel)) {
//									setFieldValue(rs, rsmd, object, i, field);
//									break;
//								}
//							}
//						}
//						list.add(object);
//					}
//				}
//			}
//		} catch (Exception e) {
//            e.printStackTrace();
//        }
//        return list;
//	}
//	private static <M> void setFieldValue(ResultSet rs, ResultSetMetaData rsmd, M object, int columnIndex, Field field)
//			throws SQLException, IllegalAccessException {
//		field.setAccessible(true);
//		Object value = rs.getObject(columnIndex);
//
//		if (value != null) {
//			Class<?> fieldType = field.getType();
//			try {
//				// 类型转换
//				Object convertedValue = convertValue(value, fieldType);
//				field.set(object, convertedValue);
//			} catch (Exception e) {
//				// 如果转换失败，尝试直接设置
//				field.set(object, value);
//			}
//		}
//	}
//
//
//	public static <M> M executeQueryForSingleValue(String sql, Class<M> clazz, Object... params) {
//		try {
//			Class.forName(DRIVER).newInstance();
//			try (Connection conn = DriverManager.getConnection(URL2, USER_NAME, PASSWORD);
//				 PreparedStatement ps = conn.prepareStatement(sql)) {
//				//设置参数
//				setParameters(ps, params);
//				for (String setting : hiveQarams) {
//					ps.execute(setting);
//				}
//				Thread logThread = new Thread(new LogRunnable((HiveStatement) ps));
//				logThread.setDaemon(true);
//				logThread.start();
//
//				try (ResultSet rs = ps.executeQuery()) {
//					if (rs.next()) {
//						// 获取第一列的值
//						Object value = rs.getObject(1);
//						return convertValue(value, clazz);
//					} else {
//						return getDefaultValue(clazz);
//					}
//				}
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//			return getDefaultValue(clazz);
//		}
//	}
//
//	public static <M> List<M> executeQueryForObjectList(String sql, Class<M> clazz, Object... params) {
//		List<M> list = new ArrayList<>();
//		try {
//			Class.forName(DRIVER).newInstance();
//			try (Connection conn = DriverManager.getConnection(URL2, USER_NAME, PASSWORD);
//				 PreparedStatement ps = conn.prepareStatement(sql)) {
//				//设置参数
//				setParameters(ps, params);
//				for (String setting : hiveQarams) {
//					ps.execute(setting);
//				}
//				Thread logThread = new Thread(new LogRunnable((HiveStatement) ps));
//				logThread.setDaemon(true);
//				logThread.start();
//
//				try (ResultSet rs = ps.executeQuery()) {
//					Field[] fields = clazz.getDeclaredFields();
//					ResultSetMetaData rsmd = rs.getMetaData();
//					int columnCount = rsmd.getColumnCount();
//
//					while (rs.next()) {
//						M object = clazz.getDeclaredConstructor().newInstance();
//						for (int i = 1; i <= columnCount; i++) {
//							String columnLabel = rsmd.getColumnLabel(i);
//							for (Field field : fields) {
//								if (field.getName().equalsIgnoreCase(columnLabel)) {
//									setFieldValue(rs, rsmd, object, i, field);
//									break;
//								}
//							}
//						}
//						list.add(object);
//					}
//				}
//			}
//		} catch (Exception  e) {
//			e.printStackTrace();
//		}
//		return list;
//	}
//
//
//	private static void setParameters(PreparedStatement ps, Object... params) throws SQLException {
//		if (params != null && params.length > 0) {
//			for (int i = 0; i < params.length; i++) {
//				ps.setObject(i + 1, params[i]);
//			}
//		}
//	}
//
//	/**
//	 * 值类型转换
//	 */
//	@SuppressWarnings("unchecked")
//	private static <T> T convertValue(Object value, Class<T> targetType) {
//		if (value == null) {
//			return getDefaultValue(targetType);
//		}
//
//		// 如果已经是目标类型，直接返回
//		if (targetType.isInstance(value)) {
//			return (T) value;
//		}
//
//		try {
//			// 常用类型转换
//			if (targetType == String.class) {
//				return (T) value.toString();
//			} else if (targetType == Integer.class || targetType == int.class) {
//				return (T) Integer.valueOf(value.toString());
//			} else if (targetType == Long.class || targetType == long.class) {
//				return (T) Long.valueOf(value.toString());
//			} else if (targetType == Double.class || targetType == double.class) {
//				return (T) Double.valueOf(value.toString());
//			} else if (targetType == Float.class || targetType == float.class) {
//				return (T) Float.valueOf(value.toString());
//			} else if (targetType == Boolean.class || targetType == boolean.class) {
//				return (T) Boolean.valueOf(value.toString());
//			} else if (targetType == java.util.Date.class) {
//				if (value instanceof java.sql.Date) {
//					return (T) new java.util.Date(((java.sql.Date) value).getTime());
//				} else if (value instanceof java.sql.Timestamp) {
//					return (T) new java.util.Date(((java.sql.Timestamp) value).getTime());
//				} else if (value instanceof String) {
//					// 简单日期字符串解析（可根据需要扩展）
//					return (T) java.sql.Date.valueOf(value.toString());
//				}
//			}
//		} catch (Exception e) {
//			// 转换失败，返回默认值
//			return getDefaultValue(targetType);
//		}
//
//		// 默认尝试强制转换
//		try {
//			return (T) value;
//		} catch (ClassCastException e) {
//			return getDefaultValue(targetType);
//		}
//	}
//
//	/**
//	 * 获取类型的默认值
//	 */
//	@SuppressWarnings("unchecked")
//	private static <T> T getDefaultValue(Class<T> clazz) {
//		if (clazz == boolean.class) return (T) Boolean.FALSE;
//		if (clazz == byte.class) return (T) Byte.valueOf((byte) 0);
//		if (clazz == short.class) return (T) Short.valueOf((short) 0);
//		if (clazz == int.class) return (T) Integer.valueOf(0);
//		if (clazz == long.class) return (T) Long.valueOf(0L);
//		if (clazz == float.class) return (T) Float.valueOf(0.0f);
//		if (clazz == double.class) return (T) Double.valueOf(0.0);
//		if (clazz == char.class) return (T) Character.valueOf('\0');
//		return null;
//	}
//
//
//	private static <M> void judgeType(ResultSet rs, ResultSetMetaData rsmd, M m, int i, Field field) throws SQLException, IllegalAccessException, IOException {
//		field.setAccessible(true);
//		String fieldType = field.getType().getSimpleName();
//		switch (fieldType) {
//			case "boolean":
//			case "Boolean":
//				field.set(m, rs.getBoolean(i));
//				break;
//			case "byte":
//			case "Byte":
//				field.set(m, rs.getByte(i));
//				break;
//			case "short":
//			case "Short":
//				field.set(m, rs.getShort(i));
//				break;
//			case "int":
//			case "Integer":
//				field.set(m, rs.getInt(i));
//				break;
//			case "long":
//			case "Long":
//				field.set(m, rs.getLong(i));
//				break;
//			case "float":
//			case "Float":
//				field.set(m, rs.getFloat(i));
//				break;
//			case "double":
//			case "Double":
//				field.set(m, rs.getDouble(i));
//				break;
//			case "Date":
//				field.set(m, rs.getDate(i));
//				break;
//			case "Timestamp":
//				field.set(m, rs.getTimestamp(i));
//				break;
//			case "Time":
//				field.set(m, rs.getTime(i));
//				break;
//			case "String":
//				field.set(m, rs.getString(i));
//				break;
//			case "byte[]":
//				InputStream binaryStream = rs.getBlob(i).getBinaryStream();
//				byte[] bs = new byte[binaryStream.available()];
//				binaryStream.read(bs);
//				field.set(m, bs);
//				break;
//		}
//	}
//
//    /**
//     * 执行hive语句
//     * @param hql
//     * @return List<String> 返回每行数据 逗号分隔
//     */
//     public static void executeHiveSQL(String hql) {
//
//     	 Connection conn = null;
//         Statement stmt = null;
//         try {
// 			Class.forName(DRIVER).newInstance();
// 			conn = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
// 			//设置参数
// 			stmt = conn.createStatement();
//
// 			Thread logThread = new Thread(new LogRunnable((HiveStatement) stmt));
// 			logThread.setDaemon(true);
// 			logThread.start();
//
// 			System.out.println("");
// 			System.out.println("----------------------------------------------------begin("+DateUtil.getTodayDate("yyyy-MM-dd HH:mm:ss")+")------------------------------------------------------");
//
// 			for(String setting:hiveQarams){
//                System.out.println(setting);
//                stmt.execute(setting);
// 			}
//			long start = System.currentTimeMillis();
//			System.out.println(hql);
// 			stmt.execute(hql);
//			System.out.println("-------------------------finish(耗时："+(System.currentTimeMillis()-start)/1000/60+" 分 ,结束时间 "+DateUtil.getTodayDate("yyyy-MM-dd HH:mm:ss")+")------------------------");
//
//			stmt.close();
// 			conn.close();
// 		} catch (Exception e) {
// 			e.printStackTrace();
// 		}finally {
// 			try {
// 				stmt.close();
// 				conn.close();
// 			} catch (Exception e) {
// 				e.printStackTrace();
// 			}
// 		}
//     }
//
//	/**
//	 * 执行hive语句
//	 * @param hql
//	 * @return List<String> 返回每行数据 逗号分隔
//	 */
//	public static void executeHiveSQLWithHeads(List<String> heads,String hql) {
//
//		Connection conn = null;
//		Statement stmt = null;
//		try {
//			Class.forName(DRIVER).newInstance();
//			conn = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
//			//设置参数
//			stmt = conn.createStatement();
//
//			Thread logThread = new Thread(new LogRunnable((HiveStatement) stmt));
//			logThread.setDaemon(true);
//			logThread.start();
//
//			System.out.println("");
//            System.out.println("----------------------------------------------------begin("+DateUtil.getTodayDate("yyyy-MM-dd HH:mm:ss")+")------------------------------------------------------");
//            for(String setting:hiveQarams){
//                System.out.println(setting);
//                stmt.execute(setting);
//            }
//			for(String setting:heads){
//                System.out.println(setting);
//                stmt.execute(setting);
//			}
//			long start = System.currentTimeMillis();
//			System.out.println(hql);
//			stmt.execute(hql);
//			System.out.println("-------------------------finish(耗时："+(System.currentTimeMillis()-start)/1000/60+" 分 ,结束时间 "+DateUtil.getTodayDate("yyyy-MM-dd HH:mm:ss")+")------------------------");
//
//			stmt.close();
//			conn.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}finally {
//			try {
//				stmt.close();
//				conn.close();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//	}
//    public static Connection getConnection() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
//		Class.forName(DRIVER).newInstance();
//		return DriverManager.getConnection(URL, USER_NAME, PASSWORD);
//	}
//	public static void initEnvConfig(Connection connection) throws SQLException {
//		try(final Statement statement = connection.createStatement()){
//			for(String setting:hiveQarams){
//				System.out.println(setting);
//				statement.execute(setting);
//			}
//		}
//	}
//	public static class LogRunnable implements Runnable{
//
//     	private final HiveStatement hiveStatement;
//
//		public LogRunnable(HiveStatement hiveStatement) {
//			this.hiveStatement = hiveStatement;
//		}
//		private void updateQueryLog() {
//			try {
//				List<String> queryLogs = hiveStatement.getQueryLog();
//				for (String log : queryLogs)
//				{
//					System.out.println("进度信息-->"+log);
//				}
//
//			} catch (Exception e) {
//
//			}
//		}
//		public void run() {
//			try {
//				while (hiveStatement.hasMoreLogs()) {
//					updateQueryLog();
//					Thread.sleep(1000);
//				}
//			} catch (InterruptedException e) {
//				e.getStackTrace();
//			}
//		}
//	}
//}

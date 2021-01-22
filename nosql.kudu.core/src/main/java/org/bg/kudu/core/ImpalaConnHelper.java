package org.bg.kudu.core;

import org.bg.kudu.core.lib.ImpalaConnPoolHelper;
import org.bg.kudu.core.lib.KuduSessionPoolHelper;
import org.bg.kudu.util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * impala链接管理
 * @author xiatiansong
 *
 */
public class ImpalaConnHelper implements Closeable {

	private static final Log LOG = LogFactory.getLog(ImpalaConnHelper.class);

	private static ImpalaConnHelper INSTANCE;

	/**
	 * 根据kudu master 集群ip初始化客户端
	 * @param url
	 * @return
	 */
	public synchronized static ImpalaConnHelper getInstance(String url, String username, String password, String driverName, String database) {
		if (INSTANCE == null) {
			INSTANCE = new ImpalaConnHelper(url, username, password, driverName, database);
		}
		return INSTANCE;
	}

	private ImpalaConnHelper(String url, String username, String password, String driverName, String database) {
		ImpalaConnPoolHelper.getInstance(url, username, password, driverName, database);
	}

	/**
	 * 查询数据
	 * 
	 * @param sql
	 * @return
	 */
	public ResultSet executeQuery(String sql) {
		LOG.info(sql);
		ImpalaConnPoolHelper icp = null;
		ImpalaHelper ih = null;
		ResultSet rs = null;
		try {
			icp = ImpalaConnPoolHelper.getInstance();
			ih = icp.borrow();
			rs = ih.executeQuery(sql);
		} catch (Exception e) {
			LOG.error(LogUtil.getAllStackTrace(e));
		} finally {
			icp.restore(ih);
		}
		return rs;
	}

	/**
	 * 执行增、删、改/创建、删除表
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public int execute(String sql) {
		LOG.info(sql);
		ImpalaConnPoolHelper icp = null;
		ImpalaHelper ih = null;
		int rs = 0;
		try {
			icp = ImpalaConnPoolHelper.getInstance();
			ih = icp.borrow();
			rs = ih.execute(sql);
		} catch (Exception e) {
			LOG.error(LogUtil.getAllStackTrace(e));
		} finally {
			icp.restore(ih);
		}
		return rs;
	}
	
	public void closeResultSet(ResultSet rs) throws SQLException {
		if (null != rs) {
			Statement stmt = rs.getStatement();
			rs.close();
			stmt.close();
		}
	}

	@Override
	public void close() {
		KuduSessionPoolHelper.getInstance().close();
	}
}
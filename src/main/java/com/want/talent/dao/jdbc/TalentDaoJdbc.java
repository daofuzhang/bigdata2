package com.want.talent.dao.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.stereotype.Service;

import com.want.JobApplication;
import com.want.talent.dao.TalentDao;
import com.want.talent.vo.AbiScore;
import com.want.talent.vo.AudEvent;

@Service
@Configurable
public class TalentDaoJdbc implements TalentDao {
	private Logger logger = LoggerFactory.getLogger(TalentDaoJdbc.class);

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	@Autowired
	private SqlSession sqlSession;

	private Connection getConn() throws SQLException {
		return sqlSession.getConfiguration().getEnvironment().getDataSource().getConnection();
	}

	private void closeConn(Connection conn, PreparedStatement ps, ResultSet rs) {
		try {
			if (rs != null)
				rs.close();
		} catch (SQLException e) {
			logger.error("TalentDaoJdbc closeConn rs error "+e.getMessage());
		}
		try {
			if (ps != null)
				ps.close();
		} catch (SQLException e) {
			logger.error("TalentDaoJdbc closeConn ps error "+e.getMessage());
		}
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			logger.error("TalentDaoJdbc closeConn conn error "+e.getMessage());
		}
	}

	private static String REPLACE_INTO_ABI_SCORE_SQL = "REPLACE INTO ABI_SCORE (EMP_ID,TEMPLATE,TITLE,SCORE,TITLE_KIND) VALUES (?,?,?,?,?)";

	@Override
	public void replaceAbiscore(List<AbiScore> list) throws Exception {

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(REPLACE_INTO_ABI_SCORE_SQL);
			int count = 0;
			for (AbiScore abiScore : list) {
				ps.setString(1, abiScore.getEmpId());
				ps.setString(2, abiScore.getTemplate());
				ps.setString(3, abiScore.getTitle());
				ps.setString(4, abiScore.getScore());
				ps.setString(5, abiScore.getTitleKind());
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					logger.error("TalentDaoJdbc replaceAbiscore rollback conn error "+e.getMessage());
				}
			}
			logger.error("TalentDaoJdbc replaceAbiscore  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}
	
	private static String REPLACE_INTO_AUD_EVENT_SQL = "REPLACE INTO AUD_EVENT (EMP_ID,AUD_ID,AUD_DATE,AUD_DESC) VALUES (?,?,?,?)";

	@Override
	public void replaceAudEvent(List<AudEvent> list) throws Exception {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(REPLACE_INTO_AUD_EVENT_SQL);
			int count = 0;
			for (AudEvent abiEvent : list) {
				ps.setString(1, abiEvent.getEmpId());
				ps.setString(2, abiEvent.getAudId());
				ps.setString(3, abiEvent.getAudDate());
				ps.setString(4, abiEvent.getAudDesc());
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
			logger.error("TalentDaoJdbc replaceAudEvent  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}
	
	private static String INSERT_PUNISH_INFO_SQL = "INSERT INTO PUNISH_INFO (EMP_ID,PUNISH_DATE,PUNISH_KIND,PUNISH_TYPE,PUNISH_TIMES,CREATE_DATE) VALUES (?,?,?,?,?,?)";

	@Override
	public void insertPunishInfo(Queue<Map<String, String>> queue) throws Exception {

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(INSERT_PUNISH_INFO_SQL);
			int count = 0;
			Map<String, String> map;
			while ((map = queue.poll()) != null) {
				ps.setString(1, map.get("PERNR"));
				ps.setString(2, map.get("BEGDA"));
				ps.setString(3, map.get("ZJCLX"));
				ps.setString(4, map.get("ZJCLB"));
				ps.setString(5, map.get("ZTIMES"));
				ps.setString(6, sdf.format(new Date()));
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
			logger.error("TalentDaoJdbc insertPunishInfo  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	private static String REPLACE_KPI_EVENT_SQL = "REPLACE INTO KPI_EVENT (EMP_ID,KPI_YEAR,KPI_TYPE,SCORE_TYPE,STRENGTHS,WEAKNESSES,PLAN,TRAINING,CREATE_DATE) VALUES (?,?,?,?,?,?,?,?,?)";

	@Override
	public void replaceKpiEvent(Queue<Map<String, String>> queue) throws Exception {

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(REPLACE_KPI_EVENT_SQL);
			int count = 0;
			Map<String, String> map;
			while ((map = queue.poll()) != null) {
				ps.setString(1, map.get("PERNR"));
				ps.setString(2, map.get("ZKHND"));
				ps.setString(3, map.get("ZKHLX"));
				ps.setString(4, map.get("ZPGLX"));
				ps.setString(5, map.get("ZZCYD"));
				ps.setString(6, map.get("ZGSZD"));
				ps.setString(7, map.get("ZFZJH"));
				ps.setString(8, map.get("ZXLXQ"));
				ps.setString(9, sdf.format(new Date()));
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
			logger.error("TalentDaoJdbc replaceKpiEvent  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	private static String INSERT_KPI_INFO_SQL = "INSERT INTO KPI_INFO (EMP_ID,KPI_YEAR,KPI_TYPE,KPI_LEVEL,KPI_SCORE,CREATE_DATE) VALUES (?,?,?,?,?,?)";

	@Override
	public void insertKpiInfo(Queue<Map<String, String>> queue) throws Exception {

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(INSERT_KPI_INFO_SQL);
			int count = 0;
			Map<String, String> map;
			while ((map = queue.poll()) != null) {
				ps.setString(1, map.get("PERNR"));
				ps.setString(2, map.get("ZKHND"));
				ps.setString(3, map.get("ZKHLX"));
				ps.setString(4, map.get("ZKHDD"));
				ps.setString(5, map.get("ZSCORE"));
				ps.setString(6, sdf.format(new Date()));
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
			logger.error("TalentDaoJdbc insertKpiInfo  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	private static String INSERT_KPI_SCORE_SQL = "INSERT INTO KPI_SCORE (EMP_ID,KPI_YEAR,KPI_KIND,KPI_TYPE,KPI_TARGET,WORK_CONTENT,RATE,SELF_SCORE,FIRST_SCORE,SECOND_SCORE,CREATE_DATE) VALUES (?,?,?,?,?,?,?,?,?,?,?)";

	@Override
	public void insertKpiScore(Queue<Map<String, String>> queue) throws Exception {

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(INSERT_KPI_SCORE_SQL);
			int count = 0;
			Map<String, String> map;
			while ((map = queue.poll()) != null) {
				ps.setString(1, map.get("PERNR"));
				ps.setString(2, map.get("ZKHND"));
				ps.setString(3, map.get("ZKHLX"));
				ps.setString(4, map.get("ZPFLB"));
				ps.setString(5, map.get("ZGZZZ"));
				ps.setString(6, map.get("ZJTNR"));
				ps.setString(7, map.get("ZQZ"));
				ps.setString(8, map.get("ZZPFS"));
				ps.setString(9, map.get("ZMAFS"));
				ps.setString(10, map.get("ZMBFS"));
				ps.setString(11, sdf.format(new Date()));
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
			logger.error("TalentDaoJdbc insertKpiScore  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	private static String REPLACE_CHANGELOG_SQL = "REPLACE INTO CHANGE_LOG (EMP_ID,CHANGE_TYPE,CHANGE_DATE,OLD_POS_ID,OLD_POS_NAME,OLD_LEVEL,OLD_JOB,OLD_DEPARTMENT,NEW_POS_ID,NEW_POS_NAME,NEW_LEVEL,NEW_JOB,NEW_DEPARTMENT,CHANGE_LOG,CREATE_DATE)    VALUES    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	@Override
	public void replaceChangeLog(Queue<Map<String, String>> queue) throws Exception {

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<String, String> map = null;
		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(REPLACE_CHANGELOG_SQL);
			int count = 0;
			while ((map = queue.poll()) != null) {
				ps.setString(1, map.get("PERNR"));
				ps.setString(2, map.get("MASSN"));
				ps.setString(3, map.get("BEGDA"));
				ps.setString(4, map.get("ZPLANS"));
				ps.setString(5, map.get("ZPLSTX"));
				ps.setString(6, map.get("ZTRFGR"));
				ps.setString(7, map.get("ZSTLTX"));
				ps.setString(8, map.get("ZORGEH"));
				ps.setString(9, map.get("PLANS"));
				ps.setString(10, map.get("PLSTX"));
				ps.setString(11, map.get("TRFGR"));
				ps.setString(12, map.get("STLTX"));
				ps.setString(13, map.get("ORGEH"));
				ps.setString(14, map.get("ZYDSM"));
				ps.setString(15, sdf.format(new Date()));
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException ex) {
				logger.error("TalentDaoJdbc replaceChangeLog rollback error "+e.getMessage());
				ex.printStackTrace();
			}
			logger.error("TalentDaoJdbc replaceChangeLog  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	private static String REPLACE_EMPINFO_SQL = "REPLACE INTO EMP_INFO (EMP_ID,EMP_NAME,EMP_EDUCATE,MARRIED,SEX,BIRTHDAY,ENTRY_DATE,JOB_LEVEL,JOB_KIND,LOCATION,IS_LECTOR,CREATE_DATE)    VALUES    (?,?,?,?,?,?,?,?,?,?,?,?)";

	@Override
	public void replaceEmpInfo(Queue<Map<String, String>> queue) throws Exception {

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(REPLACE_EMPINFO_SQL);
			int count = 0;
			Map<String, String> map;
			while ((map = queue.poll()) != null) {
				ps.setString(1, map.get("PERNR"));
				ps.setString(2, map.get("NAME"));
				ps.setString(3, map.get("STEXT"));
				ps.setString(4, map.get("FAMST"));
				ps.setString(5, map.get("GESCH"));
				ps.setString(6, map.get("GBDAT"));
				if (map.get("DAT03").startsWith("0000")) {
					ps.setString(7, null);
				} else {
					ps.setString(7, map.get("DAT03"));
				}
				ps.setString(8, map.get("TRFGR"));
				ps.setString(9, map.get("STELL"));
				ps.setString(10, map.get("PERSG"));
				ps.setString(11, map.get("ZNBJS"));
				ps.setString(12, sdf.format(new Date()));
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
			logger.error("TalentDaoJdbc replaceEmpInfo  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	private static String REPLACE_POSINFO_SQL = "INSERT INTO POS_INFO"
			+ "(EMP_ID,POS_ID,POS_NAME,ORG_ID,ORG_NAME,DEPARTMENT,IS_POS_MAIN,IS_DIVISION_DIRECTOR,DIRECTOR_POS_ID,IS_DEPARTMENT_DIRECTOR,DIRECTOR_EMP_ID,IS_TOP_DIRECTOR,CREATE_DATE)    "
			+ "VALUES    (?,?,?,?,?,?,?,?,?,?,?,?,?)";

	@Override
	public void insertPosInfo(Queue<Map<String, String>> queue) throws Exception {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(REPLACE_POSINFO_SQL);
			int count = 0;
			Map<String, String> map;
			while ((map = queue.poll()) != null) {
				ps.setString(1, map.get("PERNR"));// EMP_ID
				ps.setString(2, map.get("PLANS"));// POS_ID
				ps.setString(3, map.get("PLSTX"));// POS_NAME
				ps.setString(4, map.get("ORGEH"));// ORG_ID
				ps.setString(5, map.get("ORGTX"));// ORG_NAME
				ps.setString(6, map.get("LEVEL"));// DEPARTMENT
				ps.setString(7, map.get("ZZGW"));// IS_POS_MAIN
				ps.setString(8, map.get("ZDWZG"));// IS_POS_MASTER IS_DIVISION_DIRECTOR 是否单位主管
				ps.setString(9, map.get("ZGSOBID"));// MANAGER_POS_ID DIRECTOR_POS_ID
				ps.setString(10, map.get("ZBMZG"));// IS_DEPARTMENT_DIRECTOR 是否部门主管
				ps.setString(11, map.get("ZSJZG"));// DIRECTOR_EMP_ID
				ps.setString(12, map.get("ZDWZGZG"));// IS_TOP_DIRECTOR 是否单位最高主管
				ps.setString(13, sdf.format(new Date()));
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
			logger.error("TalentDaoJdbc insertPosInfo  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	private static String INSERT_TRAINING_LOG_SQL = "INSERT INTO TRAINING_LOG (EMP_ID,TRAINING_TYPE,TRAINING_ID,TRAINING_NAME,TRAINING_TYPE_D,TRAINING_ID_D,TRAINING_NAME_D,TRAINING_DATE,CREATE_DATE) VALUES(?,?,?,?,?,?,?,?,?)";

	public void insertTrainingLog(Queue<Map<String, String>> queue) throws Exception {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(INSERT_TRAINING_LOG_SQL);
			int count = 0;
			Map<String, String> map;
			while ((map = queue.poll()) != null) {
				ps.setString(1, map.get("PERNR"));
				ps.setString(2, map.get("ZPXLX1"));
				ps.setString(3, map.get("ZPXBM1"));
				ps.setString(4, map.get("ZPXMC1"));
				ps.setString(5, map.get("ZPXLX2"));
				ps.setString(6, map.get("ZPXBM2"));
				ps.setString(7, map.get("ZPXMC2"));
				ps.setString(8, map.get("BEGDA"));
				ps.setString(9, sdf.format(new Date()));
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}

			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			try {
				if (conn != null)
					conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
			logger.error("TalentDaoJdbc insertTrainingLog  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	private static String INSERT_TEACHING_SQL = "INSERT INTO TEACHING (EMP_ID,TEACH_TYPE,TEACH_ID,TEACH_DATE,TEACH_NAME,TEACH_TYPE_D,TEACH_ID_D,TEACH_NAME_D,TEACHER,HOURS,CREATE_DATE) VALUES(?,?,?,?,?,?,?,?,?,?,?)";

	@Override
	public void insertTeaching(Queue<Map<String, String>> queue) throws Exception {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(INSERT_TEACHING_SQL);
			int count = 0;
			Map<String, String> map;
			while ((map = queue.poll()) != null) {
				ps.setString(1, map.get("PERNR"));
				ps.setString(2, map.get("ZPXLX1"));
				ps.setString(3, map.get("ZPXBM1"));
				ps.setString(4, map.get("BEGDA"));
				ps.setString(5, map.get("ZPXMC1"));
				ps.setString(6, map.get("ZPXLX2"));
				ps.setString(7, map.get("ZPXBM2"));
				ps.setString(8, map.get("ZPXMC2"));
				ps.setString(9, map.get("ZPXJS"));
				ps.setString(10, map.get("NHOURS"));
				ps.setString(11, sdf.format(new Date()));
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}

			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			try {
				if (conn != null)
					conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
			logger.error("TalentDaoJdbc insertTeaching  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	private static String INSERT_ATT_RECORD_SQL = "REPLACE INTO ATT_RECORD (EMP_ID,ATT_TYPE,YEARMONTH,LEAVE_HOURS,OVER_HOURS,CREATE_DATE) VALUES(?,?,?,?,?,?)";

	@Override
	public void replaceAttRecord(Queue<Map<String, String>> queue) throws Exception {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(INSERT_ATT_RECORD_SQL);
			int count = 0;
			Map<String, String> map;
			while ((map = queue.poll()) != null) {
				ps.setString(1, map.get("PERNR"));
				ps.setString(2, map.get("ZKQLB"));
				ps.setString(3, map.get("ZKQNY"));
				ps.setString(4, map.get("ZQJSS"));
				ps.setString(5, map.get("ZJBSS"));
				ps.setString(6, sdf.format(new Date()));
				ps.addBatch();
				count++;
				if (count > 9999) {
					count = 0;
					ps.executeBatch();
					ps.clearBatch();
				}

			}
			ps.executeBatch();
			conn.commit();
		} catch (Exception e) {
			try {
				if (conn != null)
					conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
			logger.error("TalentDaoJdbc replaceAttRecord  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	@Override
	public void deleteOldData(String tableName, String maxDate) throws Exception {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConn();
			conn.setAutoCommit(true);
			ps = conn.prepareStatement("DELETE FROM " + tableName + " WHERE CREATE_DATE < ?");
			ps.setString(1, maxDate);
			ps.execute();
		} catch (Exception e) {
			logger.error("TalentDaoJdbc deleteOldData  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}

	@Override
	public void deleteNewData(String tableName, String maxDate) throws Exception {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConn();
			conn.setAutoCommit(true);
			ps = conn.prepareStatement("DELETE FROM " + tableName + " WHERE CREATE_DATE >= ?");
			ps.setString(1, maxDate);
			ps.execute();
		} catch (Exception e) {
			logger.error("TalentDaoJdbc deleteNewData  error "+e.getMessage());
			throw (e);
		} finally {
			closeConn(conn, ps, rs);
		}
	}


}

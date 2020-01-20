package com.want.talent.dao;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.want.talent.vo.AbiScore;
import com.want.talent.vo.AudEvent;

public interface TalentDao {
	public void replaceAbiscore(List<AbiScore> list) throws Exception;
	
	public void replaceAudEvent(List<AudEvent> list) throws Exception;
	
	public void insertPunishInfo(Queue<Map<String, String>> queue) throws Exception;

	public void insertPosInfo(Queue<Map<String, String>> queue) throws Exception;

	public void replaceEmpInfo(Queue<Map<String, String>> queue) throws Exception;

	public void replaceChangeLog(Queue<Map<String, String>> queue) throws Exception;

	public void insertKpiScore(Queue<Map<String, String>> queue) throws Exception;

	public void insertKpiInfo(Queue<Map<String, String>> queue) throws Exception;

	public void replaceKpiEvent(Queue<Map<String, String>> queue) throws Exception;

	public void insertTrainingLog(Queue<Map<String, String>> queue) throws Exception;

	public void insertTeaching(Queue<Map<String, String>> queue) throws Exception;

	public void replaceAttRecord(Queue<Map<String, String>> queue) throws Exception;

	public void deleteOldData(String tableName,String maxDate) throws Exception;
	
	public void deleteNewData(String tableName,String maxDate) throws Exception;

}

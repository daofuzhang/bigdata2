package com.want;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.want.talent.dao.TalentDao;

@Component
@Scope("prototype")
public class TalnetJobThread implements Runnable {
	private Queue<Map<String, String>> queue = null;

	private String flow = "";
	private String date = "";
	private StringBuilder errMsgBuilder = null;
	private Set<String> errorSet = null;
	@Autowired
	private TalentDao dao;

	public void setErrorSet(Set<String> errorSet) {
		this.errorSet = errorSet;
	}

	public void setQueue(Queue<Map<String, String>> queue) {
		this.queue = queue;
	}

	public void setFlow(String flow) {
		this.flow = flow;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public void setErrMsgBuilder(StringBuilder errMsgBuilder) {
		this.errMsgBuilder = errMsgBuilder;
	}

	@Override
	public void run() {
		try {
			if (queue != null && !queue.isEmpty())
				switch (flow) {
				case "INIT_PUNISH_INFO":
				case "PUNISH_INFO":
					dao.insertPunishInfo(queue);
					break;
				case "KPI_EVENT":
					dao.replaceKpiEvent(queue);
					break;
				case "KPI_INFO":
					dao.insertKpiInfo(queue);
					break;
				case "KPI_SCORE":
					dao.insertKpiScore(queue);
					break;
				case "EMP_INFO":
					dao.replaceEmpInfo(queue);
					break;
				case "POS_INFO":
					dao.insertPosInfo(queue);
					break;
				case "CHANGE_LOG":
				case "INIT_CHANGE_LOG":
					dao.replaceChangeLog(queue);
					break;
				case "INIT_TRAINING_LOG":
				case "TRAINING_LOG":
					dao.insertTrainingLog(queue);
					break;
				case "INIT_TEACHING_LOG":
				case "TEACHING":
					dao.insertTeaching(queue);
					break;
				case "ATT_RECORD":
				case "INIT_ATT_RECORD":
					dao.replaceAttRecord(queue);
					break;
				default:
					break;
				}
		} catch (Exception e) {
			errorSet.add(flow);
			errMsgBuilder.append("<p>"+date + " " + flow + ":" + e.getMessage() + "</p>");
			e.printStackTrace();
		}
	}

}

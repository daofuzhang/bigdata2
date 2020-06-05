package com.want.rfc;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import com.sap.conn.jco.AbapException;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoField;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoMetaData;
import com.sap.conn.jco.JCoParameterList;
import com.sap.conn.jco.JCoTable;
import com.want.JobCommandLineRunner;

@Component
public class SapDao {

	private Logger logger = LoggerFactory.getLogger(SapDao.class);

	private StringBuilder msgBuilder;
	
	public void setMsgBuilder(StringBuilder msgBuilder) {
		this.msgBuilder = msgBuilder;
	}

	public Queue<Map<String, String>> getSAPQueue(String functionName, String resultTable, Map<String, String> queryMap)
			throws JCoException {

		Queue<Map<String, String>> resultQueue = new LinkedBlockingQueue<Map<String, String>>();
		JCoDestination dest;
		try {
			dest = JCoDestinationManager.getDestination(JobCommandLineRunner.DEST_NAME);
			JCoFunction function = dest.getRepository().getFunction(functionName);
			if (function == null)
				throw new RuntimeException("BAPI_COMPANYCODE_GETLIST not found in SAP.");

			JCoParameterList input = function.getImportParameterList();

			if (queryMap != null) {
				Set<String> parmakeys = queryMap.keySet();
				for (String parmakey : parmakeys) {
					input.setValue(parmakey, queryMap.get(parmakey));
				}
			}
			JCoTable tables = function.getTableParameterList().getTable(resultTable);
			
			try {
				function.execute(dest);
			} catch (AbapException e) {
				System.out.println(function);
				throw (e);
			}

			JCoParameterList params = function.getExportParameterList();
			if (params != null) {
				System.out.println("Export:");
				for (JCoField jCoField : params) {
					System.out.println(jCoField.getName() + ":" + jCoField.getString());
				}
			}
			
			JCoMetaData imd = tables.getMetaData();
			String fields[] = new String[imd.getFieldCount()];
			for (int j = 0; j < imd.getFieldCount(); j++) {
				fields[j] = imd.getName(j);
			}

			System.out.println("row:" + tables.getNumRows());
			msgBuilder.append("<p>"+functionName+" rows:" + tables.getNumRows()+"</p>");
			for (int i = 0; i < tables.getNumRows(); i++) {
				tables.setRow(i);
				HashMap<String, String> datahm = new HashMap<String, String>(imd.getFieldCount());
				for (int z = 0; z < fields.length; z++) {
					datahm.put(fields[z], tables.getString(fields[z]));
				}
				resultQueue.offer(datahm);
			}
			
			
			if(resultQueue.size()>0) {
				System.out.println(resultQueue.element());
				msgBuilder.append("<p>element:" + resultQueue.element()+"</p>");
			}
		} catch (JCoException e) {
			logger.error("SapDao getSAPQueue error "+e.getMessage());
			throw (e);
		}
		return resultQueue;
	}

	
	public Queue<Map<String, String>> getSAPChangeQueueByTable(String functionName, String resultTable,String compField,Queue compQueue)
			throws JCoException {

		Queue<Map<String, String>> resultQueue = new LinkedBlockingQueue<Map<String, String>>();
		JCoDestination dest;
		try {
			dest = JCoDestinationManager.getDestination(JobCommandLineRunner.DEST_NAME);
			JCoFunction function = dest.getRepository().getFunction(functionName);
			if (function == null)
				throw new RuntimeException("BAPI_COMPANYCODE_GETLIST not found in SAP.");

			JCoParameterList input = function.getImportParameterList();
			JCoTable inputTable = function.getTableParameterList().getTable("I_WERKS");
			int quota = 10;
			while(quota>0&&compQueue.size()!=0){
				System.out.println(compQueue.peek());
				inputTable.appendRow();
				inputTable.setValue(compField, compQueue.poll());
				quota--;
			}
			
			JCoTable tables = function.getTableParameterList().getTable(resultTable);
			
			try {
				function.execute(dest);
			} catch (AbapException e) {
				System.out.println(function);
				throw (e);
			}

			JCoParameterList params = function.getExportParameterList();
			if (params != null) {
				System.out.println("Export:");
				for (JCoField jCoField : params) {
					System.out.println(jCoField.getName() + ":" + jCoField.getString());
				}
			}
			
			JCoMetaData imd = tables.getMetaData();
			String fields[] = new String[imd.getFieldCount()];
			for (int j = 0; j < imd.getFieldCount(); j++) {
				fields[j] = imd.getName(j);
			}


			System.out.println("row:" + tables.getNumRows());
			msgBuilder.append("<p>"+functionName+" rows:" + tables.getNumRows()+"</p>");
			for (int i = 0; i < tables.getNumRows(); i++) {
				tables.setRow(i);
				HashMap<String, String> datahm = new HashMap<String, String>(imd.getFieldCount());
				for (int z = 0; z < fields.length; z++) {
					datahm.put(fields[z], tables.getString(fields[z]));
				}
				resultQueue.offer(datahm);
			}
			
			
			if(resultQueue.size()>0) {
				System.out.println(resultQueue.element());
				msgBuilder.append("<p>element:" + resultQueue.element()+"</p>");
			}
		} catch (JCoException e) {
			logger.error("SapDao getSAPChangeQueueByTable error "+e.getMessage());
			throw (e);
		}
		return resultQueue;
	}

}

package com.want;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.format.datetime.standard.DateTimeFormatterFactory;
import org.springframework.stereotype.Component;

import com.sap.conn.jco.ext.DataProviderException;
import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;
import com.want.rfc.SapDao;
import com.want.talent.dao.TalentDao;
import com.want.util.SendMailUtils;

@Component
public class JobCommandLineRunner implements CommandLineRunner {

	public static final String DEST_NAME = "TalentBigData";
	@Autowired
	private SapDao jco;

	@Autowired
	private ApplicationContext applicationContext;
	@Autowired
	private TalentDao dao;

	boolean isPrd = true;
	
	public void run(String... args) throws Exception {

		if (args == null || args.length == 0) {
			System.out.println("args is null");
			return;
		}
		// 初始化DEST
		MyDestinationDataProvider myProvider = new MyDestinationDataProvider();
		try {
			com.sap.conn.jco.ext.Environment.registerDestinationDataProvider(myProvider);
		} catch (IllegalStateException providerAlreadyRegisteredException) {
			throw new Error(providerAlreadyRegisteredException);
		}
		myProvider.changeProperties(DEST_NAME, getRfcProperties());

		String date = new SimpleDateFormat("yyyyMMdd").format(new Date());
		LocalDate yesterday = LocalDate.now().minusDays(1);
		String yesterdayStr = yesterday.format(new DateTimeFormatterFactory("yyyyMMdd").createDateTimeFormatter());
		StringBuilder msgBuilder = new StringBuilder();
		jco.setMsgBuilder(msgBuilder);;
		ExecutorService es = Executors.newCachedThreadPool();
		Set<String> errorSet = new HashSet<>();
		List<String> delFlows = new ArrayList<>();

		if (args[0].equals("INIT_CHANGE_LOG")) {
			try {
				Queue<String> compQueue = new LinkedBlockingQueue<String>();
				for (int i = 1; i < args.length; i++) {
					compQueue.offer(args[i]);
				}
				while(compQueue.size()!=0) {
					System.out.println("INIT_CHANGE_LOG["+compQueue.peek()+"]:START......QueueSize:"+compQueue.size());
					Queue<Map<String, String>> queue = null;
					queue = jco.getSAPChangeQueueByTable("ZRFCHR037", "T_ZHRS054", "WERKS",compQueue);
					TalnetJobThread tjt = applicationContext.getBean(TalnetJobThread.class);
					tjt.setDate(date);
					tjt.setFlow("INIT_CHANGE_LOG");
					tjt.setQueue(queue);
					tjt.setErrMsgBuilder(msgBuilder);
					tjt.setErrorSet(errorSet);
					int threadsize = 1;
					if (queue != null) {
						threadsize = 1 + queue.size() / 20000;
						if (threadsize > 5) {
							threadsize = 5;
						}
					}
					for (int i = 0; i < threadsize; i++) {
						es.execute(tjt);
					}
				}
			} catch (Exception e) {
				msgBuilder.append("<p>" + date + " INIT_CHANGE_LOG:" + e.getMessage() + "</p>");
				e.printStackTrace();
			}
		} if (args[0].equals("INIT_ATT_RECORD")) {
			try {
				for (int i = 1; i < args.length; i++) {
					System.out.println("INIT_ATT_RECORD:START......:"+args[i]);
					
					HashMap<String, String> querymap = new HashMap<String, String>();
					querymap.put("PI_BEGDA",args[i]);
					Queue<Map<String, String>> queue = jco.getSAPQueue("ZRFCHR043", "T_ZHRS060", querymap);
					
					TalnetJobThread tjt = applicationContext.getBean(TalnetJobThread.class);
					tjt.setDate(date);
					tjt.setFlow("INIT_ATT_RECORD");
					tjt.setQueue(queue);
					tjt.setErrMsgBuilder(msgBuilder);
					tjt.setErrorSet(errorSet);
					
					int threadsize = 1;
					if (queue != null) {
						threadsize = 1 + queue.size() / 20000;
						if (threadsize > 5) {
							threadsize = 5;
						}
					}
					for (int j = 0; j < threadsize; j++) {
						es.execute(tjt);
					}
				}
			} catch (Exception e) {
				msgBuilder.append("<p>" + date + " INIT_CHANGE_LOG:" + e.getMessage() + "</p>");
				e.printStackTrace();
			}
		} else {
			for (String flow : args) {
				System.out.println(flow + ":START......");
				HashMap<String, String> querymap = new HashMap<String, String>();
				Queue<Map<String, String>> queue = null;
				String queryDate = date;
				try {
//					System.out.println("INIT ENV MB: " + (double) (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024*1024));
			        
					switch (flow) {

					case "INIT_PUNISH_INFO":
						delFlows.add(flow);//該表沒有唯一識別項
						queryDate = "";
					case "PUNISH_INFO":
						querymap.put("IV_BEGDA", yesterdayStr);
						queue = jco.getSAPQueue("ZRFCHR031", "T_ZHRT045", querymap);//ZRFCHR031	奖惩信息	IV_BEGDA是唯一的入参，为空的时候是全量，取近2年的数据；不为空则为增量，取日期为IV_BEGDA的人员奖惩信息；
						break;
					case "KPI_EVENT":
						querymap.put("PI_BEGDA", queryDate);
						queue = jco.getSAPQueue("ZRFCHR032", "T_ZHRS046", querymap);//ZRFCHR032	绩效考核面谈信息	每半年取最近一次的考核数据，按需同步
						break;
					case "KPI_INFO":
						delFlows.add(flow);//該表沒有唯一識別項
						querymap.put("PI_BEGDA", queryDate);
						queue = jco.getSAPQueue("ZRFCHR033", "T_ZHRS047", querymap);//ZRFCHR033	绩效考核信息	每半年取最近一次的考核数据，按需同步
						break;
					case "KPI_SCORE":
						delFlows.add(flow);//該表沒有唯一識別項
						querymap.put("PI_BEGDA", queryDate);
						queue = jco.getSAPQueue("ZRFCHR034", "T_ZHRS048", querymap);//ZRFCHR034	绩效考核评分信息	每半年取最近一次的考核数据，按需同步
						break;
					case "EMP_INFO":
						querymap.put("I_BEGDA", queryDate);
						queue = jco.getSAPQueue("ZRFCHR035", "T_ZHRS049", querymap);//ZRFCHR035	基本资料	每天全量更新
						break;
					case "POS_INFO":
						delFlows.add(flow);//該表沒有唯一識別項
						querymap.put("I_BEGDA", queryDate);
						queue = jco.getSAPQueue("ZRFCHR036", "T_ZHRS050", querymap);//ZRFCHR036	人员岗位信息	每天全量更新
						break;
					case "CHANGE_LOG":
						querymap.put("I_BEGDA", yesterdayStr);
						queue = jco.getSAPQueue("ZRFCHR037", "T_ZHRS054", querymap);//ZRFCHR037	异动信息	I_BEGDA入参，为空的时候是全量，取I_WERKS输入参数对应的数据；不为空则为增量，取异动日期为I_BEGDA的异动人员信息；
						break;
					case "INIT_TRAINING_LOG":
						delFlows.add(flow);//該表沒有唯一識別項
						queryDate = "";
					case "TRAINING_LOG":
						querymap.put("I_BEGDA", yesterdayStr);
						queue = jco.getSAPQueue("ZRFCHR038", "T_ZHRS055", querymap);//ZRFCHR038	培训信息	I_BEGDA是唯一的入参，为空的时候是全量，数据截止到系统日期减1；不为空则为增量，取更新日期为I_BEGDA的培训信息；
						break;
					case "INIT_TEACHING_LOG":
						delFlows.add(flow);//該表沒有唯一識別項
						queryDate = "";
					case "TEACHING":
						querymap.put("I_BEGDA", yesterdayStr);
						queue = jco.getSAPQueue("ZRFCHR039", "T_ZHRS059", querymap);//ZRFCHR039	讲师信息	I_BEGDA是唯一的入参，为空的时候是全量，数据截止到系统日期减1；不为空则为增量，取更新日期为I_BEGDA的培训信息；
						break;
					case "ATT_RECORD":
						querymap.put("PI_BEGDA", queryDate);
						queue = jco.getSAPQueue("ZRFCHR043", "T_ZHRS060", querymap);//ZRFCHR043	考勤记录	PI_BEGDA是唯一的入参，为空的时候是全量，全量模式取最近6个月；不为空则为增量，取输入日期对应的上个月的考勤；
						break;
					default:
						break;
					}
//					System.out.println("After JcoData MB: " + (double) (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024*1024));
			        
					int threadsize = 1;
					if (queue != null) {
						threadsize = 1 + queue.size() / 20000;
						if (threadsize > 5) {
							threadsize = 5;
						}
					}
					
					TalnetJobThread tjt = applicationContext.getBean(TalnetJobThread.class);
					tjt.setDate(date);
					tjt.setFlow(flow);
					tjt.setQueue(queue);
					tjt.setErrMsgBuilder(msgBuilder);
					tjt.setErrorSet(errorSet);
					for (int i = 0; i < threadsize; i++) {
						es.execute(tjt);
					}
//					System.out.println("running dao MB: " + (double) (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024*1024));
			        
				} catch (Exception e) {
					msgBuilder.append("<p>" + date + " " + flow + ":" + e.getMessage() + "</p>");
					e.printStackTrace();
				}

			}
		}

		SendMailUtils sm = applicationContext.getBean(SendMailUtils.class);
		es.shutdown();
		while (!es.awaitTermination(1, TimeUnit.SECONDS)) {
		}
		for (String flow : delFlows) {
			if (errorSet.contains(flow)) {
				dao.deleteNewData(flow, date);
			} else {
				dao.deleteOldData(flow, date);
			}
		}

		if (isPrd && msgBuilder.length() != 0) {
			sm.send(msgBuilder.toString());
		} else {
			System.err.println("errMsg:" + msgBuilder.toString());
		}
	}

	@Value("${jco.client.ashost}")
	private String jcoAshost;
	@Value("${jco.client.sysnr}")
	private String jcoSysnr;
	@Value("${jco.client.client}")
	private String jcoClient;
	@Value("${jco.client.user}")
	private String jcoUser;
	@Value("${jco.client.passwd}")
	private String jcoPasswd;
	@Value("${jco.client.lang}")
	private String jcoLang;

	private Properties getRfcProperties() {
		Properties connectProperties = new Properties();
		connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, jcoAshost);
		connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR, jcoSysnr);
		connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, jcoClient);
		connectProperties.setProperty(DestinationDataProvider.JCO_USER, jcoUser);
		connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, jcoPasswd);
		connectProperties.setProperty(DestinationDataProvider.JCO_LANG, jcoLang);

		// Max time in ms to wait for a connection, if the max allowed number of
		// connections is allocated by the application.
		connectProperties.setProperty(DestinationDataProvider.JCO_MAX_GET_TIME, String.valueOf(1000 * 60 * 10));

		return connectProperties;
	}

	static class MyDestinationDataProvider implements DestinationDataProvider {
		private DestinationDataEventListener eL;
		private HashMap<String, Properties> secureDBStorage = new HashMap<String, Properties>();

		public Properties getDestinationProperties(String destinationName) {
			try {
				Properties p = secureDBStorage.get(destinationName);
				if (p != null) {
					if (p.isEmpty())
						throw new DataProviderException(DataProviderException.Reason.INVALID_CONFIGURATION,
								"destination configuration is incorrect", null);

					return p;
				}

				return null;
			} catch (RuntimeException re) {
				throw new DataProviderException(DataProviderException.Reason.INTERNAL_ERROR, re);
			}
		}

		public void setDestinationDataEventListener(DestinationDataEventListener eventListener) {
			this.eL = eventListener;
		}

		public boolean supportsEvents() {
			return true;
		}

		void changeProperties(String destName, Properties properties) {
			synchronized (secureDBStorage) {
				if (properties == null) {
					if (secureDBStorage.remove(destName) != null)
						eL.deleted(destName);
				} else {
					secureDBStorage.put(destName, properties);
					eL.updated(destName); // create or updated
				}
			}
		}
	}
}

/**
 * -------------------------------------------------------
 * @FileName：JobApplication.java
 * @Description：简要描述本文件的内容
 * @Author：Luke.Tsai
 * @Copyright  www.want-want.com  Ltd.  All rights reserved.
 * 注意：本内容仅限于旺旺集团内部传阅，禁止外泄以及用于其他商业目的
 * -------------------------------------------------------
 */
package com.want;

import org.jasypt.encryption.StringEncryptor;
import org.jasypt.encryption.pbe.PooledPBEStringEncryptor;
import org.jasypt.encryption.pbe.config.SimpleStringPBEConfig;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class JobApplication {

	public static void main(String[] args) {
		args = new String[] { "PUNISH_INFO"};
//		args = new String[] { "PUNISH_INFO","TRAINING_LOG","TEACHING" };
//		args = new String[] { "KPI_SCORE"};
//		args = new String[] { "PUNISH_INFO", "EMP_INFO","POS_INFO", "TRAINING_LOG","TEACHING","ATT_RECORD" };
		// "KPI_EVENT", "KPI_INFO", "KPI_SCORE",
//		args = new String[] { "POS_INFO", "TEACHING","TRAINING_LOG"};
//		args = new String[] { "INIT_CHANGE_LOG","C11","C21","C31","C41","C51","C61","C71","C81","C91","CA1","CB1","CC1","CC2","CC3","CC4","CD1","CE1","CE2","CF1","CF2","CG1","CH1","CI1","CJ1","CK1","CL1","CM1","CM2","CM3","CN1","CO1","CP1","CQ1","CR1","CS1","CT1","CT2","CU1","CU2","CU3","CU4","CV1","CV2","CW1","CX1","CY1","CY2","CZ1","F11","F12","F13","F14","F15","F16","F17","F18","F21","F22","F23","F24","F31","F32","F33","F34","F35","F36","F37","F41","F42","F43","F44","F45","F46","F51","F52","F53","F54","F55","F61","F62","F63","F64","F65","F71","F72","F73","F74","F75","F76","F77","F78","F81","F82","F83","F84","F85","F86","F87","F91","F92","F93","F94","FA1","FA2","FA3","FA4","FB1","FB2","FB3","FB4","FB5","FC1","FC2","FD1","FD2","FD3","FD4","FE1","FE2","FE3","FF1","FF2","FG1","FH1","FH2","FH3","FH4","FH5","FI1","FI2","FJ1","FJ2","FJ3","FJ4","FJ5","FK1","FK2","FL1","FL2","FL3","FM1","FM2","FN1","FN2","FO1","FO2","H11","H12","H13","H14","H21","ML1","TMP","U11","U21","U31","U41","U51","U61","U71","U72","UA1","UC1","UC2","UD1","UE1","UF1","UG1","UH1","UI1","UJ1","UK1","UM1","UN1","UO1","UP1","UQ1","UR1","WWWW","ZZZZ"};
//		args = new String[] { "INIT_ATT_RECORD","20190201","20190301","20190401","20190501","20190601","20190701","20190801"};
//		args = new String[] { "INIT_ATT_RECORD","20190801"};
//		args = new String[] { "TEACHING","TRAINING_LOG"};
//		args = new String[] { "INIT_TEACHING_LOG"};
		SpringApplicationBuilder sab = new SpringApplicationBuilder(JobApplication.class);
		sab.bannerMode(Banner.Mode.OFF).web(false).run(args);
//		StringEncryptor se = new JobApplication().stringEncryptor();
	}

	
	
    @Bean("jasyptStringEncryptor")
    public StringEncryptor stringEncryptor() {
        PooledPBEStringEncryptor encryptor = new PooledPBEStringEncryptor();
        SimpleStringPBEConfig config = new SimpleStringPBEConfig();
        config.setPassword(System.getenv("CAS_PBE_PASSWORD"));
        config.setAlgorithm("PBEWithMD5AndDES");
        config.setKeyObtentionIterations("1000");
        config.setPoolSize("1");
        config.setProviderName("SunJCE");
        config.setSaltGeneratorClassName("org.jasypt.salt.RandomSaltGenerator");
        config.setStringOutputType("base64");
        encryptor.setConfig(config);
        return encryptor;
    }
    
    
    
}

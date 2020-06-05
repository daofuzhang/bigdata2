/**
 * -------------------------------------------------------
 * @FileName：SendMailUtils.java
 * @Description：简要描述本文件的内容
 * @Author：Dirk.Lee
 * @Copyright  www.want-want.com  Ltd.  All rights reserved.
 * 注意：本内容仅限于旺旺集团内部传阅，禁止外泄以及用于其他商业目的
 * -------------------------------------------------------
 */
package com.want.util;

import java.io.UnsupportedEncodingException;
import java.util.Properties;
import javax.mail.Authenticator;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SendMailUtils {
	private Logger logger = LoggerFactory.getLogger(SendMailUtils.class);

	
	@Value("${mail.smtp.host}")
	private String host;
	@Value("${mail.smtp.user}")
	private String user;
	@Value("${mail.smtp.password}")
	private String password;
	@Value("${mail.smtp.from}")
	private String from;

	@Value("${mail.smtp.mailTo}")
	private String mailTo;

	@Value("${mail.smtp.subject}")
	private String subject;

	public void send(String text) throws UnsupportedEncodingException {
		Properties mailConfig = new Properties();
		mailConfig.put("mail.host", host);
		mailConfig.put("mail.smtp.auth", "true");
		mailConfig.put("mail.transport.protocol", "smtp");
		Authenticator authenticator = new SimpleAuthenticator(user, password);
		Session session = Session.getInstance(mailConfig, authenticator);
		String[] to = mailTo.split(",");
		MimeMessage mimeMsg = new MimeMessage(session);
		try {
			mimeMsg.setSubject(subject, "utf-8");
			mimeMsg.setFrom(new InternetAddress(from));
			for (int i = 0; i < to.length; i++) {
				mimeMsg.addRecipient(RecipientType.TO, new InternetAddress(to[i]));
			}

			Multipart multipart = new MimeMultipart();
			if (text != null) {
				MimeBodyPart messageBodyPart = new MimeBodyPart();
				messageBodyPart.setContent(text, "text/html;charset=UTF-8");
				multipart.addBodyPart(messageBodyPart);
			}
			mimeMsg.setContent(multipart);

			Transport.send(mimeMsg);
		} catch (MessagingException e) {
			logger.error("SendMailUtils send error:"+e.getMessage(), e);
		}
	}
}

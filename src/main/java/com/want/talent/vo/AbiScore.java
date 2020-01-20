package com.want.talent.vo;

/**
 * -------------------------------------------------------
 * @FileName：AbiScore.java
 * @Description：简要描述本文件的内容
 * @Author：80005463
 * @Copyright  www.want-want.com  Ltd.  All rights reserved.
 * 注意：本内容仅限于旺旺集团内部传阅，禁止外泄以及用于其他商业目的
 * -------------------------------------------------------
 */


/**
 * @author 80005463
 *
 */
public class AbiScore {

    private String empId;
    private String template;
    private String title;
    private String titleKind;
    private String score;
    /**
     * @return the empId
     */
    public String getEmpId() {
        return empId;
    }
    /**
     * @param empId the empId to set
     */
    public void setEmpId(String empId) {
        this.empId = empId;
    }
    /**
     * @return the template
     */
    public String getTemplate() {
        return template;
    }
    /**
     * @param template the template to set
     */
    public void setTemplate(String template) {
        this.template = template;
    }
    /**
     * @return the title
     */
    public String getTitle() {
        return title;
    }
    /**
     * @param title the title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }
    /**
     * @return the score
     */
    public String getScore() {
        return score;
    }
    /**
     * @param score the score to set
     */
    public void setScore(String score) {
        this.score = score;
    }
	public String getTitleKind() {
		return titleKind;
	}
	public void setTitleKind(String titleKind) {
		this.titleKind = titleKind;
	}
	@Override
	public String toString() {
		return "AbiScore [empId=" + empId + ", template=" + template + ", title=" + title + ", titleKind=" + titleKind
				+ ", score=" + score + "]";
	}
	public AbiScore(String empId, String template, String title, String titleKind, String score) {
		super();
		this.empId = empId;
		this.template = template;
		this.title = title;
		this.titleKind = titleKind;
		this.score = score;
	}
    
    
}

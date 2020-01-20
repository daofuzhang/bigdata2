package com.want.talent.vo;

public class AudEvent {
	private String empId;
	private String audId;
	private String audDate;
	private String audDesc;
	
	@Override
	public String toString() {
		return "AudEvent [empId=" + empId + ", audId=" + audId + ", audDate=" + audDate + ", audDesc=" + audDesc + "]";
	}
	public AudEvent(String empId, String audId, String audDate, String audDesc) {
		super();
		this.empId = empId;
		this.audId = audId;
		this.audDate = audDate;
		this.audDesc = audDesc;
	}
	public String getEmpId() {
		return empId;
	}
	public void setEmpId(String empId) {
		this.empId = empId;
	}
	public String getAudId() {
		return audId;
	}
	public void setAudId(String audId) {
		this.audId = audId;
	}
	public String getAudDate() {
		return audDate;
	}
	public void setAudDate(String audDate) {
		this.audDate = audDate;
	}
	public String getAudDesc() {
		return audDesc;
	}
	public void setAudDesc(String audDesc) {
		this.audDesc = audDesc;
	}
	
	
}

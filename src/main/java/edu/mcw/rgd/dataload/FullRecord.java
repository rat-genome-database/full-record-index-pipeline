package edu.mcw.rgd.dataload;

import edu.mcw.rgd.datamodel.Dumpable;
import edu.mcw.rgd.process.Dumper;
import edu.mcw.rgd.process.Utils;

import java.util.Date;

/// represents a row in FULL_RECORD_INDEX table
public class FullRecord implements Dumpable {

    private int studyId;
    private int experimentId;
    private int experimentRecordId;

    private String studyName;
    private String experimentName;
    private String termAcc;
    private String primaryTermAcc;
    private String aspect;
    private Date lastUpdateDate;

    private String rowid;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FullRecord record = (FullRecord) o;

        if (getStudyId() != record.getStudyId()) return false;
        if (getExperimentId() != record.getExperimentId()) return false;
        if (getExperimentRecordId() != record.getExperimentRecordId()) return false;
        if (!getStudyName().equals(record.getStudyName())) return false;
        if (!getExperimentName().equals(record.getExperimentName())) return false;
        if (!getTermAcc().equals(record.getTermAcc())) return false;
        if (!getPrimaryTermAcc().equals(record.getPrimaryTermAcc())) return false;
        return Utils.stringsAreEqual(getAspect(), record.getAspect());
    }

    @Override
    public int hashCode() {
        int result = getStudyId();
        result = 31 * result + getExperimentId();
        result = 31 * result + getExperimentRecordId();
        result = 31 * result + getStudyName().hashCode();
        result = 31 * result + getExperimentName().hashCode();
        result = 31 * result + getTermAcc().hashCode();
        result = 31 * result + getPrimaryTermAcc().hashCode();
        result = 31 * result + Utils.defaultString(getAspect()).hashCode();
        return result;
    }

    public int getStudyId() {
        return studyId;
    }

    public void setStudyId(int studyId) {
        this.studyId = studyId;
    }

    public int getExperimentId() {
        return experimentId;
    }

    public void setExperimentId(int experimentId) {
        this.experimentId = experimentId;
    }

    public int getExperimentRecordId() {
        return experimentRecordId;
    }

    public void setExperimentRecordId(int experimentRecordId) {
        this.experimentRecordId = experimentRecordId;
    }

    public String getStudyName() {
        return studyName;
    }

    public void setStudyName(String studyName) {
        this.studyName = studyName;
    }

    public String getExperimentName() {
        return experimentName;
    }

    public void setExperimentName(String experimentName) {
        this.experimentName = experimentName;
    }

    public String getTermAcc() {
        return termAcc;
    }

    public void setTermAcc(String termAcc) {
        this.termAcc = termAcc;
    }

    public String getPrimaryTermAcc() {
        return primaryTermAcc;
    }

    public void setPrimaryTermAcc(String primaryTermAcc) {
        this.primaryTermAcc = primaryTermAcc;
    }

    public String getAspect() {
        return aspect;
    }

    public void setAspect(String aspect) {
        this.aspect = aspect;
    }

    public Date getLastUpdateDate() {
        return lastUpdateDate;
    }

    public void setLastUpdateDate(Date lastUpdateDate) {
        this.lastUpdateDate = lastUpdateDate;
    }

    public String getRowid() {
        return rowid;
    }

    public void setRowid(String rowid) {
        this.rowid = rowid;
    }

    /**
     * dumps object attributes as pipe delimited string
     * @return pipe-delimited String of object attributes
     */
    public String dump(String delimiter) {

        return new Dumper(delimiter)
            .put("STUDY_ID", getStudyId())
            .put("STUDY_NAME", getStudyName())
            .put("EXP_ID", getExperimentId())
            .put("EXP_NAME", getExperimentName())
            .put("REC_ID", getExperimentRecordId())
            .put("TERM_ACC", getTermAcc())
            .put("PRIMARY_TERM_ACC", getPrimaryTermAcc())
            .put("ASPECT", getAspect())
            .put("LAST_UPDATE", getLastUpdateDate())
            .dump();
    }
}
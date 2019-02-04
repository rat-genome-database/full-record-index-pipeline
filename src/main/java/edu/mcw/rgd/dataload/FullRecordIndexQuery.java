package edu.mcw.rgd.dataload;

import org.springframework.jdbc.object.MappingSqlQuery;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FullRecordIndexQuery extends MappingSqlQuery {

    public FullRecordIndexQuery(DataSource ds, String query) {
        super(ds, query);
    }

    protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {
        FullRecord r = new FullRecord();
        r.setRowid(rs.getString("rowid"));
        r.setAspect(rs.getString("aspect"));
        r.setExperimentId(rs.getInt("experiment_id"));
        r.setExperimentName(rs.getString("experiment_name"));
        r.setExperimentRecordId(rs.getInt("experiment_record_id"));
        r.setPrimaryTermAcc(rs.getString("primary_term_acc"));
        r.setStudyId(rs.getInt("study_id"));
        r.setStudyName(rs.getString("study_name"));
        r.setTermAcc(rs.getString("term_acc"));
        return r;
    }
}

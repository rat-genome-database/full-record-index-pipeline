package edu.mcw.rgd.dataload;

import edu.mcw.rgd.dao.impl.OntologyXDAO;
import edu.mcw.rgd.dao.impl.PhenominerDAO;
import edu.mcw.rgd.datamodel.ontologyx.Ontology;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.pheno.Experiment;
import edu.mcw.rgd.datamodel.pheno.Record;
import edu.mcw.rgd.datamodel.pheno.Study;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

import java.sql.Types;
import java.util.*;

/**
 * @author mtutaj
 * @since 2/6/15
 * <p>
 * encapsulates all dao code
 */
public class FullRecordIndexDao {

    private Logger logInserted = LogManager.getLogger("inserted");
    private Logger logDeleted = LogManager.getLogger("deleted");
    private PhenominerDAO pdao = new PhenominerDAO();
    private OntologyXDAO odao = new OntologyXDAO();

    public List<Study> getStudies() throws Exception {
        return pdao.getStudies();
    }

    public List<Experiment> getExperiments(int studyId) throws Exception {
        return pdao.getExperiments(studyId);
    }

    public List<Record> getRecords(int expId) throws Exception {
        return pdao.getRecords(expId);
    }

    public List<Term> getAllActiveTermAncestors(String termAcc) throws Exception {
        return odao.getAllActiveTermAncestors(termAcc);
    }

    public void loadAspectMap() throws Exception {
        for( Ontology o: odao.getOntologies() ) {
            if( !Utils.isStringEmpty(o.getAspect()) ) {
                ontId2AspectMap.put(o.getId(), o.getAspect());
            }
        }
    }
    static Map<String,String> ontId2AspectMap = new HashMap<String, String>();

    public String getAspect(String termAcc) {

        String aspect = null;
        int colonPos = termAcc.indexOf(':');
        if( colonPos > 0 ) {
            aspect = ontId2AspectMap.get(termAcc.substring(0, colonPos));
        }
        return aspect;
    }

    public List<FullRecord> getFullRecordsForStudy(int studyId) throws Exception {
        String sql = "SELECT ROWID,i.* FROM full_record_index i WHERE study_id=?";
        FullRecordIndexQuery q = new FullRecordIndexQuery(pdao.getDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        List<FullRecord> records = q.execute(studyId);
        return records;
    }

    public int refreshLastUpdateDate( Collection<FullRecord> records, Date refreshDate ) throws Exception {

        BatchSqlUpdate su = new BatchSqlUpdate(pdao.getDataSource(), "UPDATE full_record_index SET last_update_date=? WHERE ROWID=?",
                new int[]{Types.TIMESTAMP, Types.VARCHAR});
        su.compile();

        for( FullRecord r: records ) {
            su.update(refreshDate, r.getRowid());
        }
        return pdao.executeBatch(su);
    }

    public void insertRecords(Collection<FullRecord> records) throws Exception {
        String sql = "INSERT INTO full_record_index (experiment_record_id, term_acc, primary_term_acc, aspect, study_id, "+
                "study_name, experiment_id, experiment_name, last_update_date) VALUES (?,?,?,?,?,?,?,?,SYSDATE)";

        for( FullRecord r: records ) {
            logInserted.debug(r.dump("|"));
            pdao.update(sql, r.getExperimentRecordId(), r.getTermAcc(), r.getPrimaryTermAcc(), r.getAspect(),
                    r.getStudyId(), r.getStudyName(), r.getExperimentId(), r.getExperimentName());
        }
    }

    public int deleteRecords( Collection<FullRecord> records ) throws Exception {

        BatchSqlUpdate su = new BatchSqlUpdate(pdao.getDataSource(), "DELETE FROM full_record_index WHERE ROWID=?",
                new int[]{Types.VARCHAR});
        su.compile();

        for( FullRecord r: records ) {
            logDeleted.debug(r.dump("|"));
            su.update(r.getRowid());
        }
        pdao.executeBatch(su);

        return records.size();
    }

    public int deleteStaleRecords( Date cutoffDate ) throws Exception {

        String sql = "SELECT ROWID,i.* FROM full_record_index i WHERE last_update_date < ?";
        FullRecordIndexQuery q = new FullRecordIndexQuery(pdao.getDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.TIMESTAMP));
        List<FullRecord> staleRecords = q.execute(cutoffDate);

        return deleteRecords(staleRecords);
    }
}

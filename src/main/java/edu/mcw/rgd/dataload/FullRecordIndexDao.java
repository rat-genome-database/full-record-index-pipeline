package edu.mcw.rgd.dataload;

import edu.mcw.rgd.dao.impl.OntologyXDAO;
import edu.mcw.rgd.dao.impl.PhenominerDAO;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.pheno.Experiment;
import edu.mcw.rgd.datamodel.pheno.Record;
import edu.mcw.rgd.datamodel.pheno.Study;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    public void deleteAllFromFullRecordIndex() throws Exception {
        int deleted = pdao.deleteAllFromFullRecordIndex();
        logDeleted.info("  deleted rows: "+deleted);
    }

    public List<Study> getStudies() throws Exception {
        return pdao.getStudies();
    }

    public List<Experiment> getExperiments(int studyId) throws Exception {
        return pdao.getExperiments(studyId);
    }

    public List<Record> getRecords(int expId) throws Exception {
        return pdao.getRecords(expId);
    }

    public void insertToIndex(int recordId, String accId, String primaryAccId, int studyId, String studyName,
                              int experimentId, String experimentName) throws Exception {
        logInserted.debug("r:"+recordId+" t{"+accId+","+primaryAccId+"} s{"+studyId+","+studyName
                +"} e{"+experimentId+","+experimentName+"}");
        pdao.insertToIndex(recordId, accId, primaryAccId, studyId, studyName, experimentId, experimentName);
    }

    public List<Term> getAllActiveTermAncestors(String termAcc) throws Exception {
        return odao.getAllActiveTermAncestors(termAcc);
    }
}

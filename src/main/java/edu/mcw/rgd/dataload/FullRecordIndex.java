package edu.mcw.rgd.dataload;

import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.pheno.Condition;
import edu.mcw.rgd.datamodel.pheno.Experiment;
import edu.mcw.rgd.datamodel.pheno.Record;
import edu.mcw.rgd.datamodel.pheno.Study;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: jdepons
 * Date: 4/26/12
 * Pipeline job to index experiment record table
 */
public class FullRecordIndex {

    Logger log = LogManager.getLogger("core");
    FullRecordIndexDao dao = new FullRecordIndexDao();

    static int rowsInserted = 0;
    private String version;

    public static void main(String[] args) throws Exception {

        long time0 = System.currentTimeMillis();
        FullRecordIndex fri = new FullRecordIndex();
        try {
            fri.log.info("Starting full record index pipeline");
            fri.runPipeline();
            fri.log.info("  inserted rows: " + rowsInserted);
            fri.log.info("OK full record index pipeline: time elapsed " + Utils.formatElapsedTime(time0, System.currentTimeMillis()));
        }catch (Exception e) {
            fri.log.error(e);
        }
    }

    public void runPipeline() throws Exception {

        dao.deleteAllFromFullRecordIndex();
        List<Study> studies = dao.getStudies();

        for (Study s: studies) {
            List<Experiment> experiments = dao.getExperiments(s.getId());

            for (Experiment e: experiments) {
                List<Record> records = dao.getRecords(e.getId());

                for (Record r: records) {

                    if (r.getCurationStatus() == 40) {

                        dao.insertToIndex(r.getId(), r.getMeasurementMethod().getAccId(),r.getMeasurementMethod().getAccId(), s.getId(), s.getName(),e.getId(),e.getName());
                        List<Term> parents = dao.getAllActiveTermAncestors(r.getMeasurementMethod().getAccId());
                        for(Term t: parents) {
                            dao.insertToIndex(r.getId(),t.getAccId(),r.getMeasurementMethod().getAccId(), s.getId(), s.getName(),e.getId(),e.getName());
                            rowsInserted++;
                        }


                        dao.insertToIndex(r.getId(), r.getClinicalMeasurement().getAccId(),r.getClinicalMeasurement().getAccId(), s.getId(), s.getName(),e.getId(),e.getName());
                        parents = dao.getAllActiveTermAncestors(r.getClinicalMeasurement().getAccId());
                        for(Term t: parents) {
                            dao.insertToIndex(r.getId(),t.getAccId(), r.getClinicalMeasurement().getAccId(), s.getId(), s.getName(),e.getId(),e.getName());
                            rowsInserted++;
                        }

                        dao.insertToIndex(r.getId(),r.getSample().getStrainAccId(), r.getSample().getStrainAccId(), s.getId(), s.getName(),e.getId(),e.getName());
                        parents = dao.getAllActiveTermAncestors(r.getSample().getStrainAccId());
                        for(Term t: parents) {
                            dao.insertToIndex(r.getId(),t.getAccId(),r.getSample().getStrainAccId(), s.getId(), s.getName(),e.getId(),e.getName());
                            rowsInserted++;
                        }


                        List<Condition> conditions = r.getConditions();
                        for (Condition cond: conditions) {
                            dao.insertToIndex(r.getId(),cond.getOntologyId(), cond.getOntologyId(), s.getId(), s.getName(),e.getId(),e.getName());
                            parents = dao.getAllActiveTermAncestors(cond.getOntologyId());
                            for(Term t: parents) {
                                dao.insertToIndex(r.getId(),t.getAccId(),cond.getOntologyId(), s.getId(), s.getName(),e.getId(),e.getName());
                                rowsInserted++;
                            }

                        }
                    }
                }

            }

        }

    }


    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}


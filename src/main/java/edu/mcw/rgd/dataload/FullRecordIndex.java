package edu.mcw.rgd.dataload;

import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.pheno.Condition;
import edu.mcw.rgd.datamodel.pheno.Experiment;
import edu.mcw.rgd.datamodel.pheno.Record;
import edu.mcw.rgd.datamodel.pheno.Study;
import edu.mcw.rgd.process.Utils;
import org.apache.commons.collections.CollectionUtils;
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

    private String version;

    public static void main(String[] args) throws Exception {

        FullRecordIndex fri = new FullRecordIndex();
        try {
            fri.runPipeline();
        }catch (Exception e) {
            fri.log.error(e);
            throw e;
        }
    }

    public void runPipeline() throws Exception {

        Date date0 = new Date();
        long time0 = date0.getTime();
        log.info("Starting full record index pipeline");

        int rowsIncoming = 0;
        int rowsUpToDate = 0;
        int rowsInserted = 0;
        int rowsDeleted = 0;

        dao.loadAspectMap();

        List<Study> studies = dao.getStudies();

        int i = 0;
        for (Study s: studies) {
            System.out.println((++i)+"/"+studies.size());

            List<FullRecord> fullRecordsInRgd = dao.getFullRecordsForStudy(s.getId());
            List<FullRecord> fullRecordsIncoming = new ArrayList<FullRecord>();

            List<Experiment> experiments = dao.getExperiments(s.getId());

            for (Experiment e: experiments) {
                List<Record> records = dao.getRecords(e.getId());

                for (Record r: records) {

                    if (r.getCurationStatus() != 40) {
                        continue;
                    }

                    addIncomingRecord(r.getId(), r.getMeasurementMethod().getAccId(),r.getMeasurementMethod().getAccId(), s.getId(), s.getName(),e.getId(),e.getName(), fullRecordsIncoming);
                    List<Term> parents = dao.getAllActiveTermAncestors(r.getMeasurementMethod().getAccId());
                    for(Term t: parents) {
                        addIncomingRecord(r.getId(),t.getAccId(),r.getMeasurementMethod().getAccId(), s.getId(), s.getName(),e.getId(),e.getName(), fullRecordsIncoming);
                    }

                    addIncomingRecord(r.getId(), r.getClinicalMeasurement().getAccId(),r.getClinicalMeasurement().getAccId(), s.getId(), s.getName(),e.getId(),e.getName(), fullRecordsIncoming);
                    parents = dao.getAllActiveTermAncestors(r.getClinicalMeasurement().getAccId());
                    for(Term t: parents) {
                        addIncomingRecord(r.getId(),t.getAccId(), r.getClinicalMeasurement().getAccId(), s.getId(), s.getName(),e.getId(),e.getName(), fullRecordsIncoming);
                    }

                    addIncomingRecord(r.getId(),r.getSample().getStrainAccId(), r.getSample().getStrainAccId(), s.getId(), s.getName(),e.getId(),e.getName(), fullRecordsIncoming);
                    parents = dao.getAllActiveTermAncestors(r.getSample().getStrainAccId());
                    for(Term t: parents) {
                        addIncomingRecord(r.getId(),t.getAccId(),r.getSample().getStrainAccId(), s.getId(), s.getName(),e.getId(),e.getName(), fullRecordsIncoming);
                    }


                    List<Condition> conditions = r.getConditions();
                    for (Condition cond: conditions) {
                        addIncomingRecord(r.getId(),cond.getOntologyId(), cond.getOntologyId(), s.getId(), s.getName(),e.getId(),e.getName(), fullRecordsIncoming);
                        parents = dao.getAllActiveTermAncestors(cond.getOntologyId());
                        for(Term t: parents) {
                            addIncomingRecord(r.getId(),t.getAccId(),cond.getOntologyId(), s.getId(), s.getName(),e.getId(),e.getName(), fullRecordsIncoming);
                        }
                    }
                }
            }

            if( fullRecordsIncoming.isEmpty() && fullRecordsInRgd.isEmpty() ) {
                continue;
            }

            rowsIncoming += fullRecordsIncoming.size();

            Collection<FullRecord> fullRecordsUpToDate = CollectionUtils.intersection(fullRecordsInRgd, fullRecordsIncoming);
            rowsUpToDate += fullRecordsUpToDate.size();
            dao.refreshLastUpdateDate(fullRecordsUpToDate, date0);

            Collection<FullRecord> fullRecordsForInsert = CollectionUtils.subtract(fullRecordsIncoming, fullRecordsInRgd);
            rowsInserted += fullRecordsForInsert.size();
            dao.insertRecords(fullRecordsForInsert);

            Collection<FullRecord> fullRecordsForDelete = CollectionUtils.subtract(fullRecordsInRgd, fullRecordsIncoming);
            rowsDeleted += fullRecordsForDelete.size();
            dao.deleteRecords(fullRecordsForDelete);
        }

        int staleRowsDeleted = dao.deleteStaleRecords(date0);

        if( rowsIncoming!=0 ) {
            log.info("  incoming rows:   " + Utils.formatThousands(rowsIncoming));
        }
        if( rowsUpToDate!=0 ) {
            log.info("  up-to-date rows: " + Utils.formatThousands(rowsUpToDate));
        }
        if( rowsInserted!=0 ) {
            log.info("  inserted rows:   " + Utils.formatThousands(rowsInserted));
        }
        if( rowsDeleted!=0 ) {
            log.info("  deleted rows:    " + Utils.formatThousands(rowsDeleted));
        }
        if( staleRowsDeleted!=0 ) {
            log.info("  deleted stale rows: " + Utils.formatThousands(staleRowsDeleted));
        }

        log.info("=== OK === time elapsed " + Utils.formatElapsedTime(time0, System.currentTimeMillis()));
    }

    void addIncomingRecord(int recordId, String accId, String primaryAccId, int studyId, String studyName,
                           int experimentId, String experimentName, List<FullRecord> fullRecords) {

        String aspect = dao.getAspect(accId);
        if( aspect==null ) {
            log.warn(" NULL aspect for term_acc:"+accId+", study_id:"+studyId+", exp_id:"+experimentId);
            return;
        }

        FullRecord r = new FullRecord();
        r.setExperimentRecordId(recordId);
        r.setTermAcc(accId);
        r.setPrimaryTermAcc(primaryAccId);
        r.setStudyId(studyId);
        r.setStudyName(studyName);
        r.setExperimentId(experimentId);
        r.setExperimentName(experimentName);
        r.setAspect(aspect);

        fullRecords.add(r);
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}


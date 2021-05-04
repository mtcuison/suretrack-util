/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.suretrack;

import java.sql.ResultSet;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import java.sql.SQLException;
import org.rmj.replication.utility.LogWrapper;

/**
 *
 * @author kalyptus
 */
public class SendSupport {
    private static LogWrapper logwrapr = new LogWrapper("SureTrack.SendSupport", "D:/GGC_Java_Systems/temp/SendSupport.log");
    private static GRiderX local_app = null;
    private static GRiderX main_app = null;
    public static void main(String[] args) {
        logwrapr.info("Executing SendSupport...");

        //load driver
        local_app = new GRiderX("gRider");
        
        //check error from previous loading
        if(!local_app.getErrMsg().isEmpty()){
            logwrapr.severe(local_app.getMessage() + local_app.getErrMsg());
            return;
        }

        //use the connection from localhost server...
        //this utility should be used by the branch 
        if(!local_app.setOnline(false)){
            logwrapr.severe("Unable to connect to main server...");
            return;
        }
        
        local_app.beginTrans();
        
        ResultSet rs = null;
        int lnctr = 0;
        
        try {
            StringBuilder lsSQL = new StringBuilder();
            lsSQL.append("SELECT a.sTransNox, IFNULL(b.cMntrSent, 'X') cMntrSent")
                .append(" FROM Support_Request_Master a") 
                   .append(" LEFT JOIN Status_Monitor b ON a.sTransNox = b.sTransNox")
                .append(" WHERE a.sTransNox LIKE " + SQLUtil.toSQL(local_app.getBranchCode() + "%")) 
                  .append(" AND IFNULL(b.cMntrSent, '0') = '0'") 
                .append(" ORDER BY a.sTransNox ASC");
            
            rs = local_app.getConnection().createStatement().executeQuery(lsSQL.toString());  
            
            while(rs.next()){
                
                //Count the number of records...
                lnctr++;
                
                if(rs.getString("cMntrSent").compareToIgnoreCase("X") == 0){
                    StringBuilder lsUpdate = new StringBuilder();
                    lsUpdate.append("INSERT INTO Status_Monitor")
                              .append(" SET sTransNox = " + SQLUtil.toSQL(rs.getString("sTransNox")))
                                 .append(", cStatusxx = '0'")
                                 .append(", cMntrSent = '0'");
                    local_app.getConnection().createStatement().executeUpdate(lsUpdate.toString());
                
                    lsUpdate = new StringBuilder();
                    lsUpdate.append("UPDATE Support_Request_Master")
                           .append(" SET cMntrCrtd = '1'")
                           .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs.getString("sTransNox"))); 
                    local_app.getConnection().createStatement().executeUpdate(lsUpdate.toString());
                }
            }//while(rs.next()){
            local_app.commitTrans();
        } 
        catch (SQLException ex){
            logwrapr.severe("set_trackerid_value: SQLException error detected.", ex);
            local_app.rollbackTrans();
            return;
        }
        
        //If there are no record found just exit the utility
        if(lnctr == 0){
            logwrapr.info("No record to send...");
            return;
        }
        
        main_app = new GRiderX("gRider");
        
        //check error from previous loading
        if(!main_app.getErrMsg().isEmpty()){
            logwrapr.severe(main_app.getMessage() + main_app.getErrMsg());
            return;
        }
        
        //connect to the main server
        if(!main_app.setOnline(true)){
            logwrapr.severe("Unable to connect to main server...");
            return;
        }
        
        main_app.beginTrans();
        local_app.beginTrans();
        
        try {
            //position the cursor to the first record...
            rs.beforeFirst();
            while(rs.next()){
                System.out.println("Row: " + String.valueOf(rs.getRow()));
                StringBuilder lsSQL = new StringBuilder();
                lsSQL.append("SELECT *")
                    .append(" FROM Support_Request_Master")
                    .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs.getString("sTransNox")));    
                
                //check for the availability of the record in the localhost server
                ResultSet rs_rec = local_app.getConnection().createStatement().executeQuery(lsSQL.toString());
                
                //check for the availability of the record in the main server
                ResultSet main_rec = main_app.getConnection().createStatement().executeQuery(lsSQL.toString());
                
                if(rs_rec.next() && !main_rec.next()){
                    lsSQL = new StringBuilder();
                    lsSQL.append("INSERT INTO Support_Request_Master")
                        .append(" SET sTransNox = " + SQLUtil.toSQL(rs_rec.getString("sTransNox")))
                           .append(", dTransact = " + SQLUtil.toSQL(rs_rec.getString("dTransact")))    
                           .append(", sRqstdIDx = " + SQLUtil.toSQL(rs_rec.getString("sRqstdIDx")))    
                           .append(", sRqstgCde = " + SQLUtil.toSQL(rs_rec.getString("sRqstgCde")))    
                           .append(", sSubjectx = " + SQLUtil.toSQL(rs_rec.getString("sSubjectx")))    
                           .append(", sModuleID = " + SQLUtil.toSQL(rs_rec.getString("sModuleID")))    
                           .append(", sReferNox = " + SQLUtil.toSQL(rs_rec.getString("sReferNox")))    
                           .append(", sRequestX = " + SQLUtil.toSQL(rs_rec.getString("sRequestX")))    
                           .append(", sAssgndID = " + SQLUtil.toSQL(rs_rec.getString("sAssgndID")))    
                           .append(", nPriority = " + SQLUtil.toSQL(rs_rec.getString("nPriority")))    
                           .append(", cStatusxx = " + SQLUtil.toSQL(rs_rec.getString("cStatusxx")))    
                           .append(", cMntrCrtd = '0'")    
                           .append(", cNeedVrfy = " + SQLUtil.toSQL(rs_rec.getString("cNeedVrfy")))    
                           .append(", cAudtVrfy = " + SQLUtil.toSQL(rs_rec.getString("cAudtVrfy")))    
                           .append(", dReceived = " + SQLUtil.toSQL(main_app.getServerDate()))    
                           .append(", sModified = " + SQLUtil.toSQL(rs_rec.getString("sModified")))    
                           .append(", dModified = " + SQLUtil.toSQL(rs_rec.getString("dModified")));  
                    main_app.getConnection().createStatement().executeUpdate(lsSQL.toString());
                    
                    lsSQL = new StringBuilder();
                    lsSQL.append("SELECT *")
                        .append(" FROM Support_Request_Detail")
                        .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs.getString("sTransNox")));    
                    ResultSet rs_detl = local_app.getConnection().createStatement().executeQuery(lsSQL.toString());
                    while(rs_detl.next()){
                        lsSQL = new StringBuilder();
                        lsSQL.append("INSERT INTO Support_Request_Detail")
                            .append(" SET sTransNox = " + SQLUtil.toSQL(rs_detl.getString("sTransNox")))
                               .append(", nEntryNox = " + rs_detl.getInt("nEntryNox"))    
                               .append(", sOldValue = " + SQLUtil.toSQL(rs_detl.getString("sOldValue")))    
                               .append(", sNewValue = " + SQLUtil.toSQL(rs_detl.getString("sNewValue")))    
                               .append(", sRemarksx = " + SQLUtil.toSQL(rs_detl.getString("sRemarksx")))    
                               .append(", dModified = " + SQLUtil.toSQL(rs_detl.getString("dModified")));    
                        main_app.getConnection().createStatement().executeUpdate(lsSQL.toString());
                    }
                    
                    lsSQL = new StringBuilder();
                    lsSQL.append("UPDATE Support_Request_Master")
                        .append(" SET dReceived = " + SQLUtil.toSQL(main_app.getServerDate()))
                        .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs_rec.getString("sTransNox"))); 
                    local_app.getConnection().createStatement().executeUpdate(lsSQL.toString());

                    lsSQL = new StringBuilder();
                    lsSQL.append("UPDATE Status_Monitor")
                        .append(" SET cMntrSent = '1'")
                        .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs_rec.getString("sTransNox"))); 
                    local_app.getConnection().createStatement().executeUpdate(lsSQL.toString());
                }
            }
            main_app.commitTrans();
            local_app.commitTrans();
        } 
        catch (SQLException ex){
            logwrapr.severe("set_trackerid_value: SQLException error detected.", ex);
            local_app.rollbackTrans();
            main_app.rollbackTrans();
        }  
    }    
}

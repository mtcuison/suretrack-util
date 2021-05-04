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
public class RequestStatus {
    private static LogWrapper logwrapr = new LogWrapper("SureTrack.RequestStatus", "D:/GGC_Java_Systems/temp/RequestStatus.log");
    private static GRiderX local_app = null;
    private static GRiderX main_app = null;
    public static void main(String[] args) {
        logwrapr.info("Executing RequestStatus...");

        //load driver
        local_app = new GRiderX("gRider");
        main_app = new GRiderX("gRider");
        
        //check error from previous loading
        if(!local_app.getErrMsg().isEmpty()){
            logwrapr.severe(local_app.getMessage() + local_app.getErrMsg());
            return;
        }

        //check error from previous loading
        if(!main_app.getErrMsg().isEmpty()){
            logwrapr.severe(main_app.getMessage() + main_app.getErrMsg());
            return;
        }
        
        //use the connection from localhost server...
        //this utility should be used by the branch 
        if(!local_app.setOnline(false)){
            logwrapr.severe("Unable to connect to local server...");
            return;
        }

        if(!main_app.setOnline(true)){
            logwrapr.severe("Unable to connect to main server...");
            return;
        }
        
        //Update Tracker ID
        try {
            //Extract list of support request from the branch...
            StringBuilder lsSQL = new StringBuilder();
            lsSQL.append("SELECT sTransNox")
                .append(" FROM Support_Request_Master a") 
                .append(" WHERE sRqstgCde = " + SQLUtil.toSQL(local_app.getBranchCode())) 
                  .append(" AND IFNULL(sTrackrID, '') = ''")
                  .append(" AND cStatusxx = '0'")
                .append(" ORDER BY sTransNox ASC");
            ResultSet rs_local = local_app.getConnection().createStatement().executeQuery(lsSQL.toString());  
            System.out.println(lsSQL.toString());

            local_app.beginTrans();
            main_app.beginTrans();

            while(rs_local.next()){
                //Get the new status of the support request from main office
                lsSQL = new StringBuilder();
                lsSQL.append("SELECT a.sTransNox, b.sTrackrID")
                    .append(" FROM Support_Request_Master a") 
                       .append(" LEFT JOIN Tracker_Monitor b ON a.sTransNox = b.sTransNox")
                    .append(" WHERE b.cMntrSent = '0'")
                      .append(" AND a.sTransNox = " + SQLUtil.toSQL(rs_local.getString("sTransNox")));
                ResultSet rs_main = main_app.getConnection().createStatement().executeQuery(lsSQL.toString());  
                System.out.println(lsSQL.toString());
                
                if(rs_main.next()){
                    lsSQL = new StringBuilder();
                    lsSQL.append("UPDATE Support_Request_Master")
                        .append(" SET sTrackrID = " + SQLUtil.toSQL(rs_main.getString("sTrackrID")))
                        .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs_main.getString("sTransNox"))); 
                    local_app.getConnection().createStatement().executeUpdate(lsSQL.toString());
                    System.out.println(lsSQL.toString());

                    lsSQL = new StringBuilder();
                    lsSQL.append("UPDATE Tracker_Monitor")
                        .append(" SET cMntrSent = '1'")
                        .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs_main.getString("sTransNox"))); 
                    main_app.getConnection().createStatement().executeUpdate(lsSQL.toString());
                    System.out.println(lsSQL.toString());
                }
            }//while(rs.next()){

            local_app.commitTrans();
            main_app.commitTrans();
        } 
        catch (SQLException ex){
            logwrapr.severe("monitor_trackerid: SQLException error detected.", ex);
            local_app.rollbackTrans();
            main_app.commitTrans();
        }
        
        //Update Status
        try {
            //Extract list of support request from the branch...
            StringBuilder lsSQL = new StringBuilder();
            lsSQL.append("SELECT sTransNox")
                .append(" FROM Support_Request_Master a") 
                .append(" WHERE sRqstgCde = " + SQLUtil.toSQL(local_app.getBranchCode())) 
                  .append(" AND cStatusxx = '0'") 
                .append(" ORDER BY sTransNox ASC");
            ResultSet rs_local = local_app.getConnection().createStatement().executeQuery(lsSQL.toString());  
            System.out.println(lsSQL.toString());

            local_app.beginTrans();
            main_app.beginTrans();

            while(rs_local.next()){
                //Get the new status of the support request from main office
                lsSQL = new StringBuilder();
                lsSQL.append("SELECT a.sTransNox, IFNULL(a.sRemarksx, '') sRemarksx, a.sPickerID, a.dPostedxx, b.cStatusxx")
                    .append(" FROM Support_Request_Master a") 
                       .append(" LEFT JOIN Status_Monitor b ON a.sTransNox = b.sTransNox")
                    .append(" WHERE b.cMntrSent = '0'") 
                    .append(" AND a.sTransNox = " + SQLUtil.toSQL(rs_local.getString("sTransNox")));
                ResultSet rs_main = main_app.getConnection().createStatement().executeQuery(lsSQL.toString());  
                
                if(rs_main.next()){
                    System.err.println("Update Support: " + rs_main.getString("sTransNox"));
                    lsSQL = new StringBuilder();
                    lsSQL.append("UPDATE Support_Request_Master")
                        .append(" SET cStatusxx = " + SQLUtil.toSQL(rs_main.getString("cStatusxx")))
                           .append(", sRemarksx = " + SQLUtil.toSQL(rs_main.getString("sRemarksx")))    
                           .append(", sPickerID = " + SQLUtil.toSQL(rs_main.getString("sPickerID")))    
                           .append(", dPostedxx = " + SQLUtil.toSQL(rs_main.getString("dPostedxx")))    
                        .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs_main.getString("sTransNox"))); 
                    local_app.getConnection().createStatement().executeUpdate(lsSQL.toString());
                    System.out.println(lsSQL.toString());
                    
                    lsSQL = new StringBuilder();
                    lsSQL.append("UPDATE Status_Monitor")
                        .append(" SET cMntrSent = '1'")
                        .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs_main.getString("sTransNox"))); 
                    main_app.getConnection().createStatement().executeUpdate(lsSQL.toString());
                    System.out.println(lsSQL.toString());
                }
            }//while(rs.next()){

            local_app.commitTrans();
            main_app.commitTrans();
        } 
        catch (SQLException ex){
            logwrapr.severe("monitor_status: SQLException error detected.", ex);
            local_app.rollbackTrans();
            main_app.commitTrans();
        }
    }    
}

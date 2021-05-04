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
public class AssignID {
    private static LogWrapper logwrapr = new LogWrapper("AssignID", "temp/AssignID.log");
    private static GRiderX main_app = null;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        //load driver
        main_app = new GRiderX("gRider");
        
        //check error from previous loading
        if(!main_app.getErrMsg().isEmpty()){
            logwrapr.severe(main_app.getMessage() + main_app.getErrMsg());
            System.exit(1);
        }

        //use the connection from main server...
        if(!main_app.setOnline(true)){
            logwrapr.severe("Unable to connect to main server...");
            System.exit(1);
        }
        
        String lsTrackerID = getTracker();
        int lnNextID = Integer.valueOf(lsTrackerID.substring(6));

        main_app.beginTrans();
        
        try {
            StringBuilder lsSQL = new StringBuilder();
            lsSQL.append("SELECT a.sTransNox, a.sTrackrID, IFNULL(b.sTransNox, '') sReferNox")
                .append(" FROM Support_Request_Master a") 
                   .append(" LEFT JOIN Tracker_Monitor b ON a.sTransNox = b.sTransNox")
                .append(" WHERE a.cStatusxx = '0'") 
                .append(" ORDER BY a.dReceived ASC");
            ResultSet rs = main_app.getConnection().createStatement().executeQuery(lsSQL.toString());    
            logwrapr.info(lsSQL.toString());
            
            while(rs.next()){
                if(rs.getString("sReferNox").isEmpty()){
                    lnNextID++;
                    String lsTrackrNw = lsTrackerID.substring(0, 5) + "X" + String.format("%06d", lnNextID); 

                    StringBuilder lsUpdate = new StringBuilder();
                    lsUpdate.append("UPDATE Support_Request_Master")
                           .append(" SET sTrackrID = " + SQLUtil.toSQL(lsTrackrNw))
                           .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs.getString("sTransNox"))); 
                    main_app.getConnection().createStatement().executeUpdate(lsUpdate.toString());
                    
                    lsUpdate = new StringBuilder();
                    lsUpdate.append("INSERT INTO Tracker_Monitor")
                           .append(" SET sTransNox = " + SQLUtil.toSQL(rs.getString("sTransNox")))
                              .append(", sTrackrID = " + SQLUtil.toSQL(lsTrackrNw))
                              .append(", cMntrSent = '0'"); 
                    main_app.getConnection().createStatement().executeUpdate(lsUpdate.toString());

                    lsUpdate = new StringBuilder();
                    lsUpdate.append("UPDATE xxxOtherConfig")
                           .append(" SET sValuexxx = " + SQLUtil.toSQL(lsTrackrNw))
                           .append(" WHERE sConfigID = 'TrackerID'"); 
                    main_app.getConnection().createStatement().executeUpdate(lsUpdate.toString());
                    
                } //if(rs.getString("sReferNox").isEmpty()){
            }//while(rs.next()){
            main_app.commitTrans();
        } 
        catch (SQLException ex){
            logwrapr.severe("set_trackerid_value: SQLException error detected.", ex);
            main_app.rollbackTrans();
        }
    }    
    
    static String getTracker(){
        StringBuilder lsID = new StringBuilder();
        //update value of TrackerID in xxxOtherConfig...
        try {
            StringBuilder lsSQL = new StringBuilder();
            lsSQL.append("SELECT sValuexxx")
               .append("  FROM xxxOtherConfig") 
               .append("  WHERE sConfigID = 'TrackerID'"); 
            ResultSet rs = main_app.getConnection().createStatement().executeQuery(lsSQL.toString());    
            
            if(rs.next()){
                lsID.append(rs.getString("sValuexxx"));
            }
        } 
        catch (SQLException ex){
            logwrapr.severe("set_trackerid_value: SQLException error detected.", ex);
        }

        return lsID.toString();
    }
}

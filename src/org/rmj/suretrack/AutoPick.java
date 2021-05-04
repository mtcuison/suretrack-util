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
public class AutoPick {
    private static LogWrapper logwrapr = new LogWrapper("AutoPick", "temp/AutoPick.log");
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
        
        main_app.beginTrans();
        
        try {
            StringBuilder lsSQL = new StringBuilder();
            lsSQL.append("SELECT sTransNox, sAssgndID")
                .append(" FROM Support_Request_Master a") 
                .append(" WHERE cStatusxx = '0'") 
                  .append(" AND IFNULL(sPickerID, '') = ''")
                  .append(" AND TIMESTAMPDIFF(MINUTE, IFNULL(dReceived, dModified), NOW()) > 30 ")  
                .append(" ORDER BY dReceived ASC");
            logwrapr.info(lsSQL.toString());

            ResultSet rs = main_app.getConnection().createStatement().executeQuery(lsSQL.toString());    
            
            while(rs.next()){
                StringBuilder lsUpdate = new StringBuilder();
                lsUpdate.append("UPDATE Support_Request_Master")
                       .append(" SET sPickerID = " + SQLUtil.toSQL(rs.getString("sAssgndID")))
                       .append(" WHERE sTransNox = " + SQLUtil.toSQL(rs.getString("sTransNox"))); 
                main_app.getConnection().createStatement().executeUpdate(lsUpdate.toString());
            }//while(rs.next()){
            main_app.commitTrans();
        } 
        catch (SQLException ex){
            logwrapr.severe("set_picker_value: SQLException error detected.", ex);
            main_app.rollbackTrans();
        }
    }    
}

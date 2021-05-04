/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.rmj.suretrack;

import java.sql.ResultSet;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agent.GRiderX;
import java.sql.SQLException;
import java.util.Calendar;
import org.rmj.replication.utility.LogWrapper;

/**
 *
 * @author kalyptus
 */
public class ResetID{
    private static LogWrapper logwrapr = new LogWrapper("ResetID", "temp/ResetID.log");
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
        
        //update value of TrackerID in xxxOtherConfig...
        try {

            Calendar c1 = Calendar.getInstance();
            String lsYM = String.format("%H", Integer.valueOf(String.format("%tY%tm", c1, c1))) ; 
            
            StringBuilder lsSQL = new StringBuilder();
            lsSQL.append("SELECT sValuexxx")
                .append(" FROM xxxOtherConfig") 
                .append(" WHERE sConfigID = 'TrackerID'") 
                  .append(" AND sValuexxx LIKE " + SQLUtil.toSQL(lsYM + "%"))  ; 
            ResultSet rs = main_app.getConnection().createStatement().executeQuery(lsSQL.toString());    
            
            if(!rs.next()){
                String lsUpd = "UPDATE xxxOtherConfig" + 
                              " SET sValuexxx = " + SQLUtil.toSQL(lsYM + "X000000") + 
                              " WHERE sConfigID = 'TrackerID'";
                main_app.getConnection().createStatement().executeUpdate(lsUpd);
            }            
            
        } 
        catch (SQLException ex){
            logwrapr.severe("set_trackerid_value: SQLException error detected.", ex);
        }
    }
}

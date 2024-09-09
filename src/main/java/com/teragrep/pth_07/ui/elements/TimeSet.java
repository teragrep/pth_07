/*
 * Teragrep DPL Spark Integration PTH-07
 * Copyright (C) 2022  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_07.ui.elements;

import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectWatcher;
import org.apache.zeppelin.interpreter.InterpreterContext;

public class TimeSet extends AbstractUserInterfaceElement {
    private AngularObject<String> timeSet;
    protected TimeSet(InterpreterContext interpreterContext) {
        super(interpreterContext);

        AngularObjectWatcher angularObjectWatcher = new AngularObjectWatcher(getInterpreterContext()) {
            @Override
            public void watch(Object o, Object o1, InterpreterContext interpreterContext) {
                if(LOGGER.isTraceEnabled()) {
                    LOGGER.trace("TimeSet <- {}", o.toString());
                    LOGGER.trace("TimeSet -> {}", o1.toString());
                }
            }
        };

        timeSet = getInterpreterContext().getAngularObjectRegistry().add(
                "TimeSet",
                "",
                getInterpreterContext().getNoteId(),
                getInterpreterContext().getParagraphId(),
                true
        );
        timeSet.addWatcher(angularObjectWatcher);
    }

    @Override
    protected void draw() {

    }

    @Override
    public void emit() {
        timeSet.emit();
    }
/*
    public void initialTimeSet() {
        //setting up initial variables
        htmlHandler.setAngular("TimeSet", "All");
        String message = "The dpl.ui.timesets flag is either not present or set to false";
        if(timesets){
            message = "The dpl.ui.timesets flag was found and is set to true, rendering the Timeset UI";
        }
    }

    public void parseTimeSet() {
        boolean timesets = config.getBoolean("dpl.ui.timesets");
        try{
            if (timesets){
                //handler.messager("Starting the UI update loop");
                String resultOutput = "";
                String newTimeset  = (String) htmlHandler.getAngular("TimeSet");
                //parsing the TimeSet
                String result = "Not Set";
                switch (newTimeset) {
                    case "RC":
                        //parsing all Relative timeset input
                        result = htmlHandler.RelativeTimeset();
                        break;
                    case "RTC":
                        //parsing all Real-Time timeset input
                        result = htmlHandler.RealTimeSet();
                        break;
                    case "DC":
                        result = htmlHandler.DateRange();
                        break;
                    case "DTC":
                        result = htmlHandler.DateTimeRange();
                        break;
                    case "AC":
                        result = htmlHandler.AdvancedRange();
                        break;
                    default:
                        //procesing a preset
                        if (newTimeset != null) {
                            resultOutput = "The value of variable TimeSet: " + newTimeset;
                        }else {
                            resultOutput = "The value of the TimeSet is null";
                        }
                        break;
                }
                if(!result.equals("Error")){
                    if(!result.equals("Not Set")){
                        resultOutput = result;
                    }
                }else {
                    resultOutput = "There was an error in parsing the timeset, with result equal: " + result;
                }
                //The example value of variable AngularObject{noteId='2G4YKUSZT', paragraphId='paragraph_1620392276599_2013720257', object=Timeset 2, name='selected'}
                htmlHandler.timesetMessageUpdate(resultOutput);
            }
        }catch(Exception e)
        {
            System.out.println(e);
            htmlHandler.messager("There was an error in the loop cycle:" + e);
        }
    }



    public String RelativeTimeset(){
        //set/clean the warning
        setAngular("RCMessage", "");
        //getting all objects
        String value = (String) getAngular("RCNum");
        String to = (String) getAngular("RCTo");
        String snap = (String) getAngular("RCSnap");
        String measure = (String) getAngular("RCMeasure");
        //initialize for parsing
        Integer valueP;
        Boolean snapP;
        String toP;
        String measureP;
        //checking if value is num
        try{
            valueP = (int) Double.parseDouble(value);
        }catch(NullPointerException|IndexOutOfBoundsException|NumberFormatException e){
            setAngular("RCMessage", "Input is invalid, please use numbers" + e);
            return("Error");
        }
        //parsing others
        try{
            toP = to;
            snapP = Boolean.parseBoolean(snap);
            measureP = timeShort(measure);

        }catch(NullPointerException|IndexOutOfBoundsException e){
            setAngular("RCMessage", "Something went wrong with " + e);
            return "Error";//in case something goes wrong
        }
        //return output string
        return("The Relative setting: (Snapping:" + snapP.toString() + "); From last "+ valueP.toString() + measureP + ", snapping to "+ toP );
    }

    public String RealTimeSet(){
        //set/clean the warning
        setAngular("RTCMessage", "");
        //getting all objects
        String value = (String) getAngular("RTCNum");
        String measure = (String) getAngular("RTCMeasure");
        //initialize for parsing
        Integer valueP;
        String measureP;
        //checking if value is num
        try{
            valueP = (int) Double.parseDouble(value);
        }catch(NullPointerException|IndexOutOfBoundsException|NumberFormatException e){
            setAngular("RTCMessage", "Input is invalid, please use numbers" + e);
            return("Error");
        }
        //parsing others
        try{
            measureP = timeShort(measure);

        }catch(NullPointerException|IndexOutOfBoundsException e){
            setAngular("RTCMessage", "Something went wrong with " + e);
            return "Error";//in case something goes wrong
        }
        //return output string
        return("The Real-Time setting: From last "+ valueP.toString() + measureP + ", to current time");
    }

    public String DateRange(){
        //used for parsing the input strings
        //set/clean the warning
        setAngular("DCMessage", "");
        //getting all objects
        String result;
        try{
            String input1 = (String) getAngular("DC1");
            String input2 = (String) getAngular("DC2");
            String measure = (String) getAngular("DCMeasure");
            result = timeMeasure(measure, input1, input2);
        }catch(NullPointerException|IndexOutOfBoundsException e){
            setAngular("DCMessage", "Something went wrong with " + e);
            return "Error";//in case something goes wrong
        }
        //return output string
        return(result);
    }

    public String DateTimeRange(){
        //used for parsing the input strings
        //set/clean the warning
        setAngular("DTCMessage", "");
        //getting all objects
        String result;
        try{
            String input1 = (String) getAngular("DTC1");
            String input2 = (String) getAngular("DTC2");
            String measure = (String) getAngular("DTCMeasure");
            result = timeMeasure(measure, input1, input2);
        }catch(NullPointerException|IndexOutOfBoundsException e){
            setAngular("DTCMessage", "Something went wrong with " + e);
            return "Error";//in case something goes wrong
        }
        //return output string
        return(result);
    }

    public String AdvancedRange(){
        //used for parsing the input strings
        //set/clean the warning
        setAngular("ACMessage", "");
        //getting all objects
        String result;
        try{
            String input1 = (String) getAngular("AC1");
            String input2 = (String) getAngular("AC2");
            result = "User Advanced Timeset: from " + input1 + " to " + input2;
        }catch(NullPointerException|IndexOutOfBoundsException e){
            setAngular("ACMessage", "Something went wrong with " + e);
            return "Error";//in case something goes wrong
        }
        //return output string
        return(result);
    }


  private String timeShort(String longer){
    String shorter;
    switch(longer)
        {
            case "Seconds ago":
                shorter = "s";
                break;
            case "Minutes ago":
                shorter = "min";
                break;
            case "Hours ago":
                shorter = "h";
                break;
            case "Days ago":
                shorter = "d";
                break;
            case "Weeks ago":
                shorter = "w";
                break;
            case "Month ago":
                shorter = "month";
                break;
            case "Years ago":
                shorter = "y";
                break;
            default:
                shorter = "s";
        }
        return shorter;
  }
  private String timeMeasure(String choice, String Time1, String Time2){
    String result;
    //here goes custom logic on each choice, but currently only a placeholder
    switch(choice)
        {
            case "Between":
                result = "Showing records in the time range between " + Time1 + " and "+ Time2;
                break;
            case "Before":
                result = "Showing records created before "+ Time1;
                break;
            case "Since":
                result = "Showing records starting from " + Time1 + "and up to now";
                break;
            default:
                result = "Measure not recognized";
        }
        return result;
  }
 */

}

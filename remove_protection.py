# Set Quality Flag to 0 for a CWMS TS ID

from hec.script                     import MessageBox, Constants
import java
import time,calendar,datetime
from time                           import mktime
from hec.dssgui                     import ListSelection
from time				            import mktime
import inspect
import DBAPI
import os
import urllib
from hec.heclib.util	            import HecTime
from hec.hecmath		            import TimeSeriesMath
from hec.io			                import TimeSeriesContainer
from rma.services		            import ServiceLookup
from java.util			            import TimeZone
from hec.data.tx                    import QualityTx
from java.text 						import SimpleDateFormat
from java.util 						import Date


print '='
print '='
print '='
print '=================================================================================='
print '====================================== START LOG RUN ============================='
print '=================================================================================='
print '='
print '='
print '='


# Define the date-time string and the pattern used to parse it
# datetime_string = '2022-02-16 14:30:00'
# datetime_pattern = 'yyyy-MM-dd HH:mm:ss'

# Create a SimpleDateFormat object with the specified pattern
# date_formatter = SimpleDateFormat(datetime_pattern)

# Parse the date-time string and convert it to a Date object
# date_obj = date_formatter.parse(datetime_string)
# print(date_obj)


try:
    #CurDate = datetime.datetime.now() - datetime.timedelta(days=(21*365))
    #CurDate = '2017-12-31 23:59:59.000000'
    #print 'CurDate = ' + str(CurDate)
    #EndTwStr = CurDate.strftime('%d%b%Y %H%M')
    #print 'EndTwStr = ' + EndTwStr

    #StartTw  = datetime.datetime.now() - datetime.timedelta(days=(24*365))
    #StartTw  = '2015-01-01 01:01:01.000000'
    #print 'StartTw = ' + str(StartTw)
    #StartTwStr = StartTw.strftime('%d%b%Y %H%M')
    #print 'StartTwStr = ' + StartTwStr
    #sys.exit()
    
    # hard code start and end time window
    StartTwStr = '01Jan1990 0000'
    EndTwStr = '31Dec2000 2400'

    print 'StartTwStr = ' + StartTwStr
    print 'EndTwStr = ' + EndTwStr
    

    try :
        CwmsDb = DBAPI.open()
        if not CwmsDb : raise Exception
        CwmsDb.setTimeWindow(StartTwStr, EndTwStr)
        CwmsDb.setOfficeId('MVS')
        CwmsDb.setTimeZone('GMT')
    except : pass


    # Single TS ID OPTION, STAGE REV and 29 GOES HERE
    # Data = ['Hermann-Missouri.Stage.Inst.15Minutes.0.lrgsShef-rev']

    # Multiple TS ID OPTION, STAGE REV and 29 GOES HERE
    Data = ['Mel Price TW-Mississippi.Stage.Inst.30Minutes.0.lrgsShef-rev','Mel Price TW-Mississippi.Stage.Inst.30Minutes.0.29']



# Loop and Set
    for x in Data:

        Tsc = CwmsDb.get(x)

        print "len(Tsc.values) = " + str(len(Tsc.values))

        for i in range(Tsc.numberValues) :

            # Write over to protected data
            CwmsDb.setOverrideProtection(True)

            # This will get all values from the database. Otherwise it will not get the null values
            CwmsDb.setTrimMissing(False)

            print "-----i------ start = " + str(Tsc.quality[i])

            # Remove protection flag
            Tsc.quality[i] = QualityTx.clearProtected_int(Tsc.quality[i])

            # force set quality to 0, not used
            #Tsc.quality[i] = 0
            print "-----i------ end = " + str(Tsc.quality[i])
            
        CwmsDb.put(Tsc)

        print 'EndTwStr = ' + EndTwStr
        print 'StartTwStr = ' + StartTwStr
        
    #******************************************************    
    print '='
    print '='
    print '='
    print '=================================================================================='
    print '====================================== END LOG RUN ==============================='
    print '=================================================================================='
    print '='
    print '='
    print '='
    
    MessageBox.showInformation('Completed', 'Alert')

finally :
    # close the connection in the finally block
    if CwmsDb:
        CwmsDb.close()
        print("Oracle database connection closed")
            
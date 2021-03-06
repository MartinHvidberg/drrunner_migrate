# Running Data Reviewer, via ArcPy
# This .py file is the Command line version.
# It's very short, it simply calls the main executing module,
# after setting up a few parameters.

import sys
import os.path
import DRrunner_execute
import DRrunner_reporter

if __name__ == "__main__":
    
    # *** Setup parameters
    
    # Set reviewer workspace
    reviewer_db = r"C:\Martin\Work\Data_Reviewer\DRrunner.gdb"    
    # reviewer batch job file
    str_filename_rbj = r"C:\Martin\Work\NamesA_DomainsCheck.rbj"    
    # production database - contains data to validate
    str_data_db = "C:\Martin\Work\GNDB.gdb" # "C:/gisdata/Edit_Sample.sde"    
    
    # Check existance and validity of the above parameters
    if not os.path.exists(reviewer_db):
        print "Can't find reviewer_db: "+reviewer_db
        sys.exit(-1)
    if not os.path.exists(str_filename_rbj):
        print "Can't find str_filename_rbj: "+str_filename_rbj
        sys.exit(-2)
    if not os.path.exists(str_data_db):
        print "Can't find str_data_db: "+str_data_db
        sys.exit(-3)
        
    # *** Run the checks
     
    # Run
    lst_run_ids = list()
    lst_run_ids.append(DRrunner_execute.DRrun(reviewer_db, str_filename_rbj, str_data_db))
    print "Done running - returning following ids:"
    for run_id in lst_run_ids:
        print "    runID: "+str(run_id)
    
    # *** Optionally display the errors found ...
     
    lst_error_reports = list()
    for id_i in lst_run_ids:
        dic_error_report = DRrunner_reporter.make_report_of_errors(reviewer_db, id_i)
        print "\nThis is what we got:\n"+str(dic_error_report)
        DRrunner_reporter.show_report(dic_error_report)
    DRrunner_reporter.email_reports(lst_error_reports)
    

# Music that accompanied the coding of this script:
#   
import pickle
import sys
import os
import random
import tarfile
import pathlib

"""
source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

usage:  python3 metricsCollector.py /path/to/workflow
where: comes from JobArchiver
"""

def collectStats(report):
    # Let's collect some statistics from the WM job report
    with open(report, "br") as f:
        p = pickle.load(f)
    
    WMTimes = {}
    
    # Get total WM wallclock time
    WMTimes['WM_wrapper'] = p.WMTiming.WMTotalWallClockTime
    
    # Get all steps
    for step in p.steps:
        pStep = getattr(p, step)
        # This is the time from the step measured in the submit wrapper
        WMTimes[step] = pStep.stopTime - pStep.startTime
        # These are times measured from cmsRun steps
        if 'cmsRun' in step:
            WMTimes["{}--WMCMSSWSubprocess".format(step)] = pStep.WMCMSSWSubprocess.wallClockTime
            WMTimes["{}--TotalJobTime".format(step)] = pStep.performance.cmssw.Timing.TotalJobTime
    
   
    allsteps_wcTime = 0. 
    allcmsRuns_totalJobTime = 0.
    allcmsRuns_wcTime = 0.
    WM_subprocess_eff = {}
    for k in WMTimes.keys():
        if 'cmsRun' in k and len(k.split('--')) == 1:
            # Subprocess efficiency per cmsRun step: TotalJobTime / WMCMSSWSubprocess
            WM_subprocess_eff[k] = WMTimes['{}--TotalJobTime'.format(k)] / WMTimes['{}--WMCMSSWSubprocess'.format(k)] * 100
    
        if '--TotalJobTime' in k:
            # Eff w.r.t totalJobTime: Sum of all CMSSW TotalJobTimes / WM_Wrapper
            allcmsRuns_totalJobTime += WMTimes[k]

        if '--WMCMSSWSubprocess' in k:
            allcmsRuns_wcTime += WMTimes[k]

        # Sum wall clock time of all steps
        if len(k.split('--')) == 1 and k != 'WM_wrapper':
            #print("[allSteps_wcTime] Including step {} = {}".format(k, WMTimes[k]))
            allsteps_wcTime += WMTimes[k]
    #print("allsteps_wcTime = {}".format(allsteps_wcTime))
    
    WM_eff_totalJobTime = allcmsRuns_totalJobTime / WMTimes['WM_wrapper'] * 100
    WM_eff_subprocStepsTime = allcmsRuns_wcTime / WMTimes['WM_wrapper'] * 100
    WM_eff_steps = allsteps_wcTime  / WMTimes['WM_wrapper'] * 100
    
    print("WMTimes collected: {}".format(WMTimes))
    print("Efficiency of SumOfAllSteps vs Wrapper (WC time) = {} %".format(WM_eff_steps))
    print("- cmsRun metrics")
    for k in WMTimes.keys():
        if 'cmsRun' in k and len(k.split('--')) == 1:
            print("[{}] Efficiency of cmsRun step TotalJobTime vs WMCMSSWSubprocess = {}".format(k, WM_subprocess_eff[k]))
            if WM_subprocess_eff[k] < 90.0:
                print("Warning, take a look at the efficiency above")
    if allcmsRuns_totalJobTime > 0. and allcmsRuns_wcTime > 0.:
        print("Total Efficiency of cmsRuns totalJobTime vs Wrapper WC Time= {} %".format(WM_eff_totalJobTime))
        print("Total Efficiency of cmsRuns SubProcess WC Times vs Wrapper WC Time = {} %".format(WM_eff_subprocStepsTime))
    
    for k,v in WM_subprocess_eff.items():
        print('WM_subprocess_eff[{0}] = {1} %'.format(k, v))


if __name__ == "__main__":
    # Just one argument for the workflow path
    path = sys.argv[1]
    
    # Check if path exists
    if not os.path.exists(path):
        print("Path {} does not exist.".format(path))
        sys.exit(1)
    
    # Untar 1 random file subdirectory
    subdirs = [ f.path for f in os.scandir(path) if f.is_dir() ]
    #if len(subdirs) > 5:
    #    subdirs = random.sample(subdirs, 5)
    #print("Subdirs: {}".format(subdirs))

    # Pick up to 10 files per subdirectory
    jobFiles = []
    for subdir in subdirs:
        files = [os.path.join(subdir, f) for f in os.listdir(subdir) if os.path.isfile(os.path.join(subdir, f))]
        if len(files) > 10:
            subdirFiles = random.sample(files, 10)
        else:
            subdirFiles = files

        jobFiles += subdirFiles

    reports = {}
    for f in jobFiles:
        print("file = {}".format(f))
        try:
            reportPath = os.getcwd()
            reportTarballLocation = "{}/Report.0.pkl".format(os.path.basename(f).rstrip('.tar.bz2'))
            #print("ReportTarballLocation = {}".format(reportTarballLocation))
            tarfile.open(f).extract(reportTarballLocation, path=reportPath)
            reports[f] = os.path.join(reportPath, reportTarballLocation)
            #print("Adding report path:{}".format(reportPath))
        except KeyError:
            print("Could not extract report file from: {}, skipping.".format(f))

    # Now, lets collect stats per report
    for f, report in reports.items():
        print("---------------------------------")
        print("Report file: {}".format(f))
        try:
            collectStats(report)
        except Exception as e:
            print("There was an error collecting stats from report: {}. Skip".format(report))
            print(e) 

#!bin/linux-x86_64/isegIoc

## You may have to change vcs to something else
## everywhere it appears in this file

epicsEnvSet("ARCH","linux-x86")
epicsEnvSet("IOC","isegIoc")
epicsEnvSet("TOP","./")
epicsEnvSet("EPICS_BASE","/epics-local/base-7.0.7")
epicsEnvSet(ISEGPORT, "ISEGHAL")
epicsEnvSet(ICSMINIHAL, "hal://ts2-cm-antenna-hvps.tn.esss.lu.se:1454/can0,user,pass")

## Register all support components
dbLoadDatabase "dbd/isegIoc.dbd"
isegIoc_registerRecordDeviceDriver pdbbase

#connect to the remote device
isegHalConnect( "$(ISEGPORT)", "$(ICSMINIHAL)")

cd "${TOP}"
iocInit



/*******************************************************************************
 * Copyright (C) 2015 Florian Feldbauer <f.feldbauer@him.uni-mainz.de>
 *                    - Helmholtz-Institut Mainz
 *
 * This file is part of devIsegHal
 *
 * devIsegHal is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * devIseghal is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 * version 2.0.0; May 25, 2015
 *
*******************************************************************************/

/**
 * @file devIsegHalAi.c
 * @author F.Feldbauer
 * @date 25 May 2015
 * @brief Device Support implementation for ai records
 */

/*_____ I N C L U D E S ______________________________________________________*/

/* ANSI C includes  */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* EPICS includes */
#include <aiRecord.h>
#include <alarm.h>
#include <dbAccess.h>
#include <errlog.h>
#include <epicsExport.h>
#include <epicsTypes.h>
#include <iocLog.h>
#include <iocsh.h>
#include <recGbl.h>

/* local includes */
#include "devIsegHal.h"

/*_____ D E F I N I T I O N S ________________________________________________*/
static long devIsegHalInitRecord_ai( aiRecord *prec );
static long devIsegHalRead_ai( dbCommon *prec, char* value ); 

/*_____ G L O B A L S ________________________________________________________*/
devIsegHal_dset_t devIsegHalAi = {
  7,
  NULL,
  devIsegHalInit,
  devIsegHalInitRecord_ai,
  devIsegHalGetIoIntInfo,
  devIsegHalRead,
  NULL,
  devIsegHalRead_ai
};
epicsExportAddress( dset, devIsegHalAi );

/*_____ L O C A L S __________________________________________________________*/

/*_____ F U N C T I O N S ____________________________________________________*/

/**-----------------------------------------------------------------------------
 * @brief   Initialization of ai records
 * @param   [in]  prec   Address of the record calling this function
 * @return  In case of error return -1, otherwise return 2 (no conversion)
 *----------------------------------------------------------------------------*/
static long devIsegHalInitRecord_ai( aiRecord *prec ){
  prec->pact = (epicsUInt8)true; /* disable record */

  devIsegHal_rec_t conf = { &prec->inp, "R", "R4", false };
  long status = devIsegHalInitRecord( (dbCommon*)prec, &conf );
  if( status != 0 ) return ERROR;

  devIsegHal_info_t* pinfo = (devIsegHal_info_t*)prec->dpvt;
  if( strlen( prec->egu ) == 0 ) strcpy( prec->egu, pinfo->unit );

  prec->linr = 0;
  prec->pact = (epicsUInt8)false; /* enable record */

  return DO_NOT_CONVERT;
}

/**-----------------------------------------------------------------------------
 * @brief   Parse value from cstring for ai Records
 * @param   [in]  prec   Address of the record calling this function
 * @param   [in]  value  Address of cstring containing value
 * @return  -1 in case of error
 *           2 (no conversion)
 *----------------------------------------------------------------------------*/
static long devIsegHalRead_ai( dbCommon *prec, char* value ) {
  aiRecord *pai = (aiRecord*)prec;
  epicsFloat64 buffer = 0.;
	//printf( "devIsegHalRead_ai: Reading item '%s'\n", prec->name);
  if( sscanf( value, "%lf", &buffer ) != 1 ) {
    return ERROR;
  }
  pai->val = buffer;
  return DO_NOT_CONVERT;
}


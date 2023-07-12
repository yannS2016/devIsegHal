//******************************************************************************
// Copyright (C) 2015 Florian Feldbauer <f.feldbauer@him.uni-mainz.de>
//                    - Helmholtz-Institut Mainz
//                    iseg Spezialelektronik GmbH
//
// This file is part of deviseg
//
// deviseg is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 3 of the License, or
// (at your option) any later version.
//
// deviseg is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
//
// version 2.0.0; May 25, 2015
//
//******************************************************************************

//! @file devIsegHal.cpp
//! @author F.Feldbauer
//! @date 25 May 2015
//! @brief Global functions of devIsegHal

//_____ I N C L U D E S ________________________________________________________

// ANSI C/C++ includes
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <list>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>

// EPICS includes
#include <alarm.h>
#include <dbAccess.h>
#include <errlog.h>
#include <epicsExport.h>
#include <epicsThread.h>
#include <epicsExit.h>
#include <epicsTypes.h>
#include <iocLog.h>
#include <iocsh.h>
#include <recGbl.h>
#include <epicsMessageQueue.h>
// local includes
#include "devIsegHalClasses.hpp"

//_____ D E F I N I T I O N S __________________________________________________

//_____ G L O B A L S __________________________________________________________

//_____ L O C A L S ____________________________________________________________
#define RECV_Q_SIZE 1000        /* Num messages to buffer */


typedef enum
{
      GET_ITEM = 0,
      SET_ITEM = 1,
      CLOSE_CONN = 2,
} devIsegHal_req_t;

typedef struct {
      devIsegHal_info_t *pdata;
      devIsegHal_req_t reqType;
      char value[VALUE_SIZE];
} devIsegHal_queue_t;

static isegHalThread* myIsegHalThread = NULL;
static epicsMessageQueueId isegClientQueue = NULL;

//_____ F U N C T I O N S ______________________________________________________
double timespec_diff( const struct timespec * stop, const struct timespec * start )
{
  long	start_sec	= start->tv_sec;
  long	start_nsec	= start->tv_nsec;

  if ( stop->tv_nsec < start_nsec ) {
    long	nSeconds = ( start_nsec - stop->tv_nsec ) / 1e9 + 1;
    start_nsec -= 1e9 * nSeconds;
    start_sec  += nSeconds;
  }
  if ( stop->tv_nsec - start_nsec > 1e9 ) {
    long nSeconds = ( stop->tv_nsec - start_nsec) / 1e9;
    start_nsec += 1e9 * nSeconds;
    start_sec  -= nSeconds;
  }

  double	nSeconds = stop->tv_sec  - start_sec;
  double	nNSec	 = stop->tv_nsec - start_nsec;

  return nSeconds + ( nNSec / 1.0e9 );
}

static std::ostream& operator<<( std::ostream& ost, const IsegResult& result ) {
  switch( result ) {
    case ISEG_OK:                  ost << "ISEG_OK";                  break;
    case ISEG_ERROR:               ost << "ISEG_ERROR";               break;
    case ISEG_WRONG_SESSION_NAME:  ost << "ISEG_WRONG_SESSION_NAME";  break;
    case ISEG_WRONG_USER:          ost << "ISEG_WRONG_USER";          break;
    case ISEG_WRONG_PASSWORD:      ost << "ISEG_WRONG_PASSWORD";      break;
    case ISEG_NOT_AUTHORIZED:      ost << "ISEG_NOT_AUTHORIZED";      break;
    case ISEG_NO_SSL_SUPPORT:      ost << "ISEG_NO_SSL_SUPPORT";      break;
    default:                       ost << (int)result;
  }
  return ost;
}

//------------------------------------------------------------------------------
//! @brief       D'tor of class isegHalConnectionHandler
//!
//! Disconnects all registered interfaces
//------------------------------------------------------------------------------
isegHalConnectionHandler::~isegHalConnectionHandler() {
/*  std::vector< std::string >::iterator it = _interfaces.begin();
  for( ; it != _interfaces.end(); ++it ) {
  	IsegResult status = iseg_disconnect( it->c_str() );
    if ( ISEG_OK != status ) {
      std::cerr << "\033[31;1m Cannot disconnect from isegHAL interface '"
                << (*it) << "'.\033[0m"
                << std::endl;
  	}
  }*/
  devIsegHal_queue_t qmsg = { NULL, CLOSE_CONN, NULL};
  if (epicsMessageQueueTrySend(isegClientQueue, &qmsg, sizeof(devIsegHal_queue_t))){
       fprintf( stderr, "\033[31;1m: isegHal Mgt Queue Overflowed \033[0m\n" );
  }
 _interfaces.clear();
std::cout << "(" << __FUNCTION__ << ") Cleaning up: " << epicsThreadGetNameSelf() << std::endl;
//epicsThreadSleep(10);
}

//------------------------------------------------------------------------------
//! @brief       Get instance of the isegHALconnection handler
//! @return      Reference to singleton
//!
//! This function returns a reference to the only instance of this singleton
//! class. If no instance exists yet, it is created.
//! Using a reference for the singleton, the destructor of the class will be
//! correctly called at the end of the programm
//------------------------------------------------------------------------------
isegHalConnectionHandler& isegHalConnectionHandler::instance() {
  static isegHalConnectionHandler myInstance;
  return myInstance;
}

  /*Retrun the isegHAL server interface*/

  std::string isegHalConnectionHandler::getHalInterface() {
    return _halInterface;
  }

  std::string isegHalConnectionHandler::getName() {
          return _name;
  }

  void isegHalConnectionHandler::setName(std::string name) {
      _name = name;
  }

  void isegHalConnectionHandler::storeHalNames(std::vector<std::string> &dstinterfaces) {
 			std::vector< std::string >::iterator it = _interfaces.begin();
  		for( ; it != _interfaces.end(); ++it ) {
        dstinterfaces.push_back( (*it) );
      }
  }
//------------------------------------------------------------------------------
//! @brief       Connect to new interface
//! @param [in]  name        deviseg internal name of the interface handle
//! @param [in]  interface   name of the hardware interface
//! @return      true if interface is already connected or if successfully connected
//------------------------------------------------------------------------------
bool isegHalConnectionHandler::connect( std::string const& name, std::string const& interface ) {
  std::cout << "(" << __FUNCTION__ << ") function in  thread id: " << epicsThreadGetNameSelf() << std::endl;

  std::vector< std::string >::iterator it;
  it = std::find( _interfaces.begin(), _interfaces.end(), name );
  if( it != _interfaces.end() ) return true;

  //  std::cout << "Trying to connect to '" << interface << "'" << std::endl;

  IsegResult status = iseg_connect( name.c_str(), interface.c_str(), NULL );
  if ( ISEG_OK != status ) {
    std::cerr << "\033[31;1mCannot connect to isegHAL interface '" << interface << "'\n"
              << "  Result: " << status
              << ", Error: " << strerror( errno ) << "(" << errno << ")\033[0m"
              << std::endl;
    return false;
  }
  // iseg HAL starts collecting data from hardware after connect.
  // wait 5 secs to let all values 'initialize'
  sleep( 5 ); 

  _interfaces.push_back( name );
  _halInterface = interface;
  _name = name;
  return true;
}

//------------------------------------------------------------------------------
//! @brief       Check if an interface is connected
//! @param [in]  name    deviseg internal name of the interface handle
//------------------------------------------------------------------------------
bool isegHalConnectionHandler::connected( std::string const& name ) {
  if( name.compare( "AUTO" ) == 0 ) return true;

  std::vector< std::string >::iterator it;
  it = std::find( _interfaces.begin(), _interfaces.end(), name );
  if( it != _interfaces.end() ) return true;
  return false;
}

//------------------------------------------------------------------------------
//! @brief       Disconnect from an interface
//! @param [in]  name    deviseg internal name of the interface handle
//------------------------------------------------------------------------------
void isegHalConnectionHandler::disconnect( std::string const& name ) {
  std::vector< std::string >::iterator it;
  it = std::find( _interfaces.begin(), _interfaces.end(), name );

  if( it != _interfaces.end() ) {
  	int status = iseg_disconnect( name.c_str() );
    if ( ISEG_OK != status ) {
      std::cerr << "\033[31;1m Cannot disconnect from isegHAL interface '"
                << name << "'.\033[0m"
                << std::endl;
  		return;
  	}
    _interfaces.erase( it );
  }
}

void regCallback( dbCommon* prec, devIsegHal_info_t *pinfo ) {
    //devIsegHal_info_t *pinfo = (devIsegHal_info_t *)prec->dpvt;
    if( !pinfo->pcallback ) {
      CALLBACK *pcallback = new CALLBACK;
      callbackSetCallback( devIsegHalCallback, pcallback );
      callbackSetUser( (void*)prec, pcallback );
      callbackSetPriority( priorityLow, pcallback );
      pinfo->pcallback = pcallback;
    }
}

static void isegMgtTask() {
    std::cout << "(" << __FUNCTION__ << ") function in " << __FILE__ << " (line: "
    << __LINE__ << ") was called by thread id: " << epicsThreadGetNameSelf() << std::endl;

    std::string name = /*"_" +*/ isegHalConnectionHandler::instance().getName() + "_MOD";
    std::string interface =  isegHalConnectionHandler::instance().getHalInterface();
    isegHalConnectionHandler::instance().setName(name);

   // std::cout << "using HAL version [" << iseg_getVersionString() << "]" << std::endl;
    if( !isegHalConnectionHandler::instance().connect(name, interface ) ){
        fprintf( stderr, "\033[31;1mCannot connect to isegHAL interface %s(%s)\033[0m\n", name.c_str(), interface.c_str());
    }
    std::vector<std::string> _sinterfaces;
    //keep a copy of created interfaces for cleanup 
    isegHalConnectionHandler::instance().storeHalNames( _sinterfaces );

    devIsegHal_queue_t rmsg;
    while(1) {
      /* Wait for event from client task */
			int msg = epicsMessageQueuePending	(isegClientQueue );	
      //std::cout << "Number of Messages in the queue: " << msg << " (" << __FUNCTION__ << ")" << " thread id: " << epicsThreadGetNameSelf() << std::endl;

      int rcv = epicsMessageQueueReceive(isegClientQueue, &rmsg, sizeof(rmsg));
      //std::cout << "Pending request: " << rcv << " (" << __FUNCTION__ << ")" << "was called by thread id: " << epicsThreadGetNameSelf() << std::endl;
			if( rcv  < 1 ) continue;

			std::string _name = isegHalConnectionHandler::instance().getName();
			devIsegHal_req_t  _req = rmsg.reqType;
			/*if( _req == CLOSE_CONN) {
					std::cout << "Close Connection request: " << " (" << __FUNCTION__ << ")" << "was called by thread id: " 
										<< epicsThreadGetNameSelf() << std::endl;
              //IsegResult status = iseg_disconnect( "ETHHAL_MOD" );
              //if ( ISEG_OK != status ) {
              //  std::cerr << "\033[31;1m Cannot disconnect from isegHAL interface '"
                //  << "(*it)" << "'.\033[0m"
                  //<< std::endl;
             // }
					epicsThreadSleep(3);
					std::cout << "Done waiting, need to close socket: " << " (" << __FUNCTION__ << ")" << "was called by thread id: "<< epicsThreadGetNameSelf() << std::endl;
					continue;
			}*/
			

    	devIsegHal_info_t* _pdata = (devIsegHal_info_t*)rmsg.pdata;
			if(!_pdata || _req == CLOSE_CONN) {
/*						std::cout << "Closing socket " << " (" << __FUNCTION__ << ")" << " thread id: " 
											<< epicsThreadGetNameSelf() << std::endl;epicsThreadSleep(4);iseg_disconnect( _name.c_str() );exit(0);*/
				std::cout << "Closing socket " << " (" << __FUNCTION__ << ")" << " thread id: "
                  << epicsThreadGetNameSelf() << std::endl;
				epicsThreadSleep(1);
				//epicsExitLater(0);
  			std::vector< std::string >::iterator it = _sinterfaces.begin();
  			for( ; it != _sinterfaces.end(); ++it ) {
  				IsegResult status = iseg_disconnect( it->c_str() );
    			if ( ISEG_OK != status ) {
      			std::cerr << "\033[31;1m Cannot disconnect from isegHAL interface '"
                << (*it) << "'.\033[0m"
                << std::endl;
  				}
  			}
				//epicsExit(0);

			}
			devIsegHal_pflags_t _proc = _pdata->pflag;
      //devIsegHal_req_t  _req = rmsg.reqType;

      std::string  _value = rmsg.value;
      IsegItem item = EmptyIsegItem;

      switch(_req) {

        case GET_ITEM:
					//std::cout << "GET_ITEM ("<< _pdata->object << __FUNCTION__ << ") thread id: " << epicsThreadGetNameSelf() << std::endl;
          switch(_proc) {

            case P_ASYNC:
							//	std::cout << "GET_ITEM:P_ASYNC "<< _pdata->object<<"(" << __FUNCTION__ << ") thread id: " << epicsThreadGetNameSelf() << std::endl;
              item = iseg_getItem(_name.c_str(), (_pdata)->object);
              memcpy( _pdata->quality, item.quality,  QUALITY_SIZE );
              memcpy( _pdata->value, item.value, VALUE_SIZE );
              memcpy( _pdata->rtime, item.timeStampLastChanged, TIME_SIZE );
    #ifdef CHECK_LAST_REFRESHED
              memcpy( _pdata->rtime, item.timeStampLastChanged, TIME_SIZE );
    #endif
              _pdata->pflag = _proc;  // better be sure;
              callbackRequest( (_pdata)->pcallback );
						break;
            case P_IO_INTR:
							{

								//	std::cout << "GET_ITEM:P_IO_INTR  "<< _pdata->object<<"(" << __FUNCTION__ << ") thread id: " << epicsThreadGetNameSelf() << std::endl;
                bool quality = true;
      					bool timestampchanged = true;
              	item = iseg_getItem(_name.c_str(), (_pdata)->object);
              	if( strcmp( item.quality, ISEG_ITEM_QUALITY_OK ) != 0 ) quality = false;

             		epicsUInt32 seconds = 0;
      					epicsUInt32 microsecs = 0;
              	if( sscanf( item.timeStampLastChanged, "%u.%u", &seconds, &microsecs ) != 2 ) timestampchanged = false;
              	epicsTimeStamp time;
             		time.secPastEpoch = seconds - POSIX_TIME_AT_EPICS_EPOCH;
              	time.nsec = microsecs * 100000;

              	if( quality && timestampchanged ) {
                	//std::cout << "GET_ITEM: P_IO_INTR (" << __FUNCTION__ << ") thread id: " << epicsThreadGetNameSelf()<< std::endl;
                 	if( _pdata->time.secPastEpoch != time.secPastEpoch ||  _pdata->time.nsec != time.nsec ) {
                  	// value was updated in isegHAL
                 		// std::cout << "GET_ITEM: " << _proc << " : " << _pdata->object << " : "<<item.value <<  " (" << __FUNCTION__ << ") thread id: " << epicsThreadGetNameSelf()<< std::endl;
                                                                        memcpy( _pdata->value, item.value, VALUE_SIZE );
                  	_pdata->time = time;
                  	_pdata->pflag = _proc;  // better be sure;
                  	callbackRequest( _pdata->pcallback );
                	}
              	}
            	break;
						}
						default:
						break;
          }
				break;
        case SET_ITEM:
					{
						std::cout << "SET_ITEM: "<< _pdata->object<< " (" << __FUNCTION__ << ") thread id: " << epicsThreadGetNameSelf() << std::endl;
          	myIsegHalThread->disable();
          	//std::cout << " Value to write: "<< _value.c_str() << " run from thread: " << epicsThreadGetNameSelf() << std::endl;

          	if( iseg_setItem( _pdata->interface, _pdata->object, _value.c_str() ) != ISEG_OK ) {
            	/*fprintf( stderr, "\033[31;1m%s Error while writing value '%s': '%s'\033[0m\n", _pdata->interface,_pdata->object, _value.c_str() );*/
            	_pdata->ioStatus = ISEG_ERROR;
          	}
          	_pdata->pflag = P_ASYNC; // Normal processing write always async
          	epicsTimeGetCurrent( &_pdata->time ); // get time after successful write to device
          	callbackRequest( _pdata->pcallback );
        		break;
					}

        case CLOSE_CONN:
					{
						std::cout << "CLOSE: (" << __FUNCTION__ << ") thread id: " << epicsThreadGetNameSelf() << std::endl;
/*std::cout << "Closing Connexion: (" << __FUNCTION__ << ") thread id: " << epicsThreadGetNameSelf() << std::endl;
          	std::vector< std::string >::iterator it = _sinterfaces.begin();
          	for( ; it != _sinterfaces.end(); ++it ) {
            	IsegResult status = iseg_disconnect( it->c_str() );
            	if ( ISEG_OK != status ) {
              	std::cerr << "\033[31;1m Cannot disconnect from isegHAL interface '"
                	<< (*it) << "'.\033[0m"
                	<< std::endl;
            	}
          	}*/
        		break;
					}
				default:
				break;
      }
	
    }
 }
	bool isegInitWorkers() {

		std::cout << "Initializating Message Queue Worker Thread (" << __FUNCTION__ << ") thread id: " << epicsThreadGetNameSelf() << std::endl;

		/* Create a new message queue for this port*/
		isegClientQueue = epicsMessageQueueCreate(RECV_Q_SIZE, sizeof(devIsegHal_queue_t));
		if (isegClientQueue == NULL) return false;
				/*  std::cout << "message queue created(" << __FUNCTION__ << ") called by thread id: " << epicsThreadGetNameSelf() << std::endl;*/

		if (epicsThreadCreate("isegACtrlTask", epicsThreadPriorityHigh, epicsThreadGetStackSize(epicsThreadStackMedium),
			(EPICSTHREADFUNC)isegMgtTask, NULL) == 0) return false;

		epicsThreadSleep(2);

		std::cout << "(" << __FUNCTION__ << ") function called by thread id: " << epicsThreadGetNameSelf() << std::endl;
		return true;
	}

//------------------------------------------------------------------------------
//! @brief       Initialization of device support
//! @param [in]  after  flag telling if function is called after or before
//!                       record initialization
//! @return      In case of error return -1, otherwise return 0
//------------------------------------------------------------------------------
long devIsegHalInit( int after ) {

		if ( 0 == after ) { // before records have been initialized

			/*std::cout << "Device Support Init: Record not initialized: (" << __FUNCTION__ << ")  thread: "
  							<< epicsThreadGetNameSelf() << std::endl;*/

      static bool firstRunBefore = true;
      if ( !firstRunBefore ) return 0;
    	firstRunBefore = false;
     	// create polling thread
 			myIsegHalThread = new isegHalThread();

		} else {

/*    	std::cout << "Device Support Init: Record initialized: (" << __FUNCTION__ << ") called by thread: "
 								<< epicsThreadGetNameSelf() << std::endl;*/

			std::string const& _name_ = isegHalConnectionHandler::instance().getName();
			isegHalConnectionHandler::instance().disconnect(_name_) ;

			 static bool firstRunAfter = true;
      if ( !firstRunAfter ) return 0;
      	firstRunAfter = false;
      	// Initialise workers
      	isegInitWorkers();
      	// start thread
      	myIsegHalThread->thread.start();
    }

  return OK;

}

//------------------------------------------------------------------------------
//! @brief       Common initialization of the record
//! @param [in]  prec       Address of the record calling this function
//! @param [in]  pconf      Address of record configuration
//! @return      In case of error return -1, otherwise return 0
//------------------------------------------------------------------------------
long devIsegHalInitRecord( dbCommon *prec, const devIsegHal_rec_t *pconf ) {

/*    std::cout << prec->name << " (" << __FUNCTION__ << ") init in thread:  "
              << epicsThreadGetNameSelf() << std::endl;*/

    devIsegHal_dset_t *pdset = (devIsegHal_dset_t *)prec->dset;
    long status = OK;

    if( INST_IO != pconf->ioLink->type ) {
      std::cerr << prec->name << ": Invalid link type for INP/OUT field: "
                << pamaplinkType[ pconf->ioLink->type ].strvalue
                << std::endl;
      return ERROR;
    }

    std::vector< std::string > options;
    std::istringstream ss( pconf->ioLink->value.instio.string );
    std::string option;
    while( std::getline( ss, option, ' ' ) ) options.push_back( option );

    if( options.size() != 2 ) {
      std::cerr << prec->name << ": Invalid INP/OUT field: " << ss.str() << "\n"
                << "    Syntax is \"@<isegItem> <Interface>\"" << std::endl;
      return ERROR;
    }

    // Test if interface is connected to isegHAL server
    if( !isegHalConnectionHandler::instance().connected( options.at(1) ) ) {
      std::cerr << "\033[31;1m" << "isegHal interface " << options.at(1) << " not connected!"
                << "\033[0m" << std::endl;
      return ERROR;
    }

    IsegItemProperty isegItem = iseg_getItemProperty( options.at(1).c_str(), options.at(0).c_str() );
    if( strcmp( isegItem.quality, ISEG_ITEM_QUALITY_OK ) != 0 ) {
      fprintf( stderr, "\033[31;1m%s: Error while reading item property '%s' (Q: %s)\033[0m\n",
              prec->name, options.at(0).c_str(), isegItem.quality );
      return ERROR;
    }

    // "Vorsicht ist die Mutter der Porzelankiste",
    // or "Better safe than sorry"
    for ( size_t i = 0; i < strlen( pconf->access ); ++i ) {
      if ( NULL == strchr( isegItem.access, pconf->access[i] ) ) {
        fprintf( stderr, "\033[31;1m%s: Access rights of item '%s' don't match: %s|%s!\033[0m\n",
                prec->name, isegItem.object, pconf->access, isegItem.access );
        return ERROR;
      }
    }
    if ( strncmp( isegItem.type, pconf->type, strlen( pconf->type ) ) != 0 ) {
      fprintf( stderr, "\033[31;1m%s: DataType '%s' of '%s' not supported by this record!\033[0m\n",
              prec->name, isegItem.type, isegItem.object );
      return ERROR;
    }

    devIsegHal_info_t *pinfo = new devIsegHal_info_t;
    memcpy( pinfo->object, isegItem.object, FULLY_QUALIFIED_OBJECT_SIZE );
    strncpy( pinfo->interface, options.at(1).c_str(), 20 );
    memcpy( pinfo->unit,   isegItem.unit,   UNIT_SIZE );
    pinfo->pcallback = NULL;  // just to be sure
    pinfo->ioStatus = ISEG_OK;

    /// Get initial value from HAL
    IsegItem item = iseg_getItem( pinfo->interface, pinfo->object );
    if( strcmp( item.quality, ISEG_ITEM_QUALITY_OK ) != 0 ) {
      fprintf( stderr, "\033[31;1m%s: Error while reading value '%s' from interface '%s': '%s' (Q: %s)\033[0m\n",
              prec->name, item.object, pinfo->interface, item.value, item.quality );
    }
    memcpy( pinfo->quality, item.quality,  QUALITY_SIZE ); //  init  rec quality info

    epicsUInt32 seconds = 0;
    epicsUInt32 microsecs = 0;
    if( sscanf( item.timeStampLastChanged, "%u.%u", &seconds, &microsecs ) != 2 ) {
      fprintf( stderr, "\033[31;1m%s: Error parsing timestamp for '%s': %s\033[0m\n", prec->name, pinfo->object, item.timeStampLastChanged );
    }

    memcpy( pinfo->rtime, item.timeStampLastChanged,  TIME_SIZE ); // needed by worker thread
    pinfo->time.secPastEpoch = seconds - POSIX_TIME_AT_EPICS_EPOCH;
    pinfo->time.nsec = microsecs * 100000;

    status = pdset->conv_val_str( prec, item.value );
    if( ERROR == status ) {
      fprintf( stderr, "\033[31;1m%s: Error parsing value for '%s': %s\033[0m\n", prec->name, pinfo->object, item.value );
    }
    if( -2 == prec->tse ) prec->time = pinfo->time;

  // update interface
    std::string _interface = options.at(1) + "_MOD";
    strncpy( pinfo->interface, _interface.c_str(), 20 );

    /// I/O Intr handling
    scanIoInit( &pinfo->ioscanpvt );
    // All Record will use Async Processing
	  regCallback( prec, pinfo );

    if( pconf->registerIOInterrupt )
          myIsegHalThread->registerInterrupt( prec, pinfo ); // register output recs and add to isegHal list

    prec->dpvt = pinfo;
    prec->udf  = (epicsUInt8)false;

    return OK;
}

//------------------------------------------------------------------------------
//! @brief       Initialization of the record for the broadcast on/off switch
//! @param [in]  prec       Address of the record calling this function
//! @param [in]  pconf      Address of record configuration
//! @return      In case of error return -1, otherwise return 0
//------------------------------------------------------------------------------
long devIsegHalGlobalSwitchInit( dbCommon *prec, const devIsegHal_rec_t *pconf ) {

  if( INST_IO != pconf->ioLink->type ) {
    std::cerr << prec->name << ": Invalid link type for INP/OUT field: "
              << pamaplinkType[ pconf->ioLink->type ].strvalue
              << std::endl;
    return ERROR;
  }

  std::vector< std::string > options;
  std::istringstream ss( pconf->ioLink->value.instio.string );
  std::string option;
  while( std::getline( ss, option, ' ' ) ) options.push_back( option );

  if( options.size() != 2 ) {
    std::cerr << prec->name << ": Invalid INP/OUT field: " << ss.str() << "\n"
              << "    Syntax is \"@<{OnOff|Emergency}> <Interface>\"" << std::endl;
    return ERROR;
  }
  
  bool emergency;
  if( "OnOff" == options[0] ) {
    emergency = false;
  } else if( "Emergency" == options[0] ) {
    emergency = true;
  } else {
    std::cerr << prec->name << ": Invalid INP/OUT field: " << ss.str() << "\n"
              << "    Syntax is \"@<{OnOff|Emergency}> <Interface>\"" << std::endl;
    return ERROR;
  }
 
  // Test if interface is connected to isegHAL server
  if( !isegHalConnectionHandler::instance().connected( options.at(1) ) ) {
    std::cerr << "\033[31;1m" << "isegHal interface " << options.at(1) << " not connected!"
              << "\033[0m" << std::endl;
    return ERROR;
  }

  devIsegHal_info_t *pinfo = new devIsegHal_info_t;
  memset( pinfo->object, 0, FULLY_QUALIFIED_OBJECT_SIZE );
  pinfo->object[0] = emergency ? 'E' : 'O'; // Abuse field for iseg item to store 'O' for normal on/off and 'E' for emergency off
  strncpy( pinfo->interface, options.at(1).c_str(), 20 );
  memset( pinfo->unit, 0, UNIT_SIZE );
  pinfo->pcallback = NULL;  // just to be sure

	// All record will use Async processing
	regCallback(prec, pinfo);
  if( pconf->registerIOInterrupt ) myIsegHalThread->registerInterrupt( prec, pinfo );

  prec->dpvt = pinfo;

  return OK;
}

//------------------------------------------------------------------------------
//! @brief       Get I/O Intr Information of record
//! @param [in]  cmd   0 if record is placed in, 1 if taken out of an I/O scan list 
//! @param [in]  prec  Address of record calling this funciton
//! @param [out] ppvt  Address of IOSCANPVT structure
//! @return      ERROR in case of an error, otherwise OK
//------------------------------------------------------------------------------
long devIsegHalGetIoIntInfo( int cmd, dbCommon *prec, IOSCANPVT *ppvt ) {
  devIsegHal_info_t *pinfo = (devIsegHal_info_t *)prec->dpvt;
  *ppvt = pinfo->ioscanpvt;
  if ( 0 == cmd ) {
//		std::cout << prec->name <<" Registering IO Intr Recs (" << __FUNCTION__ << ") in thread: "
//                << epicsThreadGetNameSelf() << std::endl;
    myIsegHalThread->registerInterrupt( prec, pinfo );
  } else {
    myIsegHalThread->cancelInterrupt( pinfo );
  }
  return OK;
}

//------------------------------------------------------------------------------
//! @brief       Common read function of the records
//! @param [in]  prec  Address of record calling this funciton
//! @return      ERROR in case of an error, otherwise OK
//------------------------------------------------------------------------------
long devIsegHalRead( dbCommon *prec ) {

    devIsegHal_info_t *pinfo = (devIsegHal_info_t *)prec->dpvt;
    devIsegHal_dset_t *pdset = (devIsegHal_dset_t *)prec->dset;
    long status = OK;

  if( !prec->pact ) 
	{
      // record "normally" processed
			pinfo->pflag = P_ASYNC;
      devIsegHal_queue_t qmsg = { pinfo, GET_ITEM, NULL };
      /*std::cout << prec->name <<":== Starting async read ==: (" << __FUNCTION__ << ") in thread: "
                << epicsThreadGetNameSelf()
                << std::endl;*/
      /* Send it to the servicing task */
      prec->pact = (epicsUInt8)true; // dont forget to set
      if (epicsMessageQueueTrySend(isegClientQueue, &qmsg, sizeof(devIsegHal_queue_t))){
        fprintf( stderr, "\033[31;1m%s: isegHal Mgt Queue Overflowed '%s': %s\033[0m\n", prec->name, pinfo->object, pinfo->value );
        recGblSetSevr( prec, READ_ALARM, INVALID_ALARM ); // Set record to READ_ALARM
        return ERROR;
      }

  } 
	else 
	{
    // record forced processed by CALLBACK: an epics callback will start processing from here
		/*std::cout << prec->name << " :== Completing async read ==:" << pinfo->value <<  " :(" << __FUNCTION__ << ") in thread id: "
                << epicsThreadGetNameSelf() << std::endl;*/
		if(pinfo->pflag == P_ASYNC) { // this flag must be set before calling back here
			// Deal with read Operation data: this done after worker has called back
			if( strcmp( pinfo->quality, ISEG_ITEM_QUALITY_OK ) != 0 ) {
			fprintf( stderr, "\033[31;1m%s: Error while reading value '%s' from interface '%s': '%s' (Q: %s)\033[0m\n",
					prec->name, pinfo->object, pinfo->interface, pinfo->value, pinfo->quality );
			recGblSetSevr( prec, READ_ALARM, INVALID_ALARM ); // Set record to READ_ALARM
			return ERROR;
			}		
			epicsUInt32 seconds = 0;
			epicsUInt32 microsecs = 0;
			if( sscanf( pinfo->rtime, "%u.%u", &seconds, &microsecs ) != 2 ) {
			  fprintf( stderr, "\033[31;1m%s: Error parsing timestamp for '%s': %s\033[0m\n", prec->name, pinfo->object, pinfo->rtime );
			  recGblSetSevr( prec, READ_ALARM, INVALID_ALARM ); // Set record to READ_ALARM
			  return ERROR;
			}
			pinfo->time.secPastEpoch = seconds - POSIX_TIME_AT_EPICS_EPOCH;
			pinfo->time.nsec = microsecs * 100000;
  #ifdef CHECK_LAST_REFRESHED
			epicsTimeStamp lastRefreshed;
			if( sscanf( pinfo->rtime, "%u.%u", &lastRefreshed.secPastEpoch, &lastRefreshed.nsec ) != 2 ) {
			  fprintf( stderr, "\033[31;1m%s: Error parsing timestamp for '%s': %s\033[0m\n", prec->name, pinfo->object, pinfo->rtime );
			  recGblSetSevr( prec, READ_ALARM, INVALID_ALARM ); // Set record to READ_ALARM
			  return ERROR;
			}
			lastRefreshed.secPastEpoch -= POSIX_TIME_AT_EPICS_EPOCH;
			lastRefreshed.nsec *= 100000;
			if( epicsTime::getCurrent() - epicsTime( lastRefreshed ) >= 30.0 ) {
			/// value is older then 30 seconds
			recGblSetSevr( prec, TIMEOUT_ALARM, INVALID_ALARM );
			return ERROR;
			}
  #endif			
		}
		// IO_INTR record with errors wont callback
		status = pdset->conv_val_str( prec, pinfo->value );
		prec->pact = (epicsUInt8)false;
		if( ERROR == status ) {
          fprintf( stderr, "\033[31;1m%s: Error parsing value for '%s': %s\033[0m\n", prec->name, pinfo->object, pinfo->value );
          recGblSetSevr( prec, READ_ALARM, INVALID_ALARM ); // Set record to READ_ALARM
          return ERROR;
		}
		if( -2 == prec->tse ) {
        // timestamp is set by device support
			prec->time = pinfo->time;
		}
		prec->udf = (epicsUInt8)false; /* We modify VAL so we are responsible for UDF too*/
    }
    return status;


}

//------------------------------------------------------------------------------
//! @brief       Common write function of the records
//! @param [in]  prec   Address of record calling this function
//! @return      ERROR in case of an error, otherwise OK
//------------------------------------------------------------------------------
long devIsegHalWrite( dbCommon *prec ) {
    devIsegHal_info_t *pinfo = (devIsegHal_info_t *)prec->dpvt;
    devIsegHal_dset_t *pdset = (devIsegHal_dset_t *)prec->dset;
    long status = 0;
    if( prec->pact ) {
      if( pinfo->ioStatus != ISEG_OK) { /* write successful ?*/
        fprintf( stderr, "\033[31;1m%s Error while writing value '%s'\033[0m\n",
          pinfo->interface, pinfo->object);
          recGblSetSevr( prec, WRITE_ALARM, INVALID_ALARM ); // Set record to WRITE_ALAR
        status = ERROR;
      } else {
          /* Only for I/O Intr processing */
          if(pinfo->pflag == P_IO_INTR) {
							std::cout << prec->name << " P_IO_INTR write (" << __FUNCTION__ << ") in thread id: "
              << epicsThreadGetNameSelf() << std::endl;
            status = pdset->conv_val_str( prec, pinfo->value );/*Non normal processing, new value receive from device*/
          }
          if( -2 == prec->tse ) prec->time = pinfo->time;
            prec->pact = (epicsUInt8)false;
            prec->udf = (epicsUInt8)false;
      }
      myIsegHalThread->enable();
    					std::cout << prec->name << " :== Async write complete == : (" << __FUNCTION__ << ") in thread id: "
              << epicsThreadGetNameSelf() << std::endl;
      return status;
    }
    myIsegHalThread->disable();
    /*std::cout << prec->name << " :== Starting write async operation == : (" << __FUNCTION__ << ") in thread id: "
              << epicsThreadGetNameSelf() << std::endl;*/
    char value[VALUE_SIZE];
    status = pdset->conv_val_str( prec, value );//call devIsegHalWrite_ao is made to get rec conv val.
    if( ERROR == status ) {
      fprintf( stderr, "\033[31;1m%s: Error parsing value for '%s'\033[0m\n", prec->name, pinfo->object );
      recGblSetSevr( prec, WRITE_ALARM, INVALID_ALARM ); // Set record to WRITE_ALARM
      return ERROR;
    }
    //memcpy( pinfo->value, value, VALUE_SIZE ); 
    prec->pact = (epicsUInt8)true; // signal that the  write operation will use callback
    pinfo->ioStatus = ISEG_OK;
    // record "normally" processed
    devIsegHal_queue_t qmsg;
		pinfo->pflag = P_ASYNC; // Normal processing;
    qmsg.pdata = pinfo;
    qmsg.reqType = SET_ITEM;
    strncpy( qmsg.value, value, VALUE_SIZE );

    /* Send write request to the servicing task */
    if (epicsMessageQueueTrySend(isegClientQueue, &qmsg,
        sizeof(devIsegHal_queue_t))){
      fprintf( stderr, "\033[31;1m%s: isegHal Mgt Queue Overflowed '%s': %s\033[0m\n", prec->name, pinfo->object, value );
      recGblSetSevr( prec, WRITE_ALARM, INVALID_ALARM ); // Set record to READ_ALARM
      return ERROR;
    }
    return status;
}

//------------------------------------------------------------------------------
//! @brief       Switch all channels on/off by broadcast message
//! @param [in]  prec   Address of record calling this function
//! @return      ERROR in case of an error, otherwise OK
//------------------------------------------------------------------------------
long devIsegHalGlobalSwitchWrite( dbCommon *prec ) {
  devIsegHal_info_t *pinfo = (devIsegHal_info_t *)prec->dpvt;
  devIsegHal_dset_t *pdset = (devIsegHal_dset_t *)prec->dset;

  myIsegHalThread->disable();

  char value[VALUE_SIZE];
  value[0] = pinfo->object[0];
  long status = pdset->conv_val_str( prec, value );
  if( ERROR == status ) {
    fprintf( stderr, "\033[31;1m%s: Invalid type parameter, cannot create broadcast command.\033[0m\n", prec->name );
    recGblSetSevr( prec, SOFT_ALARM, INVALID_ALARM ); // Set record to SOFT_ALARM 
    return ERROR;
  }

  if ( iseg_setItem( pinfo->interface, "Configuration", "1" ) != ISEG_OK ) {
    fprintf( stderr, "\033[31;1m%s: Error while stopping data collector for sending broadcast.\033[0m\n",
             prec->name );
    recGblSetSevr( prec, WRITE_ALARM, INVALID_ALARM ); // Set record to WRITE_ALARM 
    iseg_setItem( pinfo->interface, "Configuration", "0"); // Restore function
    myIsegHalThread->enable();
    return ERROR; 
  }
  if ( iseg_setItem( pinfo->interface, "Write", value ) != ISEG_OK ) {
    fprintf( stderr, "\033[31;1m%s: Error while sending broadcast command.\033[0m\n",
             prec->name );
    recGblSetSevr( prec, WRITE_ALARM, INVALID_ALARM ); // Set record to WRITE_ALARM 
    iseg_setItem( pinfo->interface, "Configuration", "0"); // Restore function
    myIsegHalThread->enable();
    return ERROR; 
  }
  if ( iseg_setItem( pinfo->interface, "Configuration", "0" ) != ISEG_OK ) {
    fprintf( stderr, "\033[31;1m%s: Error while starting data collector after sending broadcast.\033[0m\n",
             prec->name );
    recGblSetSevr( prec, WRITE_ALARM, INVALID_ALARM ); // Set record to WRITE_ALARM 
    myIsegHalThread->enable();
    return ERROR; 
  }

  if( -2 == prec->tse ) {
    epicsTimeGetCurrent( &pinfo->time );
    prec->time = pinfo->time;
  }

  myIsegHalThread->enable();
  return OK;
}

//------------------------------------------------------------------------------
//! @brief       C'tor of isegHalThread
//------------------------------------------------------------------------------
isegHalThread::isegHalThread()
  : thread( *this, "isegHAL", epicsThreadGetStackSize( epicsThreadStackSmall ), 50 ),
    _run( true ),
    _pause(5.),
    _debug(0)
{
	std::cout <<"Createding isegHAL thread:  "<< _run<<"(" << __FUNCTION__ << ") was called by thread id: " << epicsThreadGetNameSelf()<< std::endl;
  _recs.clear();
}

//------------------------------------------------------------------------------
//! @brief       D'tor of isegHalThread
//------------------------------------------------------------------------------
isegHalThread::~isegHalThread() {
	std::cout << "(" << __FUNCTION__ << ") Cleaning up: " << epicsThreadGetNameSelf() << std::endl;
epicsThreadSleep(4);
  _recs.clear();
}

//------------------------------------------------------------------------------
//! @brief       Run operation of thread
//!
//! Go through the list of registered records and check the cached value
//! from isegHAL.
//! If the cached value differs from the current value in the record,
//! the record will be updated.
//! After having checked all records, the thread sleeps for 5 seconds and
//! repeats the check.
//------------------------------------------------------------------------------
void isegHalThread::run() {

std::cout <<"isegHal Thread:  "<< _run<<"(" << __FUNCTION__ << ") was called by thread id: " << epicsThreadGetNameSelf()<< std::endl;
 while( true ) {
    std::list<devIsegHal_info_t*>::iterator it = _recs.begin();
    if( _pause > 0. ) this->thread.sleep( _pause ); 

    if( !_run ) continue;

    // some "benchmarking"
    struct timespec	start;
#ifdef _POSIX_CPUTIME
	clock_gettime( CLOCK_PROCESS_CPUTIME_ID, &start );
#else
	clock_gettime( CLOCK_MONOTONIX, &start );
#endif
//std::cout << _recs.front()->object << _recs.back()->object << " : "<<_run<<_pause<<_debug<< "(" << __FUNCTION__ << ") was called by thread id: " << epicsThreadGetNameSelf()<< std::endl;
      for( ; it != _recs.end(); ++it ) {

        if( 3 <= _debug )
          printf( "isegHalThread::run: Reading item '%s'\n", (*it)->object );

/*          std::cout << "Interrupt Record : " << (*it)->object    << " was called by thread id: "
                    << epicsThreadGetNameSelf()
                    << std::endl;*/
if ((*it)) {
				(*it)->pflag = P_IO_INTR; // to be sure.
        devIsegHal_queue_t qmsg = {(*it), GET_ITEM, NULL};
        // Send it to the servicing task 
        if (epicsMessageQueueTrySend(isegClientQueue, &qmsg,
            sizeof(devIsegHal_queue_t))){
            fprintf( stderr, "\033[31;1m%s: Warning: iseg Client Mgt queue overflow.\033[0m\n",
              (*it)->object);
        }
}
      }

    // some "benchmarking"

    if( 1 <= _debug ) {
      struct timespec	stop;
#ifdef _POSIX_CPUTIME
	  clock_gettime( CLOCK_PROCESS_CPUTIME_ID, &stop );
#else
	  clock_gettime( CLOCK_MONOTONIX, &stop );
#endif
      printf( "isegHalThread::run: needed %lf seconds for %lu records\n",
               timespec_diff( &stop, &start ), (unsigned long)_recs.size() );
		}
  }
}

//------------------------------------------------------------------------------
//! @brief       Add a record to the list
//! @param [in]  prec  Address of the record to be added
//!
//! Registers a new record to be checked by the thread
//------------------------------------------------------------------------------
void isegHalThread::registerInterrupt( dbCommon* prec,  devIsegHal_info_t *pinfo ) {

  if( 1 <= _debug )
    printf( "isegHalThread: Register new record '%s'\n", prec->name );
          /*std::cout << "Registered Record : " << pinfo->object    << " thread id: "
                    << epicsThreadGetNameSelf()
                    << std::endl;*/
  _recs.push_back( pinfo );
  // to be sure that each record is only added once
  _recs.sort();
  _recs.unique();
}

//------------------------------------------------------------------------------
//! @brief       Remove a record to the list
//! @param [in]  pinfo  Address of the record's private data structure
//!
//! Removes a record from the list which is checked by the thread for updates
//------------------------------------------------------------------------------
void isegHalThread::cancelInterrupt( const devIsegHal_info_t* pinfo ) {
  std::list<devIsegHal_info_t*>::iterator it = _recs.begin();
  for( ; it != _recs.end(); ++it ) {
    if( pinfo == (*it) ) {
      _recs.erase( it );
      break;
    }
  } 
}

// Configuration routines.  Called from the iocsh function below 
extern "C" {

  static const iocshArg isegConnectArg0 = { "port", iocshArgString };
  static const iocshArg isegConnectArg1 = { "tty",  iocshArgString };
  static const iocshArg * const isegConnectArgs[] = { &isegConnectArg0, &isegConnectArg1 };
  static const iocshFuncDef isegConnectFuncDef = { "isegHalConnect", 2, isegConnectArgs };

  //----------------------------------------------------------------------------
  //! @brief       iocsh callable function to connect to isegHalServer
  //!
  //! This function can be called from the iocsh via "isegHalConnect( PORT, TTY )"
  //! PORT is the deviseg internal name of the interface which is also used inside
  //! the records.
  //! TTY is the name of the hardware interface (e.g. "can0").
  //----------------------------------------------------------------------------
  static void isegConnectCallFunc( const iocshArgBuf *args ) {
    std::cout << "using HAL version [" << iseg_getVersionString() << "]" << std::endl;
    if( !isegHalConnectionHandler::instance().connect( args[0].sval, args[1].sval ) ){
      fprintf( stderr, "\033[31;1mCannot connect to isegHAL interface %s(%s)\033[0m\n", args[0].sval, args[1].sval );
    }
  }

  // iocsh callable function to set options for polling thread
  static const iocshArg setOptArg0 = { "port", iocshArgString };
  static const iocshArg setOptArg1 = { "key", iocshArgString };
  static const iocshArg setOptArg2 = { "value",  iocshArgString };
  static const iocshArg * const setOptArgs[] = { &setOptArg0, &setOptArg1, &setOptArg2 };
  static const iocshFuncDef setOptFuncDef = { "devIsegHalSetOpt", 3, setOptArgs };

  //----------------------------------------------------------------------------
  //! @brief       iocsh callable function to set options for the polling thread
  //!
  //! This function can be called from the iocsh via "devIsegHalSetOpt( PORT, KEY, VALUE )"
  //! PORT is the deviseg internal name of the interface connected to the isegHalServer,
  //! KEY is the name of the option and VALUE the new value for the option.
  //!
  //! Possible KEYs are:
  //! Intervall  -  set the wait time after going through the list of records with the polling thread
  //! LogLevel   -  Change loglevel of isegHalServer
  //! debug      -  Enable debug output of polling thread
  //----------------------------------------------------------------------------
  static void setOptCallFunc( const iocshArgBuf *args ) {
    // Set new intervall for polling thread
    if( strcmp( args[1].sval, "Intervall" ) == 0 ) {
      double newIntervall = 0.;
      int n = sscanf( args[2].sval, "%lf", &newIntervall );
      if( 1 != n ) {
        fprintf( stderr, "\033[31;1mInvalid value for key '%s': %s\033[0m\n", args[1].sval, args[2].sval );
        return;
      }
      myIsegHalThread->changeIntervall( newIntervall );
    }

    // change log level from isegHAL server
    if( strcmp( args[1].sval, "LogLevel" ) == 0 ) {
      if( iseg_setItem( args[0].sval, "LogLevel", args[2].sval ) != ISEG_OK ) {
        fprintf( stderr, "\033[31;1mCould not change LogLevel to '%s'\033[0m\n", args[2].sval );
        return;
      }
    }

    // Set new debug level
    if( strcmp( args[1].sval, "debug" ) == 0 ) {
      unsigned newDbgLvl = 0;
      int n = sscanf( args[2].sval, "%u", &newDbgLvl );
      if( 1 != n ) {
        fprintf( stderr, "\033[31;1mInvalid value for key '%s': %s\033[0m\n", args[1].sval, args[2].sval );
        return;
      }
      myIsegHalThread->setDbgLvl( newDbgLvl );
    }

  }

  //----------------------------------------------------------------------------
  //! @brief       Register functions to EPICS
  //----------------------------------------------------------------------------
  void devIsegHalRegister( void ) {
    static bool firstTime = true;
    if ( firstTime ) {
      iocshRegister( &setOptFuncDef, setOptCallFunc );
      iocshRegister( &isegConnectFuncDef, isegConnectCallFunc );
      firstTime = false;
    }
  }
  
  epicsExportRegistrar( devIsegHalRegister );
}


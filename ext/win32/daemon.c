#include "ruby.h"
#include "ruby/encoding.h"
#include <windows.h>
#include <string.h>
#include <stdlib.h>
#include <tchar.h>

#ifndef UNICODE
#define UNICODE
#endif

#define WIN32_SERVICE_VERSION "0.8.5.1"

static HANDLE hStartEvent;
static HANDLE hStopEvent;
static HANDLE hStopCompletedEvent;
static SERVICE_STATUS_HANDLE   ssh;
static DWORD dwServiceState;

static VALUE EventHookHash = Qnil;

CRITICAL_SECTION csControlCode;
// I happen to know from looking in the header file
// that 0 is not a valid service control code
// so we will use it, the value does not matter
// as long as it will never show up in ServiceCtrl
// - Patrick Hurley
#define IDLE_CONTROL_CODE 0
static int   waiting_control_code = IDLE_CONTROL_CODE;

static VALUE service_close(VALUE);
void  WINAPI  Service_Main(DWORD dwArgc, LPTSTR *lpszArgv);
void  WINAPI  Service_Ctrl(DWORD dwCtrlCode);
void  ErrorStopService();
void  SetTheServiceStatus(DWORD dwCurrentState,DWORD dwWin32ExitCode,
                          DWORD dwCheckPoint,  DWORD dwWaitHint);

// Return an error code as a string
LPTSTR ErrorDescription(DWORD p_dwError)
{
   HLOCAL hLocal = NULL;
   static TCHAR ErrStr[1024];
   int len;

   if (!(len=FormatMessage(
      FORMAT_MESSAGE_ALLOCATE_BUFFER |
      FORMAT_MESSAGE_FROM_SYSTEM |
      FORMAT_MESSAGE_IGNORE_INSERTS,
      NULL,
      p_dwError,
      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // Default language
      (LPTSTR)&hLocal,
      0,
      NULL)))
   {
      rb_raise(rb_eStandardError, "unable to format error message");
   }
   memset(ErrStr, 0, sizeof(ErrStr));
   strncpy(ErrStr, (LPTSTR)hLocal, len-2); // remove \r\n
   LocalFree(hLocal);
   return ErrStr;
}

static VALUE rb_system_call_raise(DWORD dwError) {
	VALUE mesg, exc;
	mesg = rb_str_new2(ErrorDescription(dwError));
	rb_enc_associate(mesg, rb_default_internal_encoding()); 
	exc = rb_exc_new3(rb_eSystemCallError, mesg);
	rb_iv_set(exc, "errno", INT2FIX(dwError));
	rb_exc_raise(exc);
}

// Called by the service control manager after the call to
// StartServiceCtrlDispatcher.
void WINAPI Service_Main(DWORD dwArgc, LPTSTR *lpszArgv)
{

   int i=0;


   // Obtain the name of the service.
   LPTSTR lpszServiceName = lpszArgv[0];

   // Register the service ctrl handler.
   ssh = RegisterServiceCtrlHandler(lpszServiceName,
           (LPHANDLER_FUNCTION)Service_Ctrl);

   if(ssh == (SERVICE_STATUS_HANDLE)0){
	  DWORD dwError = GetLastError();
      ErrorStopService();
	  rb_system_call_raise(dwError);
   }

   // wait for sevice initialization
   for(i=1;TRUE;i++)
   {
    if(WaitForSingleObject(hStartEvent, 1000) == WAIT_OBJECT_0)
        break;

       SetTheServiceStatus(SERVICE_START_PENDING, 0, i, 1000);
   }

   // The service has started.
   SetTheServiceStatus(SERVICE_RUNNING, NO_ERROR, 0, 0);

   // Main loop for the service.
   while(WaitForSingleObject(hStopEvent, 1000) != WAIT_OBJECT_0)
   {
   }

   // Stop the service.
   SetTheServiceStatus(SERVICE_STOPPED, NO_ERROR, 0, 0);
}

VALUE Service_Event_Dispatch(VALUE val)
{
   VALUE func,self;
   VALUE result = Qnil;

   if(val!=Qnil) {
      self = RARRAY_PTR(val)[0];
      func = NUM2INT(RARRAY_PTR(val)[1]);

      result = rb_funcall(self,func,0);
   }

   return result;
}

VALUE Ruby_Service_Ctrl()
{
   while (WaitForSingleObject(hStopEvent,0) == WAIT_TIMEOUT)
	{
      __try
      {
         EnterCriticalSection(&csControlCode);

         // Check to see if anything interesting has been signaled
         if (waiting_control_code != IDLE_CONTROL_CODE)
         {
            // if there is a code, create a ruby thread to deal with it
            // this might be over engineering the solution, but I don't
            // want to block Service_Ctrl longer than necessary and the
            // critical section will block it.
 			if( EventHookHash!=Qnil ) {
				VALUE val = rb_hash_aref(EventHookHash, INT2NUM(waiting_control_code));
				if(val!=Qnil) {
					VALUE thread = rb_thread_create(Service_Event_Dispatch, (void*) val);
				}
			}
            // some seriously ugly flow control going on in here
            if (waiting_control_code == SERVICE_CONTROL_STOP)
               break;

            waiting_control_code = IDLE_CONTROL_CODE;
         }
      }
      __finally
      {
         LeaveCriticalSection(&csControlCode);
      }

      // This is an ugly polling loop, be as polite as possible
	   rb_thread_polling();
	}

   SetEvent(hStopCompletedEvent);

   return Qnil;
}

// Handles control signals from the service control manager.
void WINAPI Service_Ctrl(DWORD dwCtrlCode)
{
   DWORD dwState = SERVICE_RUNNING;

   // hard to image this code ever failing, so we probably
   // don't need the __try/__finally wrapper
   __try
   {
      EnterCriticalSection(&csControlCode);
      waiting_control_code = dwCtrlCode;
   }
   __finally
   {
      LeaveCriticalSection(&csControlCode);
   }

   switch(dwCtrlCode)
   {
      case SERVICE_CONTROL_STOP:
         dwState = SERVICE_STOP_PENDING;
         break;

      case SERVICE_CONTROL_SHUTDOWN:
         dwState = SERVICE_STOP_PENDING;
         break;

      case SERVICE_CONTROL_PAUSE:
         dwState = SERVICE_PAUSED;
        break;

      case SERVICE_CONTROL_CONTINUE:
         dwState = SERVICE_RUNNING;
        break;

      case SERVICE_CONTROL_INTERROGATE:
         break;

      default:
         break;
   }

   // Set the status of the service.
   SetTheServiceStatus(dwState, NO_ERROR, 0, 0);

   // Tell service_main thread to stop.
   if ((dwCtrlCode == SERVICE_CONTROL_STOP) ||
       (dwCtrlCode == SERVICE_CONTROL_SHUTDOWN))
   {
      // how long should we give ruby to clean up?
      // right now we give it forever :-)
      while (WaitForSingleObject(hStopCompletedEvent, 500) == WAIT_TIMEOUT)
      {
         SetTheServiceStatus(dwState, NO_ERROR, 0, 0);
      }

      if (!SetEvent(hStopEvent))
         ErrorStopService();
         // Raise an error here?
   }
}

//  Wraps SetServiceStatus.
void SetTheServiceStatus(DWORD dwCurrentState, DWORD dwWin32ExitCode,
                         DWORD dwCheckPoint,   DWORD dwWaitHint)
{
   SERVICE_STATUS ss;  // Current status of the service.

   // Disable control requests until the service is started.
   if (dwCurrentState == SERVICE_START_PENDING){
      ss.dwControlsAccepted = 0;
   }
   else{
      ss.dwControlsAccepted =
         SERVICE_ACCEPT_STOP|SERVICE_ACCEPT_SHUTDOWN|
         SERVICE_ACCEPT_PAUSE_CONTINUE|SERVICE_ACCEPT_SHUTDOWN;
   }

   // Initialize ss structure.
   ss.dwServiceType             = SERVICE_WIN32_OWN_PROCESS;
   ss.dwServiceSpecificExitCode = 0;
   ss.dwCurrentState            = dwCurrentState;
   ss.dwWin32ExitCode           = dwWin32ExitCode;
   ss.dwCheckPoint              = dwCheckPoint;
   ss.dwWaitHint                = dwWaitHint;

   dwServiceState = dwCurrentState;

   // Send status of the service to the Service Controller.
   if(!SetServiceStatus(ssh, &ss)){
      ErrorStopService();
   }
}

//  Handle API errors or other problems by ending the service
void ErrorStopService(){

   // If you have threads running, tell them to stop. Something went
   // wrong, and you need to stop them so you can inform the SCM.
   SetEvent(hStopEvent);

   // Stop the service.
   SetTheServiceStatus(SERVICE_STOPPED, GetLastError(), 0, 0);
}

DWORD WINAPI ThreadProc(LPVOID lpParameter){
    SERVICE_TABLE_ENTRY ste[] =
      {{TEXT(""),(LPSERVICE_MAIN_FUNCTION)Service_Main}, {NULL, NULL}};

    if (!StartServiceCtrlDispatcher(ste)){
       ErrorStopService();
       // Very questionable here, we should generate an event
       // and be polling in a green thread for the event, but
       // this really should not happen so here we go
       rb_system_call_raise(GetLastError());
    }

    return 0;
}

static VALUE daemon_allocate(VALUE klass){
   EventHookHash = rb_hash_new();
   rb_global_variable(&EventHookHash);

   return Data_Wrap_Struct(klass, 0, 0, 0);
}

/*
 * This is the method that actually puts your code into a loop and allows it
 * to run as a service.  The code that is actually run while in the mainloop
 * is what you defined in your own Daemon#service_main method.
 */
static VALUE
daemon_mainloop(VALUE self)
{
    DWORD ThreadId;
    HANDLE hThread;

    dwServiceState = 0;

    // Event hooks
    if(rb_respond_to(self,rb_intern("service_stop"))){
       rb_hash_aset(EventHookHash,INT2NUM(SERVICE_CONTROL_STOP),
          rb_ary_new3(2,self,ULL2NUM(rb_intern("service_stop"))));
    }

    if(rb_respond_to(self,rb_intern("service_pause"))){
       rb_hash_aset(EventHookHash,INT2NUM(SERVICE_CONTROL_PAUSE),
          rb_ary_new3(2,self,ULL2NUM(rb_intern("service_pause"))));
    }

    if(rb_respond_to(self,rb_intern("service_resume"))){
       rb_hash_aset(EventHookHash,INT2NUM(SERVICE_CONTROL_CONTINUE),
          rb_ary_new3(2,self,ULL2NUM(rb_intern("service_resume"))));
    }

    if(rb_respond_to(self,rb_intern("service_interrogate"))){
       rb_hash_aset(EventHookHash,INT2NUM(SERVICE_CONTROL_INTERROGATE),
          rb_ary_new3(2,self,ULL2NUM(rb_intern("service_interrogate"))));
    }

    if(rb_respond_to(self,rb_intern("service_shutdown"))){
       rb_hash_aset(EventHookHash,INT2NUM(SERVICE_CONTROL_SHUTDOWN),
          rb_ary_new3(2,self,ULL2NUM(rb_intern("service_shutdown"))));
    }

#ifdef SERVICE_CONTROL_PARAMCHANGE
    if(rb_respond_to(self,rb_intern("service_paramchange"))){
       rb_hash_aset(EventHookHash,INT2NUM(SERVICE_CONTROL_PARAMCHANGE),
          rb_ary_new3(2,self,ULL2NUM(rb_intern("service_paramchange"))));
    }
#endif

#ifdef SERVICE_CONTROL_NETBINDADD
    if(rb_respond_to(self,rb_intern("service_netbindadd"))){
       rb_hash_aset(EventHookHash,INT2NUM(SERVICE_CONTROL_NETBINDADD),
          rb_ary_new3(2,self,ULL2NUM(rb_intern("service_netbindadd"))));
    }
#endif

#ifdef SERVICE_CONTROL_NETBINDREMOVE
    if(rb_respond_to(self,rb_intern("service_netbindremove"))){
       rb_hash_aset(EventHookHash,INT2NUM(SERVICE_CONTROL_NETBINDREMOVE),
          rb_ary_new3(2,self,ULL2NUM(rb_intern("service_netbindremove"))));
    }
#endif

#ifdef SERVICE_CONTROL_NETBINDENABLE
    if(rb_respond_to(self,rb_intern("service_netbindenable"))){
       rb_hash_aset(EventHookHash,INT2NUM(SERVICE_CONTROL_NETBINDENABLE),
          rb_ary_new3(2,self,ULL2NUM(rb_intern("service_netbindenable"))));
    }
#endif

#ifdef SERVICE_CONTROL_NETBINDDISABLE
    if(rb_respond_to(self,rb_intern("service_netbinddisable"))){
       rb_hash_aset(EventHookHash,INT2NUM(SERVICE_CONTROL_NETBINDDISABLE),
          rb_ary_new3(2,self,ULL2NUM(rb_intern("service_netbinddisable"))));
    }
#endif

    // Create the event to signal the service to start.
    hStartEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
    if(hStartEvent == NULL){
       DWORD dwError = GetLastError();
       ErrorStopService();
       rb_system_call_raise(dwError);
    }

    // Create the event to signal the service to stop.
    hStopEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
    if(hStopEvent == NULL){
       DWORD dwError = GetLastError();
       ErrorStopService();
       rb_system_call_raise(dwError);
    }

    // Create the event to signal the service that stop has completed
    hStopCompletedEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
    if(hStopCompletedEvent == NULL){
       DWORD dwError = GetLastError();
       ErrorStopService();
       rb_system_call_raise(dwError);
    }

    // Create the green thread to poll for Service_Ctrl events
    rb_thread_create(Ruby_Service_Ctrl, 0);

    // Create Thread for service main
    hThread = CreateThread(NULL,0,ThreadProc,0,0,&ThreadId);
    if(hThread == INVALID_HANDLE_VALUE){
       DWORD dwError = GetLastError();
       ErrorStopService();
       rb_system_call_raise(dwError);
    }

    if(rb_respond_to(self,rb_intern("service_init"))){
       rb_funcall(self,rb_intern("service_init"),0);
    }

 SetEvent(hStartEvent);

    // Call service_main method
    if(rb_respond_to(self,rb_intern("service_main"))){
       rb_funcall(self,rb_intern("service_main"),0);
    }

    while(WaitForSingleObject(hStopEvent, 1000) != WAIT_OBJECT_0)
    {
    }

    // Close the event handle and the thread handle.
    if(!CloseHandle(hStopEvent)){
       DWORD dwError = GetLastError();
       ErrorStopService();
       rb_system_call_raise(dwError);
    }

    // Wait for Thread service main
    WaitForSingleObject(hThread, INFINITE);

    return self;
}

/*
 * Returns the state of the service (as an constant integer) which can be any
 * of the service status constants, e.g. RUNNING, PAUSED, etc.
 * 
 * This method is typically used within your service_main method to setup the
 * loop.  For example:
 * 
 * class MyDaemon < Daemon
 *    def service_main
 * 		while state == RUNNING || state == PAUSED || state == IDLE
 * 			# Your main loop here
 * 		end
 * 	end
 * end
 * 
 * See the Daemon#running? method for an abstraction of the above code.
 */
static VALUE daemon_state(VALUE self){
   return UINT2NUM(dwServiceState);
}

/*
 * Returns whether or not the service is in a running state, i.e. the service
 * status is either RUNNING, PAUSED or IDLE.
 * 
 * This is typically used within your service_main method to setup the main
 * loop.  For example:
 * 
 * class MyDaemon < Daemon
 *    def service_main
 *       while running?
 *          # Your main loop here
 *       end
 *    end
 * end
 */
static VALUE daemon_is_running(VALUE self){
   VALUE v_bool = Qfalse;
   if(
      (dwServiceState == SERVICE_RUNNING) ||
      (dwServiceState == SERVICE_PAUSED) ||
      (dwServiceState == 0)
   ){
      v_bool = Qtrue;
   }
	
   return v_bool;	
}

void Init_daemon()
{
   VALUE mWin32, cDaemon;
   int i = 0;

   // Modules and classes
   mWin32   = rb_define_module("Win32");
   cDaemon  = rb_define_class_under(mWin32, "Daemon", rb_cObject);

   // Daemon class and instance methods
   rb_define_alloc_func(cDaemon, daemon_allocate);
   rb_define_method(cDaemon, "mainloop", daemon_mainloop, 0);
   rb_define_method(cDaemon, "state", daemon_state, 0);
   rb_define_method(cDaemon, "running?", daemon_is_running, 0);

   // Intialize critical section used by green polling thread
   InitializeCriticalSection(&csControlCode);

   // Constants
   rb_define_const(cDaemon, "VERSION", rb_str_new2(WIN32_SERVICE_VERSION));
   rb_define_const(cDaemon, "CONTINUE_PENDING", INT2NUM(SERVICE_CONTINUE_PENDING));
   rb_define_const(cDaemon, "PAUSE_PENDING", INT2NUM(SERVICE_PAUSE_PENDING));
   rb_define_const(cDaemon, "PAUSED", INT2NUM(SERVICE_PAUSED));
   rb_define_const(cDaemon, "RUNNING", INT2NUM(SERVICE_RUNNING));
   rb_define_const(cDaemon, "START_PENDING", INT2NUM(SERVICE_START_PENDING));
   rb_define_const(cDaemon, "STOP_PENDING", INT2NUM(SERVICE_STOP_PENDING));
   rb_define_const(cDaemon, "STOPPED", INT2NUM(SERVICE_STOPPED));
   rb_define_const(cDaemon, "IDLE", INT2NUM(0));  
}
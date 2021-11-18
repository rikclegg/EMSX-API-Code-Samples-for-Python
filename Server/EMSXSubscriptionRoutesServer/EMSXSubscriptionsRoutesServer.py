# EMSXSubscriptionRoutesServer.py

import sys
import blpapi
import datetime

ORDER_ROUTE_FIELDS              = blpapi.Name("OrderRouteFields")

SLOW_CONSUMER_WARNING           = blpapi.Name("SlowConsumerWarning")
SLOW_CONSUMER_WARNING_CLEARED   = blpapi.Name("SlowConsumerWarningCleared")

SESSION_STARTED                 = blpapi.Name("SessionStarted")
SESSION_TERMINATED              = blpapi.Name("SessionTerminated")
SESSION_STARTUP_FAILURE         = blpapi.Name("SessionStartupFailure")
SESSION_CONNECTION_UP           = blpapi.Name("SessionConnectionUp")
SESSION_CONNECTION_DOWN         = blpapi.Name("SessionConnectionDown")

SERVICE_OPENED                  = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE            = blpapi.Name("ServiceOpenFailure")

SUBSCRIPTION_FAILURE            = blpapi.Name("SubscriptionFailure")
SUBSCRIPTION_STARTED            = blpapi.Name("SubscriptionStarted")
SUBSCRIPTION_TERMINATED         = blpapi.Name("SubscriptionTerminated")
SUBSCRIPTION_ACTIVATED          = blpapi.Name("SubscriptionStreamsActivated")

AUTHORIZATION_SUCCESS           = blpapi.Name("AuthorizationSuccess")
AUTHORIZATION_FAILURE           = blpapi.Name("AuthorizationFailure")

SEATTYPE_INVALID = -1
SEATTYPE_BPS = 0
SEATTYPE_NONBPS = 1

d_emsx = "//blp/emapisvc_beta"
d_auth = "//blp/apiauth"
d_host = "10.137.22.38"
d_port = 8294
d_user = "EMSXAPI\\SERVER_API"
d_ip = "10.137.22.38"

routeSubscriptionID=blpapi.CorrelationId(99)

bEnd=False

class SessionEventHandler():
    
    def sendAuthRequest(self,session):
                
        authService = session.getService(d_auth)
        authReq = authService.createAuthorizationRequest()
        authReq.set("emrsId",d_user)
        authReq.set("ipAddress", d_ip)
        self.identity = session.createIdentity()
        
        print ("Sending authorization request: %s" % (authReq))
        
        session.sendAuthorizationRequest(authReq, self.identity)
        
        print ("Authorization request sent.")

    
    def createRouteSubscription(self, session):
        
        print ("Create Route subscription")

        routeTopic = d_emsx + "/route;team=RJCSERVER1?fields="
        #routeTopic = d_emsx + "/route?fields="
        routeTopic = routeTopic + "API_SEQ_NUM,"
        routeTopic = routeTopic + "EMSX_AMOUNT,"
        routeTopic = routeTopic + "EMSX_APA_MIC,"
        routeTopic = routeTopic + "EMSX_AVG_PRICE,"
        routeTopic = routeTopic + "EMSX_BROKER,"
        routeTopic = routeTopic + "EMSX_BROKER_COMM,"
        routeTopic = routeTopic + "EMSX_BROKER_LEI,"
        routeTopic = routeTopic + "EMSX_BROKER_SI,"
        routeTopic = routeTopic + "EMSX_BSE_AVG_PRICE,"
        routeTopic = routeTopic + "EMSX_BSE_FILLED,"
        routeTopic = routeTopic + "EMSX_BROKER_STATUS,"
        routeTopic = routeTopic + "EMSX_BUYSIDE_LEI,"
        routeTopic = routeTopic + "EMSX_CLEARING_ACCOUNT,"
        routeTopic = routeTopic + "EMSX_CLEARING_FIRM,"
        routeTopic = routeTopic + "EMSX_CLIENT_IDENTIFICATION,"
        routeTopic = routeTopic + "EMSX_COMM_DIFF_FLAG,"
        routeTopic = routeTopic + "EMSX_COMM_RATE,"
        routeTopic = routeTopic + "EMSX_CURRENCY_PAIR,"
        routeTopic = routeTopic + "EMSX_CUSTOM_ACCOUNT,"
        routeTopic = routeTopic + "EMSX_DAY_AVG_PRICE,"
        routeTopic = routeTopic + "EMSX_DAY_FILL,"
        routeTopic = routeTopic + "EMSX_EXCHANGE_DESTINATION,"
        routeTopic = routeTopic + "EMSX_EXEC_INSTRUCTION,"
        routeTopic = routeTopic + "EMSX_EXECUTE_BROKER,"
        routeTopic = routeTopic + "EMSX_FILL_ID,"
        routeTopic = routeTopic + "EMSX_FILLED,"
        routeTopic = routeTopic + "EMSX_GPI,"
        routeTopic = routeTopic + "EMSX_GTD_DATE,"
        routeTopic = routeTopic + "EMSX_HAND_INSTRUCTION,"
        routeTopic = routeTopic + "EMSX_IS_MANUAL_ROUTE,"
        routeTopic = routeTopic + "EMSX_LAST_CAPACITY,"
        routeTopic = routeTopic + "EMSX_LAST_FILL_DATE,"
        routeTopic = routeTopic + "EMSX_LAST_FILL_TIME,"
        routeTopic = routeTopic + "EMSX_LAST_FILL_TIME_MICROSEC,"
        routeTopic = routeTopic + "EMSX_LAST_MARKET,"
        routeTopic = routeTopic + "EMSX_LAST_PRICE,"
        routeTopic = routeTopic + "EMSX_LAST_SHARES,"
        routeTopic = routeTopic + "EMSX_LEG_FILL_DATE_ADDED,"
        routeTopic = routeTopic + "EMSX_LEG_FILL_PRICE,"
        routeTopic = routeTopic + "EMSX_LEG_FILL_SEQ_NO,"
        routeTopic = routeTopic + "EMSX_LEG_FILL_SHARES,"
        routeTopic = routeTopic + "EMSX_LEG_FILL_SIDE,"
        routeTopic = routeTopic + "EMSX_LEG_FILL_TICKER,"
        routeTopic = routeTopic + "EMSX_LEG_FILL_TIME_ADDED,"
        routeTopic = routeTopic + "EMSX_LIMIT_PRICE,"
        routeTopic = routeTopic + "EMSX_MIFID_II_INSTRUCTION,"
        routeTopic = routeTopic + "EMSX_MISC_FEES,"
        routeTopic = routeTopic + "EMSX_ML_ID,"
        routeTopic = routeTopic + "EMSX_ML_LEG_QUANTITY,"
        routeTopic = routeTopic + "EMSX_ML_NUM_LEGS,"
        routeTopic = routeTopic + "EMSX_ML_PERCENT_FILLED,"
        routeTopic = routeTopic + "EMSX_ML_RATIO,"
        routeTopic = routeTopic + "EMSX_ML_REMAIN_BALANCE,"
        routeTopic = routeTopic + "EMSX_ML_STRATEGY,"
        routeTopic = routeTopic + "EMSX_ML_TOTAL_QUANTITY,"
        routeTopic = routeTopic + "EMSX_NOTES,"
        routeTopic = routeTopic + "EMSX_NSE_AVG_PRICE,"
        routeTopic = routeTopic + "EMSX_NSE_FILLED,"
        routeTopic = routeTopic + "EMSX_ORDER_TYPE,"
        routeTopic = routeTopic + "EMSX_OTC_FLAG,"
        routeTopic = routeTopic + "EMSX_P_A,"
        routeTopic = routeTopic + "EMSX_PERCENT_REMAIN,"
        routeTopic = routeTopic + "EMSX_PRINCIPAL,"
        routeTopic = routeTopic + "EMSX_QUEUED_DATE,"
        routeTopic = routeTopic + "EMSX_QUEUED_TIME,"
        routeTopic = routeTopic + "EMSX_QUEUED_TIME_MICROSEC,"
        routeTopic = routeTopic + "EMSX_REASON_CODE,"
        routeTopic = routeTopic + "EMSX_REASON_DESC,"
        routeTopic = routeTopic + "EMSX_REMAIN_BALANCE,"
        routeTopic = routeTopic + "EMSX_ROUTE_AS_OF_DATE,"
        routeTopic = routeTopic + "EMSX_ROUTE_AS_OF_TIME_MICROSEC,"
        routeTopic = routeTopic + "EMSX_ROUTE_CREATE_DATE,"
        routeTopic = routeTopic + "EMSX_ROUTE_CREATE_TIME,"
        routeTopic = routeTopic + "EMSX_ROUTE_CREATE_TIME_MICROSEC,"
        routeTopic = routeTopic + "EMSX_ROUTE_ID,"
        routeTopic = routeTopic + "EMSX_ROUTE_LAST_UPDATE_TIME,"
        routeTopic = routeTopic + "EMSX_ROUTE_LAST_UPDATE_TIME_MICROSEC,"
        routeTopic = routeTopic + "EMSX_ROUTE_PRICE,"
        routeTopic = routeTopic + "EMSX_ROUTE_REF_ID,"
        routeTopic = routeTopic + "EMSX_SEQUENCE,"
        routeTopic = routeTopic + "EMSX_SETTLE_AMOUNT,"
        routeTopic = routeTopic + "EMSX_SETTLE_DATE,"
        routeTopic = routeTopic + "EMSX_STATUS,"
        routeTopic = routeTopic + "EMSX_STOP_PRICE,"
        routeTopic = routeTopic + "EMSX_STRATEGY_END_TIME,"
        routeTopic = routeTopic + "EMSX_STRATEGY_PART_RATE1,"
        routeTopic = routeTopic + "EMSX_STRATEGY_PART_RATE2,"
        routeTopic = routeTopic + "EMSX_STRATEGY_START_TIME,"
        routeTopic = routeTopic + "EMSX_STRATEGY_STYLE,"
        routeTopic = routeTopic + "EMSX_STRATEGY_TYPE,"
        routeTopic = routeTopic + "EMSX_TIF,"
        routeTopic = routeTopic + "EMSX_TIME_STAMP,"
        routeTopic = routeTopic + "EMSX_TIME_STAMP_MICROSEC,"
        routeTopic = routeTopic + "EMSX_TRADE_REPORTING_INDICATOR,"
        routeTopic = routeTopic + "EMSX_TRANSACTION_REPORTING_MIC,"
        routeTopic = routeTopic + "EMSX_TYPE,"
        routeTopic = routeTopic + "EMSX_URGENCY_LEVEL,"
        routeTopic = routeTopic + "EMSX_USER_COMM_AMOUNT,"
        routeTopic = routeTopic + "EMSX_USER_COMM_RATE,"
        routeTopic = routeTopic + "EMSX_USER_FEES,"
        routeTopic = routeTopic + "EMSX_USER_NET_MONEY,"
        routeTopic = routeTopic + "EMSX_WAIVER_FLAG,"
        routeTopic = routeTopic + "EMSX_WORKING"
        subscriptions = blpapi.SubscriptionList()
        
        subscriptions.add(topic=routeTopic,correlationId=routeSubscriptionID)

        session.subscribe(subscriptions,identity=self.identity)

        print("Sent route subscription")


    def processAdminEvent(self,event):  
        print("Processing ADMIN event")

        for msg in event:
            if msg.messageType() == SLOW_CONSUMER_WARNING:
                print("Warning: Entered Slow Consumer status")
                
            elif msg.messageType() == SLOW_CONSUMER_WARNING_CLEARED:
                print("Slow consumer status cleared")
                
            else:
                print(msg)


    def processSessionStatusEvent(self,event,session):  
        print("Processing SESSION_STATUS event")

        for msg in event:
            if msg.messageType() == SESSION_STARTED:
                print("Session started...")
                session.openServiceAsync(d_auth)
                
            elif msg.messageType() == SESSION_STARTUP_FAILURE:
                print("Error: Session startup failed")
                
            elif msg.messageType() == SESSION_TERMINATED:
                print ("Terminating...")
                try:
                    exit()
                except:
                    pass

            else:
                print(msg)
                

    def processServiceStatusEvent(self,event,session):
        print ("Processing SERVICE_STATUS event")
        
        for msg in event:
            
            if msg.messageType() == SERVICE_OPENED:
                
                serviceName = msg.asElement().getElementAsString("serviceName")
                
                print("Service opened [%s]" % (serviceName))

                if serviceName==d_auth:
                    
                    print("Auth service opened... Opening EMSX service...")
                    session.openServiceAsync(d_emsx)
                
                elif serviceName==d_emsx:
                    
                    print("EMSX service opened... Sending authorization request...")
                    
                    self.sendAuthRequest(session)
                
            elif msg.messageType() == SERVICE_OPEN_FAILURE:
                    print("Error: Service Failed to open")
                
                
    
    def processAuthorizationStatusEvent(self,event):
        
        print ("Processing AUTHORIZATION_STATUS event")

        for msg in event:

            print("AUTHORIZATION_STATUS message: %s" % (msg))


                
    def processResponseEvent(self, event, session):
        print("Processing RESPONSE event")
        
        for msg in event:
            
            print("MESSAGE: %s" % msg.toString())
            print("CORRELATION ID: %d" % msg.correlationIds()[0].value())

            if msg.messageType() == AUTHORIZATION_SUCCESS:
                print("Authorization successful....")

                seatType = self.identity.getSeatType()
                print ("SeatType: %s" % (seatType))

                if seatType == SEATTYPE_INVALID:
                    print("Seat type is invalid")
                elif seatType == SEATTYPE_BPS:
                    print("Seat type is BPS")
                elif seatType == SEATTYPE_NONBPS:
                    print("Seat type is NONBPS")
                else:
                    print("Unknown seat type")

                self.createRouteSubscription(session)

            elif msg.messageType() == AUTHORIZATION_FAILURE:
                print("Authorization failed....")
                # insert code here to automatically retry authorization...

            else:
                print ("Unexpected message...")
                print (msg)
                    
            

    def processSubscriptionStatusEvent(self, event, session):
        
        print ("Processing SUBSCRIPTION_STATUS event")

        for msg in event:
            
            if msg.messageType() == SUBSCRIPTION_STARTED:
                
                if msg.correlationIds()[0].value() == routeSubscriptionID.value():
                    print ("Route subscription started successfully")
                    
            elif msg.messageType() == SUBSCRIPTION_FAILURE:
                print ("Error: Subscription failed", file=sys.stderr)
                print ("MESSAGE: %s" % (msg), file=sys.stderr)
                    
                reason = msg.getElement("reason")
                errorcode = reason.getElementAsInteger("errorCode")
                description = reason.getElementAsString("description")
            
                print ("Error: (%d) %s" % (errorcode, description), file=sys.stderr)                
                
            elif msg.messageType() == SUBSCRIPTION_TERMINATED:
                print ("Subscription terminated")
                print ("MESSAGE: %s" % (msg))

            elif msg.messageType() == SUBSCRIPTION_ACTIVATED:
                print ("Subscription activated")
                #print ("MESSAGE: %s" % (msg))

            else:
                print ("Unexpected message")
                print ("MESSAGE: %s" % (msg))


    def processSubscriptionDataEvent(self, event):
        
        #print ("Processing SUBSCRIPTION_DATA event")
        
        for msg in event:
            
            if msg.messageType() == ORDER_ROUTE_FIELDS:

                event_status = msg.getElementAsInteger("EVENT_STATUS")
                
                if msg.correlationIds()[0].value() == routeSubscriptionID.value():
                
                    if event_status == 1: # Heartbeat
                        #print ("Heartbeat...")
                        pass

                    elif event_status == 11:    # End-of-Initialpaint
                        print ("Route - End of initial paint")

                    else: # Excepting regular subscription data event
                        print ("")
                    
                        # This section shows how to extract the values from the message
                        
                        api_seq_num = msg.getElementAsInteger("API_SEQ_NUM") if msg.hasElement("API_SEQ_NUM") else 0
                        emsx_amount = msg.getElementAsInteger("EMSX_AMOUNT") if msg.hasElement("EMSX_AMOUNT") else 0
                        emsx_apa_mic = msg.getElementAsString("EMSX_APA_MIC") if msg.hasElement("EMSX_APA_MIC") else ""
                        emsx_avg_price = msg.getElementAsFloat("EMSX_AVG_PRICE") if msg.hasElement("EMSX_AVG_PRICE") else 0
                        emsx_broker = msg.getElementAsString("EMSX_BROKER") if msg.hasElement("EMSX_BROKER") else ""
                        emsx_broker_comm = msg.getElementAsFloat("EMSX_BROKER_COMM") if msg.hasElement("EMSX_BROKER_COMM") else 0
                        emsx_broker_lei = msg.getElementAsString("EMSX_BROKER_LEI") if msg.hasElement("EMSX_BROKER_LEI") else ""
                        emsx_broker_si = msg.getElementAsString("EMSX_BROKER_SI") if msg.hasElement("EMSX_BROKER_SI") else ""
                        emsx_broker_status = msg.getElementAsString("EMSX_BROKER_STATUS") if msg.hasElement("EMSX_BROKER_STATUS") else ""
                        emsx_bse_avg_price = msg.getElementAsFloat("EMSX_BSE_AVG_PRICE") if msg.hasElement("EMSX_BSE_AVG_PRICE") else 0
                        emsx_bse_filled = msg.getElementAsInteger("EMSX_BSE_FILLED") if msg.hasElement("EMSX_BSE_FILLED") else 0
                        emsx_buyside_lei = msg.getElementAsString("EMSX_BUYSIDE_LEI") if msg.hasElement("EMSX_BUYSIDE_LEI") else ""
                        emsx_clearing_account = msg.getElementAsString("EMSX_CLEARING_ACCOUNT") if msg.hasElement("EMSX_CLEARING_ACCOUNT") else ""
                        emsx_clearing_firm = msg.getElementAsString("EMSX_CLEARING_FIRM") if msg.hasElement("EMSX_CLEARING_FIRM") else ""
                        emsx_client_identification = msg.getElementAsString("EMSX_CLIENT_IDENTIFICATION") if msg.hasElement("EMSX_CLIENT_IDENTIFICATION") else ""
                        emsx_comm_diff_flag = msg.getElementAsString("EMSX_COMM_DIFF_FLAG") if msg.hasElement("EMSX_COMM_DIFF_FLAG") else ""
                        emsx_comm_rate = msg.getElementAsFloat("EMSX_COMM_RATE") if msg.hasElement("EMSX_COMM_RATE") else 0
                        emsx_currency_pair = msg.getElementAsString("EMSX_CURRENCY_PAIR") if msg.hasElement("EMSX_CURRENCY_PAIR") else ""
                        emsx_custom_account = msg.getElementAsString("EMSX_CUSTOM_ACCOUNT") if msg.hasElement("EMSX_CUSTOM_ACCOUNT") else ""
                        emsx_day_avg_price = msg.getElementAsFloat("EMSX_DAY_AVG_PRICE") if msg.hasElement("EMSX_DAY_AVG_PRICE") else 0
                        emsx_day_fill = msg.getElementAsInteger("EMSX_DAY_FILL") if msg.hasElement("EMSX_DAY_FILL") else 0
                        emsx_exchange_destination = msg.getElementAsString("EMSX_EXCHANGE_DESTINATION") if msg.hasElement("EMSX_EXCHANGE_DESTINATION") else ""
                        emsx_exec_instruction = msg.getElementAsString("EMSX_EXEC_INSTRUCTION") if msg.hasElement("EMSX_EXEC_INSTRUCTION") else ""
                        emsx_execute_broker = msg.getElementAsString("EMSX_EXECUTE_BROKER") if msg.hasElement("EMSX_EXECUTE_BROKER") else ""
                        emsx_fill_id = msg.getElementAsInteger("EMSX_FILL_ID") if msg.hasElement("EMSX_FILL_ID") else 0
                        emsx_filled = msg.getElementAsInteger("EMSX_FILLED") if msg.hasElement("EMSX_FILLED") else 0
                        emsx_gpi = msg.getElementAsString("EMSX_GPI") if msg.hasElement("EMSX_GPI") else ""
                        emsx_gtd_date = msg.getElementAsInteger("EMSX_GTD_DATE") if msg.hasElement("EMSX_GTD_DATE") else 0
                        emsx_hand_instruction = msg.getElementAsString("EMSX_HAND_INSTRUCTION") if msg.hasElement("EMSX_HAND_INSTRUCTION") else ""
                        emsx_is_manual_route = msg.getElementAsInteger("EMSX_IS_MANUAL_ROUTE") if msg.hasElement("EMSX_IS_MANUAL_ROUTE") else 0
                        emsx_last_capacity = msg.getElementAsString("EMSX_LAST_CAPACITY") if msg.hasElement("EMSX_LAST_CAPACITY") else ""
                        emsx_last_fill_date = msg.getElementAsInteger("EMSX_LAST_FILL_DATE") if msg.hasElement("EMSX_LAST_FILL_DATE") else 0
                        emsx_last_fill_time = msg.getElementAsInteger("EMSX_LAST_FILL_TIME") if msg.hasElement("EMSX_LAST_FILL_TIME") else 0
                        emsx_last_fill_time_microsec = msg.getElementAsFloat("EMSX_LAST_FILL_TIME_MICROSEC") if msg.hasElement("EMSX_LAST_FILL_TIME_MICROSEC") else 0
                        emsx_last_market = msg.getElementAsString("EMSX_LAST_MARKET") if msg.hasElement("EMSX_LAST_MARKET") else ""
                        emsx_last_price = msg.getElementAsFloat("EMSX_LAST_PRICE") if msg.hasElement("EMSX_LAST_PRICE") else 0
                        emsx_last_shares = msg.getElementAsInteger("EMSX_LAST_SHARES") if msg.hasElement("EMSX_LAST_SHARES") else 0
                        emsx_leg_fill_date_added = msg.getElementAsInteger("EMSX_LEG_FILL_DATE_ADDED") if msg.hasElement("EMSX_LEG_FILL_DATE_ADDED") else 0
                        emsx_leg_fill_price = msg.getElementAsFloat("EMSX_LEG_FILL_PRICE") if msg.hasElement("EMSX_LEG_FILL_PRICE") else 0
                        emsx_leg_fill_seq_no = msg.getElementAsInteger("EMSX_LEG_FILL_SEQ_NO") if msg.hasElement("EMSX_LEG_FILL_SEQ_NO") else 0
                        emsx_leg_fill_shares = msg.getElementAsFloat("EMSX_LEG_FILL_SHARES") if msg.hasElement("EMSX_LEG_FILL_SHARES") else 0
                        emsx_leg_fill_side = msg.getElementAsString("EMSX_LEG_FILL_SIDE") if msg.hasElement("EMSX_LEG_FILL_SIDE") else ""
                        emsx_leg_fill_ticker = msg.getElementAsString("EMSX_LEG_FILL_TICKER") if msg.hasElement("EMSX_LEG_FILL_TICKER") else ""
                        emsx_leg_fill_time_added = msg.getElementAsInteger("EMSX_LEG_FILL_TIME_ADDED") if msg.hasElement("EMSX_LEG_FILL_TIME_ADDED") else 0
                        emsx_limit_price = msg.getElementAsFloat("EMSX_LIMIT_PRICE") if msg.hasElement("EMSX_LIMIT_PRICE") else 0
                        emsx_mifid_ii_instruction = msg.getElementAsString("EMSX_MIFID_II_INSTRUCTION") if msg.hasElement("EMSX_MIFID_II_INSTRUCTION") else ""
                        emsx_misc_fees = msg.getElementAsFloat("EMSX_MISC_FEES") if msg.hasElement("EMSX_MISC_FEES") else 0
                        emsx_ml_id = msg.getElementAsString("EMSX_ML_ID") if msg.hasElement("EMSX_ML_ID") else ""
                        emsx_ml_leg_quantity = msg.getElementAsInteger("EMSX_ML_LEG_QUANTITY") if msg.hasElement("EMSX_ML_LEG_QUANTITY") else 0
                        emsx_ml_num_legs = msg.getElementAsInteger("EMSX_ML_NUM_LEGS") if msg.hasElement("EMSX_ML_NUM_LEGS") else 0
                        emsx_ml_percent_filled = msg.getElementAsFloat("EMSX_ML_PERCENT_FILLED") if msg.hasElement("EMSX_ML_PERCENT_FILLED") else 0
                        emsx_ml_ratio = msg.getElementAsFloat("EMSX_ML_RATIO") if msg.hasElement("EMSX_ML_RATIO") else 0
                        emsx_ml_remain_balance = msg.getElementAsFloat("EMSX_ML_REMAIN_BALANCE") if msg.hasElement("EMSX_ML_REMAIN_BALANCE") else 0
                        emsx_ml_strategy = msg.getElementAsString("EMSX_ML_STRATEGY") if msg.hasElement("EMSX_ML_STRATEGY") else ""
                        emsx_ml_total_quantity = msg.getElementAsInteger("EMSX_ML_TOTAL_QUANTITY") if msg.hasElement("EMSX_ML_TOTAL_QUANTITY") else 0
                        emsx_notes = msg.getElementAsString("EMSX_NOTES") if msg.hasElement("EMSX_NOTES") else ""
                        emsx_nse_avg_price = msg.getElementAsFloat("EMSX_NSE_AVG_PRICE") if msg.hasElement("EMSX_NSE_AVG_PRICE") else 0
                        emsx_nse_filled = msg.getElementAsInteger("EMSX_NSE_FILLED") if msg.hasElement("EMSX_NSE_FILLED") else 0
                        emsx_order_type = msg.getElementAsString("EMSX_ORDER_TYPE") if msg.hasElement("EMSX_ORDER_TYPE") else ""
                        emsx_otc_flag = msg.getElementAsString("EMSX_OTC_FLAG") if msg.hasElement("EMSX_OTC_FLAG") else ""
                        emsx_p_a = msg.getElementAsString("EMSX_P_A") if msg.hasElement("EMSX_P_A") else ""
                        emsx_percent_remain = msg.getElementAsFloat("EMSX_PERCENT_REMAIN") if msg.hasElement("EMSX_PERCENT_REMAIN") else 0
                        emsx_principal = msg.getElementAsFloat("EMSX_PRINCIPAL") if msg.hasElement("EMSX_PRINCIPAL") else 0
                        emsx_queued_date = msg.getElementAsInteger("EMSX_QUEUED_DATE") if msg.hasElement("EMSX_QUEUED_DATE") else 0
                        emsx_queued_time = msg.getElementAsInteger("EMSX_QUEUED_TIME") if msg.hasElement("EMSX_QUEUED_TIME") else 0
                        emsx_queued_time_microsec = msg.getElementAsFloat("EMSX_QUEUED_TIME_MICROSEC") if msg.hasElement("EMSX_QUEUED_TIME_MICROSEC") else ""
                        emsx_reason_code = msg.getElementAsString("EMSX_REASON_CODE") if msg.hasElement("EMSX_REASON_CODE") else ""
                        emsx_reason_desc = msg.getElementAsString("EMSX_REASON_DESC") if msg.hasElement("EMSX_REASON_DESC") else ""
                        emsx_remain_balance = msg.getElementAsFloat("EMSX_REMAIN_BALANCE") if msg.hasElement("EMSX_REMAIN_BALANCE") else 0
                        emsx_route_as_of_date = msg.getElementAsInteger("EMSX_ROUTE_AS_OF_DATE") if msg.hasElement("EMSX_ROUTE_AS_OF_DATE") else 0
                        emsx_route_as_of_time_microsec = msg.getElementAsFloat("EMSX_ROUTE_AS_OF_TIME_MICROSEC") if msg.hasElement("EMSX_ROUTE_AS_OF_TIME_MICROSEC") else 0
                        emsx_route_create_date = msg.getElementAsInteger("EMSX_ROUTE_CREATE_DATE") if msg.hasElement("EMSX_ROUTE_CREATE_DATE") else 0
                        emsx_route_create_time = msg.getElementAsInteger("EMSX_ROUTE_CREATE_TIME") if msg.hasElement("EMSX_ROUTE_CREATE_TIME") else 0
                        emsx_route_create_time_microsec = msg.getElementAsFloat("EMSX_ROUTE_CREATE_TIME_MICROSEC") if msg.hasElement("EMSX_ROUTE_CREATE_TIME_MICROSEC") else 0
                        emsx_route_id = msg.getElementAsInteger("EMSX_ROUTE_ID") if msg.hasElement("EMSX_ROUTE_ID") else 0
                        emsx_route_last_update_time = msg.getElementAsInteger("EMSX_ROUTE_LAST_UPDATE_TIME") if msg.hasElement("EMSX_ROUTE_LAST_UPDATE_TIME") else 0
                        emsx_route_last_update_time_microsec = msg.getElementAsFloat("EMSX_ROUTE_LAST_UPDATE_TIME_MICROSEC") if msg.hasElement("EMSX_ROUTE_LAST_UPDATE_TIME_MICROSEC") else 0
                        emsx_route_price = msg.getElementAsFloat("EMSX_ROUTE_PRICE") if msg.hasElement("EMSX_ROUTE_PRICE") else 0
                        emsx_route_ref_id = msg.getElementAsString("EMSX_ROUTE_REF_ID") if msg.hasElement("EMSX_ROUTE_REF_ID") else ""
                        emsx_sequence = msg.getElementAsInteger("EMSX_SEQUENCE") if msg.hasElement("EMSX_SEQUENCE") else 0
                        emsx_settle_amount = msg.getElementAsFloat("EMSX_SETTLE_AMOUNT") if msg.hasElement("EMSX_SETTLE_AMOUNT") else 0
                        emsx_settle_date = msg.getElementAsInteger("EMSX_SETTLE_DATE") if msg.hasElement("EMSX_SETTLE_DATE") else 0
                        emsx_status = msg.getElementAsString("EMSX_STATUS") if msg.hasElement("EMSX_STATUS") else ""
                        emsx_stop_price = msg.getElementAsFloat("EMSX_STOP_PRICE") if msg.hasElement("EMSX_STOP_PRICE") else 0
                        emsx_strategy_end_time = msg.getElementAsInteger("EMSX_STRATEGY_END_TIME") if msg.hasElement("EMSX_STRATEGY_END_TIME") else 0
                        emsx_strategy_part_rate1 = msg.getElementAsFloat("EMSX_STRATEGY_PART_RATE1") if msg.hasElement("EMSX_STRATEGY_PART_RATE1") else 0
                        emsx_strategy_part_rate2 = msg.getElementAsFloat("EMSX_STRATEGY_PART_RATE2") if msg.hasElement("EMSX_STRATEGY_PART_RATE2") else 0
                        emsx_strategy_start_time = msg.getElementAsInteger("EMSX_STRATEGY_START_TIME") if msg.hasElement("EMSX_STRATEGY_START_TIME") else 0
                        emsx_strategy_style = msg.getElementAsString("EMSX_STRATEGY_STYLE") if msg.hasElement("EMSX_STRATEGY_STYLE") else ""
                        emsx_strategy_type = msg.getElementAsString("EMSX_STRATEGY_TYPE") if msg.hasElement("EMSX_STRATEGY_TYPE") else ""
                        emsx_tif = msg.getElementAsString("EMSX_TIF") if msg.hasElement("EMSX_TIF") else ""
                        emsx_time_stamp = msg.getElementAsInteger("EMSX_TIME_STAMP") if msg.hasElement("EMSX_TIME_STAMP") else 0
                        emsx_time_stamp_microsec = msg.getElementAsFloat("EMSX_TIME_STAMP_MICROSEC") if msg.hasElement("EMSX_TIME_STAMP_MICROSEC") else 0
                        emsx_trade_reporting_indicator = msg.getElementAsString("EMSX_TRADE_REPORTING_INDICATOR") if msg.hasElement("EMSX_TRADE_REPORTING_INDICATOR") else ""
                        emsx_transaction_reporting_mic = msg.getElementAsString("EMSX_TRANSACTION_REPORTING_MIC") if msg.hasElement("EMSX_TRANSACTION_REPORTING_MIC") else ""
                        emsx_type = msg.getElementAsString("EMSX_TYPE") if msg.hasElement("EMSX_TYPE") else ""
                        emsx_urgency_level = msg.getElementAsInteger("EMSX_URGENCY_LEVEL") if msg.hasElement("EMSX_URGENCY_LEVEL") else ""
                        emsx_user_comm_amount = msg.getElementAsFloat("EMSX_USER_COMM_AMOUNT") if msg.hasElement("EMSX_USER_COMM_AMOUNT") else 0
                        emsx_user_comm_rate = msg.getElementAsFloat("EMSX_USER_COMM_RATE") if msg.hasElement("EMSX_USER_COMM_RATE") else 0
                        emsx_user_fees = msg.getElementAsFloat("EMSX_USER_FEES") if msg.hasElement("EMSX_USER_FEES") else 0
                        emsx_user_net_money = msg.getElementAsFloat("EMSX_USER_NET_MONEY") if msg.hasElement("EMSX_USER_NET_MONEY") else 0
                        emsx_waiver_flag = msg.getElementAsString("EMSX_WAIVER_FLAG") if msg.hasElement("EMSX_WORKING") else ""
                        emsx_working = msg.getElementAsInteger("EMSX_WORKING") if msg.hasElement("EMSX_WORKING") else 0
                        emsx_route_as_of_date = msg.getElementAsInteger("EMSX_ROUTE_AS_OF_DATE") if msg.hasElement("EMSX_ROUTE_AS_OF_DATE") else 0
                        
                        print ("ROUTE MESSAGE: CorrelationID(%d)   Status(%d)" % (msg.correlationIds()[0].value(),event_status))
                        #print ("MESSAGE: %s" % (msg))
                        
                        print ("API_SEQ_NUM: %d" % (api_seq_num))
                        print ("EMSX_AMOUNT: %d" % (emsx_amount))
                        print ("EMSX_APA_MIC: %s" % (emsx_apa_mic))
                        print ("EMSX_AVG_PRICE: %d" % (emsx_avg_price))
                        print ("EMSX_BROKER: %s" % (emsx_broker))
                        print ("EMSX_BROKER_COMM: %d" % (emsx_broker_comm))
                        print ("EMSX_BROKER_LEI: %s" % (emsx_broker_lei))
                        print ("EMSX_BROKER_SI: %s" % (emsx_broker_si))
                        print ("EMSX_BROKER_STATUS: %s" % (emsx_broker_status))
                        print ("EMSX_BSE_AVG_PRICE: %d" % (emsx_bse_avg_price))
                        print ("EMSX_BSE_FILLED: %d" % (emsx_bse_filled))
                        print ("EMSX_BUYSIDE_LEI: %s" % (emsx_buyside_lei))
                        print ("EMSX_CLEARING_ACCOUNT: %s" % (emsx_clearing_account))
                        print ("EMSX_CLEARING_FIRM: %s" % (emsx_clearing_firm))
                        print ("EMSX_CLIENT_IDENTIFICATION: %s" % (emsx_client_identification))
                        print ("EMSX_COMM_DIFF_FLAG: %s" % (emsx_comm_diff_flag))
                        print ("EMSX_COMM_RATE: %d" % (emsx_comm_rate))
                        print ("EMSX_CURRENCY_PAIR: %s" % (emsx_currency_pair))
                        print ("EMSX_CUSTOM_ACCOUNT: %s" % (emsx_custom_account))
                        print ("EMSX_DAY_AVG_PRICE: %d" % (emsx_day_avg_price))
                        print ("EMSX_DAY_FILL: %d" % (emsx_day_fill))
                        print ("EMSX_EXCHANGE_DESTINATION: %s" % (emsx_exchange_destination))
                        print ("EMSX_EXEC_INSTRUCTION: %s" % (emsx_exec_instruction))
                        print ("EMSX_EXECUTE_BROKER: %s" % (emsx_execute_broker))
                        print ("EMSX_FILL_ID: %d" % (emsx_fill_id))
                        print ("EMSX_FILLED: %d" % (emsx_filled))
                        print ("EMSX_GPI: %s" % (emsx_gpi))
                        print ("EMSX_GTD_DATE: %d" % (emsx_gtd_date))
                        print ("EMSX_HAND_INSTRUCTION: %s" % (emsx_hand_instruction))
                        print ("EMSX_IS_MANUAL_ROUTE: %d" % (emsx_is_manual_route))
                        print ("EMSX_LAST_CAPACITY: %s" % (emsx_last_capacity))
                        print ("EMSX_LAST_FILL_DATE: %d" % (emsx_last_fill_date))
                        print ("EMSX_LAST_FILL_TIME: %d" % (emsx_last_fill_time))
                        print ("EMSX_LAST_FILL_TIME_MICROSEC: %0.8f" % (emsx_last_fill_time_microsec))
                        print ("EMSX_LAST_MARKET: %s" % (emsx_last_market))
                        print ("EMSX_LAST_PRICE: %d" % (emsx_last_price))
                        print ("EMSX_LAST_SHARES: %d" % (emsx_last_shares))
                        print ("EMSX_LEG_FILL_DATE_ADDED: %d" % (emsx_leg_fill_date_added))
                        print ("EMSX_LEG_FILL_PRICE: %0.8f" % (emsx_leg_fill_price))
                        print ("EMSX_LEG_FILL_SEQ_NO: %d" % (emsx_leg_fill_seq_no))
                        print ("EMSX_LEG_FILL_SHARES: %0.8f" % (emsx_leg_fill_shares))
                        print ("EMSX_LEG_FILL_SIDE: %s" % (emsx_leg_fill_side))
                        print ("EMSX_LEG_FILL_TICKER: %s" % (emsx_leg_fill_ticker))
                        print ("EMSX_LEG_FILL_TIME_ADDED: %d" % (emsx_leg_fill_time_added))
                        print ("EMSX_LIMIT_PRICE: %0.8f" % (emsx_limit_price))
                        print ("EMSX_MIFID_II_INSTRUCTION: %s" % (emsx_mifid_ii_instruction))
                        print ("EMSX_MISC_FEES: %d" % (emsx_misc_fees))
                        print ("EMSX_ML_ID: %s" % (emsx_ml_id))
                        print ("EMSX_ML_LEG_QUANTITY: %d" % (emsx_ml_leg_quantity))
                        print ("EMSX_ML_NUM_LEGS: %d" % (emsx_ml_num_legs))
                        print ("EMSX_ML_PERCENT_FILLED: %d" % (emsx_ml_percent_filled))
                        print ("EMSX_ML_RATIO: %d" % (emsx_ml_ratio))
                        print ("EMSX_ML_REMAIN_BALANCE: %d" % (emsx_ml_remain_balance))
                        print ("EMSX_ML_STRATEGY: %s" % (emsx_ml_strategy))
                        print ("EMSX_ML_TOTAL_QUANTITY: %d" % (emsx_ml_total_quantity))
                        print ("EMSX_NOTES: %s" % (emsx_notes))
                        print ("EMSX_NSE_AVG_PRICE: %d" % (emsx_nse_avg_price))
                        print ("EMSX_NSE_FILLED: %d" % (emsx_nse_filled))
                        print ("EMSX_ORDER_TYPE: %s" % (emsx_order_type))
                        print ("EMSX_OTC_FLAG: %s" % (emsx_otc_flag))
                        print ("EMSX_P_A: %s" % (emsx_p_a))
                        print ("EMSX_PERCENT_REMAIN: %d" % (emsx_percent_remain))
                        print ("EMSX_PRINCIPAL: %d" % (emsx_principal))
                        print ("EMSX_QUEUED_DATE: %d" % (emsx_queued_date))
                        print ("EMSX_QUEUED_TIME: %d" % (emsx_queued_time))
                        print ("EMSX_QUEUED_TIME_MICROSEC: %0.8f" % (emsx_queued_time_microsec))
                        print ("EMSX_REASON_CODE: %s" % (emsx_reason_code))
                        print ("EMSX_REASON_DESC: %s" % (emsx_reason_desc))
                        print ("EMSX_REMAIN_BALANCE: %d" % (emsx_remain_balance))
                        print ("EMSX_ROUTE_AS_OF_DATE: %d" % (emsx_route_as_of_date))
                        print ("EMSX_ROUTE_AS_OF_TIME_MICROSEC: %0.8f" % (emsx_route_as_of_time_microsec))
                        print ("EMSX_ROUTE_CREATE_DATE: %d" % (emsx_route_create_date))
                        print ("EMSX_ROUTE_CREATE_TIME: %d" % (emsx_route_create_time))
                        print ("EMSX_ROUTE_CREATE_TIME_MICROSEC: %0.8f" % (emsx_route_create_time_microsec))
                        print ("EMSX_ROUTE_ID: %d" % (emsx_route_id))
                        print ("EMSX_ROUTE_LAST_UPDATE_TIME: %d" % (emsx_route_last_update_time))
                        print ("EMSX_ROUTE_LAST_UPDATE_TIME_MICROSEC: %0.8f" % (emsx_route_last_update_time_microsec))
                        print ("EMSX_ROUTE_PRICE: %d" % (emsx_route_price))
                        print ("EMSX_ROUTE_REF_ID: %s" % (emsx_route_ref_id))
                        print ("EMSX_SEQUENCE: %d" % (emsx_sequence))
                        print ("EMSX_SETTLE_AMOUNT: %d" % (emsx_settle_amount))
                        print ("EMSX_SETTLE_DATE: %d" % (emsx_settle_date))
                        print ("EMSX_STATUS: %s" % (emsx_status))
                        print ("EMSX_STOP_PRICE: %d" % (emsx_stop_price))
                        print ("EMSX_STRATEGY_END_TIME: %d" % (emsx_strategy_end_time))
                        print ("EMSX_STRATEGY_PART_RATE1: %d" % (emsx_strategy_part_rate1))
                        print ("EMSX_STRATEGY_PART_RATE2: %d" % (emsx_strategy_part_rate2))
                        print ("EMSX_STRATEGY_START_TIME: %s" % (emsx_strategy_start_time))
                        print ("EMSX_STRATEGY_STYLE: %s" % (emsx_strategy_style))
                        print ("EMSX_STRATEGY_TYPE: %s" % (emsx_strategy_type))
                        print ("EMSX_TIF: %s" % (emsx_tif))
                        print ("EMSX_TIME_STAMP: %d" % (emsx_time_stamp))
                        print ("EMSX_TIME_STAMP_MICROSEC: %0.8f" % (emsx_time_stamp_microsec))
                        print ("EMSX_TRADE_REPORTING_INDICATOR: %s" % (emsx_trade_reporting_indicator))
                        print ("EMSX_TRANSACTION_REPORTING_MIC: %s" % (emsx_transaction_reporting_mic))
                        print ("EMSX_TYPE: %s" % (emsx_type))
                        print ("EMSX_URGENCY_LEVEL: %d" % (emsx_urgency_level))
                        print ("EMSX_USER_COMM_AMOUNT: %d" % (emsx_user_comm_amount))
                        print ("EMSX_USER_COMM_RATE: %d" % (emsx_user_comm_rate))
                        print ("EMSX_USER_FEES: %d" % (emsx_user_fees))
                        print ("EMSX_USER_NET_MONEY: %d" % (emsx_user_net_money))
                        print ("EMSX_WAIVER_FLAG: %s" % (emsx_waiver_flag))
                        print ("EMSX_WORKING: %d" % (emsx_working))
                        print ("EMSX_ROUTE_AS_OF_DATE: %d" % (emsx_route_as_of_date))
            
                else:
                    print ("Error - Unexpected message:" + msg, file=sys.stderr)


    def processMiscEvents(self, event):
        
        print("Processing " + event.eventType() + " event")
        
        for msg in event:

            print("MISC MESSAGE: %s" % (msg.tostring()))


    def processEvent(self, event, session):
        try:
            
            if event.eventType() == blpapi.Event.ADMIN:
                self.processAdminEvent(event)
            
            if event.eventType() == blpapi.Event.SESSION_STATUS:
                self.processSessionStatusEvent(event,session)

            elif event.eventType() == blpapi.Event.SERVICE_STATUS:
                self.processServiceStatusEvent(event,session)

            elif event.eventType() == blpapi.Event.AUTHORIZATION_STATUS:
                self.processAuthorizationStatusEvent(event)

            elif event.eventType() == blpapi.Event.RESPONSE:
                self.processResponseEvent(event,session)

            elif event.eventType() == blpapi.Event.SUBSCRIPTION_STATUS:
                self.processSubscriptionStatusEvent(event, session)

            elif event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                self.processSubscriptionDataEvent(event)
            
            
            else:
                self.processMiscEvents(event)
                
        except:
            print("Exception:  %s" % sys.exc_info()[0])
            
        return False

                
def main():
    
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(d_host)
    sessionOptions.setServerPort(d_port)

    print("Connecting to %s:%d" % (d_host,d_port))

    eventHandler = SessionEventHandler()

    session = blpapi.Session(sessionOptions, eventHandler.processEvent)

    if not session.startAsync():
        print("Failed to start session.")
        return
    
    input()
    session.stop()


if __name__ == "__main__":
    print("Bloomberg - EMSX API Example - Server - Route Subscription")
    main()

__copyright__ = """
Copyright 2020. Bloomberg Finance L.P.
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:  The above
copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""

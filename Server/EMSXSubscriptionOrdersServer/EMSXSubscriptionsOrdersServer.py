# EMSXSubscriptionOrdersServer.py

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

orderSubscriptionID=blpapi.CorrelationId(99)

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

    
    def createOrderSubscription(self, session):
        
        print ("Create Order subscription")

        orderTopic = d_emsx + "/order;team=RJCSERVER1?fields="
        #orderTopic = d_emsx + "/order?fields="
        orderTopic = orderTopic + "API_SEQ_NUM,"
        orderTopic = orderTopic + "EMSX_ACCOUNT,"
        orderTopic = orderTopic + "EMSX_AMOUNT,"
        orderTopic = orderTopic + "EMSX_ARRIVAL_PRICE,"
        orderTopic = orderTopic + "EMSX_ASSET_CLASS,"
        orderTopic = orderTopic + "EMSX_ASSIGNED_TRADER,"
        orderTopic = orderTopic + "EMSX_AVG_PRICE,"
        orderTopic = orderTopic + "EMSX_BASKET_NAME,"
        orderTopic = orderTopic + "EMSX_BASKET_NUM,"
        orderTopic = orderTopic + "EMSX_BLOCK_ID,"
        orderTopic = orderTopic + "EMSX_BROKER,"
        orderTopic = orderTopic + "EMSX_BROKER_COMM,"
        orderTopic = orderTopic + "EMSX_BSE_AVG_PRICE,"
        orderTopic = orderTopic + "EMSX_BSE_FILLED,"
        orderTopic = orderTopic + "EMSX_BUYSIDE_LEI,"
        orderTopic = orderTopic + "EMSX_CFD_FLAG,"
        orderTopic = orderTopic + "EMSX_CLIENT_IDENTIFICATION,"
        orderTopic = orderTopic + "EMSX_COMM_DIFF_FLAG,"
        orderTopic = orderTopic + "EMSX_COMM_RATE,"
        orderTopic = orderTopic + "EMSX_CUSTOM_NOTE1,"
        orderTopic = orderTopic + "EMSX_CUSTOM_NOTE2,"
        orderTopic = orderTopic + "EMSX_CUSTOM_NOTE3,"
        orderTopic = orderTopic + "EMSX_CUSTOM_NOTE4,"
        orderTopic = orderTopic + "EMSX_CUSTOM_NOTE5,"
        orderTopic = orderTopic + "EMSX_CURRENCY_PAIR,"
        orderTopic = orderTopic + "EMSX_DATE,"
        orderTopic = orderTopic + "EMSX_DAY_AVG_PRICE,"
        orderTopic = orderTopic + "EMSX_DAY_FILL,"
        orderTopic = orderTopic + "EMSX_DIR_BROKER_FLAG,"
        orderTopic = orderTopic + "EMSX_EXCHANGE,"
        orderTopic = orderTopic + "EMSX_EXCHANGE_DESTINATION,"
        orderTopic = orderTopic + "EMSX_EXEC_INSTRUCTION,"
        orderTopic = orderTopic + "EMSX_FILL_ID,"
        orderTopic = orderTopic + "EMSX_FILLED,"
        orderTopic = orderTopic + "EMSX_GPI,"
        orderTopic = orderTopic + "EMSX_GTD_DATE,"
        orderTopic = orderTopic + "EMSX_HAND_INSTRUCTION,"
        orderTopic = orderTopic + "EMSX_IDLE_AMOUNT,"
        orderTopic = orderTopic + "EMSX_INVESTOR_ID,"
        orderTopic = orderTopic + "EMSX_ISIN,"
        orderTopic = orderTopic + "EMSX_LIMIT_PRICE,"
        orderTopic = orderTopic + "EMSX_MIFID_II_INSTRUCTION,"
        orderTopic = orderTopic + "EMSX_MOD_PEND_STATUS,"
        orderTopic = orderTopic + "EMSX_NOTES,"
        orderTopic = orderTopic + "EMSX_NSE_AVG_PRICE,"
        orderTopic = orderTopic + "EMSX_NSE_FILLED,"
        orderTopic = orderTopic + "EMSX_ORD_REF_ID,"
        orderTopic = orderTopic + "EMSX_ORDER_AS_OF_DATE,"
        orderTopic = orderTopic + "EMSX_ORDER_AS_OF_TIME_MICROSEC,"
        orderTopic = orderTopic + "EMSX_ORDER_TYPE,"
        orderTopic = orderTopic + "EMSX_ORIGINATE_TRADER,"
        orderTopic = orderTopic + "EMSX_ORIGINATE_TRADER_FIRM,"
        orderTopic = orderTopic + "EMSX_PERCENT_REMAIN,"
        orderTopic = orderTopic + "EMSX_PM_UUID,"
        orderTopic = orderTopic + "EMSX_PORT_MGR,"
        orderTopic = orderTopic + "EMSX_PORT_NAME,"
        orderTopic = orderTopic + "EMSX_PORT_NUM,"
        orderTopic = orderTopic + "EMSX_POSITION,"
        orderTopic = orderTopic + "EMSX_PRINCIPAL,"
        orderTopic = orderTopic + "EMSX_PRODUCT,"
        orderTopic = orderTopic + "EMSX_QUEUED_DATE,"
        orderTopic = orderTopic + "EMSX_QUEUED_TIME,"
        orderTopic = orderTopic + "EMSX_QUEUED_TIME_MICROSEC,"
        orderTopic = orderTopic + "EMSX_REASON_CODE,"
        orderTopic = orderTopic + "EMSX_REASON_DESC,"
        orderTopic = orderTopic + "EMSX_REMAIN_BALANCE,"
        orderTopic = orderTopic + "EMSX_ROUTE_ID,"
        orderTopic = orderTopic + "EMSX_ROUTE_PRICE,"
        orderTopic = orderTopic + "EMSX_SEC_NAME,"
        orderTopic = orderTopic + "EMSX_SEDOL,"
        orderTopic = orderTopic + "EMSX_SEQUENCE,"
        orderTopic = orderTopic + "EMSX_SETTLE_AMOUNT,"
        orderTopic = orderTopic + "EMSX_SETTLE_DATE,"
        orderTopic = orderTopic + "EMSX_SI,"
        orderTopic = orderTopic + "EMSX_SIDE,"
        orderTopic = orderTopic + "EMSX_START_AMOUNT,"
        orderTopic = orderTopic + "EMSX_STATUS,"
        orderTopic = orderTopic + "EMSX_STEP_OUT_BROKER,"
        orderTopic = orderTopic + "EMSX_STOP_PRICE,"
        orderTopic = orderTopic + "EMSX_STRATEGY_END_TIME,"
        orderTopic = orderTopic + "EMSX_STRATEGY_PART_RATE1,"
        orderTopic = orderTopic + "EMSX_STRATEGY_PART_RATE2,"
        orderTopic = orderTopic + "EMSX_STRATEGY_START_TIME,"
        orderTopic = orderTopic + "EMSX_STRATEGY_STYLE,"
        orderTopic = orderTopic + "EMSX_STRATEGY_TYPE,"
        orderTopic = orderTopic + "EMSX_TICKER,"
        orderTopic = orderTopic + "EMSX_TIF,"
        orderTopic = orderTopic + "EMSX_TIME_STAMP,"
        orderTopic = orderTopic + "EMSX_TIME_STAMP_MICROSEC,"
        orderTopic = orderTopic + "EMSX_TRAD_UUID,"
        orderTopic = orderTopic + "EMSX_TRADE_DESK,"
        orderTopic = orderTopic + "EMSX_TRADER,"
        orderTopic = orderTopic + "EMSX_TRADER_NOTES,"
        orderTopic = orderTopic + "EMSX_TS_ORDNUM,"
        orderTopic = orderTopic + "EMSX_TYPE,"
        orderTopic = orderTopic + "EMSX_UNDERLYING_TICKER,"
        orderTopic = orderTopic + "EMSX_USER_COMM_AMOUNT,"
        orderTopic = orderTopic + "EMSX_USER_COMM_RATE,"
        orderTopic = orderTopic + "EMSX_USER_FEES,"
        orderTopic = orderTopic + "EMSX_USER_NET_MONEY,"
        orderTopic = orderTopic + "EMSX_WORK_PRICE,"
        orderTopic = orderTopic + "EMSX_WORKING,"
        orderTopic = orderTopic + "EMSX_YELLOW_KEY"

        subscriptions = blpapi.SubscriptionList()
        
        subscriptions.add(topic=orderTopic,correlationId=orderSubscriptionID)

        session.subscribe(subscriptions,identity=self.identity)

        print("Sent order subscription")


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

                self.createOrderSubscription(session)

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
                
                if msg.correlationIds()[0].value() == orderSubscriptionID.value():
                    print ("Order subscription started successfully")
                    
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
                
                if msg.correlationIds()[0].value() == orderSubscriptionID.value():
                
                    if event_status == 1: # Heartbeat
                        #print ("Heartbeat...")
                        pass

                    elif event_status == 11:    # End-of-Initialpaint
                        print ("Order - End of initial paint")

                    else: # Excepting regular subscription data event
                        print ("")
                    
                        # This section shows how to extract the values from the message
                        
                        api_seq_num = msg.getElementAsInteger("API_SEQ_NUM") if msg.hasElement("API_SEQ_NUM") else 0
                        emsx_account = msg.getElementAsString("EMSX_ACCOUNT") if msg.hasElement("EMSX_ACCOUNT") else ""
                        emsx_amount = msg.getElementAsInteger("EMSX_AMOUNT") if msg.hasElement("EMSX_AMOUNT") else 0
                        emsx_arrival_price = msg.getElementAsFloat("EMSX_ARRIVAL_PRICE") if msg.hasElement("EMSX_ARRIVAL_PRICE") else 0
                        emsx_asset_class = msg.getElementAsString("EMSX_ASSET_CLASS") if msg.hasElement("EMSX_ASSET_CLASS") else ""
                        emsx_assigned_trader = msg.getElementAsString("EMSX_ASSIGNED_TRADER") if msg.hasElement("EMSX_ASSIGNED_TRADER") else ""
                        emsx_avg_price = msg.getElementAsFloat("EMSX_AVG_PRICE") if msg.hasElement("EMSX_AVG_PRICE") else 0
                        emsx_basket_name = msg.getElementAsString("EMSX_BASKET_NAME") if msg.hasElement("EMSX_BASKET_NAME") else ""
                        emsx_basket_num = msg.getElementAsInteger("EMSX_BASKET_NUM") if msg.hasElement("EMSX_BASKET_NUM") else 0
                        emsx_block_id = msg.getElementAsString("EMSX_BLOCK_ID") if msg.hasElement("EMSX_BLOCK_ID") else ""
                        emsx_broker = msg.getElementAsString("EMSX_BROKER") if msg.hasElement("EMSX_BROKER") else ""
                        emsx_broker_comm = msg.getElementAsFloat("EMSX_BROKER_COMM") if msg.hasElement("EMSX_BROKER_COMM") else 0
                        emsx_bse_avg_price = msg.getElementAsFloat("EMSX_BSE_AVG_PRICE") if msg.hasElement("EMSX_BSE_AVG_PRICE") else 0
                        emsx_bse_filled = msg.getElementAsInteger("EMSX_BSE_FILLED") if msg.hasElement("EMSX_BSE_FILLED") else 0
                        emsx_buyside_lei = msg.getElementAsString("EMSX_BUYSIDE_LEI") if msg.hasElement("EMSX_BUYSIDE_LEI") else ""
                        emsx_cfd_flag = msg.getElementAsString("EMSX_CFD_FLAG") if msg.hasElement("EMSX_CFD_FLAG") else ""
                        emsx_client_identification = msg.getElementAsString("EMSX_CLIENT_IDENTIFICATION") if msg.hasElement("EMSX_CLIENT_IDENTIFICATION") else ""
                        emsx_comm_diff_flag = msg.getElementAsString("EMSX_COMM_DIFF_FLAG") if msg.hasElement("EMSX_COMM_DIFF_FLAG") else ""
                        emsx_comm_rate = msg.getElementAsFloat("EMSX_COMM_RATE") if msg.hasElement("EMSX_COMM_RATE") else 0
                        emsx_currency_pair = msg.getElementAsString("EMSX_CURRENCY_PAIR") if msg.hasElement("EMSX_CURRENCY_PAIR") else ""
                        emsx_custom_note1 = msg.getElementAsString("EMSX_CUSTOM_NOTE1") if msg.hasElement("EMSX_CUSTOM_NOTE1") else ""
                        emsx_custom_note2 = msg.getElementAsString("EMSX_CUSTOM_NOTE2") if msg.hasElement("EMSX_CUSTOM_NOTE2") else ""
                        emsx_custom_note3 = msg.getElementAsString("EMSX_CUSTOM_NOTE3") if msg.hasElement("EMSX_CUSTOM_NOTE3") else ""
                        emsx_custom_note4 = msg.getElementAsString("EMSX_CUSTOM_NOTE4") if msg.hasElement("EMSX_CUSTOM_NOTE4") else ""
                        emsx_custom_note5 = msg.getElementAsString("EMSX_CUSTOM_NOTE5") if msg.hasElement("EMSX_CUSTOM_NOTE5") else ""                                               
                        emsx_date = msg.getElementAsInteger("EMSX_DATE") if msg.hasElement("EMSX_DATE") else 0
                        emsx_day_avg_price = msg.getElementAsFloat("EMSX_DAY_AVG_PRICE") if msg.hasElement("EMSX_DAY_AVG_PRICE") else 0
                        emsx_day_fill = msg.getElementAsInteger("EMSX_DAY_FILL") if msg.hasElement("EMSX_DAY_FILL") else 0
                        emsx_dir_broker_flag = msg.getElementAsString("EMSX_DIR_BROKER_FLAG") if msg.hasElement("EMSX_DIR_BROKER_FLAG") else ""
                        emsx_exchange = msg.getElementAsString("EMSX_EXCHANGE") if msg.hasElement("EMSX_EXCHANGE") else ""
                        emsx_exchange_destination = msg.getElementAsString("EMSX_EXCHANGE_DESTINATION") if msg.hasElement("EMSX_EXCHANGE_DESTINATION") else ""
                        emsx_exec_instruction = msg.getElementAsString("EMSX_EXEC_INSTRUCTION") if msg.hasElement("EMSX_EXEC_INSTRUCTION") else ""
                        emsx_fill_id = msg.getElementAsInteger("EMSX_FILL_ID") if msg.hasElement("EMSX_FILL_ID") else 0
                        emsx_filled = msg.getElementAsInteger("EMSX_FILLED") if msg.hasElement("EMSX_FILLED") else 0
                        emsx_gpi = msg.getElementAsString("EMSX_GPI") if msg.hasElement("EMSX_GPI") else ""
                        emsx_gtd_date = msg.getElementAsInteger("EMSX_GTD_DATE") if msg.hasElement("EMSX_GTD_DATE") else 0
                        emsx_hand_instruction = msg.getElementAsString("EMSX_HAND_INSTRUCTION") if msg.hasElement("EMSX_HAND_INSTRUCTION") else ""
                        emsx_idle_amount = msg.getElementAsInteger("EMSX_IDLE_AMOUNT") if msg.hasElement("EMSX_IDLE_AMOUNT") else 0
                        emsx_investor_id = msg.getElementAsString("EMSX_INVESTOR_ID") if msg.hasElement("EMSX_INVESTOR_ID") else ""
                        emsx_isin = msg.getElementAsString("EMSX_ISIN") if msg.hasElement("EMSX_ISIN") else ""
                        emsx_limit_price = msg.getElementAsFloat("EMSX_LIMIT_PRICE") if msg.hasElement("EMSX_LIMIT_PRICE") else 0
                        emsx_mifid_ii_instruction = msg.getElementAsString("EMSX_MIFID_II_INSTRUCTION") if msg.hasElement("EMSX_MIFID_II_INSTRUCTION") else ""
                        emsx_mod_pend_status = msg.getElementAsString("EMSX_MOD_PEND_STATUS") if msg.hasElement("EMSX_MOD_PEND_STATUS") else ""
                        emsx_notes = msg.getElementAsString("EMSX_NOTES") if msg.hasElement("EMSX_NOTES") else ""
                        emsx_nse_avg_price = msg.getElementAsFloat("EMSX_NSE_AVG_PRICE") if msg.hasElement("EMSX_NSE_AVG_PRICE") else 0
                        emsx_nse_filled = msg.getElementAsInteger("EMSX_NSE_FILLED") if msg.hasElement("EMSX_NSE_FILLED") else 0
                        emsx_order_as_of_date = msg.getElementAsInteger("EMSX_ORDER_AS_OF_DATE") if msg.hasElement("EMSX_ORDER_AS_OF_DATE") else 0
                        emsx_order_as_of_time_microsec = msg.getElementAsFloat("EMSX_ORDER_AS_OF_TIME_MICROSEC") if msg.hasElement("EMSX_ORDER_AS_OF_TIME_MICROSEC") else 0
                        emsx_ord_ref_id = msg.getElementAsString("EMSX_ORD_REF_ID") if msg.hasElement("EMSX_ORD_REF_ID") else ""
                        emsx_order_type = msg.getElementAsString("EMSX_ORDER_TYPE") if msg.hasElement("EMSX_ORDER_TYPE") else ""
                        emsx_originate_trader = msg.getElementAsString("EMSX_ORIGINATE_TRADER") if msg.hasElement("EMSX_ORIGINATE_TRADER") else ""
                        emsx_originate_trader_firm = msg.getElementAsString("EMSX_ORIGINATE_TRADER_FIRM") if msg.hasElement("EMSX_ORIGINATE_TRADER_FIRM") else ""
                        emsx_percent_remain = msg.getElementAsFloat("EMSX_PERCENT_REMAIN") if msg.hasElement("EMSX_PERCENT_REMAIN") else 0
                        emsx_pm_uuid = msg.getElementAsInteger("EMSX_PM_UUID") if msg.hasElement("EMSX_PM_UUID") else 0
                        emsx_port_mgr = msg.getElementAsString("EMSX_PORT_MGR") if msg.hasElement("EMSX_PORT_MGR") else ""
                        emsx_port_name = msg.getElementAsString("EMSX_PORT_NAME") if msg.hasElement("EMSX_PORT_NAME") else ""
                        emsx_port_num = msg.getElementAsInteger("EMSX_PORT_NUM") if msg.hasElement("EMSX_PORT_NUM") else 0
                        emsx_position = msg.getElementAsString("EMSX_POSITION") if msg.hasElement("EMSX_POSITION") else ""
                        emsx_principle = msg.getElementAsFloat("EMSX_PRINCIPAL") if msg.hasElement("EMSX_PRINCIPAL") else 0
                        emsx_product = msg.getElementAsString("EMSX_PRODUCT") if msg.hasElement("EMSX_PRODUCT") else ""
                        emsx_queued_date = msg.getElementAsInteger("EMSX_QUEUED_DATE") if msg.hasElement("EMSX_QUEUED_DATE") else 0
                        emsx_queued_time = msg.getElementAsInteger("EMSX_QUEUED_TIME") if msg.hasElement("EMSX_QUEUED_TIME") else 0
                        emsx_queued_time_microsec = msg.getElementAsFloat("EMSX_QUEUED_TIME_MICROSEC") if msg.hasElement("EMSX_QUEUED_TIME_MICROSEC") else 0
                        emsx_reason_code = msg.getElementAsString("EMSX_REASON_CODE") if msg.hasElement("EMSX_REASON_CODE") else ""
                        emsx_reason_desc = msg.getElementAsString("EMSX_REASON_DESC") if msg.hasElement("EMSX_REASON_DESC") else ""
                        emsx_remain_balance = msg.getElementAsFloat("EMSX_REMAIN_BALANCE") if msg.hasElement("EMSX_REMAIN_BALANCE") else 0
                        emsx_route_id = msg.getElementAsInteger("EMSX_ROUTE_ID") if msg.hasElement("EMSX_ROUTE_ID") else 0
                        emsx_route_price = msg.getElementAsFloat("EMSX_ROUTE_PRICE") if msg.hasElement("EMSX_ROUTE_PRICE") else 0
                        emsx_sec_name = msg.getElementAsString("EMSX_SEC_NAME") if msg.hasElement("EMSX_SEC_NAME") else ""
                        emsx_sedol = msg.getElementAsString("EMSX_SEDOL") if msg.hasElement("EMSX_SEDOL") else ""
                        emsx_sequence = msg.getElementAsInteger("EMSX_SEQUENCE") if msg.hasElement("EMSX_SEQUENCE") else 0
                        emsx_settle_amount = msg.getElementAsFloat("EMSX_SETTLE_AMOUNT") if msg.hasElement("EMSX_SETTLE_AMOUNT") else 0
                        emsx_settle_date = msg.getElementAsInteger("EMSX_SETTLE_DATE") if msg.hasElement("EMSX_SETTLE_DATE") else 0
                        emsx_si = msg.getElementAsString("EMSX_SI") if msg.hasElement("EMSX_SI") else ""
                        emsx_side = msg.getElementAsString("EMSX_SIDE") if msg.hasElement("EMSX_SIDE") else ""
                        emsx_start_amount = msg.getElementAsInteger("EMSX_START_AMOUNT") if msg.hasElement("EMSX_START_AMOUNT") else 0
                        emsx_status = msg.getElementAsString("EMSX_STATUS") if msg.hasElement("EMSX_STATUS") else ""
                        emsx_step_out_broker = msg.getElementAsString("EMSX_STEP_OUT_BROKER") if msg.hasElement("EMSX_STEP_OUT_BROKER") else ""
                        emsx_stop_price = msg.getElementAsFloat("EMSX_STOP_PRICE") if msg.hasElement("EMSX_STOP_PRICE") else 0
                        emsx_strategy_end_time = msg.getElementAsInteger("EMSX_STRATEGY_END_TIME") if msg.hasElement("EMSX_STRATEGY_END_TIME") else 0
                        emsx_strategy_part_rate1 = msg.getElementAsFloat("EMSX_STRATEGY_PART_RATE1") if msg.hasElement("EMSX_STRATEGY_PART_RATE1") else 0
                        emsx_strategy_part_rate2 = msg.getElementAsFloat("EMSX_STRATEGY_PART_RATE2") if msg.hasElement("EMSX_STRATEGY_PART_RATE2") else 0
                        emsx_strategy_style = msg.getElementAsString("EMSX_STRATEGY_STYLE") if msg.hasElement("EMSX_STRATEGY_STYLE") else ""
                        emsx_strategy_type = msg.getElementAsString("EMSX_STRATEGY_TYPE") if msg.hasElement("EMSX_STRATEGY_TYPE") else ""
                        emsx_ticker = msg.getElementAsString("EMSX_TICKER") if msg.hasElement("EMSX_TICKER") else ""
                        emsx_tif = msg.getElementAsString("EMSX_TIF") if msg.hasElement("EMSX_TIF") else ""
                        emsx_time_stamp = msg.getElementAsInteger("EMSX_TIME_STAMP") if msg.hasElement("EMSX_TIME_STAMP") else 0
                        emsx_time_stamp_microsec = msg.getElementAsFloat("EMSX_TIME_STAMP_MICROSEC") if msg.hasElement("EMSX_TIME_STAMP_MICROSEC") else 0
                        emsx_trad_uuid = msg.getElementAsInteger("EMSX_TRAD_UUID") if msg.hasElement("EMSX_TRAD_UUID") else 0
                        emsx_trade_desk = msg.getElementAsString("EMSX_TRADE_DESK") if msg.hasElement("EMSX_TRADE_DESK") else ""
                        emsx_trader = msg.getElementAsString("EMSX_TRADER") if msg.hasElement("EMSX_TRADER") else ""
                        emsx_trader_notes = msg.getElementAsString("EMSX_TRADER_NOTES") if msg.hasElement("EMSX_TRADER_NOTES") else ""
                        emsx_ts_ordnum = msg.getElementAsInteger("EMSX_TS_ORDNUM") if msg.hasElement("EMSX_TS_ORDNUM") else 0
                        emsx_type = msg.getElementAsString("EMSX_TYPE") if msg.hasElement("EMSX_TYPE") else ""
                        emsx_underlying_ticker = msg.getElementAsString("EMSX_UNDERLYING_TICKER") if msg.hasElement("EMSX_UNDERLYING_TICKER") else ""
                        emsx_user_comm_amount = msg.getElementAsFloat("EMSX_USER_COMM_AMOUNT") if msg.hasElement("EMSX_USER_COMM_AMOUNT") else 0
                        emsx_user_comm_rate = msg.getElementAsFloat("EMSX_USER_COMM_RATE") if msg.hasElement("EMSX_USER_COMM_RATE") else 0
                        emsx_user_fees = msg.getElementAsFloat("EMSX_USER_FEES") if msg.hasElement("EMSX_USER_FEES") else 0
                        emsx_user_net_money = msg.getElementAsFloat("EMSX_USER_NET_MONEY") if msg.hasElement("EMSX_USER_NET_MONEY") else 0
                        emsx_user_work_price = msg.getElementAsFloat("EMSX_WORK_PRICE") if msg.hasElement("EMSX_WORK_PRICE") else 0
                        emsx_working = msg.getElementAsInteger("EMSX_WORKING") if msg.hasElement("EMSX_WORKING") else 0
                        emsx_yellow_key = msg.getElementAsString("EMSX_YELLOW_KEY") if msg.hasElement("EMSX_YELLOW_KEY") else ""
                        
                        print ("ORDER MESSAGE: CorrelationID(%d)   Status(%d)" % (msg.correlationIds()[0].value(),event_status))
                        #print ("MESSAGE: %s" % (msg))
                        
                        print ("API_SEQ_NUM: %d" % (api_seq_num))
                        print ("EMSX_ACCOUNT: %s" % (emsx_account))
                        print ("EMSX_AMOUNT: %d" % (emsx_amount))
                        print ("EMSX_ARRIVAL_PRICE: %d" % (emsx_arrival_price))
                        print ("EMSX_ASSET_CLASS: %s" % (emsx_asset_class))
                        print ("EMSX_ASSIGNED_TRADER: %s" % (emsx_assigned_trader))
                        print ("EMSX_AVG_PRICE: %d" % (emsx_avg_price))
                        print ("EMSX_BASKET_NAME: %s" % (emsx_basket_name))
                        print ("EMSX_BASKET_NUM: %d" % (emsx_basket_num))
                        print ("EMSX_BLOCK_ID: %s" % (emsx_block_id))
                        print ("EMSX_BROKER: %s" % (emsx_broker))
                        print ("EMSX_BROKER_COMM: %d" % (emsx_broker_comm))
                        print ("EMSX_BSE_AVG_PRICE: %d" % (emsx_bse_avg_price))
                        print ("EMSX_BSE_FILLED: %d" % (emsx_bse_filled))
                        print ("EMSX_BUYSIDE_LEI: %s" % (emsx_buyside_lei))
                        print ("EMSX_CFD_FLAG: %s" % (emsx_cfd_flag))
                        print ("EMSX_CLIENT_IDENTIFICATION: %s" % (emsx_client_identification))
                        print ("EMSX_COMM_DIFF_FLAG: %s" % (emsx_comm_diff_flag))
                        print ("EMSX_COMM_RATE: %d" % (emsx_comm_rate))
                        print ("EMSX_CUSTOM_NOTE1: %s" % (emsx_custom_note1))
                        print ("EMSX_CUSTOM_NOTE2: %s" % (emsx_custom_note2))
                        print ("EMSX_CUSTOM_NOTE3: %s" % (emsx_custom_note3))
                        print ("EMSX_CUSTOM_NOTE4: %s" % (emsx_custom_note4))
                        print ("EMSX_CUSTOM_NOTE5: %s" % (emsx_custom_note5))    
                        print ("EMSX_CURRENCY_PAIR: %s" % (emsx_currency_pair))
                        print ("EMSX_DATE: %d" % (emsx_date))
                        print ("EMSX_DAY_AVG_PRICE: %d" % (emsx_day_avg_price))
                        print ("EMSX_DAY_FILL: %d" % (emsx_day_fill))
                        print ("EMSX_DIR_BROKER_FLAG: %s" % (emsx_dir_broker_flag))
                        print ("EMSX_EXCHANGE: %s" % (emsx_exchange))
                        print ("EMSX_EXCHANGE_DESTINATION: %s" % (emsx_exchange_destination))
                        print ("EMSX_EXEC_INSTRUCTION: %s" % (emsx_exec_instruction))
                        print ("EMSX_FILL_ID: %d" % (emsx_fill_id))
                        print ("EMSX_FILLED: %d" % (emsx_filled))
                        print ("EMSX_GPI: %s" % (emsx_gpi))
                        print ("EMSX_GTD_DATE: %d" % (emsx_gtd_date))
                        print ("EMSX_HAND_INSTRUCTION: %s" % (emsx_hand_instruction))
                        print ("EMSX_IDLE_AMOUNT: %d" % (emsx_idle_amount))
                        print ("EMSX_INVESTOR_ID: %s" % (emsx_investor_id))
                        print ("EMSX_ISIN: %s" % (emsx_isin))
                        print ("EMSX_LIMIT_PRICE: %0.8f" % (emsx_limit_price))
                        print ("EMSX_MIFID_II_INSTRUCTION: %s" % (emsx_mifid_ii_instruction))
                        print ("EMSX_MOD_PEND_STATUS: %s" % (emsx_mod_pend_status))
                        print ("EMSX_NOTES: %s" % (emsx_notes))
                        print ("EMSX_NSE_AVG_PRICE: %d" % (emsx_nse_avg_price))
                        print ("EMSX_NSE_FILLED: %d" % (emsx_nse_filled))
                        print ("EMSX_ORD_REF_ID: %s" % (emsx_ord_ref_id))
                        print ("EMSX_ORDER_AS_OF_DATE: %d" % (emsx_order_as_of_date))
                        print ("EMSX_ORDER_AS_OF_TIME_MICROSEC: %0.8f" % (emsx_order_as_of_time_microsec))
                        print ("EMSX_ORDER_TYPE: %s" % (emsx_order_type))
                        print ("EMSX_ORIGINATE_TRADER: %s" % (emsx_originate_trader))
                        print ("EMSX_ORIGINATE_TRADER_FIRM: %s" % (emsx_originate_trader_firm))
                        print ("EMSX_PERCENT_REMAIN: %d" % (emsx_percent_remain))
                        print ("EMSX_PM_UUID: %d" % (emsx_pm_uuid))
                        print ("EMSX_PORT_MGR: %s" % (emsx_port_mgr))
                        print ("EMSX_PORT_NAME: %s" % (emsx_port_name))
                        print ("EMSX_PORT_NUM: %d" % (emsx_port_num))
                        print ("EMSX_POSITION: %s" % (emsx_position))
                        print ("EMSX_PRINCIPAL: %d" % (emsx_principle))
                        print ("EMSX_PRODUCT: %s" % (emsx_product))
                        print ("EMSX_QUEUED_DATE: %d" % (emsx_queued_date))
                        print ("EMSX_QUEUED_TIME: %d" % (emsx_queued_time))
                        print ("EMSX_QUEUED_TIME_MICROSEC: %0.8f" % (emsx_queued_time_microsec))
                        print ("EMSX_REASON_CODE: %s" % (emsx_reason_code))
                        print ("EMSX_REASON_DESC: %s" % (emsx_reason_desc))
                        print ("EMSX_REMAIN_BALANCE: %d" % (emsx_remain_balance))
                        print ("EMSX_ROUTE_ID: %d" % (emsx_route_id))
                        print ("EMSX_ROUTE_PRICE: %d" % (emsx_route_price))
                        print ("EMSX_SEC_NAME: %s" % (emsx_sec_name))
                        print ("EMSX_SEDOL: %s" % (emsx_sedol))
                        print ("EMSX_SEQUENCE: %d" % (emsx_sequence))
                        print ("EMSX_SETTLE_AMOUNT: %d" % (emsx_settle_amount))
                        print ("EMSX_SETTLE_DATE: %d" % (emsx_settle_date))
                        print ("EMSX_SI: %s" % (emsx_si))
                        print ("EMSX_SIDE: %s" % (emsx_side))
                        print ("EMSX_START_AMOUNT: %d" % (emsx_start_amount))
                        print ("EMSX_STATUS: %s" % (emsx_status))
                        print ("EMSX_STEP_OUT_BROKER: %s" % (emsx_step_out_broker))
                        print ("EMSX_STOP_PRICE: %d" % (emsx_stop_price))
                        print ("EMSX_STRATEGY_END_TIME: %d" % (emsx_strategy_end_time))
                        print ("EMSX_STRATEGY_PART_RATE1: %d" % (emsx_strategy_part_rate1))
                        print ("EMSX_STRATEGY_PART_RATE2: %d" % (emsx_strategy_part_rate2))
                        print ("EMSX_STRATEGY_STYLE: %s" % (emsx_strategy_style))
                        print ("EMSX_STRATEGY_TYPE: %s" % (emsx_strategy_type))
                        print ("EMSX_TICKER: %s" % (emsx_ticker))
                        print ("EMSX_TIF: %s" % (emsx_tif))
                        print ("EMSX_TIME_STAMP: %d" % (emsx_time_stamp))
                        print ("EMSX_TIME_STAMP_MICROSEC: %0.8f" % (emsx_time_stamp_microsec))
                        print ("EMSX_TRAD_UUID: %d" % (emsx_trad_uuid))
                        print ("EMSX_TRADE_DESK: %s" % (emsx_trade_desk))
                        print ("EMSX_TRADER: %s" % (emsx_trader))
                        print ("EMSX_TRADER_NOTES: %s" % (emsx_trader_notes))
                        print ("EMSX_TS_ORDNUM: %d" % (emsx_ts_ordnum))
                        print ("EMSX_TYPE: %s" % (emsx_type))
                        print ("EMSX_UNDERLYING_TICKER: %s" % (emsx_underlying_ticker))
                        print ("EMSX_USER_COMM_AMOUNT: %d" % (emsx_user_comm_amount))
                        print ("EMSX_USER_COMM_RATE: %d" % (emsx_user_comm_rate))
                        print ("EMSX_USER_FEES: %d" % (emsx_user_fees))
                        print ("EMSX_USER_NET_MONEY: %d" % (emsx_user_net_money))
                        print ("EMSX_WORK_PRICE: %d" % (emsx_user_work_price))
                        print ("EMSX_WORKING: %d" % (emsx_working))
                        print ("EMSX_YELLOW_KEY: %s" % (emsx_yellow_key))
            
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
    print("Bloomberg - EMSX API Example - Server - Order Subscription")
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

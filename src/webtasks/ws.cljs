;; Copyright © 2016 Dynamic Object Language Labs Inc.
;;
;; This software is licensed under the terms of the
;; Apache License, Version 2.0 which can be found in
;; the file LICENSE at the root of this distribution.

(ns webtasks.ws
  "Websocket API for ClojureScript"
  (:require
   [avenir.utils :as au :refer [assoc-if]]
   [webtasks.tasks :as tasks :refer [success! error!]]
   [cljs.core.async :as async :refer [<! >! put! alts!]]
   [cognitect.transit :as transit]))

;; websockets -------------------------------------------------------------------------

;; this is the default ws atom
;; HOWEVER, users can supply their own as an optional argument
;; (in the case where 2 or more websockets are needed)
(defonce ws (atom nil))

;; {
;;  :ws-url ws-url         ;; the WebSocket endpoint
;;  :id id                 ;; short string for naming persistent deferreds
;;  :web-socket web-socket ;; the actual websocket
;;  :read-ch read-ch       ;; the channel for incoming events from the server
;;  :write-ch write-ch     ;; the channel for outgoing events from the client
;;  :status status         ;; one of: :connecting :open :closing :closed
;;  :reader reader     ;; transit reader
;;  :writer writer     ;; transit writer
;;  :deferreds
;;     {:connecting dconnecting
;;      :open       dopen
;;      :closing    dclosing
;;      :closed     dclosed
;;      :message    dmessage
;;     }
;;   :retry {
;;     :initial 500 ;; initial delay in ms
;;     :factor 3  ;; increase for progressive non-connecxtinos
;;     :dtimeout   ;; deferred for the timeout
;;     :delay  0  ;; current delay (0 for do not reconnect)
;;   }
;;  }

(def retry-policy-default
  { :initial 1000  ;; initial delay in ms
    :factor 3     ;; increase for progressive non-connecxtinos
    :dtimeout nil  ;; deferred for the timeout
    :delay   1000  ;; current delay (0 for do not reconnect)
  })

;; example client function --------------------------------------

(defn echo [& args]
  (println "(echo" (apply pr-str args) ")")
  (cond
    (zero? (count args))
    true
    (= 1 (count args))
    (first args)
    :else
    args))

(declare write)

(defn invoke-rmethod [ws return rmethod args]
  (if (or (= rmethod :success-fn) (= rmethod :error-fn))
    (let [return (first args)
          returns (get @ws :returns)]
      (if (returns return)
        (do
          (if (= rmethod :success-fn)
            (apply success! args)
            (apply error! args))
          (swap! ws update :returns disj return))
        (println "ERROR: invalid return" return "for" rmethod "args" args)))
    (let [rmethods (:rmethods @ws)
          rmethod-fn (get rmethods rmethod)
          rv (if rmethod-fn
               (apply rmethod-fn args)
               (do
                 (println "INVALID METHOD" rmethod)
                 :invalid-method))
          result (if (or (nil? rv) (= rv :invalid-method))
                   :error-fn
                   :success-fn)]
      (when return ;; server has a deferred
        (write ws {:rmethod result :args [return rv]})
        ))))

;; https://developer.mozilla.org/en-US/docs/Web/API/WebSocket
;; https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent#Status_codes

(def CLOSE_NORMAL 1000)
(def CLOSE_GOING_AWAY 1001)
(def CLOSE_PROTOCOL_ERROR 1002)
(def UNSUPPORTED 1003)
(def CLOSE_NO_STATUS 1005)
(def CLOSE_ABNORMAL 1006)
(def CLOSE_NOT_UTF8 1007)
(def CLOSE_BAD_POLICY 1008)
(def CLOSE_TOO_LARGE 1009)
(def CLOSE_MISSING_EXTENSION 1010)
(def CLOSE_UNEXPECTED 1011)
(def CLOSE_TLS_FAIL 1015)

(def close-event-codes
  {CLOSE_NORMAL ;; 1000
   "Normal closure; the connection successfully completed whatever purpose for which it was created."
   CLOSE_GOING_AWAY ;; 1001
   "The endpoint is going away, either because of a server failure or because the browser is navigating away from the page that opened the connection."
   CLOSE_PROTOCOL_ERROR ;; 1002
   "The endpoint is terminating the connection due to a protocol error."
   UNSUPPORTED ;; 1003
   "The connection is being terminated because the endpoint received data of a type it cannot accept (for example, a text-only endpoint received binary data)."
   CLOSE_NO_STATUS ;; 1005
   "Indicates that no status code was provided even though one was expected."
   CLOSE_ABNORMAL ;; 1006
   "Indicates that no status code was provided even though one was expected."
   CLOSE_NOT_UTF8 ;; 1007
   "The endpoint is terminating the connection because a message was received that contained inconsistent data (e.g., non-UTF-8 data within a text message)."
   CLOSE_BAD_POLICY ;; 1008
   "The endpoint is terminating the connection because it received a message that violates its policy. This is a generic status code, used when codes 1003 and 1009 are not suitable."
   CLOSE_TOO_LARGE ;; 1009
   "The endpoint is terminating the connection because a data frame was received that is too large."
   CLOSE_MISSING_EXTENSION ;; 1010
   "The client is terminating the connection because it expected the server to negotiate one or more extension, but the server didn't."
   CLOSE_UNEXPECTED ;; 1011
   "The server is terminating the connection because it encountered an unexpected condition that prevented it from fulfilling the request."
   CLOSE_TLS_FAIL ;; 1015
   "Indicates that the connection was closed due to a failure to perform a TLS handshake (e.g., the server certificate can't be verified)."})
;; 3000–3999 Available for use by libraries and frameworks. May not be used by applications.
;; 4000–4999 Available for use by applications.

(defn get-reason [code & [reason]]
  (or reason (get close-event-codes code "Unknown reason")))

;; task has :ws
(defn xmit-fn [task chan value]
  (let [{:keys [web-socket status writer]} @(:ws task)
        v (transit/write writer value)]
    (if (or (= status :open) (= status :closing))
      (do
        ;; (println "xmit-fn" value "as" v) ;; DEBUG
        (.send web-socket v)
        )
      (println "ERROR xmit-fn cannot send with ws status:" status "value" value))))


(declare reconnect)

(defn retry-connect
  ([]
   (retry-connect ws))
  ([my-ws]
   (let [ws my-ws
         {:keys [retry]} @ws
         {:keys [delay initial factor dtimeout]} retry]
     ;; ignore current value of dtimeout here (was probably just used)
     ;; retry was disabled OR we are open again!
     ;; (println "retry-connect...")
     (if (or (zero? delay) (reconnect ws nil))
       (do
         ;; (println "retry-connect DONE")
         (if dtimeout (error! dtimeout false))
         (swap! ws update :retry assoc :delay initial :dtimeout nil)
         true)
       (let [delay (* delay factor)
             dstart (tasks/chain true)
             dtimeout (tasks/sleep dstart delay)
             dretry (tasks/chain dtimeout #(retry-connect ws))]
         ;; (println "retry-connect delay" delay "dtimeout" dtimeout)
         (swap! ws update :retry assoc :delay delay :dtimeout dtimeout)
         true)))))

(defn retry-enable
  ([]
   (retry-enable ws))
  ([my-ws]
   (let [ws my-ws
         {:keys [retry]} @ws
         {:keys [dtimeout initial]} retry
         ;; _ (println "retry-connect ENABLED")
         _ (if dtimeout (error! dtimeout false)) ;; clear stale timeout
         delay (or initial 1000)
         dstart (tasks/chain true)
         dtimeout (tasks/sleep dstart delay)
         dretry (tasks/chain dtimeout #(retry-connect ws))]
     (println "retry-connect delay" delay "dtimeout" dtimeout "INITIAL")
     (swap! ws update :retry assoc :delay delay :initial delay :dtimeout dtimeout))))

(defn retry-disable
  ([]
   (retry-disable ws))
  ([my-ws]
   (let [ws my-ws
         {:keys [retry]} @ws
         {:keys [dtimeout initial]} retry]
     (if dtimeout (error! dtimeout false))
     (println "retry-connect DISABLED")
     (swap! ws update :retry assoc :delay 0 :dtimeout nil))))

(declare get-deferred)

;; ws-onopen (#object[Event [object Event]])
;; event does not hold interesting info
(defn ws-onopen [ws]
  (fn [& args]
    (tasks/timeout
      (fn []
        (swap! ws assoc :status :open)
        ;; are we retrying? if so then stop!
        (let [{:keys [retry]} @ws
              {:keys [dtimeout delay initial]} retry
              delay-if-enabled (if (pos? delay) initial 0)]
          (if dtimeout (error! dtimeout false))
          ;; reset retry
          (swap! ws update :retry assoc :delay delay-if-enabled :dtimeout nil))
        ;; give things a minute to settle down
        (tasks/timeout
          #(success! (get-deferred ws :open) true)
          30)
        ))))

;; FIXME: does not work with mobile safari
;; FYI
;; - http://stackoverflow.com/questions/5574385/websockets-on-ios
(defn ws-onmessage [ws]
  (fn [ev]
    (put! (:read-ch @ws) (.-data ev))))

;; NOTE these don't work
;; (.close web-socket code reason)
;; (.close web-socket code)
(defn ws-close [ws]
  (let [{:keys [status web-socket deferreds retry]} @ws
        {:keys [dtimeout initial delay]} retry
        closed (:closed deferreds)]
    (println "ws-close status" status)
    (when (not= status :closed)
      (if web-socket ;; might have been removed on disconnect
        (.close web-socket))
      (swap! ws assoc :status :closed)
      (success! closed true true)
      ;; FIXME error! any pending returns
      (when (and (pos? delay) (not dtimeout)) ;; retry is enabled
        (retry-enable ws)))))

(defn ws-onclose [ws]
  (fn [ev]
    (tasks/timeout
      (fn []
        (let [was-clean (.-wasClean ev)
              reason (.-reason ev)
              reason (if (empty? reason) nil reason)
              code (.-code ev)
              error {:was-clean was-clean :code code
                     :reason (get-reason code reason)}]
          (println "ws-onclose" error) ;; DEBUG
          (ws-close ws)
          )))))

(defn ws-onerror [ws]
  (fn [ev]
    (tasks/timeout
      #(ws-close ws))))

;; task has :ws
(defn recv-fn [task chan value]
  (let [ws (:ws task)
        {:keys [status reader deferreds]} @ws
        dmessage (:message deferreds)
        {:keys [message error]} (transit/read reader value)]
    (if error
      (do
        (println "recv-fn ERROR" error) ;; DEBUG
        (ws-close ws))
      (let [{:keys [rmethod args return]} message]
        (if rmethod
          (invoke-rmethod ws return rmethod args)
          (success! dmessage message))))))

;; USER API -----------------------------------------------------

;; id one of the status or :message
(defn get-deferred
  ([id]
   (get-deferred ws id))
  ([my-ws id]
   (get-in @my-ws [:deferreds id])))

(defn write
  ([value]
   (write ws value))
  ([my-ws value]
   (let [ws my-ws
         {:keys [status write-ch]} @ws
         {:keys [message error]} value
         value (if (or message error) value {:message value})]
     (if (= status :open)
       (do
         (put! write-ch value))
       (println "ERROR: cannot write when status =" status)))))

(defn rmethod
  ([my-ws return rmethod & args]
   (let [ws my-ws
         message (assoc-if {:rmethod rmethod :args args}
                   :return return)]
     (when return ;; save as an expected return
       (swap! ws update :returns conj return))
     (write ws message)
     return)))

(defn close
  ([]
   (close ws))
  ([my-ws]
   (let [ws my-ws
         {:keys [status deferreds]} @ws
         closing (:closing deferreds)]
     (if (or (nil? status) (= status :closed))
       (do
         ;; (println "close-ws: already closed")
         false)
       (let [code CLOSE_GOING_AWAY
             error {:code code :reason (get-reason code)}
             msg {:error error}]
         ;; (println "CLOSE-WS" msg)
         (write ws msg)
         (swap! ws assoc :status :closing)
         (success! closing true true)
         true)))))

(defn open?
  ([]
   (open? ws))
  ([my-ws]
   (= (:status @my-ws) :open)))

(defn connecting?
  ([]
   (connecting? ws))
  ([my-ws]
   (= (:status @my-ws) :connecting)))

(defn closing?
  ([]
   (closing? ws))
  ([my-ws]
   (= (:status @my-ws) :closing)))

(defn closed?
  ([]
   (closed? ws))
  ([my-ws]
   (let [status (:status @my-ws)]
     (or (nil? status) (= status :closed)))))

(defn on-open
  ([f]
   (on-open ws f))
  ([my-ws f]
   (tasks/on-realized (get-deferred my-ws :open) f)))

(defn on-connecting
  ([f]
   (on-connecting ws f))
  ([my-ws f]
   (tasks/on-realized (get-deferred my-ws :connecting) f)))

(defn on-closing
  ([f]
   (on-closing ws f))
  ([my-ws f]
   (tasks/on-realized (get-deferred my-ws :closing) f)))

(defn on-closed
  ([f]
   (on-closed ws f))
  ([my-ws f]
   (tasks/on-realized (get-deferred my-ws :closed) f)))

(defn on-message
  ([f]
   (on-message ws f))
  ([my-ws f]
   (tasks/on-realized (get-deferred my-ws :message) f)))

(defn add-rmethod
  ([rmethod rmethod-fn]
   (add-rmethod ws rmethod rmethod-fn))
  ([my-ws rmethod rmethod-fn]
   (if-not (keyword? rmethod)
     (println "ERROR rmethod must be a keyword:" rmethod)
     (if-not (fn? rmethod-fn)
       (println "ERROR rmethod-fn must be a function:" rmethod-fn)
       (swap! my-ws update-in [:rmethods] assoc rmethod rmethod-fn)))))

(defn disconnect
  ([]
   (disconnect ws))
  ([my-ws]
   (let [ws my-ws
         {:keys [ws-url id status read-ch write-ch]} @ws]
     (retry-disable my-ws)
     (if-not (or (nil? status) (= status :closed))
       (close ws))
     (tasks/timeout
       (fn []
         (when write-ch
           (tasks/remove-task write-ch))
         (when read-ch
           (tasks/remove-task read-ch))
         (reset! ws {:ws-url ws-url :id id})
         ) 30)
     )))

;; :connecting
;; :open
;; :closing
;; :closed
;; :message

(defn connect
  ([ws-url]
   (connect ws ws-url "ws"))
  ([my-ws ws-url]
   (connect my-ws ws-url "ws"))
  ([my-ws ws-url id]
   ;; (println "connect:" ws-url "id" id)
   (if-not (exists? js/WebSocket)
     (println "sorry, WebSockets not supported in this browser")
     (let [ws my-ws
           web-socket nil
           {:keys [read-ch write-ch reader writer rmethods]} @ws
           read-ch (if (tasks/ready? read-ch) read-ch (async/chan 16))
           write-ch (if (tasks/ready? write-ch) write-ch (async/chan 16))
           id (or id "ws")
           _ (tasks/add-task read-ch
               {:action recv-fn :ws ws :desc (str id " read-ch")})
           _ (tasks/add-task write-ch
               {:action xmit-fn :ws ws :desc (str id " write-ch")})
           reader (or reader (transit/reader :json))
           writer (or writer (transit/writer :json))
           status :connecting
           dstopping (tasks/deferred-persistent
                       (str id "-stopping"))
           deferreds {:connecting (tasks/deferred-persistent
                                    (str id "-connecting"))
                      :open       (tasks/deferred-persistent
                                    (str id "-open"))
                      :closing    (tasks/deferred-persistent
                                    (str id "-closing"))
                      :closed     (tasks/deferred-persistent
                                    (str id "-closed"))
                      :message    (tasks/deferred-persistent
                                    (str id "-message"))
                      :stopping   dstopping }
           rmethods (if rmethods
                      (assoc rmethods :echo echo)
                      {:echo echo})
           ws-state {:ws-url ws-url :id id :web-socket web-socket
                     :read-ch read-ch :write-ch write-ch
                     :reader reader :writer writer
                     :status status :deferreds deferreds
                     :returns #{} :rmethods rmethods
                     :retry retry-policy-default}]
       (reset! ws ws-state)
       ;; be sure to disconnect before task stop
       (tasks/connect :deferring-stopping dstopping)
       (tasks/on-realized dstopping #(disconnect ws))
       ;; The following causes WebKit to complain
       ;; (set! (.-binaryType web-socket) "ArrayBuffer")
       ;;  "Private" callbacks start with "ws-"
       ;; It's important to give all task infrastructure
       ;; a little time to get wired up before we start firing events
       (tasks/timeout
         #(let [;; _ (println "WEB-SOCKET request...")
                web-socket (js/WebSocket. ws-url)]
            (set! (.-onmessage web-socket) (ws-onmessage ws))
            (set! (.-onopen web-socket) (ws-onopen ws))
            (set! (.-onclose web-socket) (ws-onclose ws))
            ;; (set! (.-onunload web-socket) (ws-unload ws))
            (set! (.-onerror web-socket) (ws-onerror ws))
            (swap! ws assoc :web-socket web-socket)
            ;; (println "WEB-SOCKET done")
            ;; (println "DEBUG WS" (remove-fn @ws))
            ) 100)
       ws))))

;; if status is
;; :open GOOD, do nothing
;; nil or :closed -> connect
;; :connecting -- wait a minute
;; :closing -- wait and then reopen
(defn reconnect
  ([ws-url]
   (reconnect ws ws-url))
  ([my-ws my-url]
   (let [ws my-ws
         {:keys [ws-url id status]} @ws
         ws-url (or my-url ws-url)]
     (println "reconnect:" ws-url "status" status)
     (cond
       (= status :open)
       (do
         ;; (println "awesome, already open")
         true)
       (= status :connecting)
       (do
         ;; (println "currently connecting.. just sit tight")
         true)
       (or (nil? status) (= status :closed))
       (do
         ;; (println "was closed, going to connect now...")
         (connect ws ws-url id)
         false)
       (= status :closing)
       (println "currently closing.. fix with upon :closed here")
       :else
       (println "unexpected status:" status)))))

(defn setup-deferreds
  ([d-fns]
   (setup-deferreds ws d-fns))
  ([my-ws d-fns]
   (let [{:keys [connecting open closing closed message]} d-fns]
     (if connecting (on-connecting my-ws connecting))
     (if open (on-open my-ws open))
     (if closing (on-closing my-ws closing))
     (if closed (on-closed my-ws closed))
     (if message (on-message my-ws message)))))

(defn setup [ws-url &
             [{:keys [connecting open closing closed message my-ws]} :as d-fns]]
  (let [ws (or my-ws ws)
        d-fns (first d-fns)]
    (if (open? ws)
      (do ;; already open
        ;; (println "ALREADY OPEN")
        (setup-deferreds ws d-fns)
        (if open (open)))
      (do ;; need to reconnect
        ;; (println "NEED TO RECONNECT")
        (reconnect ws ws-url)
        (setup-deferreds ws d-fns)))))

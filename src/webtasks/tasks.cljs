;; Copyright © 2016 Dynamic Object Language Labs Inc.
;;
;; This software is licensed under the terms of the
;; Apache License, Version 2.0 which can be found in
;; the file LICENSE at the root of this distribution.

(ns webtasks.tasks
  "Asynchronous tasks for ClojureScript"
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [clojure.string :as string]
            [cljs.core.async :as async :refer [<! >! put! alts! close!]]))

(def initial-state {:stopping false ;; stop is requested
                    :going false ;; in the go loop
                    :task-id 0 ;; next task-id
                    :operations nil ;; operations channel
                    :tasks nil ;; tasks by chan
                    :deferreds nil ;; deferreds by d
                    })

(defonce state (atom initial-state))

(def deferred-prefix "deferr")
(def transient-prefix (str deferred-prefix "ed-"))
(def persistent-prefix  (str deferred-prefix "ing-"))

;; core.async helpers -------------------------------------

;; waiting on http://dev.clojure.org/jira/browse/ASYNC-126
(defn closed? [ch]
  (.-closed ch))

;; true if ch is not closed
(defn ready? [ch]
  (and ch (not (closed? ch))))

;; NOTE uses basic JavaScript timer
(defn timeout
  ([f]
   (js/setTimeout f))
  ([f msec]
   (js/setTimeout f msec)))

;; deferred ----------------------------------------------------

(defn deferred? [d]
  (and (keyword? d) (string/starts-with? (name d) deferred-prefix)))

(defn deferred-transient? [d]
  (and (keyword? d) (string/starts-with? (name d) transient-prefix)))

(defn deferred-persistent? [d]
  (and (keyword? d) (string/starts-with? (name d) persistent-prefix)))

(defn success-fn-default [v]
  (println "success-fn" v))

(defn error-fn-default [v]
  (println "error-fn" v))

(defn set-deferred! [d success-fn error-fn & [connect]]
  (when (deferred? d)
    (let [connect (or connect #{})]
      (swap! state assoc-in [:deferreds d]
        {:success-fn success-fn :error-fn error-fn :connect connect})
      d)))

(defn get-deferred [d]
  (when (deferred? d)
    (get-in @state [:deferreds d])))

(defn get-all-deferreds []
  (keys (:deferreds @state)))

(defn remove-deferred! [d]
  (if-not d
    (println "remove-deferred!: d is nil")
    (swap! state
      (fn [st]
        (let [deferreds (:deferreds st)
              deferred (get deferreds d)]
          (if deferred
            (assoc st :deferreds (dissoc deferreds d))
            st))))))

(defn- deferred-transient []
  (keyword (gensym transient-prefix)))

;; returns deferred id (unless it's reserved)
(defn deferred
  ([]
   (deferred (deferred-transient)))
  ([d]
   ;; if there is ALREADY an success-fn ...preserve it!
   (if (deferred-persistent? d)
     (deferred d (:success-fn (get-deferred d)))
     (deferred d nil)))
  ([d success-fn]
   ;; if there is ALREADY an error-fn ...preserve it!
   (if (deferred-persistent? d)
     (deferred d success-fn (:error-fn (get-deferred d)))
     (deferred d success-fn nil)))
  ([d success-fn error-fn]
   (if (not (or (nil? d) (deferred? d)))
     (println "deferred: d must be nil or a deferred keyword:" d)
     (do
       (set-deferred! d success-fn error-fn)
       d))))

;; id is an unique string
(defn deferred-persistent [id]
  ;; it may be wise, here, to ensure id is not currently in use
  (deferred (keyword (str persistent-prefix id))))

(defn on-realized
  ([d success-fn]
   (do
     (swap! state assoc-in [:deferreds d :success-fn] success-fn)
     d))
  ([d success-fn error-fn]
   (do ;; don't alter connect
     (swap! state update-in [:deferreds d] assoc
       :success-fn success-fn :error-fn error-fn)
     d)))

;; will create the deferred if it does not exist
;; will set the functions ONLY if they are not already set
(defn on-realized-default
  ([d success-fn-default]
   (let [dmap (get-deferred d)
         dmap (or dmap (get-deferred (deferred d)))
         {:keys [success-fn]} dmap]
     (when-not success-fn
       (swap! state assoc-in [:deferreds d :success-fn] success-fn-default))
     d))
  ([d success-fn-default error-fn-default]
   (let [dmap (get-deferred d)
         dmap (or dmap (get-deferred (deferred d)))
         {:keys [success-fn error-fn]} dmap]
     (when-not success-fn
       (swap! state assoc-in [:deferreds d :success-fn] success-fn-default))
     (when-not error-fn
       (swap! state assoc-in [:deferreds d :error-fn] error-fn-default))
     d)))

(defn success! [d v & [no-retry]]
  (when d
    (let [{:keys [stopping going operations]} @state]
      (if (and going (not stopping))
        (put! operations [:success d v])
        (when-not no-retry
          (println "success!" d "...starting up.. will try again")
          (timeout #(success! d v) 200))))))

(defn error! [d v]
  (when d
    (let [{:keys [stopping going operations]} @state]
      (if (and going (not stopping))
        (put! operations [:error d v])
        (do
          ;; DEBUG
          ;; (println "error!" d "...starting up.. will try again")
          (timeout #(error! d v) 200))))))

(defn realize [result d v]
  (let [{:keys [success-fn error-fn connect]} (get-deferred d)
        success-fn (or success-fn success-fn-default)
        error-fn (or error-fn error-fn-default)]
    (if (= result :success-fn)
      (success-fn v)
      (error-fn v))
    (if (deferred-transient? d)
      (remove-deferred! d))
    (when-not (empty? connect)
      (doseq [c connect]
        (realize result c v)))))

;; d is usually a deferred, but could be a value
;; e & fs are all functions taking one arg
(defn chain
  ([d]
   (cond
     (deferred? d)
     (let [dchain (deferred)
           d-success #(success! dchain %)
           d-error #(error! dchain %)]
       (on-realized d d-success d-error)
       dchain)
     :else ;; must be a value
     (let [d-val (deferred)
           dchain (chain d-val)]
       (timeout #(success! d-val d) 10)
       dchain)))
  ([d e]
   (if (deferred? d)
     (cond
       (fn? e)
       (let [e-val (deferred)
             dchain (deferred)]
         (on-realized e-val #(success! dchain %) #(error! dchain %))
         (on-realized d
           #(let [rv (e %)]
              (if (deferred? rv)
                (on-realized rv
                  (fn [ev] (success! e-val ev))
                  (fn [ev] (error! e-val ev)))
                (if rv (success! e-val rv) (error! e-val rv))))
           #(error! e-val %))
         dchain)
       :else
       (println "ERROR chain: e is not a fn:" e))
     (chain (chain d) e))) ;; d is a val
  ([d e & fs]
   (if (deferred? d)
     (cond
       (fn? e)
       (let [e-val (deferred)
             dchain (apply chain e-val fs)]
         (on-realized d
           #(let [rv (e %)]
              (if (deferred? rv)
                (on-realized rv
                  (fn [ev] (success! e-val ev))
                  (fn [ev] (error! e-val ev)))
                (if rv (success! e-val rv) (error! e-val rv))))
           #(error! e-val %))
         dchain)
       :else
       (println "ERROR chain: e is not a fn:" e))
     (apply chain (chain d) e fs)))) ;; d is a val

;; d e & fs are all deferreds
;; might have values? (zip (chain maybe-value) ...)
(defn zip
  ([d]
   (cond
     (deferred? d)
     (let [dzip (deferred)
           d-success #(success! dzip [%])
           d-error #(error! dzip %)]
       (on-realized d d-success d-error)
       dzip)
     :else
     (println "ERROR zip: d is not a deferred:" d)))
  ([d e]
   (cond
     (not (deferred? d))
     (println "ERROR zip: d is not a deferred:" d)
     (not (deferred? e))
     (println "ERROR zip: e is not a deferred:" e)
     :else
     (let [dzip (deferred)
           d-val (atom {:finished false :val nil})
           e-val (atom {:finished false :val nil})
           check-fn (fn [me other]
                      (fn [v]
                        (swap! me assoc :finished true :val v)
                        (let [{:keys [finished val]} @other]
                          (if finished
                            (success! dzip [(:val @d-val) (:val @e-val)])))))]
       (on-realized d
         (check-fn d-val e-val)
         #(error! dzip %))
       (on-realized e
         (check-fn e-val d-val)
         #(error! dzip %))
       (println "ZIP-2(d e)" d e "=>" dzip)
       dzip)))
  ([d e & fs]
   (cond
     (not (deferred? d))
     (println "ERROR zip: d is not a deferred:" d)
     (not (deferred? e))
     (println "ERROR zip: e is not a deferred:" e)
     :else
     (let [dzip (deferred)
           dfs (apply zip fs)
           fs-val (atom {:done false :vals nil})
           d-val (atom {:finished false :val nil})
           e-val (atom {:finished false :val nil})
           check-fn (fn [me other]
                      (fn [v]
                        (swap! me assoc :finished true :val v)
                        (let [{:keys [finished val]} @other
                              {:keys [done vals]} @fs-val]
                          (if (and finished done)
                            (success! dzip
                              (vec (cons (:val @d-val)
                                     (cons (:val @e-val) vals))))))))]
       (on-realized d
         (check-fn d-val e-val)
         #(error! dzip %))
       (on-realized e
         (check-fn e-val d-val)
         #(error! dzip %))
       (on-realized dfs
         (fn [v]
           (swap! fs-val assoc :done true :vals v))
         #(error! dzip %))
       dzip))))

;; Conveys the realized value of a into b.
;; returns b so these can be put in a ->
;; (-> a (connect b) (connect c) (connect d))
(defn connect [a b]
  (if-not (get-deferred a) ;; must actually be defined already
    (println "ERROR connect: a is not deferred" a)
    (if-not (deferred? b)
      (println "ERROR connect: b is not deferred" b)
      (do
        (swap! state update-in [:deferreds a :connect] conj b)
        b))))

(defn catch [d error-handler]
  (if-not (deferred? d)
    (println "ERROR catch: d is not deferred" d)
    (let [e-val (deferred)
          dcatch (deferred)]
      (on-realized e-val #(success! dcatch %))
      (on-realized d #(success! e-val %)
        #(let [rv (error-handler %)]
           (success! e-val rv)))
      ;; (println "CATCH" d e-val "=>" dcatch)
      dcatch)))

;; final-fn takes no args
(defn finally [d final-fn]
  (if-not (deferred? d)
    (println "ERROR finally: d is not deferred" d)
    (let [final-val (deferred)
          f (fn [& args] (final-fn))]
      (connect d final-val)
      (on-realized final-val f f)
      ;; (println "FINALLY" d final-val "map" (remove-fn (get-deferred final-val)))
      d)))

(defn sleep [d msec]
  (if-not (deferred? d)
    (println "ERROR sleep: d is not deferred" d)
    (let [dsleep (deferred)]
      (on-realized d
        (fn [v] (timeout #(success! dsleep v) msec))
        #(error! dsleep %))
      ;; (println "SLEEP" d msec "=>" dsleep)
      dsleep)))

(defn timeout! [d msec & [timeout-val]]
  (if-not (deferred? d)
    (println "ERROR sleep: d is not deferred" d)
    (let [dtimeout (deferred)
          finished (atom false)]
      (on-realized d
        #(do
           (reset! finished true)
           (success! dtimeout %))
        #(do
           (reset! finished true)
           (error! dtimeout %)))
      (timeout #(when-not @finished (error! dtimeout timeout-val)) msec)
      ;; (println "TIMEOUT!" d msec "=>" dtimeout)
      dtimeout)))

;; task functions -----------------------------------------

;; immediate
(defn add-task! [chan task]
  (swap! state assoc-in [:tasks chan] task))

(defn add-task
  ([chan]
   (add-task chan {}))
  ([chan task]
   (let [{:keys [id action desc]} task
         {:keys [stopping going operations tasks]} @state]
     (if-not (fn? action)
       (println "add-task: action is not a function")
       (if (and going (not stopping))
         (let [id (or id (:task-id (swap! state update-in [:task-id] inc)))
               desc (or desc id)
               task (assoc task :action action :id id :desc desc)]
           (put! operations [:add chan task]))
         (do
           ;; DEBUG
           ;; (println "add-task: ...starting up.. will try again")
           (timeout #(add-task chan task) 200))))
     id)))

(defn update-in-task [chan korks f v]
  (let [tasks (:tasks @state)
        task (get tasks chan)
        desc (:desc task)
        korks (if (vector? korks) korks [korks])
        ks (vec (cons :tasks (cons chan korks)))]
    (if-not task
      (println "update-in-task: failed, task not found")
      (swap! state update-in ks f v))))

;; immediate
(defn remove-task! [chan]
  (if-not chan
    (println "remove-chan!: chan is nil")
    (swap! state
      (fn [st]
        (let [tasks (:tasks st)
              task (get tasks chan)]
          (if task
            (do
              (close! chan) ;; Closing a closed channel is a no-op
              (assoc st :tasks (dissoc tasks chan)))
            st))))))

(defn remove-task [chan]
  (let [{:keys [going operations]} @state]
    (if going
      (put! operations [:remove chan])
      (remove-task! chan))))

(defn remove-task-by-id [id]
  (doseq [[chan task] (:tasks @state)]
    (if (= id (:id task))
      (remove-task chan))))

;; immediately realize error! for transient deferreds
;; remove any connect for transient deferreds
(defn- preserve-persistent [deferreds]
  (let [ds (keys deferreds)]
    (doseq [d ds]
      (if (deferred-transient? d)
        (realize :error-fn d false) ;; remove from map
        (let [dmap (get-deferred d)
              {:keys [success-fn error-fn connect]} dmap
              connect (set (filter deferred-persistent? connect))]
          (set-deferred! d success-fn error-fn connect))))
    (:deferreds @state)))

(defn operation [task chan value]
  (let [[op chan task] value]
    (case op
      :stop
      (let [{:keys [going tasks task-id deferreds]} @state
            deferreds (preserve-persistent deferreds)]
        (doseq [chan (keys tasks)]
          (close! chan))
        (reset! state (assoc initial-state
                        :going going :task-id task-id
                        :deferreds deferreds)))
      :add
      (add-task! chan task)
      :remove
      (remove-task! chan)
      :success
      (realize :success-fn chan task) ;; chan is d, task is v
      :error
      (realize :error-fn chan task) ;; chan is d, task is v
      :debug
      (println "operation :debug")
      true))) ;; always end a case with true

(defn idle [task chan value]
  (println "IDLE"))

(def idle-task
  {:id -1 :action idle :desc "idle-task"})

(defn on-started [f]
  (on-realized :deferring-started f))

(defn on-stopping [f]
  (on-realized :deferring-stopping f))

(defn stop
  ([]
   (stop (constantly true)))
  ([cb]
   ;; DEBUG
   (println "tasks stop -------------------------")
   (swap! state assoc :stopping true)
   (let [{:keys [going operations]} @state]
     (if (and going operations)
       (do
         ;; call the stopping deferred
         (realize :success-fn :deferring-stopping true)
         (timeout
           (fn []
             (println "Requesting go loop stop...")
             (put! operations [:stop])  ;; break out of go loop
             (timeout #(stop cb) 50)
             ) 20))
       (if going
         (do
           (println "Waiting for go loop to exit...")
           (timeout #(stop cb) 30))
         (do
           (swap! state assoc :stopping false)
           (cb))))))) ;; call the callback

(defn start
  ([]
   (if (:going @state)
     (println "start: already running")
     (let [operations (async/chan 10)
           started "simple timestamp";; (timestamp-now)
           ]
       (on-realized-default :deferring-started
         #(println "STARTED HOOK" %))
       (on-realized-default :deferring-stopping
         #(println "STOPPING HOOK"))
       (println "tasks start ------------------------")
       ;; add operations channel and task
       (swap! state assoc :operations operations)
       (add-task! operations {:action operation :desc "operations"})
       (go
         (println "STARTING TASK LOOP" started)
         (swap! state assoc :going true :stopping (:stopping @state))
         ;; call the starting deferred
         (realize :success-fn :deferring-started started)
         (loop [nprev 0]
           (let [tasks (:tasks @state)
                 n (count tasks)
                 chans (keys tasks)
                 [value chan] (alts! chans)
                 task (get tasks chan idle-task)
                 action (:action task)]
             ;; DEBUG
             ;; (println "ACTION" (:desc task) "VALUE:" (remove-fn value))
             (action task chan value)
             (when (closed? chan)
               ;; (println "channel was closed, removing:" (:desc task))
               (remove-task! chan))
             (if (pos? (count (:tasks @state)))
               (recur n))))
         (println "EXITING TASK LOOP" started)
         (swap! state assoc :going false)
         )))))

(defn restart []
  (stop start))

;; will only start if not already running
(defn initialize []
  (if (:going @state)
    (realize :success-fn :deferring-started true)
    (start)))

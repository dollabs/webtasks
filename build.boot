;; Copyright © 2016 Dynamic Object Language Labs Inc.
;;
;; This software is licensed under the terms of the
;; Apache License, Version 2.0 which can be found in
;; the file LICENSE at the root of this distribution.

(def project 'dollabs/webtasks)
(def version "0.2.3")
(def description "Asynchronous tasks and websockets for ClojureScript")
(def project-url "https://github.com/dollabs/webtasks")

(set-env!
  :resource-paths #{"src"}
  :source-paths   #{"test"}
  :dependencies   '[[org.clojure/clojure        "1.8.0"    :scope "provided"]
                    [org.clojure/clojurescript  "1.9.473"  :scope "provided"]
                    [org.clojure/core.async     "0.2.395"]
                    [com.cognitect/transit-cljs "0.8.239"]
                    [avenir                     "0.2.2"]
                    ;; cljs-dev
                    [com.cemerick/piggieback    "0.2.1"          :scope "test"]
                    [weasel                     "0.7.0"          :scope "test"]
                    [org.clojure/tools.nrepl    "0.2.12"         :scope "test"]
                    [adzerk/boot-reload         "0.5.1"          :scope "test"]
                    [pandeiro/boot-http         "0.7.6"          :scope "test"]
                    [adzerk/boot-cljs           "1.7.228-2"      :scope "test"]
                    [adzerk/boot-cljs-repl      "0.3.3"          :scope "test"]
                    ;; testing/development
                    [adzerk/boot-test           "1.2.0"          :scope "test"]
                    [crisptrutski/boot-cljs-test "0.3.0"         :scope "test"]
                    [adzerk/bootlaces           "0.1.13"         :scope "test"]
                    ;; api docs
                    [boot-codox                 "0.10.3"         :scope "test"]
                    ])

(require
  '[adzerk.boot-cljs      :refer [cljs]]
  '[adzerk.boot-cljs-repl :refer [cljs-repl start-repl repl-env]]
  '[pandeiro.boot-http :refer [serve]]
  '[adzerk.boot-reload    :refer [reload]]
  '[adzerk.boot-test :refer [test]]
  '[crisptrutski.boot-cljs-test :refer [test-cljs]]
  '[adzerk.bootlaces :refer :all]
  '[codox.boot :refer [codox]])

(bootlaces! version)

(task-options!
  pom {:project     project
       :version     version
       :description description
       :url         project-url
       :scm         {:url project-url}
       :license     {"Apache-2.0" "http://opensource.org/licenses/Apache-2.0"}}
  cljs {:source-map true}
  serve {:dir "target"}
  test-cljs {:js-env :phantom
             :namespaces #{"testing.webtasks.tasks" "testing.webtasks.ws"}}
  codox {:language :clojurescript
         :source-paths ["src"]
         :name (name project)
         :version version
         :output-path  "doc/api"
         :source-uri (str project-url "/blob/master/{filepath}#L{line}")})

(deftask testing
  "merge source paths in for testing"
  []
  (merge-env! :source-paths #{"test"})
  identity)

(deftask tests
  "Test CLJS and leave artifacts in target for debugging"
  [e js-env VAL kw "Set the :js-env for test-cljs (:phantom)."]
  (comp
    (sift :add-resource #{"html"})
    (testing)
    (test-cljs :js-env (or js-env :phantom))
    (target :dir #{"target"})))

(deftask build-cljs
  "Compile ClojureScript"
  []
  (comp
    (notify
      :visual true
      :title "CLJS"
      :messages {:success "http://localhost:3000 is ready\n"})
    (speak)
    (sift :include #{#"~$"} :invert true) ;; don't include emacs backups
    (cljs)
    (target :dir #{"target"})))

(deftask development
  "Example task to change ClojureScript options for development"
  [p port PORT int "The port for the ClojureScript nREPL"]
  (task-options!
    cljs {:optimizations :none :source-map true}
    reload {;; :ws-port 9001
            }
    repl {:port (or port 8082)
          :middleware '[cemerick.piggieback/wrap-cljs-repl]})
  identity)

;; This will start an nREPL server on 8082 that a remote IDE
;; can connect to for the CLJS REPL.
(deftask cljs-dev
  "Starts CLJS nREPL"
  [p port PORT int "The port for the ClojureScript nREPL"]
  (comp
    (development :port port)
    (serve)
    (watch)
    (reload)
    (cljs-repl)
    (build-cljs)))

;; For Emacs if you Customize your Cider Boot Parameters to 'cider-boot'
;; then this task will be invoked upon M-x cider-jack-in-clojurescript
;; which is on C-c M-J
;; CIDER will then open two REPL buffers for you, one for CLJS
;; and another for CLJ. FFI:
;; https://cider.readthedocs.io/en/latest/up_and_running/#clojurescript-usage

;; This task is commented out here for users that have not copied
;; a profile.boot file to ~/.boot/ which defines the cider task:
;;
;; (deftask cider-boot
;;   "Cider boot params task"
;;   []
;;   (comp
;;     (cider) ;; defined in profile.boot
;;     ;; FFI https://github.com/boot-clj/boot/wiki/Cider-REPL
;;     ;;     https://cider.readthedocs.io/en/latest/installation/
;;     (cljs-dev)))

(deftask local
  "Build jar and install to local repo."
  []
  (comp
    (sift :include #{#"~$"} :invert true) ;; don't include emacs backups
    (pom)
    (jar)
    (install)))

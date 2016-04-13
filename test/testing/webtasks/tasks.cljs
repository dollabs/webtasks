;; Copyright © 2016 Dynamic Object Language Labs Inc.
;;
;; This software is licensed under the terms of the
;; Apache License, Version 2.0 which can be found in
;; the file LICENSE at the root of this distribution.

(ns testing.webtasks.tasks
  (:require [clojure.string :as string]
            [cljs.test :as test :refer-macros [deftest testing is]]
            [webtasks.tasks :as tasks]))

(deftest test-deferred
  (testing "test deferred"
    (is (tasks/deferred? (tasks/deferred)))))

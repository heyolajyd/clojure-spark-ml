(ns clojurespark-ml.core
  (require [flambo.conf :as conf]
           [flambo.api :as f]
           [flambo.tuple :as ft]
           [clojure.string :as s])
  (:import [org.apache.spark.mllib.linalg Vectors]
           [org.apache.spark.mllib.regression LabeledPoint]
           [org.apache.spark.mllib.classification LogisticRegressionWithLBFGS]
           [org.apache.spark.mllib.evaluation BinaryClassificationMetrics])
  (:gen-class))

;; Make spark context
(def sc
  (let [cfg (-> (conf/spark-conf)
              (conf/master "local[*]")
              (conf/app-name "Clo-Spark ML")
              (conf/set "spark.akka.timeout" "300"))]
    (f/spark-context cfg)))


;; Define dataset resource
(def wine-dataset "winequality-red.csv")

;; Load and parse the data
(def prep-data
  "Variable that parses data"
  (let [result (-> (f/text-file sc wine-dataset)
                   (.zipWithIndex)
                   (f/map f/untuple)
                   (f/filter (f/fn [[line idx]]
                                (< 0 idx)))
                   (f/map (f/fn [[line _]]
                                (->> (s/split line #";")
                                (map #(Float/parseFloat %))))))]
    result))


;; Label Data points of a the prepared data
(def labelled-data
  "Function takes in Parsed data"
  (f/map prep-data (f/fn [[_ _ citric-acid _ _ _ total-so2 _ _ _ al-oh quality]]
                     (let [good (if (<= 6 quality) 0.0 1.0)
                           pred (double-array [citric-acid total-so2 al-oh])]
                     (LabeledPoint. good (Vectors/dense pred))))))


;; Define a training dataset
(def training-data
    (-> (f/sample labelled-data false 0.8 1234)
        (f/cache)))


;; Define test dataset
(def validation-data
    (-> (.subtract labelled-data training-data)
        (f/cache)))


;; classifier
(def classifier
    (doto (LogisticRegressionWithLBFGS.)
          (.setIntercept true)))


;; Model
(def model
  (do
    (doto (.optimizer classifier)
          (.setRegParam 0.0001))
    (doto (.run classifier (.rdd training-data))
            (.clearThreshold))))

(defn metrics [ds model]
         (let [pl (f/map ds (f/fn [point]
                              (let [y (.label point)
                                    x (.features point)]
                                (ft/tuple (.predict model x) y))))
               metrics (BinaryClassificationMetrics. (.rdd pl))]
           [(.areaUnderROC metrics) (.areaUnderPR metrics)]))

(defn is-good? [model citric-acid
                      total-sulfur-dioxide
                      alcohol]
          (let [point (-> (double-array [citric-acid
                                         total-sulfur-dioxide
                                         alcohol])
                          (Vectors/dense))
                prob (.predict model point)]
            (< 0.5 prob)))

;; [prediction label] for sample training-data:  [0.7800174890996759 0.7471259498290513]
;; [prediction label] for sample validation-data:  [0.7785138248847927 0.7160113864756079]

(defn -main
  "Main entry to the application ..."
  [& args]
  (println "wine xtics: Citric: 0.0 SO2: 34.0 Al-OH: 9.399999618530273, good wine? " (is-good? model 0.0 34.0 9.399999618530273))
  (println "Hello, World!"))

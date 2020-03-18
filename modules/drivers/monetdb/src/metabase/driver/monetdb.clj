(ns metabase.driver.monetdb
  "MonetDB driver. Builds off of the SQL-JDBC driver."
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [honeysql.core :as hsql]
            [java-time :as t]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.db.spec :as dbspec]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.util
             [honeysql-extensions :as hx]
             [i18n :refer [trs]]
             [ssh :as ssh]])
  (:import [java.sql DatabaseMetaData ResultSet ResultSetMetaData Types]
           [java.time LocalDateTime OffsetDateTime OffsetTime ZonedDateTime]))

(driver/register! :monetdb, :parent :sql-jdbc)

(def ^:private ^:const min-supported-mysql-version 5.7)
(def ^:private ^:const min-supported-mariadb-version 10.2)

(defmethod driver/display-name :monetdb [_] "MonetDB")

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

; (defn- mariadb? [^DatabaseMetaData metadata]
;   (= (.getDatabaseProductName metadata) "MariaDB"))

; (defn- db-version [^DatabaseMetaData metadata]
;   (Double/parseDouble
;    (format "%d.%d" (.getDatabaseMajorVersion metadata) (.getDatabaseMinorVersion metadata))))

; (defn- unsupported-version? [^DatabaseMetaData metadata]
;   (< (db-version metadata)
;      (if (mariadb? metadata) min-supported-mariadb-version min-supported-mysql-version)))

(defn- warn-on-unsupported-versions [driver details]
  (let [jdbc-spec (sql-jdbc.conn/details->connection-spec-for-testing-connection driver details)]
    (trs "Cannot connect")))

(defmethod driver/can-connect? :monetdb
  [driver details]
  ;; delegate to parent method to check whether we can connect; if so, check if it's an unsupported version and issue
  ;; a warning if it is
  (when ((get-method driver/can-connect? :sql-jdbc) driver details)
    (warn-on-unsupported-versions driver details)
    true))

(defmethod driver/supports? [:monetdb :full-join] [_ _] false)

(defmethod driver/connection-properties :monetdb
  [_]
  (ssh/with-tunnel-config
    [driver.common/default-host-details
     (assoc driver.common/default-port-details :default 3306)
     driver.common/default-dbname-details
     driver.common/default-user-details
     driver.common/default-password-details
     driver.common/default-ssl-details
     (assoc driver.common/default-additional-options-details
       :placeholder  "tinyInt1isBit=false")]))

(defmethod sql.qp/add-interval-honeysql-form :monetdb
  [driver hsql-form amount unit]
  ;; MySQL doesn't support `:millisecond` as an option, but does support fractional seconds
  (if (= unit :millisecond)
    (recur driver hsql-form (/ amount 1000.0) :second)
    (hsql/call :date_add hsql-form (hsql/raw (format "INTERVAL %s %s" amount (name unit))))))

(defmethod driver/humanize-connection-error-message :monetdb
  [_ message]
  (condp re-matches message
    #"^Communications link failure\s+The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.$"
    (driver.common/connection-error-messages :cannot-connect-check-host-and-port)

    #"^Unknown database .*$"
    (driver.common/connection-error-messages :database-name-incorrect)

    #"Access denied for user.*$"
    (driver.common/connection-error-messages :username-or-password-incorrect)

    #"Must specify port after ':' in connection string"
    (driver.common/connection-error-messages :invalid-hostname)

    #".*"                               ; default
    message))

(defmethod driver/db-default-timezone :monetdb
  [_ db]
  (let [spec                             (sql-jdbc.conn/db->pooled-connection-spec db)
        sql                              (str "SELECT NOW;")
        [{:keys [global system offset]}] (jdbc/query spec sql)
        the-valid-id                     (fn [zone-id]
                                           (when zone-id
                                             (try
                                               (.getId (t/zone-id zone-id))
                                               (catch Throwable _))))]
    ))

;; MySQL LIKE clauses are case-sensitive or not based on whether the collation of the server and the columns
;; themselves. Since this isn't something we can really change in the query itself don't present the option to the
;; users in the UI
(defmethod driver/supports? [:monetdb :case-sensitivity-string-filter-options] [_ _] false)


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql.qp/unix-timestamp->timestamp [:monetdb :seconds] [_ _ expr]
  (hsql/call :from_unixtime expr))

(defn- date-format [format-str expr] (hsql/call :date_format expr (hx/literal format-str)))
(defn- str-to-date [format-str expr] (hsql/call :str_to_date expr (hx/literal format-str)))


(defmethod sql.qp/->float :monetdb
  [_ value]
  ;; no-op as MySQL doesn't support cast to float
  value)


;; Since MySQL doesn't have date_trunc() we fake it by formatting a date to an appropriate string and then converting
;; back to a date. See http://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_date-format for an
;; explanation of format specifiers
(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defmethod sql.qp/date [:monetdb :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:monetdb :minute]          [_ _ expr] (trunc-with-format "%Y-%m-%d %H:%i" expr))
(defmethod sql.qp/date [:monetdb :minute-of-hour]  [_ _ expr] (hx/minute expr))
(defmethod sql.qp/date [:monetdb :hour]            [_ _ expr] (trunc-with-format "%Y-%m-%d %H" expr))
(defmethod sql.qp/date [:monetdb :hour-of-day]     [_ _ expr] (hx/hour expr))
(defmethod sql.qp/date [:monetdb :day]             [_ _ expr] (hsql/call :date expr))
(defmethod sql.qp/date [:monetdb :day-of-week]     [_ _ expr] (hsql/call :dayofweek expr))
(defmethod sql.qp/date [:monetdb :day-of-month]    [_ _ expr] (hsql/call :dayofmonth expr))
(defmethod sql.qp/date [:monetdb :day-of-year]     [_ _ expr] (hsql/call :dayofyear expr))
(defmethod sql.qp/date [:monetdb :month-of-year]   [_ _ expr] (hx/month expr))
(defmethod sql.qp/date [:monetdb :quarter-of-year] [_ _ expr] (hx/quarter expr))
(defmethod sql.qp/date [:monetdb :year]            [_ _ expr] (hsql/call :makedate (hx/year expr) 1))

;; To convert a YEARWEEK (e.g. 201530) back to a date you need tell MySQL which day of the week to use,
;; because otherwise as far as MySQL is concerned you could be talking about any of the days in that week
(defmethod sql.qp/date [:monetdb :week] [_ _ expr]
  (str-to-date "%X%V %W"
               (hx/concat (hsql/call :yearweek expr)
                          (hx/literal " Sunday"))))

;; mode 6: Sunday is first day of week, first week of year is the first one with 4+ days
(defmethod sql.qp/date [:monetdb :week-of-year] [_ _ expr]
  (hx/inc (hx/week expr 6)))

(defmethod sql.qp/date [:monetdb :month] [_ _ expr]
  (str-to-date "%Y-%m-%d"
               (hx/concat (date-format "%Y-%m" expr)
                          (hx/literal "-01"))))

;; Truncating to a quarter is trickier since there aren't any format strings.
;; See the explanation in the H2 driver, which does the same thing but with slightly different syntax.
(defmethod sql.qp/date [:monetdb :quarter] [_ _ expr]
  (str-to-date "%Y-%m-%d"
               (hx/concat (hx/year expr)
                          (hx/literal "-")
                          (hx/- (hx/* (hx/quarter expr)
                                      3)
                                2)
                          (hx/literal "-01"))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.sync/database-type->base-type :monetdb
  [_ database-type]
  ({
    :HUGEINT    :type/BigInteger  ;;Added
    :hugeint    :type/BigInteger  ;;Added
    :BIGINT     :type/BigInteger
    :bigint     :type/BigInteger  ;;Added
    :MEDIUMINT  :type/Integer
    :mediumint  :type/Integer
    :INT        :type/Integer
    :int        :type/Integer     ;;Added
    :INTEGER    :type/Integer
    :integer    :type/Integer     ;;Added
    :SMALLINT   :type/Integer
    :smallint   :type/Integer     ;;Added
    :TINYINT    :type/Integer
    :tinyint    :type/Integer     ;;Added
    :DOUBLE     :type/Float
    :double     :type/Float       ;;Added
    :FLOAT      :type/Float
    :float      :type/Float       ;;Added
    :DECIMAL    :type/Decimal
    :decimal    :type/Decimal     ;;Added
    :DEC        :type/Decimal     ;;Added
    :dec        :type/Decimal     ;;Added
    :REAL       :type/Float
    :real       :type/Float       ;;Added
    :NUMERIC    :type/Decimal
    :numeric    :type/Decimal     ;;Added
    :CHAR       :type/Text
    :char       :type/Text        ;;Added
    :VARCHAR    :type/Text
    :varchar    :type/Text        ;;Added
    :STRING     :type/Text        ;;Added
    :string     :type/Text        ;;Added
    :CHARACTER  :type/Text        ;;Added
    :character  :type/Text        ;;Added
    :CLOB       :type/Text        ;;Added
    :clob       :type/Text        ;;Added
    :TEXT       :type/Text
    :text       :type/Text
    :LONGTEXT   :type/Text
    :longtext   :type/Text        ;;Added
    :MEDIUMTEXT :type/Text        
    :mediumtext :type/Text        ;;Added
    :TINYTEXT   :type/Text        
    :tinytext   :type/Text        ;;Added
    :BOOLEAN    :type/Boolean     ;;Added
    :boolean    :type/Boolean     ;;Added
    :BOOL       :type/Boolean     ;;Added
    :bool       :type/Boolean     ;;Added
    :BIT        :type/Boolean
    :bit        :type/Boolean     ;;Added
    :BINARY     :type/*
    :binary     :type/*           ;;Added
    :BLOB       :type/*           
    :blob       :type/*           ;;Added
    :LONGBLOB   :type/*
    :longblob   :type/*           ;;Added
    :MEDIUMBLOB :type/*
    :mediumblob :type/*           ;;Added
    :TINYBLOB   :type/*
    :tinyblob   :type/*           ;;Added
    :DATE       :type/Date
    :date       :type/Date        ;;Added
    :DATETIME   :type/DateTime    
    :datetime   :type/DateTime    ;;Added
    :TIMESTAMP  :type/DateTimeWithLocalTZ ; stored as UTC in the database
    :timestamp  :type/DateTimeWithLocalTZ ; stored as UTC in the database - Added
    :TIME       :type/Time
    :time       :type/Time        ;;Added
    :VARBINARY  :type/*           
    :varbinary  :type/*           ;;Added
    :ENUM       :type/*
    :enum       :type/*           ;;Added
    :SET        :type/*
    :set        :type/*           ;;Added
    :YEAR       :type/Integer
    :year       :type/Integer     ;;Added
   }
   ;; strip off " UNSIGNED" from end if present
   (keyword (str/replace (name database-type) #"\sUNSIGNED$" ""))))

(def ^:private default-connection-args
  "Map of args for the MySQL/MariaDB JDBC connection string."
  { ;; 0000-00-00 dates are valid in MySQL; convert these to `null` when they come back because they're illegal in Java
   :zeroDateTimeBehavior "convertToNull"
   ;; Force UTF-8 encoding of results
   :useUnicode           true
   :characterEncoding    "UTF8"
   :characterSetResults  "UTF8"
   ;; GZIP compress packets sent between Metabase server and MySQL/MariaDB database
   :useCompression       true})

(defmethod sql-jdbc.conn/connection-details->spec :monetdb
   ;; jdbc:monetdb://host:port/database_name

   [_ {:keys [user password db host port ssl]
       :or {user "monetdb", password "monetdb", db "", host "localhost", port "50000"}
       :as opts}]
   (merge {
     :classname "nl.cwi.monetdb.jdbc.MonetDriver" ; must be in classpath
     :subprotocol "monetdb"
     :subname (str "//" host ":" port "/" db)
     :dbname           db
     :password           password
     :user               user
     :encrypt            (boolean ssl)
     :delimiters "`"}
   
     (dissoc opts :host :port :db)))


   ; [_ {:keys [user password db host port ssl]
   ;     :or   {user "monetdb", password "monetdb", db "", host "localhost", port "50000"}
   ;     :as   details}]
   ; (-> {:applicationName    config/mb-app-id-string
   ;      :classname "nl.cwi.monetdb.jdbc.MonetDriver"
   ;      :subprotocol        "monetdb"
   ;      ;; it looks like the only thing that actually needs to be passed as the `subname` is the host; everything else
   ;      ;; can be passed as part of the Properties
   ;      :subname            (str "//" host ":" port "/" db)
   ;      ;; everything else gets passed as `java.util.Properties` to the JDBC connection.  (passing these as Properties
   ;      ;; instead of part of the `:subname` is preferable because they support things like passwords with special
   ;      ;; characters)
   ;      :database           db
   ;      :password           password
   ;      ;; Wait up to 10 seconds for connection success. If we get no response by then, consider the connection failed
   ;      :loginTimeout       10
   ;      ;; apparently specifying `domain` with the official SQLServer driver is done like `user:domain\user` as opposed
   ;      ;; to specifying them seperately as with jTDS see also:
   ;      ;; https://social.technet.microsoft.com/Forums/sqlserver/en-US/bc1373f5-cb40-479d-9770-da1221a0bc95/connecting-to-sql-server-in-a-different-domain-using-jdbc-driver?forum=sqldataaccess
   ;      :user               user
   ;      :encrypt            (boolean ssl)
   ;      ;; only crazy people would want this. See https://docs.microsoft.com/en-us/sql/connect/jdbc/configuring-how-java-sql-time-values-are-sent-to-the-server?view=sql-server-ver15
   ;      :sendTimeAsDatetime false}
   ;     ;; only include `port` if it is specified; leave out for dynamic port: see
   ;     ;; https://github.com/metabase/metabase/issues/7597
   ;     (sql-jdbc.common/handle-additional-options details, :seperator-style :semicolon)))

(defmethod sql-jdbc.sync/active-tables :monetdb
  [& args]
  (apply sql-jdbc.sync/post-filtered-active-tables args))

(defmethod sql-jdbc.sync/excluded-schemas :monetdb
  [_]
  #{"INFORMATION_SCHEMA"})

(defmethod sql.qp/quote-style :monetdb [_] :monetdb)

;; If this fails you need to load the timezone definitions from your system into MySQL; run the command
;;
;;    `mysql_tzinfo_to_sql /usr/share/zoneinfo | mysql -u root mysql`
;;
;; See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html for details
;;
(defmethod sql-jdbc.execute/set-timezone-sql [:monetdb OffsetTime]
  [datetime]
  "SET TIME ZONE INTERVAL %s HOUR TO MINUTE;", (t/zone-offset datetime))

(defmethod sql-jdbc.execute/set-parameter [:monetdb OffsetTime]
  [driver ps i t]
  ;; convert to a LocalTime so MySQL doesn't get F U S S Y
  (sql-jdbc.execute/set-parameter driver ps i (t/local-time (t/with-offset-same-instant t (t/zone-offset 0)))))

;; Regardless of session timezone it seems to be the case that OffsetDateTimes get normalized to UTC inside MySQL
;;
;; Since MySQL TIMESTAMPs aren't timezone-aware this means comparisons are done between timestamps in the report
;; timezone and the local datetime portion of the parameter, in UTC. Bad!
;;
;; Convert it to a LocalDateTime, in the report timezone, so comparisions will work correctly.
;;
;; See also — https://dev.mysql.com/doc/refman/5.5/en/datetime.html
;;
;; TIMEZONE FIXME — not 100% sure this behavior makes sense
(defmethod sql-jdbc.execute/set-parameter [:monetdb OffsetDateTime]
  [driver ^java.sql.PreparedStatement ps ^Integer i t]
  (let [zone   (t/zone-id (qp.timezone/results-timezone-id))
        offset (.. zone getRules (getOffset (t/instant t)))
        t      (t/local-date-time (t/with-offset-same-instant t offset))]
    (sql-jdbc.execute/set-parameter driver ps i t)))

;; MySQL TIMESTAMPS are actually TIMESTAMP WITH LOCAL TIME ZONE, i.e. they are stored normalized to UTC when stored.
;; However, MySQL returns them in the report time zone in an effort to make our lives horrible.
;;
;; Check and see if the column type is `TIMESTAMP` (as opposed to `DATETIME`, which is the equivalent of
;; LocalDateTime), and normalize it to a UTC timestamp if so.
(defmethod sql-jdbc.execute/read-column [:monetdb Types/TIMESTAMP]
  [_ _ ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  (when-let [t (.getObject rs i LocalDateTime)]
    (if (= (.getColumnTypeName rsmeta i) "TIMESTAMP")
      (t/with-offset-same-instant (t/offset-date-time t (t/zone-id (qp.timezone/results-timezone-id))) (t/zone-offset 0))
      t)))

(defn- format-offset [t]
  (let [offset (t/format "ZZZZZ" (t/zone-offset t))]
    (if (= offset "Z")
      "UTC"
      offset)))

(defmethod unprepare/unprepare-value [:monetdb OffsetTime]
  [_ t]
  ;; MySQL doesn't support timezone offsets in literals so pass in a local time literal wrapped in a call to convert
  ;; it to the appropriate timezone
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "HH:mm:ss.SSS" t)
          (format-offset t)))

(defmethod unprepare/unprepare-value [:monetdb OffsetDateTime]
  [_ t]
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)
          (format-offset t)))

(defmethod unprepare/unprepare-value [:monetdb ZonedDateTime]
  [_ t]
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)
          (str (t/zone-id t))))
